/** Stores various indices on all files in the vault to make dataview generation fast. */
import {MetadataCache, Vault, TFile} from 'obsidian';
import {Task} from 'src/tasks';
import * as Tasks from 'src/tasks';
import * as Papa from "papaparse"
import {LiteralField, LiteralFieldRepr, LiteralTypeRepr} from "src/query";
import {parseFrontmatter} from "src/engine";

/** Aggregate index which has several sub-indices and will initialize all of them. */
export class FullIndex {
    /** How often the reload queue is checked for reloads. */
    static RELOAD_INTERVAL = 1_000;

    /** Generate a full index from the given vault. */
    static async generate(vault: Vault, cache: MetadataCache): Promise<FullIndex> {
        // TODO: Probably need to do this on a worker thread to actually get
        let tags = TagIndex.generate(vault, cache);
        let csv = CsvIndex.generate(vault, cache);
        let prefix = PrefixIndex.generate(vault);

        return Promise.all([tags, csv, prefix]).then(value => {
            return new FullIndex(vault, cache, value[0], value[1], value[2]);
        });
    }

    // Handle for the interval which does the reloading.
    reloadHandle: number;
    // Files which are currently in queue to be reloaded.
    reloadQueue: TFile[];
    // Set of paths being reloaded, used for debouncing.
    reloadSet: Set<string>;
    // Custom extra reload handlers.
    reloadHandlers: ((f: TFile) => Promise<void>)[];

    // The set of indices which we update.
    tag: TagIndex;
    csv: CsvIndex;
    prefix: PrefixIndex;

    // Other useful things to hold onto.
    vault: Vault;
    metadataCache: MetadataCache;

    constructor(vault: Vault, metadataCache: MetadataCache, tag: TagIndex, csv: CsvIndex, prefix: PrefixIndex) {
        this.vault = vault;
        this.metadataCache = metadataCache;

        this.tag = tag;
        this.csv = csv;
        this.prefix = prefix;

        this.reloadQueue = [];
        this.reloadSet = new Set();
        this.reloadHandlers = [];

        // Background task which regularly checks for reloads.
        this.reloadHandle = window.setInterval(() => this.reloadInternal(), FullIndex.RELOAD_INTERVAL);

        // TODO: Metadata cache is not updated on modify, but on metadatacache resolve.
        vault.on("modify", file => {
            if (file instanceof TFile) {
                this.queueReload(file);
            }
        });
    }

    /** Queue the file for reloading; several fast reloads in a row will be debounced. */
    public queueReload(file: TFile) {
        if (this.reloadSet.has(file.path)) return;
        this.reloadSet.add(file.path);
        this.reloadQueue.push(file);
    }

    public on(event: 'reload', handler: (a: TFile) => Promise<void>) {
        this.reloadHandlers.push(handler);
    }

    /** Utility method which regularly checks the reload queue. */
    private async reloadInternal() {
        let copy = Array.from(this.reloadQueue);
        this.reloadSet.clear();
        this.reloadQueue = [];

        for (let file of copy.filter((f) => f.extension != "csv")) {
            await Promise.all([this.tag.reloadFile(file)].concat(this.reloadHandlers.map(f => f(file))));
        }
        for (let file of copy.filter((f) => f.extension == "csv")) {
            await Promise.all([this.csv.reloadFile(file)]);
        }
    }
}

export class CsvIndex {

    vault: Vault;
    cache: MetadataCache;
    rows: Map<String, Array<LiteralFieldRepr<'object'>>>
    indexed: Map<String, Map<String, Map<any, LiteralFieldRepr<'object'>>>>

    constructor(vault: Vault, metadataCache: MetadataCache,) {
        this.vault = vault;
        this.cache = metadataCache;
        this.rows = new Map<String, Array<LiteralFieldRepr<'object'>>>();
        this.indexed = new Map<String, Map<String, Map<any, LiteralFieldRepr<'object'>>>>();
    }

    public static async generate(vault: Vault, metadataCache: MetadataCache,): Promise<CsvIndex> {
        let index = new CsvIndex(vault, metadataCache);
        for (let file of vault.getFiles().filter((f) => f.extension == "csv")) {
            await index.reloadFile(file);
        }
        return index;
    }

    async reloadFile(file: TFile) {
        let timeStart = new Date().getTime();
        const content = await this.vault.adapter.read(file.path);
        const parsed = Papa.parse(content, {
            header: true,
            skipEmptyLines: true,
            comments: true,
            dynamicTyping: true,
        });
        let rows = [] as Array<LiteralFieldRepr<'object'>>;
        if (parsed.errors.length > 0) {
            console.log(parsed.errors[0]);
        } else {
            for (let i = 0; i < parsed.data.length; ++i) {
                let row = parseFrontmatter(parsed.data[i]) as LiteralFieldRepr<'object'>;
                let col_idx = 0;
                if (row.value != null) {
                    for (const [key, _] of Object.entries(parsed.data[i])) {
                        let strKey = key.toString().replace(/ /g, "_");
                        let map = (row.value as LiteralTypeRepr<'object'>)
                        map.set(strKey, map.get(key) as LiteralField);
                        map.set(`col__${col_idx++}`, map.get(key) as LiteralField);
                    }
                }
                rows.push(row);
            }
        }

        let totalTimeMs = new Date().getTime() - timeStart;
        console.log(`Dataview: Load ${parsed.data.length} rows in ${file.path} (${totalTimeMs / 1000.0}s)`);
        this.rows.set(file.path, rows);
        this.indexed.delete(file.path);
    }

    public get(path: string): Array<LiteralFieldRepr<'object'>> {
        let result = this.rows.get(path);
        if (result) {
            return result;
        } else {
            return [] as Array<LiteralFieldRepr<'object'>>;
        }
    }

    public getMap(path: string, key: string): Map<any, LiteralFieldRepr<'object'>> {
        let cached = this.indexed.get(path);
        if (cached) {
            let forKey = cached.get(key);
            if (forKey) {
                return forKey;
            }
        }
        let forKey = new Map<any, LiteralFieldRepr<'object'>>();
        let rows = this.get(path);
        rows.forEach((row)=>{
            forKey.set(row.value.get(key)?.value,  row)
        })
        if (!cached) {
            cached = new Map<String, Map<any, LiteralFieldRepr<'object'>>>();
            this.indexed.set(path, cached);
        }
        console.log(`Dataview: build index for ${path} with key ${key}`);
        cached.set(key, forKey);
        return forKey;
    }
}

/** Index which efficiently allows querying by tags / subtags. */
export class TagIndex {

    /** Parse all subtags out of the given tag. I.e., #hello/i/am would yield [#hello/i/am, #hello/i, #hello]. */
    public static parseSubtags(tag: string): string[] {
        let result = [tag];
        while (tag.contains("/")) {
            tag = tag.substring(0, tag.lastIndexOf("/"));
            result.push(tag);
        }

        return result;
    }

    /** Parse all of the tags for the given file. */
    public static parseTags(cache: MetadataCache, path: string): Set<string> {
        let fileCache = cache.getCache(path);
        if (!fileCache) return new Set<string>();

        let allTags = new Set<string>();

        // Parse tags from in the file contents.
        let tagCache = fileCache.tags;
        if (tagCache) {
            for (let tag of tagCache) {
                if (!tag.tag || !(typeof tag.tag == 'string')) continue;
                this.parseSubtags(tag.tag).forEach(t => allTags.add(t));
            }
        }

        // Parse tags from YAML frontmatter.
        let frontCache = fileCache.frontmatter;

        // Search for the 'tags' field, since it may have wierd
        let tagsName: string | undefined = undefined;
        for (let key of Object.keys(frontCache ?? {})) {
            if (key.toLowerCase() == "tags" || key.toLowerCase() == "tag")
                tagsName = key;
        }

        if (frontCache && tagsName && frontCache[tagsName]) {
            if (Array.isArray(frontCache[tagsName])) {
                for (let tag of frontCache[tagsName]) {
                    if (!(typeof tag == 'string')) continue;

                    if (!tag.startsWith("#")) tag = "#" + tag;
                    this.parseSubtags(tag).forEach(t => allTags.add(t));
                }
            } else if (typeof frontCache[tagsName] === 'string') {
                // Assume tags is a comma-separated list.
                let tags = (frontCache[tagsName] as string).split(",").map(elem => {
                    elem = elem.trim();
                    if (!elem.startsWith("#")) elem = "#" + elem;
                    return elem;
                });

                for (let tag of tags) {
                    this.parseSubtags(tag).forEach(t => allTags.add(t));
                }
            }
        }

        return allTags;
    }

    public static async generate(vault: Vault, cache: MetadataCache): Promise<TagIndex> {
        let initialMap = new Map<string, Set<string>>();
        let initialInvMap = new Map<string, Set<string>>();
        let timeStart = new Date().getTime();

        // First time load...
        for (let file of vault.getMarkdownFiles()) {
            let allTags = TagIndex.parseTags(cache, file.path);
            initialInvMap.set(file.path, allTags);

            for (let subtag of allTags) {
                if (!initialMap.has(subtag)) initialMap.set(subtag, new Set<string>());
                initialMap.get(subtag)?.add(file.path);
            }
        }

        let totalTimeMs = new Date().getTime() - timeStart;
        console.log(`Dataview: Parsed ${initialMap.size} tags in ${initialInvMap.size} markdown files (${totalTimeMs / 1000.0}s)`);

        return new TagIndex(vault, cache, initialMap, initialInvMap);
    }

    /** Maps tags -> set of files containing that exact tag. */
    map: Map<string, Set<string>>;
    /** Cached inverse map; maps file -> tags it was last known to contain. */
    invMap: Map<string, Set<string>>;

    vault: Vault;
    cache: MetadataCache;

    constructor(vault: Vault, metadataCache: MetadataCache,
        map: Map<string, Set<string>>, invMap: Map<string, Set<string>>) {
        this.vault = vault;
        this.cache = metadataCache;

        this.map = map;
        this.invMap = invMap;
    }

    /** Returns all files which have the given tag. */
    public get(tag: string): Set<string> {
        let result = this.map.get(tag);
        if (result) {
            return new Set(result);
        } else {
            return new Set();
        }
    }

    /** Returns all tags the given file has. */
    public getInverse(file: string): Set<string> {
        let result = this.invMap.get(file);
        if (result) {
            return new Set(result);
        } else {
            return new Set();
        }
    }

    async reloadFile(file: TFile) {
        this.clearFile(file.path);
        let allTags = TagIndex.parseTags(this.cache, file.path);

        for (let subtag of allTags) {
            if (!this.map.has(subtag)) this.map.set(subtag, new Set<string>());
            this.map.get(subtag)?.add(file.path);
        }

        this.invMap.set(file.path, allTags);
    }

    /** Clears all tags for the given file so they can be re-added. */
    private clearFile(path: string) {
        let oldTags = this.invMap.get(path);
        if (!oldTags) return;

        this.invMap.delete(path);
        for (let tag of oldTags) {
            this.map.get(tag)?.delete(path);
        }
    }
}

/** A node in the prefix tree. */
export class PrefixIndexNode {
    // TODO: Instead of only storing file paths at the leaf, consider storing them at every level,
    // since this will make for faster deletes and gathers in exchange for slightly slower adds and more memory usage.
    // since we are optimizing for gather, and file paths tend to be shallow, this should be ok.
    files: Set<string>;
    element: string;
    totalCount: number;
    children: Map<string, PrefixIndexNode>;

    constructor(element: string) {
        this.element = element;
        this.files = new Set();
        this.totalCount = 0;
        this.children = new Map();
    }

    public static add(root: PrefixIndexNode, path: string) {
        let parts = path.split("/");
        let node = root;
        for (let index = 0; index < parts.length - 1; index++) {
            if (!node.children.has(parts[index])) node.children.set(parts[index], new PrefixIndexNode(parts[index]));

            node.totalCount += 1;
            node = node.children.get(parts[index]) as PrefixIndexNode;
        }

        node.totalCount += 1;
        node.files.add(path);
    }

    public static remove(root: PrefixIndexNode, path: string) {
        let parts = path.split("/");
        let node = root;
        let nodes = [];
        for (let index = 0; index < parts.length - 1; index++) {
            if (!node.children.has(parts[index])) return;

            nodes.push(node);
            node = node.children.get(parts[index]) as PrefixIndexNode;
        }

        if (!node.files.has(path)) return;
        node.files.delete(path);
        node.totalCount -= 1;

        for (let p of nodes) p.totalCount -= 1;
    }

    public static find(root: PrefixIndexNode, prefix: string): PrefixIndexNode | null {
        if (prefix.length == 0 || prefix == '/') return root;
        let parts = prefix.split("/");
        let node = root;
        for (let index = 0; index < parts.length; index++) {
            if (!node.children.has(parts[index])) return null;

            node = node.children.get(parts[index]) as PrefixIndexNode;
        }

        return node;
    }

    public static gather(root: PrefixIndexNode): Set<string> {
        let result = new Set<string>();
        PrefixIndexNode.gatherRec(root, result);
        return result;
    }

    static gatherRec(root: PrefixIndexNode, output: Set<string>) {
        for (let file of root.files) output.add(file);
        for (let child of root.children.values()) this.gatherRec(child, output);
    }
}

/** Indexes files by their full prefix - essentially a simple prefix tree. */
export class PrefixIndex {

    public static async generate(vault: Vault): Promise<PrefixIndex> {
        let root = new PrefixIndexNode("");
        let timeStart = new Date().getTime();

        // First time load...
        for (let file of vault.getMarkdownFiles()) {
            PrefixIndexNode.add(root, file.path);
        }

        let totalTimeMs = new Date().getTime() - timeStart;
        console.log(`Dataview: Parsed all file prefixes (${totalTimeMs / 1000.0}s)`);

        return Promise.resolve(new PrefixIndex(vault, root));
    }

    root: PrefixIndexNode;
    vault: Vault;

    constructor(vault: Vault, root: PrefixIndexNode) {
        this.vault = vault;
        this.root = root;

        // TODO: I'm not sure if there is an event for all files in a folder, or just the folder.
        // I'm assuming the former naively for now until I inevitably fix it.
        this.vault.on("delete", file => {
            PrefixIndexNode.remove(this.root, file.path);
        });

        this.vault.on("create", file => {
            PrefixIndexNode.add(this.root, file.path);
        });

        this.vault.on("rename", (file, old) => {
            PrefixIndexNode.remove(this.root, old);
            PrefixIndexNode.add(this.root, file.path);
        });
    }

    public get(prefix: string): Set<string> {
        let node = PrefixIndexNode.find(this.root, prefix);
        if (node == null || node == undefined) return new Set();

        return PrefixIndexNode.gather(node);
    }
}

/** Caches tasks for each file to avoid repeated re-loading. */
export class TaskCache {

    /** Create a task cache for the given vault. */
    static async generate(vault: Vault): Promise<TaskCache> {
        let initialCache: Record<string, Task[]> = {};
        let timeStart = new Date().getTime();

        // First time load...
        for (let file of vault.getMarkdownFiles()) {
            let tasks = await Tasks.findTasksInFile(vault, file);
            if (tasks.length == 0) continue;
            initialCache[file.path] = tasks;
        }

        let totalTimeMs = new Date().getTime() - timeStart;
        console.log(`Dataview: Parsed tasks in ${Object.keys(initialCache).length} markdown files (${totalTimeMs / 1000.0}s)`);

        return new TaskCache(vault, initialCache);
    }

    cache: Record<string, Task[]>;
    vault: Vault;

    constructor(vault: Vault, cache: Record<string, Task[]>) {
        this.vault = vault;
        this.cache = cache;
    }

    /** Get the tasks associated with a file path. */
    public get(file: string): Task[] | null {
        let result = this.cache[file];
        if (result === undefined) return null;
        else return result;
    }

    /** Return a map of all files -> tasks in that file. */
    public all(): Record<string, Task[]> {
        // TODO: Defensive copy.
        return this.cache;
    }

    async reloadFile(file: TFile) {
        let tasks = await Tasks.findTasksInFile(this.vault, file);
        if (tasks.length == 0) {
            delete this.cache[file.path];
        } else {
            this.cache[file.path] = tasks;
        }
    }
}
