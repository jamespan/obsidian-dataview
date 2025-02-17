---
sidebar_position: 1
---
# Queries

The dataview query language is a simple, structured, custom query language for quickly creating views on your data. It
supports:

- Fetching pages associated with tags, folders, links, and so on.
- Filtering pages / data by simple operations on fields, like comparison, existence checks, and so on.
- Sorting results based on fields.

The query language supports the following view types, described below:

1. **TABLE**: The traditional view type; one row per data point, with several columns of field data.
2. **LIST**: A list of pages which match the query. You can output a single associated value for each page.
3. **TASK**: A list of tasks whose pages match the given query.

## General Format

The general format for queries is:

~~~
```dataview
TABLE|LIST|TASK <field> [AS "Column Name"], <field>, ..., <field> FROM <source> (like #tag or "folder")
WHERE <expression> (like 'field = value')
SORT <expression> [ASC/DESC] (like 'field ASC')
... other data commands
```
~~~

Only the 'select' statement (describing what view and what fields) is required. If the `FROM` statement is omitted, the
query runs automatically over all markdown pages in your vault. If other statements (like `WHERE` or `SORT`) are
present, they are run in the order they are written. Duplicate statements are allowed (multiple `WHERE` statement, for eaxmple).

- For the different view types, only the first line (the 'select' section, where you specify the view type and fields to
display) differs. You can apply *data commands* like *WHERE* and *SORT* to any query, and you can select from any
[source](/docs/query/sources) using *FROM*.

See [expressions](expressions) for context on what expressions are, and [sources](sources) for context on what sources are.

## Query Types

### List Queries

Lists are the simplest view, and simply render a list of pages (or custom fields) which match the query.
To obtain a list of pages matching the query, simply use:

```
LIST FROM <source>
```

For example, running `LIST FROM #games/moba or #games/crpg` might show:

![List Example](/images/game-list.png)

You can render a single computed value in addition to each matching file, by adding an expression after `LIST`:

```
LIST <expression> FROM <source>
```

For example, running `LIST "File Path: " + file.path FROM "4. Archive"` might show:

![List Example](/images/file-path-list.png)

### Table Queries

Tables support tabular views of page data. You construct a table by giving a comma separated list of the YAML frontmatter fields you want to render, as so:

```
TABLE file.day, file.mtime FROM <source>
```

You can choose a heading name to render computed fields by using the `AS` syntax:

```
TABLE (file.mtime + dur(1 day)) AS next_mtime, ... FROM <source>
```

An example table query:

```
TABLE time-played AS "Time Played", length as "Length", rating as "Rating" FROM #game
SORT rating DESC
```

![Table Example](/images/game.png)

### Task Queries

Task views render all tasks whose pages match the given predicate.

```
TASK from <source>
```

For example, `TASK FROM "dataview"` might show:

![Task Example](/images/project-task.png)

## Data Commands

The different commands that dataview queries can be made up of. Commands are
executed in order, and you can have duplicate commands (so multiple `WHERE`
blocks or multiple `GROUP BY` blocks, for example).

### FROM

The `FROM` statement determines what pages will initially be collected and passed onto the other commands for further
filtering. You can select from any [source](/docs/query/sources), which currently means by folder, by tag, or by incoming/outgoing links.

- **Tags**: To select from a tag (and all its subtags), use `FROM #tag`.
- **Folders**: To select from a folder (and all its subfolders), use `FROM "folder"`.
- **Links**: You can either select links TO a file, or all links FROM a file.
  - To obtain all pages which link TO `[[note]]`, use `FROM [[note]]`.
  - To obtain all pages which link FROM `[[note]]` (i.e., all the links in that file), use `FROM outgoing([[note]])`.

You can compose these filters in order to get more advanced sources using `and` and `or`.
- For example, `#tag and "folder"` will return all pages in `folder` and with `#tag`.
- `[[Food]] or [[Exercise]]` will give any pages which link to `[[Food]]` OR `[[Exercise]]`.

### WHERE

Filter pages on fields. Only pages where the clause evaluates to `true` will be yielded.

```
WHERE <clause>
```

1. Obtain all files which were modified in the last 24 hours:

```
LIST WHERE file.mtime >= date(today) - dur(1 day)
```

2. Find all projects which are not marked complete and are more than a month old:

```
LIST FROM #projects
WHERE !completed AND file.ctime <= date(today) - dur(1 month)
```

### SORT

Sorts all results by one or more fields.

```
SORT date [ASCENDING/DESCENDING/ASC/DESC]
```

You can also give multiple fields to sort by. Sorting will be done based on the first field. Then, if a tie occurs, the second field will be used to sort the tied fields. If there is still a tie, the third sort will resolve it, and so on.

```
SORT field1 [ASCENDING/DESCENDING/ASC/DESC], ..., fieldN [ASC/DESC]
```

### GROUP BY

Group all results on a field. Yields one row per unique field value, which has 2 properties: one corresponding to the field being grouped on, and a `rows` array field which contains all of the pages that matched.

```
GROUP BY field
GROUP BY (computed_field) AS name
```

In order to make working with the `rows` array easier, Dataview supports field "swizzling". If you want the field `test` from every object in the `rows` array, then `rows.test` will automatically fetch the `test` field from every object in `rows`, yielding a new array.
You can then apply aggregation operators like `sum()` over the resulting array.

### FLATTEN

Flatten an array in every row, yielding one result row per entry in the array.

```
FLATTEN field
FLATTEN (computed_field) AS name
```

For example, flatten the `authors` field in each literature note to give one row per author:

```
table authors from #LiteratureNote
flatten authors
```

![Flatten Example](/images/flatten-authors.png)
