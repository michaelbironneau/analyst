---
id: data
title: DATA
---

The full syntax of a `DATA` block is as follows:
```
DATA 'BLOCK_NAME' [EXTERN 'BLOCK_FILE'] (
	DATA_CONTENT
) [WITH (DATA_OPTIONS)]
```

**Data Content**

The following formats are currently supported:

* `JSON_ARRAY`: a flat array, eg `[[2,3],[3,4]]`. Requires `COLUMNS` option to specify the column names.
* `JSON_OBJECTS`: an array of JSON objects, eg. `[{"a": 1, "b": 2}, {"a": 4, "b": 5}]`. Each object must have the same keys.
* `CSV`: A CSV string without headers, eg. `1, 2\n3, 4`. Values will be strings - no attempt will be made to convert them to numbers.


**Data Options**

There are two options: `COLUMNS` and `FORMAT`. 

The `COLUMNS` value is a string containing comma-separated, case-insensitive column names. It is ignored for `JSON_OBJECTS` format.

The `FORMAT` value specifies the data as a string. The default is `JSON_ARRAY`.

## Examples

```
DATA 'Users' EXTERN 'Users.json' () WITH (FORMAT = 'JSON_OBJECTS')
```

```
DATA 'Users' (
	[
	  {"Name": "Bob"},
	  {"Name": "Steve"}
	]
) WITH (FORMAT = 'JSON_OBJECTS')
```

```
DATA 'Users' (
	[
		["Bob"],
		["Steve"]
	]
) WITH (COLUMNS = 'Name')
```

