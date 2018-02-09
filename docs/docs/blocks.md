---
id: blocks
title: Block Syntax
---

The general block syntax is as follows:
```
BLOCK_TYPE 'BLOCK_NAME' [EXTERN 'BLOCK_FILE'] (
	BLOCK_CONTENT
) [WITH (BLOCK_OPTIONS)] [;]
```

Some blocks have additional syntax, outlined in the relevant pages under "Blocks".

Blocks can optionally be terminated by a semi-colon (`;`). 

Whitespace characters are ignored. 

Comments can be either inline using SQL-style `--` or multiline using `/** **/`.

Keywords and identifiers are case-insensitive. 

Three blocks follow a shorter syntax: `INCLUDE`, `DECLARE` AND `SET`: 

**`INCLUDE` Syntax**
```
INCLUDE 'INCLUDE_FILE'
```

**`DECLARE` Syntax**

```
DECLARE 'PARAMETER_NAME'
```

**`SET` Syntax**

```
SET OPTION_NAME = OPTION_VALUE
```

See below for more details on option values.

## Templating

`BLOCK_FILE` and `BLOCK_CONTENT` both admit templating using the [Go templating syntax](https://golang.org/pkg/text/template/). 

*Template parameter evaluation occurs at **compile-time***.

The template parameters are global options. These can be set using the [SET](set.md) command, using the [command-line interface](cli.md) or, if used as a Go library, passing that argument to `ExecuteString()` or `ExecuteFile()` (see [godocs](https://godoc.org/github.com/michaelbironneau/analyst)).

Example:

```
SET Mode = 'Prod';
INCLUDE `db.{{ Mode }}.aql`
```

## Block Types
Allowed block types are:

1. [`INCLUDE`](include.md) - import content from another file
2. [`CONNECTION`](connection.md) - configuration to connect to a database, email server, Excel, etc.
3. [`QUERY`](query.md) - a SQL query to get data from a database
4. [`EXEC`](exec.md) - a SQL query that returns no rows
5. [`DATA`](data.md) - a literal or flat file data source
6. [`TRANSFORM`](transform.md) - an in-memory data transformation (eg. lookup)
7. [`GLOBAL`](global.md) - a SQL statement to initialize the global database
8. [`DECLARE`](declare.md) - declaration for a global SQL parameter
9. [`SET`](set.md) - set a global option

## Block Name
The block name can contain any sequence of alphanumeric characters. There is no maximum or minimum length (`''` is a legal block name).

## External Content

It is possible to replace `BLOCK_CONTENT` by the contents of a file to keep scripts as modular as possible or to use flat file data sources.

The `BLOCK_FILE` path should be either absolute, or relative to the script location (this may be different from the directory where the `analyst` command is run).

Example:

```
DATA 'Users' EXTERN 'users.json' () WITH (COLUMNS = 'Id, Name')
```

If `EXTERN` and `BLOCK_CONTENT` are both specified, the external content will overwrite the local content.

## Block Content

The block content is specific to the block type. It can include any UTF-8 character. 

## Block Options

The syntax for block options is `WITH (OPT1_NAME = OPT1_VALUE [, OPT2_NAME = OPT2_VALUE [, ...]])`.

**Option Names and Inheritance**

Options can be set at block-level, connection-level, at GLOBAL level, or CLI/library level. The inheritance hierarchy, from highest to lowest precedence, is:

* Block-level
* Connection-level (if applicable)
* CLI/Library-level
* Global level (within `SET` block)

For example, in the below, the `DATA` block will require setting the `FORMAT` option (see [DATA](data.md)). In this case, it is inherited from the global option.

```
SET FORMAT = 'JSON_OBJECTS';

DATA 'MyData' (
	[
		{"Message": "Hello, world"}
	]
)
```

However, in the below, the block-level option overrides the global option.

```
SET FORMAT = 'CSV';

DATA 'MyData' (
	[
		{"Message": "Hello, world"}
	]
) WITH (FORMAT = 'JSON_OBJECTS')
```

**Option Value Types**

There are two primitive types of option values:

* Strings (eg. `OPT = 'Value'`)
* Numbers (eg. `OPT = 123.123`)

In addition, some options will try to coerce the value to a boolean. Boolean options determine truthiness as follows

* Non-zero numbers are truthy
* Case-insensitive variants of `'True'` are truthy
* All other strings and numbers are falsy



