---
id: query
title: QUERY
---

The general query syntax is as follows:
```
QUERY 'QUERY_NAME' [EXTERN 'QUERY_FILE'] FROM QUERY_SOURCE (
	QUERY_CONTENT
) 
[USING PARAMETER PARAM_1 [, PARAM_2 [, ...]]]
[INTO QUERY_DESTINATION [, QUERY_DESTINATION_" [, ...]]]
[WITH QUERY_OPTIONS]
[AFTER DEPENDENCY_1 [, DEPENDENCY_2 [, ...]]]
```

The query content is always a SQL statement valid for the type of query source (e.g. a Microsoft SQL query source requires a valid T-SQL query).

## Query Sources
The syntax for query source specification is as follows:

```
QUERY_SOURCE = {CONNECTION | BLOCK} SOURCE_IDENTIFIER 
				| GLOBAL
```

Queries have two types of sources:

* SQL databases
* Non-database sources

For the first type, the query content is a SQL statement that is valid for the database, and the query will be run against that database.

The the second type, Analyst will extract the data to an in-memory SQLite 3 database (not the same as the global database) and the query will be run against that. Therefore, the query needs to be valid for SQLite 3.

Note that the GLOBAL database is an SQLite3 database too.

## Query Destinations

The syntax for query destination specification is as follows:

```
QUERY_DESTINATION = CONNECTION DESTINATION_IDENTIFIER 
						| GLOBAL 
						| CONSOLE 
						| PARAMETER ( PARAMETER1_NAME [, PARAMETER2_NAME [, ...]])
```
 
If the destination is specified, then every row returned by the query will be sent to it. For example, if a connection to an Excel file is specified, then this file will be populated with all the rows returned by the query.

A query need not specify a destination. A query without a destination can be used as the source of another block, such as `TRANSFORM`.

Queries that do not specify a destination and that are not the source of any other block will **NOT** be executed.

### Connection

If a connection is specified, then connection-specific options may need to be provided. For SQL destinations, the `TABLE` option always needs to be speficied, saying which table the data should be inserted into.

### Console 

This will send rows to STDOUT on the console. There are two available output formats configurable with the `OUTPUT_FORMAT` option:

* `table` (default): Pretty-prints the output as a table.
* `json`: Formats the output as an array of JSON objects. The keys are the column names.

### Parameter

This will send the output to one or more parameters, in column order. The parameter must have been previously declared by the `DECLARE` statement.

For example in the below `@Param` will be set to 1 and `@Param2` will be set to 2.

```
DECLARE @Param;
DECLARE @Param2;

QUERY 'WriteParam' FROM GLOBAL (
	SELECT 1, 2
) INTO PARAMETER (@Param, @Param2)
```
*The query must return a single row*.

## After

By default, QUERY and EXEC blocks are scheduled to run in parallel with maximum parallelism. This means that two QUERY blocks in the same script that both specify a destination will both run concurrently, rather than the second starting after the first finishes.

The `AFTER` keyword specifies a dependency on any other block. Its meaning is that the query will not start running before the other block finishes. This can be used to synchronize parameter setting or data access (see examples below).

## Examples

Basic example:
```
QUERY 'SelectAll' FROM CONNECTION MyDb (
	SELECT * FROM MyTable
) INTO CONSOLE
```

Move data from one database to another:
```
QUERY 'MoveData' FROM CONNECTION MyDb (
	SELECT * FROM MyTable
) INTO CONNECTION MyOtherDb WITH (TABLE = 'SecondTable')
```

Ignore any rows containing NULLs and don't attempt to insert them into the destination:
```
QUERY 'MoveData' FROM CONNECTION MyDb (
	SELECT * FROM MyTable
) INTO CONNECTION MyOtherDb WITH (TABLE = 'SecondTable', DROP_NULLS = 'True')
```

Use parameters and synchronization to print @Name and @Id in that order.
```
DECLARE @Id;
DECLARE @Name;

QUERY 'GetId' FROM CONNECTION MyDb (
	SELECT Id, Name FROM MyTable LIMIT 1
) INTO PARAMETER (@Id, @Name)

QUERY 'PrintName' FROM GLOBAL (
	SELECT ? AS Name
) USING PARAMETER @Name
  INTO CONSOLE
  AFTER GetId
  
QUERY 'PrintId' FROM GLOBAL (
	SELECT ? AS Id
) USING PARAMETER @Id 
  INTO CONSOLE 
  AFTER PrintName
```