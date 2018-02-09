---
id: exec
title: EXEC
---

The general exec syntax is as follows:
```
EXEC 'EXEC_NAME' [EXTERN 'EXEC_FILE'] FROM EXEC_SOURCE (
	QUERY_CONTENT
) 
[USING PARAMETER PARAM_1 [, PARAM_2 [, ...]]]
[WITH EXEC_OPTIONS]
[AFTER DEPENDENCY_1 [, DEPENDENCY_2 [, ...]]]
```

The exec content is always a SQL statement valid for the type of query source (e.g. a Microsoft SQL query source requires a valid T-SQL query).

*An exec SQL statement returns no rows*.

## Exec Sources
The syntax for exec source specification is as follows:

```
EXEC_SOURCE = {CONNECTION | BLOCK} SOURCE_IDENTIFIER 
				| GLOBAL
```

Execs have two types of sources:

* SQL databases
* Non-database sources

For the first type, the exec content is a SQL statement that is valid for the database, and the query will be run against that database.

The the second type, Analyst will extract the data to an in-memory SQLite 3 database (not the same as the global database) and the query will be run against that. Therefore, the query needs to be valid for SQLite 3.

Note that the GLOBAL database is an SQLite3 database too.

## After

By default, QUERY and EXEC blocks are scheduled to run in parallel with maximum parallelism. This means that two QUERY blocks in the same script that both specify a destination will both run concurrently, rather than the second starting after the first finishes.

The `AFTER` keyword specifies a dependency on any other block. Its meaning is that the query will not start running before the other block finishes. This can be used to synchronize parameter setting or data access (see examples below).


## Examples 

Basic example running a scheduled delete statement:

```
EXEC 'CleanupStaging' FROM CONNECTION MyDb (
	DELETE FROM MyTable WHERE CreatedAt < DATEADD(YEAR, -1, GETDATE())
)
```

Example using synchronisation to ensure an exec runs after a query
```
/**
	Deletes all data older than one month trailing the latest entry
**/

DECLARE @LatestTime;

QUERY 'GetLatestTime' FROM CONNECTION MyDb (
	SELECT CONVERT(VARCHAR(33), Max(CreatedAt), 126) FROM MyTable
) INTO PARAMETER (@LatestTime);

EXEC 'Cleanup' FROM CONNECTION MyDb (
	DELETE FROM MyTable WHERE CreatedAt < DATEADD(DAY, -30, CAST(? AS DATETIME))
)  USING PARAMETER @LatestTime
   AFTER GetLatestTime
```
