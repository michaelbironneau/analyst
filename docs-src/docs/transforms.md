---
id: transforms
title: Transforms
---

This section explains the usage of built-in transforms: `LOOKUP`, `AGGREGATE` and `APPLY`.

## The `LOOKUP` transform

This transform performs a lookup of a base table on a lookup table based on an inner or outer join condition.

At this time only equality join conditions are supported. All column names must be fully qualified.

The syntax is as follows:

```
TRANSFORM 'TRANSFORM_NAME' FROM BASE_TABLE_SOURCE, LOOKUP_TABLE_SOURCE (
	LOOKUP FULLY_QUALIFIED_COLUMN_1, FULLY_QUALIFIED_COLUMN_2, ... FROM BASE_TABLE
	{INNER|OUTER} JOIN LOOKUP_TABLE ON QUALIFIED_JOIN_COLUMN_1 = QUALIFIED_JOIN_COLUMN_2 [AND ...] 
) [INTO TRANSFORM_DESTINATION_1 [, TRANSFORM_DESTINATION_2 [, ...]]]
  [WITH (BLOCK_OPTIONS)]
  [AFTER DEPENDENCY_1 [, DEPENDENCY_2 [,...]]]
```

**Examples**

Inner join:
```
TRANSFORM 'InnerJoinExample' FROM BLOCK GetA, BLOCK GetB (
	LOOKUP GetA.Id, GetB.Name FROM GetA
	INNER JOIN GetB ON GetA.Id = GetB.Id
)
```

Outer join:
```
TRANSFORM 'InnerJoinExample' FROM BLOCK GetA, BLOCK GetB (
	LOOKUP GetA.Id, GetB.FirstName FROM GetA
	OUTER JOIN GetB ON GetA.Id = GetB.Id AND GetA.LastName = GetB.LastName
)
```

## The `AGGREGATE` transform

The aggregate transform is used to apply zero or more aggregates, with possible grouping, to a set of input rows. 

The syntax is as follows:

```
TRANSFORM 'TRANSFORM_NAME' FROM SOURCE (
	AGGREGATE EXPRESSION_1 [AS 'ALIAS_1'], EXPRESSION_2 [AS 'ALIAS_2'], ... FROM SOURCE
	[GROUP BY COLUMN_1 [, COLUMN_2 [, ...]]]
) [INTO TRANSFORM_DESTINATION_1 [, TRANSFORM_DESTINATION_2 [, ...]]]
  [WITH (BLOCK_OPTIONS)]
  [AFTER DEPENDENCY_1 [, DEPENDENCY_2 [,...]]]
```

Where an expression is either a column name or an aggregated applied to one or more columns, i.e.

```
EXPRESSION = COLUMN_NAME | AGGREGATE(COLUMN_OR_LITERAL_1[, COLUMN_OR_LITERAL_2[,...]])
```

*Note that wildcards are not supported, e.g. `COUNT(*)` will not work*.

Available aggregates are as follows:

* `SUM`, `AVG`, `MAX`, `MIN`, `COUNT` with the usual meanings as defined in eg. [this article](http://www.sqlservercentral.com/articles/Advanced+Querying/gotchasqlaggregatefunctionsandnull/1947/)
* `ZOH`: Zero-Order-Hold (i.e. time-weighted mean for irregularly sampled series). This takes four parameters: point time (RFC3339 with or without nanoseconds), value, start, and finish times.
* `QUANTILE`: Streaming quantile. This takes two parameters: the column and the quantile, eg. `QUANTILE(Value, 0.75)` for the 75th percentile. The quantile must be the same for all entries in each group if there is a group by statement, or constant otherwise.
* `CDF`: Cumulative Distribution Function of a column evaluated at a given position. This takes two parameters: the column and the position, eg. `CDF(Value, 5)` evaluates the CDF for the column 'Value' at the point 5. The point should be constant for each group.

**Examples**
Aggregating data from an HTTP API:
```
CONNECTION 'WebAPI' (
	DRIVER = 'http',
	URL = 'https://chroniclingamerica.loc.gov/awardees.json',
	JSON_PATH = 'awardees',
	COLUMNS = 'URL, Name'
)

TRANSFORM 'CountAll' FROM CONNECTION WebAPI  (
	AGGREGATE COUNT(1) FROM WebAPI
) INTO CONSOLE WITH (OUTPUT_FORMAT = 'JSON')

```

Time-weighted mean of a timeseries:

```
GLOBAL 'CreateTables' (
    CREATE TABLE Timeseries (
        LoadId int not null,
        Variable text not null,
        Time  text not null,
        Value real
    );

    INSERT INTO Timeseries (LoadId, Variable, Time, Value)
     VALUES
     (1, 'power', '2017-12-01T11:59:00Z', 10),
     (1, 'power', '2017-12-01T12:13:01Z', 0),
     (1, 'power', '2017-12-01T12:57:00Z', 1.1),
     (2, 'power', '2017-12-01T11:52:00Z', 120),
     (2, 'power', '2017-12-01T11:45:00Z', 100),
     (3, 'power', '2017-12-01T12:33:00Z', 119),
     (3, 'power', '2017-12-01T12:20:00Z', 50),
     (3, 'power', '2017-12-01T11:59:00Z', 100),
     (1, 'temperature', '2017-12-01T11:59:00Z', 129.5),
     (1, 'temperature', '2017-12-01T12:13:01Z', 130.3);
)

TRANSFORM 'Resample' FROM GLOBAL (
    AGGREGATE LoadId, Variable, ZOH(Time, Value, '2017-12-01T12:00:00Z', '2017-12-01T12:30:00Z') As Value
    GROUP BY LoadId, Variable
) INTO CONSOLE
    WITH (Table = 'Timeseries', CONSOLE_OUTPUT_FORMAT='JSON')
```

## APPLY

The `APPLY` transform applies a scalar function to a single row. At present, only `CAST` is supported.

The source/destination types for `CAST` are as follows:

* `INT` (integer) -> `VARCHAR`, `DATETIME` (seconds since epoch)
* `VARCHAR` (string) -> `INT`, `DATETIME` (RFC3339 format with or without nanoseconds)
* `DATETIME` -> `INT` (seconds since epoch), `VARCHAR` (RFC3339 format)
* `BOOLEAN` -> `INT` (0 is False, 1 is True), `VARCHAR` (True/False)

**Example:**

```
TRANSFORM 'ParseDates' FROM GLOBAL (
    APPLY IntColumn, CAST(DateColumn AS DATETIME), ToBeRenamed As NewColumn
)
```