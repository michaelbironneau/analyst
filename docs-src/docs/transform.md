---
id: transform
title: TRANSFORM
---

The syntax of a transform block is as follows:
```
TRANSFORM [PLUGIN] 'BLOCK_NAME' [EXTERN 'TRANSFORM_FILE'] FROM TRANSFORM_SOURCE_1 [, TRANSFORM_SOURCE_2 [, ...]] (
	TRANSFORM_CONTENT
)
  [INTO TRANSFORM_DESTINATION_1 [, TRANSFORM_DESTINATION_2 [, ...]]]
  [WITH (BLOCK_OPTIONS)]
  [AFTER DEPENDENCY_1 [, DEPENDENCY_2 [,...]]]
```

Transform blocks take data from a source, manipulate it, and send it to a destination.

There are two types of transforms:

* Built-in transforms
* External plugins

## Built-in transforms

There are two built-in transforms:

* `LOOKUP`: Performs a lookup using outer or inner join condition
* `AGGREGATE`: Applies aggregates such as `SUM()` to the data

Both are documented more extensively in the [Transforms](transforms.md) page.

## External plugins

External transform plugins can be created using JSON-RPC. Some example Python source code can be found [here](https://github.com/michaelbironneau/analyst/blob/master/test_filter.py).

Creating language-specific libraries to simplify plugin creation is an area for future development and contributions are welcome.

To invoke a plugin, some mandatory options need to be specified in the `WITH` clause:

* `EXECUTABLE`: The path or alias of the executable (eg. `python`)
* `Args`: The arguments to pass, as JSON array (eg. `'["./test_filter.py"]'`)

## Transform Sources
The syntax for query source specification is as follows:

```
TRANSFORM_SOURCE = {CONNECTION | BLOCK} SOURCE_IDENTIFIER 
				| GLOBAL
```

If a transform source is a SQL database connection, then the `TABLE` option must be included. In that case, all rows from the specified table will be fetched and passed to the transform.

If a transform specifies multiple sources, then the `MULTISOURCE_ORDER` option can be used to synchronize source fetch order:

* `PARALLEL` (default): Data from all sources will be sent to the transform at the same time
* `SEQUENTIAL`: Data will first be sent from the first source, and when it finishes, the second source, and so on, until all sources are done

## Examples

### Plugin 

This example generates some dummy data, uses a plugin to filter the results and sends the rows to an Excel spreadsheet.

```
CONNECTION 'Workbook' (
	Driver = 'Excel',
	ConnectionString = '.',
	File = './output_transform.xlsx'
)

QUERY 'SliceOfData' FROM GLOBAL (
	SELECT 1 AS 'Value'
		UNION ALL
	SELECT -1 AS 'Value'
		UNION ALL
	SELECT 2 AS 'Value'
)

QUERY 'SliceOfData2' FROM GLOBAL (
	SELECT 10 AS 'Value'
		UNION ALL
	SELECT 11 AS 'Value'
		UNION ALL
	SELECT -2 AS 'Value'
)

TRANSFORM PLUGIN 'FilterNegatives' FROM BLOCK SliceOfData, BLOCK SliceOfData2 ()
INTO CONNECTION Workbook
WITH (
		Sheet = 'TestSheet', Range = 'A1:A*',
		Columns = 'Value', Multisource_Order = 'Sequential',
		Executable = 'python', Args = '["./test_filter.py"]', Overwrite = 'True'
)
```

### Built-in

This example creates two dummy tables and performs an in-memory join.

```
	GLOBAL 'CreateTables' (
		CREATE TABLE LookupTable (
			id INT PRIMARY KEY,
			first_name TEXT
		);
		CREATE TABLE BaseTable (
			lookup_id INT PRIMARY KEY,
			last_name TEXT
		);
		CREATE TABLE JoinedTable (
			first_name TEXT,
			last_name TEXT
		);
	);
	GLOBAL 'SeedTables' (
		INSERT INTO LookupTable VALUES (1, 'Bob');
		INSERT INTO LookupTable VALUES (2, 'John');
		INSERT INTO LookupTable VALUES (3, 'Steve');
		INSERT INTO BaseTable VALUES (1, 'Bobbertson');
		INSERT INTO BaseTable VALUES (2, 'Johnson');
	);
	QUERY 'FirstNames' FROM GLOBAL (
		SELECT id, first_name FROM LookupTable
	);
	QUERY 'LastNames' FROM GLOBAL (
		SELECT lookup_id, last_name FROM BaseTable
	);
	TRANSFORM 'Join' FROM BLOCK FirstNames, BLOCK LastNames (
		LOOKUP FirstNames.first_name, LastNames.last_name FROM FirstNames
		INNER JOIN LastNames ON FirstNames.id = LastNames.lookup_id
	) INTO GLOBAL WITH(Table = 'JoinedTable')
```