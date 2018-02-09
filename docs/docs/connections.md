---
id: connections
title: Connectors
---

This section discusses the different connectors available to use as a CONNECTION for queries, transforms or execs, along with their various options.

## SQL Database Connectors

All SQL Database connections must specify the `CONNECTIONSTRING` option. This is a quoted string with driver-specific options. Please use the links below to access documentation for the drivers, which includes details of how to construct a connection string.

*Database connectors can be used as either sources (`FROM`) or destinations (`INTO`)*.
### Microsoft SQL Server (`DRIVER = 'mssql'`)

This is a connector for MS SQL Server. It is documented [here](https://github.com/denisenkom/go-mssqldb).

**Example**
```
CONNECTION 'SQLServerExample' (
	DRIVER = 'mssql',
	CONNECTIONSTRING = 'server=localhost;user id=sa;database=master;connection timeout=30'
)
```

### Postgres (`DRIVER = 'postgres'`)

This is a connector for Postgres. Documentation and examples can be found [here](https://godoc.org/github.com/lib/pq).

**Example**
```
CONNECTION 'PostgresExample' (
	DRIVER = 'postgres',
	CONNECTIONSTRING = 'postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full'
)
```

### SQLite 3 (`DRIVER = 'sqlite3'`)

This is a connector for SQLite 3. It is documented [here](https://github.com/mattn/go-sqlite3).

**Example**
```
CONNECTION 'PostgresExample' (
	DRIVER = 'sqlite3',
	CONNECTIONSTRING = 'path-to-my-database.db'
)
```

## Other Connectors

### Excel

The Excel connector can be used to ingest data from Microsoft Excel 2007 and later in XLSX format.

*The Excel connector can be used as either a source (`FROM`) or a destination (`INTO`) for transforms. At present it is limited to being a destination for queries.* 

The Excel connector need not specify a connection string, but if it does, then it is equivalent to the `FILE` option below.

**Options**

* `FILE`: XLSX file with input/output. For destinations, it need not exist but the directory must. 
* `SHEET`: Name of input/output sheet.
* `RANGE`: Range of input/output data (details below).
* `COLUMNS`: (Optional) Includes the column names for input or output. If not included for a source, then the first row of the range must include column names.
* `TRANSPOSE`: (Optional, default 'False') Transpose the output from rows into columns. 
* `TEMPLATE`: (Optional) Copy the template file (XLSX) into the output directory and populate this instead of an empty spreadsheet.
* `OVERWRITE`: (Optional, default 'False') Overwrite `FILE` if it exists. 

**The `RANGE` Option**

Ranges should be specified as cell-to-cell as they are in Excel, eg. `A1:B2`. For cases where te number of input/output rows is unknown, a single wildcard should be used in the right-hand cell, e.g. `A1:B*` or `A1:*2`.

**Examples**

As source:
```
CONNECTION 'Workbook' (
		Driver = 'Excel',
		File = './sales.xlsx',
		Sheet = '2017',
		Range = 'A1:B*'
)

TRANSFORM 'SumSales' FROM CONNECTION Workbook (
	AGGREGATE Month, SUM(Sales)
	GROUP BY Month
) INTO CONSOLE
```

As destination:
```
CONNECTION 'Workbook' (
		Driver = 'Excel',
		File = './output.xlsx',
)

QUERY 'DumpData' FROM GLOBAL (
		SELECT 1 AS 'Id', 'Bob' AS 'Name'
) INTO CONNECTION Workbook
  WITH (Sheet = 'TestSheet', Range = 'A1:B1', Columns = 'Identifier, First Name')
```

## HTTP Connector

The HTTP Connector is a source-only connector used to fetch data from an HTTP endpoint that returns UTF-8 encoded JSON.

Either primitive arrays `[[1,2][3,4]]` or object arrays `[{"a": 1, "b": 2}, {"a": 3, "b": 4}]` are supported.

It supports pagination and basic authentication.

**Options**

* `URL`: The URL of the endpoint complete with the scheme and hostname, eg. `https://www.my-company.com/api/v1/endpoint`
* `COLUMNS`: Output columns, case-insensitive. For object arrays, it should match the name of the key. For primitive arrays, the values are returned in order.
* `JSON_PATH`: (Optional, default: '') The JSON Path of the JSON object in the response that includes the data. It should be left unset or blank for top-level arrays.
* `PAGE_SIZE`: (Optional, default: 50) If the API supports pagination, then this is the desired size of pages that the executor should fetch
* `PAGINATION_LIMIT_PARAMETER`: (Optional, default: '') If the API supports pagination, then this is the query parameter that should be appended with the limit eg. `https://www.my-company.com/api/v1/endpoint?limit=100`
* `PAGINATION_OFFSET_PARAMETER`: (Optional, default: '') If the API supports pagination, then this is the query parameter that should be appended with the offset eg. `https://www.my-company.com/api/v1/endpoint?limit=100&offset=100`
* `HEADERS`: (Optional, default: '') A JSON object containing a map of headers, eg. `{"Authorization": "Basic asdfasdf123="}` 
* `FORMAT`: (Optional, default: `JSON_OBJECTS`) Either `JSON_OBJECTS` (object array) or `JSON_ARRAY` (primitive array).

**Example**

```
CONNECTION 'WebAPI' (
	DRIVER = 'http',
	URL = 'https://chroniclingamerica.loc.gov/awardees.json',
	JSON_PATH = 'awardees',
	COLUMNS = 'URL, Name'
)
```

## Mandrill Connector

The [Mandrill](http://mandrill.com/) Connector is a destination-only connector used to send data via a templated email. 

It can either send all the input rows in a single email or send one email per row.

**Options**

* `API_KEY`: The Mandrill API Key.
* `RECIPIENTS`: List of recipients in the format `Name <something@domain.com>, Another Name <something2@domain2.com>`.
* `TEMPLATE`: Name of the Mandrill template to use to send the email.
* `SUBJECT`: (Optional) The subject line for the email. If blank, the default template subject line will be used.
* `SENDER`: (Optional) The sender in the format `Name <something@domain.com>`. If blank, the default template sender will be used.
* `SPLIT`: (Optional, default: 'False') If 'True', send one email per input row. If 'False', send one email with all input rows as an array of JSON objects.

**Full Example**

This script generates some dummy data and sends it via email using a passthrough transform.
```
CONNECTION 'SendTestEmail' (
	DRIVER = 'MANDRILL',
	API_KEY = 'XXXXXXXXXXXXXXXXXXXXX',
	RECIPIENTS = 'Test <test@test.com>, Test2 <test2@test2.com>',
	TEMPLATE = 'analyst-test',
	SPLIT = 'True'
)

DATA 'Values' (
    [
  		["Bob Bobbertson", 123.123],
  		["Steve Stevenson", 234.234]
	  ]
)WITH (FORMAT = 'JSON_ARRAY', COLUMNS = 'Engineer,Current');
	
TRANSFORM 'PopulateEmail' FROM BLOCK Values (
	AGGREGATE Engineer, Current
	GROUP BY Engineer, Current
) INTO CONNECTION SendTestEmail
```

The source code of the Mandrill template used, `analyst-test`, is as follows:
```
<html>
	<body>
		<h1>New test run</h1>
		<p><strong>Engineer:</strong>{{engineer}}</p>
		<p><strong>Current car mileage:</strong>{{current}}</p>   
	</body>
</html>
```