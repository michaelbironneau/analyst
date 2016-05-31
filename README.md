# Analyst

[![Go Report Card](http://goreportcard.com/badge/github.com/michaelbironneau/analyst)](https://goreportcard.com/report/github.com/michaelbironneau/analyst)
[![Build Status](https://travis-ci.org/michaelbironneau/analyst.svg?branch=master)](https://travis-ci.org/michaelbironneau/analyst/)

Analyst is a command-line tool to validate and run Analyst Query Language (AQL) scripts. AQL scripts allow you to populate .xlsx spreadsheet templates created in Excel or LibreOffice with the results of one or more SQL queries.

The results of the queries can even be combined and manipulated in-memory using a SQL-like language called [QL](https://godoc.org/github.com/cznic/ql) (so you can query different sources and join the results).

If you try and do the same thing in Excel using PowerQuery or a macro, you'll usually crash Excel. LibreOffice offers a querying feature but it is not as powerful and lacks the ability to transform query results before they populate the spreadsheet.

It would also be nice to be able to let users define parameters for their reports without having to go through the lengthy process of creating a [parameter table](https://blog.oraylis.de/2013/05/using-dynamic-parameter-values-in-power-query-queries/).

## Why not do all this in Excel?

It crashes.

It is painful to provide PowerQuery with user-defined parameters and even more painful to validate them sensibly.

It requires installing database clients on users' machines and updating credentials when they inevitably change.

Even though you can merge multiple queries, it's not as easy to operate on them as it would be using SQL.

Developers and analysts should not have to spend large amounts of time providing IT support or teaching people how to use Excel.

## Installing from binary 

Download the correct binary for your platform from the latest [release](https://github.com/michaelbironneau/analyst/releases). Put it on your PATH. 

## Installing from source

Install go. Add $GOPATH/bin to your $PATH. Run `go install github.com/michaelbironneau/analyst`.

## Usage

Here are the steps you should follow:

1. If you haven't already, create a template spreadsheet for your report. Save it as an XLSX file.
2. If you haven't already, create a connection file (see below). This tells Analyst how to connect to data sources
3. Create your AQL script (see below)
4. Run the `analyst` command (instructions below)

There are two subcommands: `run` and `validate`. The second is essentially a dry run of the first:

    analyst run

runs the script contained in the file ".analyst" in the current working directory.

	analyst run -i

runs in interactive mode, that is, prompting the user for parameters on STDIN.


	analyst run -script "path/to/script" -params "param1:val1;param2:val2"

runs the provided script, using the provided parameters.

## Example Script

Before diving into AQL, here is an example script to give you an idea of what it looks like.

This example selects some employee and salary data from two separate databases, joins them in an in-memory table, and writes the result to Excel.

	report 'Employee Salaries'

    description 'Shows the salary of each employee'

    parameter (
    	Department string
    )

    connection (
    	db1 'dbs.conn'
        db2 'dbs.conn'
    )

    template 'blank.xlsx'

    output '{{.Department}}-salaries-{{.Now.Format "2016-01"}}.xlsx'

    query 'employee' from db1 (
    	select id, name from employee
        where department like '{{.Department}}'
    ) into table emp (id int, name string)

    query 'salary' from db2 (
    	select employee_id, salary from salary
        where department like '{{.Department}}'
    ) into table sal (e_id int, value float64)

    query 'join' from tempdb(emp, sal) (
    	select emp.name, sal.value from emp, sal
        where sal.e_id == emp.id
    ) into sheet 'Salaries' range [0,0]:[n,1]

## Analyst Query Language (AQL)

An AQL script contains metadata describing the script (mandatory), a reference to some data connections, and queries.

You can find complete examples in the `testing` folder.

It looks like SQL, and like SQL is whitespace insensitive. Keywords are 
case-insensitive.

**Blocks**

An Analyst script is made of one or more blocks. Blocks mostly look like this:

	BLOCK_NAME 'BLOCK_VALUE'

or

	BLOCK_NAME (
    	KEY_1 'VALUE_1'
        KEY_2 'VALUE_2'
        ...
    )

Query blocks are a bit different in that they contain SQL and instructions on what to do with the result of the query.

### Metadata

Metadata is mandatory. It is not strictly speaking necessary to generate the report but when you come back to the script a year later, you'll thank me for it.

Some of this information (such as providing a name for each query) is used to provide error messages that are easier to read.

#### Report Name

Identifies the report by name.

	report 'Test report'

#### Report Description

Describes the report.

	description 'This describes the report'

#### Template

This is the Excel file which contains the (empty) report. It will be populated with the results of the queries. Admits templating using Go templating syntax.

	template 'path/to/template.xlsx'

#### Output file

This is the Excel file that contains the generated report. Admits templating using Go templating syntax.

	output 'path/to/output/file-{{.Parameter}}.xlsx'

### Parameters

Several blocks admit templating using Go templating syntax. To set the template parameters, you need to declare them beforehand. Each parameter has a `name` and a `type`. The type is either `string`, `number`, or `date`. 

Date parameters must be entered according to RFC3339 standard with nanoseconds, for example `2006-02-29T23:59:01.000Z`.

	parameter (
    	Param1 string
        Param2 number
    )

The parameter "Now" is always defined and is equal to the current date/time (`time.Now()`).

Parameters can be used in the following blocks:

* Template (metadata)
* Output (metadata)
* Query bodies (the SQL bit)

### Connections

A connection defines how to access external data.

Connections are stored in separate files using TOML syntax. Multiple connections can be stored in a single file, eg.

	[[Connection]]
    Name = "db1"
    Driver = "mssql"
    ConnectionString = "server=...."

    [[Connection]]
    Name = "db2"
    Driver = "postgres"
    ConnectionString = "server=..."

The connection block of the AQL script says what the name of the connection is and which file to find it in:

	connection (
    	db1 'connections.toml'
        db2 'connections.toml'
    )

### Queries

Queries contain SQL and details on where to get the data. A query can either

* Select data from an external data source and write it to a range in an Excel spreadsheet
* Select data from an external data source and write it to a temporary table (TEMPDB)
* Select data from one or more temporary table (TEMPDB) and write it to Excel

#### Select from External source into Excel
The first line says where to fetch the data; the last line says where to put it.

	query 'query1' from db1 (
    	select top 10 * from table
    ) into sheet 'Sheet1' range [0,0]:[9,0]

#### Select from External source into Temporary Table

The first line says where to fetch the data; the last line says which table to put it in and how its columns are defined.

	query 'query2' from db1 (
    	select 1
    ) into table table1 (col1 int)

#### Select from Temporary Table into Excel

The first line says where to fetch the data; the last line says which spreadsheet to put it in. Please note that the query language, which may look like SQL is **not** SQL. It is called QL and there are important syntactic differences. More documentation on this language can be found [here](https://godoc.org/github.com/cznic/ql).

	query 'query3' from tempdb(table1) (
    	select * from table1
    ) into sheet 'Sheet1' range [0,0]:[0,n]

#### Destination Excel Ranges

Analyst can write result sets to arbitrary and dynamic Excel ranges, transposing the query (rows <-> columns) as necessary.

Ranges are specified as `[x1,y1]:[x2,y2]`, where coordinates are specified as zero-based `[row,column]`. 

*At most one* of x2 and y2 can be set to 'n'. This special value means "the number of rows in the resultset". 

The result set will be transposed if necessary to satisfy the range, for example:

	query 'WillBeTransposed' from db (
    	select 1, 2, 3
    ) into sheet 'test' range [0,0]:[n,0]

will set A1 to 1, A2 to 2 and A3 to 3.

