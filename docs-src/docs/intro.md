---
id: intro
title: 30-second Introduction
---

Analyst Query Language (AQL) is a language to extract, transform and load data. Unlike other ETL languages, it is designed to have better interoperability with existing tooling, integrate with source control, and allow a modular workflow thanks to powerful templating features.

A "Hello world" in AQL looks like this:

```
DATA 'MyMessage' (
	[
	  ["Hello, World"]
	]
) INTO CONSOLE WITH (COLUMNS = "Message")

```

This code structure is called a **block**. All blocks have a type (`DATA`) and a name (`MyMessage`). They can also have options (the `WITH` clause).

A script contains one or more blocks. Data can be passed between blocks. A more useful script is as follows:

```
CONNECTION 'MyDb' (
	DRIVER = 'mssql'
	CONNECTIONSTRING = 'server=myserver;user id=sa;password=something'
)

QUERY 'GetBob' FROM CONNECTION MyDb (
	SELECT * FROM Users WHERE Name LIKE 'Bob'
) INTO CONSOLE
```

This particular script runs a query in an MS SQL database and displays the result in the console. Data can also be transformed:

```
CONNECTION 'MyDb' (
	DRIVER = 'mssql'
	CONNECTIONSTRING = 'server=myserver;user id=sa;password=something'
)

QUERY 'GetAllUserNames' FROM CONNECTION MyDb (
	SELECT Name FROM Users
)

TRANSFORM 'CountNames' FROM BLOCK GetAllUserNames (
	AGGREGATE Name, COUNT(Name) AS Cnt FROM GetAllUserNames
	GROUP BY Name
) INTO CONSOLE
```

While this transformation could have been performed in the query in this case, saving the need for an additional block, it would also work had the data come from a non-SQL source such as Excel or a text file.




