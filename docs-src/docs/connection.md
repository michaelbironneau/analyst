---
id: connection
title: CONNECTION
---

The full syntax of CONNECTION blocks is as follows
```
CONNECTION 'CONNECTION_NAME' [EXTERN 'CONNECTION_FILE'] (
	DRIVER = 'CONNECTION_DRIVER_NAME' [, CONNECTION_OPTIONS]
)
```

A connection represents a data source and/or a data sink that can be queried against or that rows can be sent to.

Connection options are driver-specific. Currently supported drivers are:

* `mssql`: Microsoft SQL Server database (Source/Sink)
* `sqlite3`: SQLite 3 database (Source/Sink)
* `postgres`: Postgres database (Source/Sink)
* `excel`: Microsoft Excel 2010+ (Source/Sink)
* `mandrill`: Mandrill email API (Sink only)
* `http`: an API served over HTTP (Source only)