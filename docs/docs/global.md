---
id: global
title: GLOBAL
---

The syntax for GLOBAL blocks is as follows:
```
GLOBAL 'BLOCK_NAME' [EXTERN 'BLOCK_FILE'] (
	GLOBAL_CONTENT
) 
```

Global blocks run a SQL statement to initialize the GLOBAL database. As such, this must be a valid SQLite3 statement.

Global blocks run sequentially before any other blocks, in the order that they are declared. They cannot be synchronized using `AFTER`.

## Example

Create a staging table, populate it, and query it. Note that it does not matter where the QUERY block is declared (and it does not need synchronization with the GLOBAL blocks) but the two GLOBAL blocks must be declared in that order:
```
GLOBAL 'CreateTable' (
	CREATE TABLE Staging (
		Id int,
		Name text
	)
)

GLOBAL 'PopulateTable' (
	INSERT INTO Staging VALUES (1, 'Bob');
	INSERT INTO Staging VALUES (2, 'Steve');
)

QUERY 'GetStagingRows' FROM GLOBAL (
	SELECT * FROM Staging
) INTO CONSOLE
```