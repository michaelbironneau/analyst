---
id: declare
title: DECLARE
---

The syntax of a DECLARE block is as follows:
```
DECLARE [PARAMETER_NAME]
```

The parameter identifier can include alphanumeric characters and special characters like `@`, but not parentheses, operators or quotation marks.

Declare blocks are used to declare parameters. These parameters can be populated using query destinations. They can be used by queries or execs using SQL driver-specific parameter templating (eg. `?`).

**All parameters must be declared before they are used**

## Example

```
DECLARE @Name;

QUERY 'GetLastUser' FROM CONNECTION MyDb (
	SELECT TOP 1 Name FROM Users ORDER BY CreatedAt DESC
) INTO PARAMETER (@Name);

EXEC 'DeleteUser' FROM CONNECTION OtherDb (
	DELETE FROM InactiveUsers WHERE Id = ?
) USING PARAMETER @Name
  AFTER GetLastUser
```