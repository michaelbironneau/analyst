---
id: set
title: SET
---

The syntax for SET blocks is as follows:
```
SET OPTION_NAME = OPTION_VALUE
```

The Option value can be a quoted string or number. 

The SET block is used to set a global option. See [Blocks](blocks.md) for more information on option inheritance and values.

#Example

In the below example, the global option `Table` is used as an option for the query destination for `MyDb2` connection, resulting in all the rows output by the query being inserted into `MyStagingTable`.

```
SET Table = 'MyStagingTable'

QUERY 'Results' FROM CONNECTION MyDb (
   SELECT * FROM MyTable
) INTO CONNECTION MyDb2
```