---
id: include
title: INCLUDE
---

`INCLUDE` imports the contents of another SQL script. This can comprise zero or more blocks.

The general syntax of `INCLUDE` is:

```
INCLUDE 'INCLUDE_FILE'
```

The INCLUDE_FILE path will be resolved relative to the main script directory, that is, from the CLI, the script passed in the `script` parameter.

**Recursion**

An included file may, in turn, include other files. The maximum nesting depth is 8 and circular dependencies are not allowed. 

If you have a use case that requires a higher nesting depth, please [open an issue](https://github.com/michaelbironneau/analyst/issues/new) describing your use case. 

## Example

```
INCLUDE '../connection.aql'
```

