---
id: cli
title: Command Line Interface
---

The `analyst` command can be used to validate or execute a script. There are two commands with identical parameters:

* `analyst validate`: Attempts to parse the script and assemble the DAG, returning any errors
* `analyst run`: Executes the script

The parameters are as follows:

* `script`: The script to evaluate/validate (default: `.analyst`)
* `params`: Global options for the script as a JSON object, eg. `{"OptName": "OptValue"}`.
* `v`: Verbose (INFO-level events)
* `vv`: Extra verbose (TRACE-level events)

## Full example

```
analyst validate --script 'myscript.aql' --params "{\"MyOpt\": 1}" --v
```