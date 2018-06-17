---
id: cli
title: Command Line Interface
---

The `analyst` command can be used to validate or execute a script. There are two commands with identical parameters:

* `analyst validate`: Attempts to parse the script and assemble the DAG, returning any errors
* `analyst run`: Executes the script
* `analyst test`: Runs the script, validating the assertions in `TEST` blocks, returning any failures as errors. *All destinations will be mocked*.

The parameters are as follows:

* `script`: The script to evaluate/validate (default: `.analyst`)
* `params`: Global options for the script as a JSON object, eg. `{"OptName": "OptValue"}`.
* `v`: Verbose (INFO-level events)
* `vv`: Extra verbose (TRACE-level events)

## Full example

```
analyst validate --script 'myscript.aql' --params "{\"MyOpt\": 1}" --v
```

## Logging

There are four log levels: `TRACE`, `INFO`, `WARNING` and `ERROR`. Any error condition causes the execution to halt and any managed transactions to be rolled back.

### Logging to Slack

You can configure the logger to send log messages above a given level to Slack using an [incoming webhook](https://api.slack.com/incoming-webhooks).

Messages will be formatted as `<NAME>: SOURCE - LEVEL - MESSAGE`. 

The options, that you can set using either `SET <option_name> = '<option_value>'` syntax or via command-line flag `params`, are:

* `SLACK_WEBHOOK_URL`: The URL of your webhook.
* `SLACK_LOG_LEVEL`: Minimum level of messages to log. One of 'TRACE', 'INFO', 'WARNING' or 'ERROR' (case-insensitive).
* `SLACK_CHANNEL` (optional): Name of Slack channel for the messages.
* `SLACK_USER` (optional): Name of Slack user for the messages.
* `SLACK_EMOJI` (optional): Emoji for message.
* `SLACK_NAME` (optional): Prefix of all messages, so that the script that caused the error can be identified ('<NAME>' above)