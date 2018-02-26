# Analyst

[![Go Report Card](http://goreportcard.com/badge/github.com/michaelbironneau/analyst)](https://goreportcard.com/report/github.com/michaelbironneau/analyst)
[![Build Status](https://travis-ci.org/michaelbironneau/analyst.svg?branch=master)](https://travis-ci.org/michaelbironneau/analyst/)
[![](https://godoc.org/github.com/michaelbironneau/analyst?status.svg)](http://godoc.org/github.com/michaelbironneau/analyst)

# Purpose

Analyst is a tool to validate and run Analyst Query Language (AQL) scripts. AQL is an ETL configuration language for developers that aims to be:
* **Declarative**: the developer defines the components, how they depend on one another, and any additional synchronization (i.e. `AFTER`); the runtime figures out the DAG and executes it
* **Intuitive**: similar syntax to SQL, but any options for external programs such as MS Excel use native conventions such as Excel Ranges
* **Maintainable**: support large jobs and code reuse through language features like `INCLUDE` and `EXTERN`
* **Extensible**: use stdin/stdout protocol and pipes to write ETL logic in any language. Native support for Python and Javascript.
* **Stateful**: Components can persist state in an SQLite3 database unique to each job run (`GLOBAL` source/destination).

It has connectors to:

* MS SQL Server (source/destination)
* Postgres (source/destination)
* SQLite3 (source/destination)
* Mandrill transactional email API (destination)
* Web APIs (source)
* Slack (for logging only)
* Flat file (source)
* Console (destination)
* Built-in in-memory SQLite3 database (source/destination)
* JSON-RPC plugins (source/destination)

# Getting Started

1. Grab the latest binary from the releases tab and place it on your PATH.
2. Create and save an AQL script.
3. Run `analyst run --script <path-to-your-script>`.

For a "hello world" example, try

```
DATA 'MyMessage' (
	[
	  ["Hello, World"]
	]
) INTO CONSOLE WITH (COLUMNS = 'Message')

```

# Documentation

**Docs are on Github pages [here](https://michaelbironneau.github.io/analyst)**.

## Table of Contents

1. Get Started
    - [30-second Introduction](https://michaelbironneau.github.io/analyst/docs/intro.html)
    - [Command-Line Interface](https://michaelbironneau.github.io/analyst/docs/cli.html)
    - [Data Flow](https://michaelbironneau.github.io/analyst/docs/data-flow.html)
    - [Block Syntax](https://michaelbironneau.github.io/analyst/docs/blocks.html)
    - [Connectors](https://michaelbironneau.github.io/analyst/docs/connections.html)
    - [Transforms](https://michaelbironneau.github.io/analyst/docs/transforms.html)
2. Recipes
    - [Data-Driven Email](https://michaelbironneau.github.io/analyst/docs/email.html)
    - [Getting Data From Web APIs](https://michaelbironneau.github.io/analyst/docs/http.html)
	- [Using Python for Execution Logic](https://michaelbironneau.github.io/analyst/docs/logic.html)
3. Blocks
    - [INCLUDE](https://michaelbironneau.github.io/analyst/docs/include.html)
    - [CONNECTION](https://michaelbironneau.github.io/analyst/docs/connection.html)
    - [QUERY](https://michaelbironneau.github.io/analyst/docs/query.html)
    - [EXEC](https://michaelbironneau.github.io/analyst/docs/exec.html)
    - [TRANSFORM](https://michaelbironneau.github.io/analyst/docs/transform.html)
    - [DATA](https://michaelbironneau.github.io/analyst/docs/data.html)
    - [DECLARE](https://michaelbironneau.github.io/analyst/docs/declare.html)
    - [GLOBAL](https://michaelbironneau.github.io/analyst/docs/global.html)
    - [SET](https://michaelbironneau.github.io/analyst/docs/set.html)

# Contributing

All contributions are welcome:

* To report any bugs or feature suggestion, please open an issue
* If you wish to fix a minor bug or issue, please open a PR directly
* For enhancements, refactoring, or major issues, please open an issue before opening a PR

# License

All source code and artifacts are released under GNU Affero General Public License v3.0, as detailed in LICENSE.md.

If this not suitable for your use case please get in touch by opening an issue or Twitter @MikeBrno.