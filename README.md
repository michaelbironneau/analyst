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
* **Statefulness**: Components can persist state in an SQLite3 database unique to each job run (`GLOBAL` source/destination).

# Documentation

[All docs are hosted here](https://michaelbironneau.github.io/analyst).

