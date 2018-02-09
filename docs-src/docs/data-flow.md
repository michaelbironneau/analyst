---
id: data-flow
title: Data Flow
---

This section explains the data model and how data flows during execution.

## Data Model

The basic unit of data in an AQL job is a **row**. A row is a keyed array of primitive values.

The keys are called **column names** and are case-insensitive. The values can be of the following types:

* String
* Number (MS SQL NUMERIC types are returned as strings due to a Microsoft issue)
* Null

At this time, other types are not explicitly supported, although you may find that if using the same driver for data input and output, a particular driver may have wider type support than documented above.



## Component Model

Every AQL `DATA`,  `EXEC`, `QUERY`, and `TRANSFORM` block represents one or more sources, transforms, or destinations:

* A *source* is read-only
* A *transform* reads from a source and outputs to another transform or a destination
* A *destination* reads from a source or transform and has no output

Under normal (failure-free) operation, the component lifecycle is as follows:

* Compilation: the component options are parsed and the component is instantiated
* Scheduling: the component is included in a DAG and waits for any dependencies to complete
* Running: the component begins to **pull** data from its upstream source
* End-of-life: the component closes its own output channel, if it has one

Note that data is pulled from upstream components by downstream components, rather than pushed.

If any component encounters an error condition, the whole flow is immediately stopped and transactions in progress are rolled back.


## Transaction Management

A transaction manager oversees all SQL destination components. In the majority of cases, it ensures atomicity accross SQL destinations, so either no statement is committed to *any* destination (including `EXEC`s) or all statements are committed.

If a connection to a database drops between the time when a statement is executed and when the transaction is committed, but after any other statement has already been committed, then the transaction manager will proceed with the commit on other destinations, and so it is possible that the transaction may be committed in all databases except the one with the dropped connection. It is a very small window (normally <1s) in which atomicity is not guaranteed, and note that this is consistent with typical 2PC behavior (see eg [Wikipedia article](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)).
	
Should that case present itself in practice, the uncommitted transaction may block other queries from proceeding and a system administrator will need to manually commit it once network connectivity has been restored. For this reason it is recommended to create alerts based on errors that may appear in AQL logs.

### Disabling Automatic Transaction Management
*Warning: Please make sure you know what you're doing!*

To disable the transaction manager for a single destination, set the option `MANAGED_TRANSACTION` to 'False'.

To disable it for all destinations, set it using a CLI flag or `SET MANAGED_TRANSACTION = 'False'`.
