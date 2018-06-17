---
id: tests
title: TEST
---

# Testing

While it is certainly possible to test scripts using external coordination, Analyst comes with built-in assertions to faciliate testing of scripts. 

It is important to note that tests will only be run in test mode, that is, with the subcommand `test` rather than `run`.

## Test blocks 

The syntax for a test block is as follows:

```
TEST BLOCK_IDENTIFIER WITH ASSERTIONS (
	ASSERTIONS
)
```

Where `BLOCK_IDENTIFIER` is the identifier of the block to be tested (either a source or a transform) and the assertions are semi-colon delimited and chosen from the below:


## Assertions

### Number of rows overall
Valid assertions are:

* `IT OUTPUTS AT LEAST {N} ROWS`
* `IT OUTPUTS AT MOST {N} ROWS`
* `IT OUTPUTS EXACTLY {N} ROWS`

### Number of distinct values, for a given column

* `COLUMN {COLUMN_NAME} HAS AT LEAST {N} DISTINCT VALUES`
* `COLUMN {COLUMN_NAME} HAS AT MOST {N} DISTINCT VALUES`
* `COLUMN {COLUMN_NAME} HAS EXACTLY {N} DISTINCT VALUES`

### Uniqueness of a column

The assertion is `COLUMN {COLUMN_NAME} HAS UNIQUE VALUES`.

### No null values

The assertion is `COLUMN {COLUMN_NAME} HAS NO NULL VALUES`.

## Example

The below example contains a passing test and a failing test. Running the test with `analyst test --script path/to/file` will return an error.

```
DATA 'Values' (
	[
		["Hello, World"],
		["Hello, World"]
	]
)
INTO CONSOLE
WITH (FORMAT = 'JSON_ARRAY', COLUMNS = 'Word')

TEST Values WITH ASSERTIONS (
	IT OUTPUTS AT LEAST 2 ROWS;
	COLUMN Word HAS UNIQUE VALUES
)
```