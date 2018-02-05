# Analyst

[![Go Report Card](http://goreportcard.com/badge/github.com/michaelbironneau/analyst)](https://goreportcard.com/report/github.com/michaelbironneau/analyst)
[![Build Status](https://travis-ci.org/michaelbironneau/analyst.svg?branch=master)](https://travis-ci.org/michaelbironneau/analyst/)
[![](https://godoc.org/github.com/michaelbironneau/analyst?status.svg)](http://godoc.org/github.com/michaelbironneau/analyst)

**I am currently augmenting much of the language to support new, much wider use cases. Please forgive the spartan documentation below; more will follow when the implementation of the new language features is complete.**

The below is as much a statement of the project objectives as an attempt at minimal documentation.

# Purpose

Analyst is a tool to validate and run Analyst Query Language (AQL) scripts. AQL is an ETL configuration language for developers that aims to be:
* **Declarative**: the developer defines the components, how they depend on one another, and any additional synchronization (i.e. `AFTER`); the runtime figures out the DAG and executes it
* **Intuitive**: similar syntax to SQL, but any options for external programs such as MS Excel use native conventions such as Excel Ranges
* **Maintainable**: support large jobs and code reuse through language features like `INCLUDE` and `EXTERN`
* **Testable**: ability to run unit tests on any component in a manner native to the language (`TEST` blocks). Run tests while the job is running and stop/rollback any actions if a test fails.
* **Extensible**: use stdin/stdout protocol and pipes to write ETL logic in any language. Native support for Python and Javascript.
* **Statefulness**: Components can persist state in an SQLite3 database unique to each job run (`GLOBAL` source/destination).

# Basic Example

Suppose that you want to create a job that computes sales bonuses. The information you need is in a database `Sales` and you want to output a list of salespeople with their associated bonuses in an Excel spreadsheet. There is a template you would like to use, `template.xlsx`. All the below files are assumed to be in the same directory.

*connections.conn*

```

CONNECTION 'SalesDb' (
	DRIVER = 'mssql',
    CONNECTIONSTRING = 'Server=Localhost;Initial_Catalog=Sales;SSPI...[etc];'
)

CONNECTION 'Bonuses' (
	DRIVER = 'Excel',
    CONNECTIONSTRING = 'bonuses.xlsx',
    SHEET = 'Summary'
)
```


*get_bonuses.aql*

```
INCLUDE 'connections.conn'

QUERY 'BonusQuery' FROM CONNECTION SalesDb (
	SELECT Emp.Name, Emp.BonusCoeff*ISNULL(SUM(S.Value),0) FROM Employee Emp
    LEFT JOIN Sales S ON S.EmployeeId = Emp.EmployeeId
) INTO CONNECTION Bonuses
WITH (Range = 'A2:B*')
```

# Full Example

*This example cannot be run on the current version as some of the features it uses are not yet fully implemented*.

This example is the same as above, except that the payroll information with the bonus coefficients are in different database, so the ETL job must make use of a lookup.

Moreover, we want to ensure that the total amount of bonuses can never exceed $1000 and that no salesperson receives no bonus at all. This will be guaranteed through monitored test conditions that will check the data before it reaches the destination.

*connections.conn*

```

CONNECTION 'SalesDb' (
	DRIVER = 'mssql',
    CONNECTIONSTRING = 'Server=Sales-01.local;Initial_Catalog=Sales;SSPI...[etc];'
)

CONNECTION 'PayrollDb' (
	DRIVER = 'mssql',
    CONNECTIONSTRING = 'Server=Payroll-01.local;Initial_Catalog=Payroll;SSPI...[etc];'
)

CONNECTION 'Bonuses' (
	DRIVER = 'Excel',
    CONNECTIONSTRING = 'bonuses.xlsx',
    SHEET = 'Summary'
)
```

*tests.aql*

```
TEST QUERY 'BonusTotalLessThan1000' FROM GLOBAL (
	SELECT * FROM (
		SELECT SUM(BONUS) AS Value FROM BonusesStaging
    ) Total WHERE Total.Value > 1000
) WITH (WHEN = 'InputEnds')

TEST SCRIPT 'AllEmployeesReceiveBonus' FROM BonusQuery (
	from analyst import Script
    
    def check_bonus(input):
    	if input.get('Value') <= 0:
        	return input
    
    if __name__ == '__main__':
	    Script.test_main(check_bonus)
    
) WITH (SCRIPT_TYPE = 'python')

```

*get_bonuses.aql*

```
INCLUDE 'connections.conn'

INCLUDE 'tests.aql'

GLOBAL (
	CREATE TABLE BonusesStaging (
    	EmployeeName TEXT,
        TotalSales REAL,
        Bonus REAL
    )
)

QUERY 'EmployeeSales' FROM CONNECTION SalesDb (
	SELECT Emp.EmployeeId, Emp.Name, ISNULL(SUM(S.Value),10) As TotalSales FROM Employee Emp
    LEFT JOIN Sales S ON S.EmployeeId = Emp.EmployeeId
)

QUERY 'BonusCoeffs' FROM CONNECTION PayrollDb (
	SELECT EmployeeId, BonusCoeff FROM Employee
)

TRANSFORM 'CoeffLookup' FROM BLOCK EmployeeSales, BLOCK BonusCoeffs (
	SELECT EmployeeSales.Name AS 'EmployeeName', EmployeeSales.TotalSales, BonusCoeffs.BonusCoeff AS 'Bonus'
	LEFT JOIN ON EmployeeId
) INTO GLOBAL (TABLE = 'BonusesStaging')

QUERY 'BonusQuery' FROM GLOBAL (
	SELECT Name, BonusCoeff*TotalSales FROM BonusesStaging
) INTO CONNECTION Bonuses
WITH (Range = 'A2:B*')
```
