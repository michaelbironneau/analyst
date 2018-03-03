package analyst

import (
	"bytes"
	"database/sql"
	"fmt"
	xlsx "github.com/360EntSecGroup-Skylar/excelize"
	"github.com/michaelbironneau/analyst/aql"
	"github.com/michaelbironneau/analyst/engine"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

func TestGlobal(t *testing.T) {
	script := `
	GLOBAL 'InitializeInputTable' (
		CREATE TABLE test (
			ID Number,
			Name Text
		);

		INSERT INTO test (ID, Name) VALUES (1, 'Bob');
    )

	GLOBAL 'InitializeOutputTable' (
		CREATE TABLE test2 (
			ID Number,
			Name Text
		);
	)

	QUERY 'Test' FROM GLOBAL (
		SELECT * FROM test
	) INTO GLOBAL WITH (TABLE = 'test2')

	`
	db, err := sql.Open(globalDbDriver, globalDbConnString)
	defer db.Close()
	Convey("Given a script making use of GLOBAL", t, func() {
		Convey("It should be processed correctly and generate the expected results", func() {
			So(err, ShouldBeNil)
			err := ExecuteString(script, &RuntimeOptions{})
			So(err, ShouldBeNil)
			var row struct {
				ID   int
				Name string
			}
			r := db.QueryRow("SELECT * FROM test2 LIMIT 1")
			err = r.Scan(&row.ID, &row.Name)
			So(err, ShouldBeNil)
			So(row.ID, ShouldEqual, 1)
			So(row.Name, ShouldEqual, "Bob")
		})
	})
}

func TestCompilerDataLiteralAndHooks(t *testing.T) {
	script := `
		DATA 'Values' EXTERN 'test.json'
			WITH (FORMAT = 'JSON_ARRAY',
                  COLUMNS = 'Number,Letter');

		TRANSFORM 'Total' FROM BLOCK Values (
			AGGREGATE SUM(Number) AS Total
		) INTO CONSOLE WITH (OUTPUT_FORMAT = 'JSON')
	`
	Convey("Given a literal data source and a query", t, func() {
		Convey("It should run without errors", func() {
			l := engine.NewConsoleLogger(engine.Trace)
			buf := bytes.NewBufferString("")
			replaceReaderHook := engine.DestinationHook(func(s string, d engine.Destination) error {
				cd, _ := d.(*engine.ConsoleDestination)
				cd.Writer = buf
				return nil
			})
			err := ExecuteString(script, &RuntimeOptions{nil, l, []interface{}{replaceReaderHook}, nil, ""})
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "[{\"Total\":3}]")
		})
	})
}

func TestCompilerDataLiteralSourceDest(t *testing.T) {
	script := `
		DATA 'MyMessage' (
		[
	  		["Hello, World"]
		]
		) INTO CONSOLE WITH (COLUMNS = 'Message', OUTPUT_FORMAT='JSON')
	`
	Convey("Given a literal data source console dest", t, func() {
		Convey("It should run without errors", func() {
			l := engine.NewConsoleLogger(engine.Trace)
			buf := bytes.NewBufferString("")
			replaceReaderHook := engine.DestinationHook(func(s string, d engine.Destination) error {
				cd, _ := d.(*engine.ConsoleDestination)
				cd.Writer = buf
				return nil
			})
			err := ExecuteString(script, &RuntimeOptions{nil, l, []interface{}{replaceReaderHook}, nil, ""})
			So(err, ShouldBeNil)
			So(buf.String(), ShouldEqual, "[{\"Message\":\"Hello, World\"}]")
		})
	})
}

func TestCompilerHTTPAutoSQL(t *testing.T) {
	script := `
	CONNECTION 'WebAPI' (
		DRIVER = 'http',
		URL = 'https://chroniclingamerica.loc.gov/awardees.json',
		JSON_PATH = 'awardees',
		COLUMNS = 'URL, Name'
	)

	QUERY 'Aggregate' FROM CONNECTION WebAPI (
		--Select how many awardees are universities
		SELECT 'The Magic Answer Is', COUNT(*) As NumberOfUniversityAwardees FROM WebAPI
		WHERE Name LIKE '%university%'
	) INTO CONSOLE
	`
	Convey("Given a script using an HTTP connection and a QUERY", t, func() {
		Convey("It should run without errors", func() {
			l := engine.NewConsoleLogger(engine.Trace)
			err := ExecuteString(script, &RuntimeOptions{nil, l, nil, nil, ""})
			//l.Close()
			So(err, ShouldBeNil)
		})

	})
}

func TestCompiler(t *testing.T) {
	script := `
	CONNECTION 'DB' (
		Driver = 'sqlite3',
	    ConnectionString = './engine/testing/test_insert.db'
	)

	CONNECTION 'Workbook' (
		Driver = 'Excel',
		File = './output.xlsx'
	)

	QUERY 'DumpData' FROM CONNECTION DB (
		SELECT 1 AS 'Id', 'Bob' AS 'Name'
	) INTO CONNECTION Workbook
	WITH (Sheet = 'TestSheet', Range = 'A1:B1',
			Columns = 'Id, Name')

	`
	Convey("Given a coordinator and an Excel data destination", t, func() {
		l := engine.NewConsoleLogger(engine.Trace)
		err := ExecuteString(script, &RuntimeOptions{nil, l, nil, nil, ""})
		So(err, ShouldBeNil)
		_, err = os.Stat("./output.xlsx")
		os.Remove("./output.xlsx") //best effort cleanup attempt
		So(err, ShouldBeNil)

	})
}

func TestUnmanagedTransaction(t *testing.T) {
	script := `
	GLOBAL 'Initialize' (
		CREATE TABLE ContactStats3 (
			id integer PRIMARY KEY,
			first_name text NOT NULL,
			calls real
		);
	);

	QUERY 'InsertResults' FROM GLOBAL (
		SELECT 1 AS id, 'Bob' AS first_name, 8 AS calls
		UNION ALL
		SELECT 2 AS id, 'Steven' AS first_name, 0 AS calls
		UNION ALL
		SELECT 3 AS id, 'Jack' AS first_name, 1 AS calls
	) INTO GLOBAL WITH (TABLE = 'ContactStats3', MANAGED_TRANSACTION = 'False',
					ROWS_PER_BATCH=2)
	`
	Convey("Given a script that uses unmanaged transaction", t, func() {
		err := ExecuteString(script, &RuntimeOptions{})
		So(err, ShouldBeNil)
		db, err := sql.Open(globalDbDriver, globalDbConnString)
		defer db.Close()
		So(err, ShouldBeNil)
		rows, err := db.Query("Select first_name, calls FROM ContactStats3")
		So(err, ShouldBeNil)
		var res struct {
			name string
			sum  float64
		}

		defer rows.Close()
		var count int
		for rows.Next() {
			count++
			err := rows.Scan(&res.name, &res.sum)
			So(err, ShouldBeNil)
			if res.name == "Bob" {
				So(res.sum, ShouldEqual, 8.0)
			} else if res.name == "Steven" {
				So(res.sum, ShouldEqual, 0.0)
			} else if res.name == "Jack" {
				So(res.sum, ShouldEqual, 1.0)
			} else {
				So(res.name, ShouldBeIn, []string{"Bob", "Steven", "Jack"}) //fails
			}
		}
		So(count, ShouldEqual, 3)
	})

}

func TestCompilerWithExecs(t *testing.T) {
	script := `
	GLOBAL 'Initialize' (
		CREATE TABLE ContactStats2 (
			id integer PRIMARY KEY,
			first_name text NOT NULL,
			calls real
		);
	);

	EXEC 'InsertResults' FROM GLOBAL (
		INSERT INTO  ContactStats2 (id, first_name, calls) VALUES (1, 'Bob', 8);
		INSERT INTO ContactStats2 (id, first_name, calls) VALUES (2, 'Steven', 0);
	)
	`
	Convey("Given a script that uses EXEC blocks", t, func() {
		err := ExecuteString(script, &RuntimeOptions{})
		So(err, ShouldBeNil)
		db, err := sql.Open(globalDbDriver, globalDbConnString)
		defer db.Close()
		So(err, ShouldBeNil)
		rows, err := db.Query("Select first_name, calls FROM ContactStats2")
		So(err, ShouldBeNil)
		var res struct {
			name string
			sum  float64
		}

		defer rows.Close()
		var count int
		for rows.Next() {
			count++
			err := rows.Scan(&res.name, &res.sum)
			So(err, ShouldBeNil)
			if res.name == "Bob" {
				So(res.sum, ShouldEqual, 8.0)
			} else if res.name == "Steven" {
				So(res.sum, ShouldEqual, 0.0)
			} else {
				So(res.name, ShouldBeIn, []string{"Bob", "Steven"}) //fails
			}
		}
		So(count, ShouldEqual, 2)
	})

}

func TestCompilerWithLookupTransform(t *testing.T) {
	script := `
	/**
		Create global tables with the base and lookup tables,
		so that we can test the lookup transform.
	**/
	GLOBAL 'CreateTables' (
		CREATE TABLE LookupTable (
			id INT PRIMARY KEY,
			first_name TEXT
		);

		CREATE TABLE BaseTable (
			lookup_id INT PRIMARY KEY,
			last_name TEXT
		);

		CREATE TABLE JoinedTable (
			first_name TEXT,
			last_name TEXT
		);
	);

	GLOBAL 'SeedTables' (
		INSERT INTO LookupTable VALUES (1, 'Bob');
		INSERT INTO LookupTable VALUES (2, 'John');
		INSERT INTO LookupTable VALUES (3, 'Steve');

		INSERT INTO BaseTable VALUES (1, 'Bobbertson');
		INSERT INTO BaseTable VALUES (2, 'Johnson');
	);

	QUERY 'FirstNames' FROM GLOBAL (
		SELECT id, first_name FROM LookupTable
	);

	QUERY 'LastNames' FROM GLOBAL (
		SELECT lookup_id, last_name FROM BaseTable
	);

	TRANSFORM 'Join' FROM BLOCK FirstNames, BLOCK LastNames (
		LOOKUP FirstNames.first_name, LastNames.last_name FROM FirstNames
		INNER JOIN LastNames ON FirstNames.id = LastNames.lookup_id
	) INTO GLOBAL WITH(Table = 'JoinedTable')
	`
	Convey("Given a script that uses builtin transforms", t, func() {
		err := ExecuteString(script, &RuntimeOptions{})
		So(err, ShouldBeNil)
		db, err := sql.Open(globalDbDriver, globalDbConnString)
		defer db.Close()
		So(err, ShouldBeNil)
		rows, err := db.Query("Select first_name, last_name from JoinedTable order by first_name")
		So(err, ShouldBeNil)
		var res struct {
			first string
			last  string
		}

		defer rows.Close()
		var count int
		var (
			haveBob  bool
			haveJohn bool
		)
		for rows.Next() {
			count++
			err := rows.Scan(&res.first, &res.last)
			So(err, ShouldBeNil)
			if res.first == "Bob" {
				haveBob = true
				So(res.last, ShouldEqual, "Bobbertson")
			} else if res.first == "John" {
				haveJohn = true
				So(res.last, ShouldEqual, "Johnson")
			} else {
				So(res.first, ShouldBeIn, []string{"Bob", "John"}) //fails
			}
		}
		So(haveBob, ShouldBeTrue)
		So(haveJohn, ShouldBeTrue)
		So(count, ShouldEqual, 2)
	})

}

func TestCompilerWithAggregateTransform(t *testing.T) {
	script := `
	SET Table = 'Result2';

	GLOBAL 'Initialize' (
		CREATE TABLE ContactStats (
			id integer PRIMARY KEY,
			first_name text NOT NULL,
			number_of_calls real
		);

		INSERT INTO  ContactStats (id, first_name, number_of_calls) VALUES (1, 'Bob', 5);
		INSERT INTO  ContactStats (id, first_name, number_of_calls) VALUES (2, 'Steven', 0);
		INSERT INTO  ContactStats (id, first_name, number_of_calls) VALUES (3, 'Bob', 3);
	);

	GLOBAL 'Result' (
		CREATE TABLE Result2 (
			first_name text PRIMARY KEY,
			calls real
		);
	)

	QUERY 'Fetch' FROM GLOBAL (
		SELECT * FROM ContactStats
	)

	TRANSFORM 'SumByFirstName' FROM BLOCK Fetch (
		AGGREGATE first_name, SUM(number_of_calls) As calls
		GROUP BY first_name
	) INTO GLOBAL
	`
	Convey("Given a script that uses builtin transforms", t, func() {
		err := ExecuteString(script, &RuntimeOptions{})
		So(err, ShouldBeNil)
		db, err := sql.Open(globalDbDriver, globalDbConnString)
		defer db.Close()
		So(err, ShouldBeNil)
		rows, err := db.Query("Select first_name, calls FROM Result2")
		So(err, ShouldBeNil)
		var res struct {
			name string
			sum  float64
		}

		defer rows.Close()
		var count int
		for rows.Next() {
			count++
			err := rows.Scan(&res.name, &res.sum)
			So(err, ShouldBeNil)
			if res.name == "Bob" {
				So(res.sum, ShouldEqual, 8.0)
			} else if res.name == "Steven" {
				So(res.sum, ShouldEqual, 0.0)
			} else {
				So(res.name, ShouldBeIn, []string{"Bob", "Steven"}) //fails
			}
		}
		So(count, ShouldEqual, 2)
	})

}

func TestCompilerWithParameters(t *testing.T) {
	script := `
	DECLARE @Id;

	GLOBAL 'Initialize' (
		CREATE TABLE Contacts (
			id integer PRIMARY KEY,
			first_name text NOT NULL
		);

		INSERT INTO  Contacts (id, first_name) VALUES (1, 'Bob');
		INSERT INTO  Contacts (id, first_name) VALUES (2, 'Steven');
		INSERT INTO  Contacts (id, first_name) VALUES (3, 'Jack');
	);

	QUERY 'GetId' FROM GLOBAL (
		SELECT 1 AS 'Id'
	) INTO PARAMETER (@Id);

	QUERY 'GetName' FROM GLOBAL (
		SELECT 4 As Id, first_name FROM Contacts
		WHERE id = ?
	)
	USING PARAMETER @Id
	INTO GLOBAL WITH (Table = 'Contacts')
	AFTER GetId
	`
	SkipConvey("Given a script that uses parameters", t, func() {
		err := ExecuteString(script, &RuntimeOptions{})
		So(err, ShouldBeNil)
		db, err := sql.Open(globalDbDriver, globalDbConnString)
		defer db.Close()
		So(err, ShouldBeNil)
		rows, err := db.Query("Select first_name FROM Contacts ORDER BY id")
		So(err, ShouldBeNil)
		var names []string
		defer rows.Close()
		for rows.Next() {
			var name string
			err := rows.Scan(&name)
			So(err, ShouldBeNil)
			names = append(names, name)
		}
		So(names, ShouldResemble, []string{"Bob", "Steven", "Jack", "Bob"})
	})

}

func TestCompilerWithEmail(t *testing.T) {
	script := `
	CONNECTION 'SendTestEmail' (
		DRIVER = 'MANDRILL',
		API_KEY = 'XIrAnHAcpAMpOONkJYjiNg',
		RECIPIENTS = 'Test <test@test.com>, Test2 <test2@test2.com>',
		TEMPLATE = 'analyst-test',
		SPLIT = 'True'
	)

	DATA 'Values' (
    [
  		["Bob Bobbertson", 123.123],
  		["Steve Stevenson", 234.234]
	  ]
	)WITH (FORMAT = 'JSON_ARRAY', COLUMNS = 'Engineer,Current');

	TRANSFORM 'PopulateEmail' FROM BLOCK Values (
		AGGREGATE Engineer, Current
		GROUP BY Engineer, Current
	) INTO CONNECTION SendTestEmail
	`
	Convey("Given a script that uses email", t, func() {
		Convey("It should run without errors", func() {
			err := ExecuteString(script, &RuntimeOptions{})
			So(err, ShouldBeNil)
		})

	})

}

func TestCompilerWithTransform(t *testing.T) {
	script := `
	CONNECTION 'Workbook' (
		Driver = 'Excel',
		File = './output_transform.xlsx'
	)

	QUERY 'SliceOfData' FROM GLOBAL (
		SELECT 1 AS 'Value'
			UNION ALL
		SELECT -1 AS 'Value'
			UNION ALL
		SELECT 2 AS 'Value'
	)

	QUERY 'SliceOfData2' FROM GLOBAL (
		SELECT 10 AS 'Value'
			UNION ALL
		SELECT 11 AS 'Value'
			UNION ALL
		SELECT -2 AS 'Value'
	)

	TRANSFORM PLUGIN 'FilterNegatives' FROM BLOCK SliceOfData, BLOCK SliceOfData2 ()
	INTO CONNECTION Workbook
	WITH (
		Sheet = 'TestSheet', Range = 'A1:A*',
			Columns = 'Value', Multisource_Order = 'Sequential',
		Executable = 'python', Args = '["./test_filter.py"]', Overwrite = 'True'
	)
	`
	Convey("Given a script with a transform and an Excel data destination", t, func() {
		l := engine.NewConsoleLogger(engine.Trace)
		Convey("It should execute without error", func() {
			err := ExecuteString(script, &RuntimeOptions{nil, l, nil, nil, ""})
			So(err, ShouldBeNil)
			_, err = os.Stat("./output_transform.xlsx")
			So(err, ShouldBeNil)
			Convey("It should return the correct output", func() {
				x, err := xlsx.OpenFile("./output_transform.xlsx")
				So(err, ShouldBeNil)
				So(x.GetCellValue("TestSheet", "A1"), ShouldEqual, "1")
				So(x.GetCellValue("TestSheet", "A2"), ShouldEqual, "2")
				So(x.GetCellValue("TestSheet", "A3"), ShouldEqual, "10")
				So(x.GetCellValue("TestSheet", "A4"), ShouldEqual, "11")
				os.Remove("./output_transform.xlsx") //best effort cleanup attempt
			})
		})

	})
}

func TestConnectionMap(t *testing.T) {
	script := `
	CONNECTION 'DB' (
		Driver = 'sqlite3',
	    ConnectionString = './engine/testing/test_insert.db'
	)

	CONNECTION 'Workbook' (
		Driver = 'Excel',
		File = './output.xlsx'
	)

	QUERY 'DumpData' FROM CONNECTION DB (
		SELECT 1 AS 'Id', 'Bob' AS 'Name'
	) INTO CONNECTION Workbook
	WITH (Sheet = 'Test', Range = '[0,0]:[0,N]')

	`
	js, err := aql.ParseString(script)
	Convey("Given a valid script with connections", t, func() {
		So(err, ShouldBeNil)
		So(len(js.Connections), ShouldEqual, 2)
		Convey("The connection map should be correctly generated", func() {
			c, err := connectionMap(js)
			So(err, ShouldBeNil)
			So(c["workbook"].Driver, ShouldEqual, "Excel")
			So(c["db"].Driver, ShouldEqual, "sqlite3")
		})
	})
}

func TestTxManagerRollback(t *testing.T) {
	script := `
	CONNECTION 'DB' (
		Driver = 'sqlite3',
	    ConnectionString = 'tx_manager_rollback_test.db'
	)

	EXEC 'CreateTables' FROM CONNECTION DB (
		CREATE TABLE Test (
			id INT PRIMARY KEY
		);
	)

	--Insert a single value into TEST
	EXEC 'InsertOne' FROM CONNECTION DB (
		INSERT INTO Test VALUES (1);
	) AFTER CreateTables;

	EXEC 'InsertTwo' FROM CONNECTION DB (
		INSERT INTO Test VALUES (2);
		INSERT INTO Test VALUES (1); --violates primary key
	) AFTER InsertOne;

	QUERY 'Dump' FROM CONNECTION DB (
		SELECT * FROM Test
	)
	INTO CONSOLE
	AFTER InsertTwo
	`
	l := engine.NewConsoleLogger(engine.Trace)
	Convey("Given a script with EXECs one of which violates PK constraint", t, func() {
		err := ExecuteString(script, &RuntimeOptions{nil, l, nil, nil, ""})
		Convey("All writes should get rolled back", func() {
			So(err, ShouldBeNil)
			db, err := sql.Open(globalDbDriver, "tx_manager_rollback_test.db")
			So(err, ShouldBeNil)
			rows, err := db.Query("SELECT * FROM Test")
			db.Close()
			os.Remove("tx_manager_rollback_test.db")
			So(err, ShouldNotBeNil) //DDL ops are transactional in sqlite3 so CREATE TABLE should have been rolled back
			if rows == nil {
				return
			}
			defer rows.Close()
			for rows.Next() {
				var id int
				rows.Scan(&id)
				fmt.Printf("Found id %v\n", id)
			}
		})

	})
}
