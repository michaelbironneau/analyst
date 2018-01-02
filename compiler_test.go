package main

import (
	"database/sql"
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
		l := &engine.ConsoleLogger{}
		err := ExecuteString(script, &RuntimeOptions{nil, l, nil})
		So(err, ShouldBeNil)
		_, err = os.Stat("./output.xlsx")
		os.Remove("./output.xlsx") //best effort cleanup attempt
		So(err, ShouldBeNil)

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

func TestCompilerWithBuiltinTransform(t *testing.T) {
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
	Convey("Given a script that uses parameters", t, func() {
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
		l := &engine.ConsoleLogger{}
		Convey("It should execute without error", func() {
			err := ExecuteString(script, &RuntimeOptions{nil, l, nil})
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
