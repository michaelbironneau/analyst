package main

import (
	"database/sql"
	"github.com/michaelbironneau/analyst/aql"
	"github.com/michaelbironneau/analyst/engine"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
	xlsx "github.com/360EntSecGroup-Skylar/excelize"
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
			err := ExecuteString(script, nil, &engine.ConsoleLogger{})
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
		ConnectionString = 'hello, world',
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
		err := ExecuteString(script, nil, l)
		So(err, ShouldBeNil)
		_, err = os.Stat("./output.xlsx")
		os.Remove("./output.xlsx") //best effort cleanup attempt
		So(err, ShouldBeNil)

	})
}

func TestCompilerWithTransform(t *testing.T) {
	script := `
	CONNECTION 'Workbook' (
		Driver = 'Excel',
		ConnectionString = 'hello, world',
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
		Convey("It should execute without error", func(){
			err := ExecuteString(script, nil, l)
			So(err, ShouldBeNil)
			_, err = os.Stat("./output_transform.xlsx")
			So(err, ShouldBeNil)
			Convey("It should return the correct output", func(){
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
		ConnectionString = 'hello, world',
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
