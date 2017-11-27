package main

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"github.com/michaelbironneau/analyst/engine"
	"os"
	"github.com/michaelbironneau/analyst/aql"
	"database/sql"
)

func cleanup(){

}


func TestGlobal(t *testing.T){
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
	Convey("Given a script making use of GLOBAL", t, func(){
		Convey("It should be processed correctly and generate the expected results", func(){
			So(err, ShouldBeNil)
			err := ExecuteString(script, nil, &engine.ConsoleLogger{})
			So(err, ShouldBeNil)
			var row struct {
				ID int
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

func TestConnectionMap(t *testing.T){
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
	Convey("Given a valid script with connections", t, func(){
		So(err, ShouldBeNil)
		So(len(js.Connections), ShouldEqual, 2)
		Convey("The connection map should be correctly generated", func(){
			c, err := connectionMap(js)
			So(err, ShouldBeNil)
			So(c["workbook"].Driver, ShouldEqual, "Excel")
			So(c["db"].Driver, ShouldEqual, "sqlite3")
		})
	})
}
