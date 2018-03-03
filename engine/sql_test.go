package engine

import (
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

func setupInsertTest() error {
	return copyFile("./testing/test_insert.db", "./testing/test_insert.db.bak")
}

func teardownInsertTest() error {
	err := os.Remove("./testing/test_insert.db")
	if err != nil {
		return err
	}
	err = copyFile("./testing/test_insert.db.bak", "./testing/test_insert.db")
	if err != nil {
		return err
	}
	return os.Remove("./testing/test_insert.db.bak")
}

func TestSQLite(t *testing.T) {

	Convey("Given a coordinator and a SQLite data destination", t, func() {
		err := setupInsertTest()
		So(err, ShouldBeNil)
		l := NewConsoleLogger(Trace)
		tm := NewTransactionManager(l)
		c := NewCoordinator(l, tm)
		sq := SQLDestination{
			Name: "sq-destination",
			Driver:           "sqlite3",
			ConnectionString: "./testing/test_insert.db",
			Table:            "test",
			Alias:            "sql-dest",
		}
		sqs := SQLSource{
			Driver:           "sqlite3",
			ConnectionString: "./testing/test_insert.db",
			Query:            "SELECT * FROM test",
		}
		sqs.SetName("sql-source")
		msg := [][]interface{}{[]interface{}{2, "Bob", 29.4}, []interface{}{4, "Fred", 27}}
		cols := []string{"ID", "Name", "Age"}
		Convey("It should retrieve results correctly", func() {
			//TEST INSERT OCCURS WITH NO ERRORS
			s := NewSliceSource(cols, msg)
			err := c.AddSource("source", "slice", s)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", "sql-dest", &sq)
			So(err, ShouldBeNil)
			err = c.Connect("source", "destination")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldBeNil)
			err = c.Execute()
			So(err, ShouldBeNil)
			//fmt.Printf("%T, %T, %T", d.Results()[0][0], d.Results()[0][1], d.Results()[0][2])
			So(sq.Columns(), ShouldResemble, cols)

			//TEST INSERTED RESULTS AND SQL DESTINATION
			l := NewConsoleLogger(Trace)
			tx := NewTransactionManager(l)
			c := NewCoordinator(l, tx)
			d := SliceDestination{Alias: "console"}
			err = c.AddSource("source", "sql-source", &sqs)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", "console", &d)
			So(err, ShouldBeNil)
			err = c.Connect("source", "destination")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldBeNil)
			err = c.Execute()
			So(err, ShouldBeNil)
			So(sq.Columns(), ShouldResemble, cols)
			So(d.Results(), ShouldResemble, msg)
			err = teardownInsertTest()
			So(err, ShouldBeNil)
		})

	})
}
