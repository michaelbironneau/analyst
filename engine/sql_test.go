package engine

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestSQLite(t *testing.T) {
	Convey("Given a coordinator and a SQLite data source", t, func() {
		c := NewCoordinator(&ConsoleLogger{})
		sq := SQLSource{
			Driver:           "sqlite3",
			ConnectionString: "./testing/test1.db",
			Query:            "SELECT * FROM test",
		}
		msg := [][]interface{}{[]interface{}{2, "Bob", 29.4}, []interface{}{4, "Fred", 27}}
		cols := []string{"ID", "Name", "Age"}
		Convey("It should execute a passthrough example correctly", func() {
			d := SliceDestination{}
			err := c.AddSource("source", &sq)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", &d)
			So(err, ShouldBeNil)
			err = c.Connect("source", "destination")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldBeNil)
			err = c.Execute()
			So(err, ShouldBeNil)
			//fmt.Printf("%T, %T, %T", d.Results()[0][0], d.Results()[0][1], d.Results()[0][2])
			So(sq.Columns(), ShouldResemble, cols)
			So(d.Results(), ShouldResemble, msg)
		})

	})
}
