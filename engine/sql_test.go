package engine

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"os"
	"io"
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

func copyFile(src, dest string) error{
	sFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sFile.Close()

	eFile, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer eFile.Close()

	_, err = io.Copy(eFile, sFile) // first var shows number of bytes
	if err != nil {
		return err
	}

	err = eFile.Sync()
	if err != nil {
		return err
	}
	return nil
}

func TestSQLite(t *testing.T) {

	Convey("Given a coordinator and a SQLite data destination", t, func() {
		err := setupInsertTest()
		So(err, ShouldBeNil)
		c := NewCoordinator(&ConsoleLogger{})
		sq := SQLDestination{
			Driver:           "sqlite3",
			ConnectionString: "./testing/test_insert.db",
			Table:            "test",
		}
		sqs := SQLSource{
			Driver:           "sqlite3",
			ConnectionString: "./testing/test_insert.db",
			Query:            "SELECT * FROM test",
		}
		msg := [][]interface{}{[]interface{}{2, "Bob", 29.4}, []interface{}{4, "Fred", 27}}
		cols := []string{"ID", "Name", "Age"}
		Convey("It should retrieve results correctly", func() {
			//TEST INSERT OCCURS WITH NO ERRORS
			s := NewSliceSource(cols, msg)
			err := c.AddSource("source", s)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", &sq)
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
			c = NewCoordinator(&ConsoleLogger{})
			d := SliceDestination{}
			err = c.AddSource("source", &sqs)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", &d)
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
