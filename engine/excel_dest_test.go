package engine

import (
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

func teardownWriteTest() error {
	return os.Remove("./testing/output.xlsx")
}

func TestExcel(t *testing.T) {
	cols := []string{"a", "b", "c"}
	Convey("Given a coordinator and an Excel data destination", t, func() {
		err := setupInsertTest()
		So(err, ShouldBeNil)
		c := NewCoordinator(&ConsoleLogger{})
		d := ExcelDestination{
			Filename: "./testing/output.xlsx",
			Template: "./testing/template.xlsx",
			Sheet:    "Test",
			Range: ExcelRange{
				X1: 1,
				X2: ExcelRangePoint{
					N: false,
					P: 3,
				},
				Y1: 2,
				Y2: ExcelRangePoint{
					N: true,
				},
			},
			Cols:      cols,
			Overwrite: true,
			Alias:     "destination2",
		}
		var e = ExcelSource{
			Name:     "test",
			Filename: "./testing/output.xlsx",
			Sheet:    "Test",
			Range: ExcelRange{
				X1: 1,
				X2: ExcelRangePoint{
					N: false,
					P: 3,
				},
				Y1: 2,
				Y2: ExcelRangePoint{
					N: true,
				},
			},
			RangeIncludesColumns: false,
			Cols:                 cols,
		}
		msg := [][]interface{}{[]interface{}{2, "Bob", 29.4}, []interface{}{4, "Fred", 27}}
		Convey("It should retrieve results correctly", func() {
			//TEST INSERT OCCURS WITH NO ERRORS
			s := NewSliceSource(cols, msg)
			err := c.AddSource("source", "slice", s)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", "destination2", &d)
			So(err, ShouldBeNil)
			err = c.Connect("source", "destination")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldBeNil)
			err = c.Execute()
			So(err, ShouldBeNil)
			//fmt.Printf("%T, %T, %T", d.Results()[0][0], d.Results()[0][1], d.Results()[0][2])

			//TEST INSERTED RESULTS AND SQL DESTINATION
			c = NewCoordinator(&ConsoleLogger{})
			e.SetName("slice")
			d := SliceDestination{Alias: "destination2"}
			err = c.AddSource("source", "slice", &e)
			So(err, ShouldBeNil)
			err = c.AddDestination("destination", "destination2", &d)
			So(err, ShouldBeNil)
			err = c.Connect("source", "destination")
			So(err, ShouldBeNil)
			err = c.Compile()
			So(err, ShouldBeNil)
			err = c.Execute()
			So(err, ShouldBeNil)
			//So(s.Columns(), ShouldResemble, cols)
			So(d.Results(), ShouldResemble, msg)
			err = teardownWriteTest()
			So(err, ShouldBeNil)
		})

	})
}
