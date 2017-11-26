package engine

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestSourceBasic(t *testing.T) {
	var e = ExcelSource{
		Name:     "test",
		Filename: "./testing/1.xlsx",
		Sheet:    "Sheet1",
		Range: ExcelRange{
			X1: 1,
			X2: ExcelRangePoint{
				N: false,
				P: 3,
			},
			Y1: 1,
			Y2: ExcelRangePoint{
				N: false,
				P: 3,
			},
		},
		RangeIncludesColumns: true,
	}
	Convey("Given a simple Excel spreadsheet", t, func() {
		Convey("It should be able to recover the messages", func() {

			d := SliceDestination{}
			l := &ConsoleLogger{}
			st := &stopper{}

			sourceStream := NewStream(e.Columns(), DefaultBufferSize)

			e.Open(sourceStream, l, st)
			d.Open(sourceStream, l, st)

			expected := [][]interface{}{[]interface{}{1, 2, 3}, []interface{}{"a", "b", "c"}}
			So(d.Results(), ShouldResemble, expected)
		})
	})
}
