package transforms

import (
	"github.com/alecthomas/participle"
	"testing"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAggregateParsing(t *testing.T) {
	parser, err := participle.Build(&Aggregate{}, aggregateLexer)
	if err != nil {
		panic(err)
	}
	Convey("Given a valid aggregate", t, func() {
		//1
		s1 := `
		AGGREGATE Func(A) AS Val, Func2(A, 'a') AS Val2, "Col2"
		GROUP BY cde`
		a := Aggregate{}
		err = parser.ParseString(s1, &a)
		So(err, ShouldBeNil)
		So(a.Select[0].Function.Function, ShouldEqual, "Func")
		So(a.Select[0].Function.Columns[0].Column, ShouldEqual, "A")
		So(*a.Select[1].Function.Columns[1].String, ShouldEqual, "a")
		So(a.Select[0].Alias, ShouldEqual, "Val")
		So(a.GroupBy[0], ShouldEqual, "cde")

	})
}
