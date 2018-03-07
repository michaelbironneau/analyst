package transforms

import (
	"github.com/alecthomas/participle"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestConvertParsing(t *testing.T) {
	parser, err := participle.Build(&Convert{}, convertLexer)
	if err != nil {
		panic(err)
	}
	Convey("Given a valid convert", t, func() {
		//1
		s1 := `
		CONVERT col1, CAST (col2 AS INT) AS alias1, CAST (col3 AS VARCHAR ), col4 AS alias2
		`
		a := Convert{}
		err = parser.ParseString(s1, &a)
		So(err, ShouldBeNil)
		So(a.Projections, ShouldHaveLength, 4)
		So(a.Projections[0].Cast, ShouldBeNil)
		So(a.Projections[0].Lookup.Column, ShouldEqual, "col1")
		So(a.Projections[0].Lookup.Alias, ShouldBeNil)
		So(a.Projections[1].Cast, ShouldNotBeNil)
		So(a.Projections[1].Cast.DestType, ShouldEqual, "INT")
		So(*a.Projections[1].Cast.Alias, ShouldEqual, "alias1")
		So(a.Projections[2].Cast.Alias, ShouldBeNil)
		So(*a.Projections[3].Lookup.Alias, ShouldEqual, "alias2")
	})
}
