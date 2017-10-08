package aql

import (
	"github.com/alecthomas/participle"
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
)

func TestQuery(t *testing.T) {
	parser, err := participle.Build(&Query{}, &definition{})
	if err != nil {
		panic(err)
	}
	Convey("It should parse query blocks successfully", t, func() {
		//1
		s1 := `QUERY 'name' FROM source (
			query_source()
		) INTO destination
		`
		b := &Query{}
		err = parser.ParseString(s1, b)
		So(err, ShouldBeNil)
		So(b.Name, ShouldEqual, "name")
		So(strings.TrimSpace(b.Content), ShouldEqual, "query_source()")
		So(b.Source, ShouldEqual, "source")
		So(b.Destination, ShouldEqual, "destination")

		//2
		s1 = `QUERY 'name' EXTERN 'sourcee'
		FROM source (
			thing''
		) INTO    destination
		`
		b = &Query{}
		err = parser.ParseString(s1, b)
		So(err, ShouldBeNil)
		So(b.Name, ShouldEqual, "name")
		So(b.Extern, ShouldEqual, "sourcee")
		So(strings.TrimSpace(b.Content), ShouldEqual, "thing''")
		So(b.Source, ShouldEqual, "source")
		So(b.Destination, ShouldEqual, "destination")

		//3
		s1 = `QUERY 'name' EXTERN 'sourcee'
		FROM source (
			thing''
		) INTO    destination
		WITH (opt1 = 'val', opt2 = 1234)
		`
		b = &Query{}
		err = parser.ParseString(s1, b)
		So(err, ShouldBeNil)
		So(b.Name, ShouldEqual, "name")
		So(b.Extern, ShouldEqual, "sourcee")
		So(strings.TrimSpace(b.Content), ShouldEqual, "thing''")
		So(b.Source, ShouldEqual, "source")
		So(b.Destination, ShouldEqual, "destination")
		So(b.Options, ShouldHaveLength, 2)
		So(b.Options[0].Key, ShouldEqual, "opt1")
		f := 1234.0
		So(b.Options[1].Value.Number, ShouldResemble, &f)

	})
}
