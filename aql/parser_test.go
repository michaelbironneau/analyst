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
		s1 := `QUERY 'name' FROM CONNECTION source (
			query_source()
		) INTO CONNECTION destination
		`
		b := &Query{}
		err = parser.ParseString(s1, b)
		So(err, ShouldBeNil)
		So(b.Name, ShouldEqual, "name")
		So(strings.TrimSpace(b.Content), ShouldEqual, "query_source()")
		So(b.Sources, ShouldHaveLength, 1)
		s := "source"
		So(b.Sources[0].Database, ShouldResemble, &s)
		So(*b.Destination.Database, ShouldEqual, "destination")

		//2
		s1 = `QUERY 'name' EXTERN 'sourcee'
		FROM GLOBAL, SCRIPT 'asdf.py' (
			thing''
		) INTO GLOBAL
		`
		b = &Query{}
		err = parser.ParseString(s1, b)
		So(err, ShouldBeNil)
		So(b.Name, ShouldEqual, "name")
		ss := "sourcee"
		So(b.Extern, ShouldResemble, &ss)
		So(strings.TrimSpace(b.Content), ShouldEqual, "thing''")
		So(b.Sources, ShouldHaveLength, 2)
		So(b.Sources[0].Global, ShouldBeTrue)
		ss = "asdf.py"
		So(b.Sources[1].Script, ShouldResemble, &ss)
		So(b.Destination.Global, ShouldBeTrue)

		//3
		s1 = `QUERY 'name' EXTERN 'sourcee'
		FROM GLOBAL (
			thing''
		) INTO CONNECTION destination
		WITH (opt1 = 'val', opt2 = 1234)
		`
		b = &Query{}
		err = parser.ParseString(s1, b)
		So(err, ShouldBeNil)
		So(b.Name, ShouldEqual, "name")
		ss = "sourcee"
		So(b.Extern, ShouldResemble, &ss)
		So(strings.TrimSpace(b.Content), ShouldEqual, "thing''")
		So(b.Sources, ShouldHaveLength, 1)
		So(b.Sources[0].Global, ShouldBeTrue)
		So(*b.Destination.Database, ShouldEqual, "destination")
		So(b.Options, ShouldHaveLength, 2)
		So(b.Options[0].Key, ShouldEqual, "opt1")
		f := 1234.0
		So(b.Options[1].Value.Number, ShouldResemble, &f)

	})
}

func TestInclude(t *testing.T) {
	parser, err := participle.Build(&Include{}, &definition{})
	if err != nil {
		panic(err)
	}
	Convey("It should parse Include blocks successfully", t, func() {
		s1 := `INCLUDE 'name' FROM 'source.txt'`
		b := &Include{}
		err = parser.ParseString(s1, b)
		So(b.Source, ShouldEqual, "source.txt")
		So(b.Name, ShouldEqual, "name")

	})
}

func TestScript(t *testing.T) {
	parser, err := participle.Build(&Script{}, &definition{})
	if err != nil {
		panic(err)
	}
	Convey("It should parse script blocks successfully", t, func() {
		//1
		s1 := `SCRIPT 'name' FROM CONNECTION source (
			query_source()
		) INTO CONNECTION destination
		`
		b := &Script{}
		err = parser.ParseString(s1, b)
		So(err, ShouldBeNil)
		So(b.Name, ShouldEqual, "name")
		So(strings.TrimSpace(b.Content), ShouldEqual, "query_source()")
		So(b.Sources, ShouldHaveLength, 1)
		s := "source"
		So(b.Sources[0].Database, ShouldResemble, &s)
		So(*b.Destination.Database, ShouldEqual, "destination")
	})
}

func TestTest(t *testing.T) {
	parser, err := participle.Build(&Test{}, &definition{})
	if err != nil {
		panic(err)
	}
	Convey("It should parse test blocks successfully", t, func() {
		//1
		s1 := `TEST SCRIPT 'name' FROM CONNECTION source (
			query_source()
		)
		`
		b := &Test{}
		err = parser.ParseString(s1, b)
		So(err, ShouldBeNil)
		So(b.Name, ShouldEqual, "name")
		So(strings.TrimSpace(b.Content), ShouldEqual, "query_source()")
		So(b.Sources, ShouldHaveLength, 1)
		s := "source"
		So(b.Sources[0].Database, ShouldResemble, &s)
		So(b.Script, ShouldBeTrue)
		So(b.Query, ShouldBeFalse)
	})
}

func TestGlobal(t *testing.T) {
	parser, err := participle.Build(&Global{}, &definition{})
	if err != nil {
		panic(err)
	}
	Convey("It should parse global blocks successfully", t, func() {
		//1
		s1 := `GLOBAL 'name' (
			query_source()
		)
		`
		b := &Global{}
		err = parser.ParseString(s1, b)
		So(err, ShouldBeNil)
		So(b.Name, ShouldEqual, "name")
		So(strings.TrimSpace(b.Content), ShouldEqual, "query_source()")
	})
}

func TestDescription(t *testing.T) {
	parser, err := participle.Build(&Description{}, &definition{})
	if err != nil {
		panic(err)
	}
	Convey("It should parse description blocks successfully", t, func() {
		//1
		s1 := `DESCRIPTION 'This is a
		description'
		`
		b := &Description{}
		err = parser.ParseString(s1, b)
		So(err, ShouldBeNil)
		So(b.Content, ShouldEqual, `This is a
		description`)
	})
}

