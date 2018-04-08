package engine

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestExpressionCondition(t *testing.T) {
	Convey("Given a slice of messages", t, func() {
		msg := [][]interface{}{[]interface{}{"as", "bs", "cs"}, []interface{}{1, 2, 3}}
		converter := mapConverter([]string{"ColA", "ColB", "colc"})
		Convey("The expression condition should be correctly evaluated", func() {
			c, err := NewSQLCondition("ColA == \"as\"")
			So(err, ShouldBeNil)
			So(c(converter(msg[0]), false), ShouldBeTrue)
			c, err = NewSQLCondition("ColB == 2")
			So(err, ShouldBeNil)
			So(c(converter(msg[0]), false), ShouldBeFalse)
			So(c(converter(msg[1]), false), ShouldBeTrue)
			So(c(nil, true), ShouldBeTrue)
		})
	})
}

func TestRowCountCondition(t *testing.T){
	Convey("Given a slice of messages", t, func(){
		msg := [][]interface{}{[]interface{}{"as", "bs", "cs"}, []interface{}{1, 2, 3}}
		converter:= mapConverter([]string{"ColA", "ColB", "colc"})
		Convey("The row count conditions should be correctly evaluted", func(){
			c, _ := HasAtLeastNRowsCondition(1)
			c2, _ := HasAtMostNRowsCondition(1)
			So(c(converter(msg[0]), false), ShouldBeTrue)
			So(c(converter(msg[1]), false), ShouldBeTrue)
			So(c(nil, true), ShouldBeTrue)
			So(c2(converter(msg[0]), false), ShouldBeTrue)
			So(c2(converter(msg[1]), false), ShouldBeFalse)
			So(c2(nil, true), ShouldBeFalse)
		})
	})
}

func TestDistinctRowCountCondition(t *testing.T){
	Convey("Given a slice of messages", t, func(){
		msg := [][]interface{}{[]interface{}{"as", "bs"}, []interface{}{"as", "bs"}, []interface{}{"cs", "ds"}}
		converter:= mapConverter([]string{"ColA", "ColB"})
		Convey("The distinct row count conditions should be correctly evaluted", func(){
			c, _ := HasAtLeastNDistinctValuesCondition("ColA", 2)
			c2, _ := HasAtMostNDistinctValuesCondition("ColA", 1)
			So(c(converter(msg[0]), false), ShouldBeTrue)
			So(c(converter(msg[1]), false), ShouldBeTrue)
			So(c(nil, true), ShouldBeFalse)
			So(c(converter(msg[2]), false), ShouldBeTrue)
			So(c(nil, true), ShouldBeTrue)

			So(c2(nil, true), ShouldBeTrue)
			So(c2(converter(msg[0]), false), ShouldBeTrue)
			So(c2(converter(msg[1]), false), ShouldBeTrue)
			So(c2(nil, true), ShouldBeTrue)
			So(c2(converter(msg[2]), false), ShouldBeFalse)
			So(c2(nil, true), ShouldBeFalse)
		})
	})
}