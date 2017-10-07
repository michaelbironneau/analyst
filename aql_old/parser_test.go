package aql

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestParseOptions(t *testing.T) {
	Convey("When parsing a single option", t, func() {
		s := " KEY   = \"asdf\" "
		Convey("It should get the key and value correctly, without error", func(){
			o, err := parseOptions(s, 1)
			So(err, ShouldBeNil)
			So(o["KEY"], ShouldResemble, "asdf")
		})

		Convey("It should return errors, if there are any", func(){
			s = "KEY \"asdf\""
			_, err := parseOptions(s, 1)
			So(err, ShouldNotBeNil)
			s = "KEY = \"asdf"
			_, err = parseOptions(s, 1)
			So(err, ShouldNotBeNil)
		})
	})
	Convey("When parsing multiple options", t, func(){
		s := " KEY = \"asdf\",   KEY2 = 123, KEY3 = true, KEY4 = 1.234"
		Convey("It should get keys and values correctly, without errors", func(){
			o, err := parseOptions(s, 1)
			So(err, ShouldBeNil)
			So(o["KEY"], ShouldResemble, "asdf")
			So(o["KEY2"], ShouldResemble, 123.00)
			So(o["KEY3"], ShouldResemble, true)
			So(o["KEY4"], ShouldResemble, 1.234)
		})
		Convey("It should return errors, if there are any", func(){
			s = "KEY = 123, KEY2 = "
			_, err := parseOptions(s, 1)
			So(err, ShouldNotBeNil)
		})
	})
}
