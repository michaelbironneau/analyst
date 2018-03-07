package transforms

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestCastToInt(t *testing.T){
	Convey("Given valid destination types", t, func(){
		Convey("It should be possible to correctly cast them to an integer", func(){
			tests := map[interface{}]int {1: 1, 2.0: 2, "10": 10, time.Unix(123,0): 123}
			for input, output := range tests {
				actual, err := castToInt(input)
				So(actual, ShouldEqual, output)
				So(err, ShouldBeNil) //place this at the bottom otherwise we won't know which case failed
			}
		})
	})
	Convey("Given invalid destination types", t, func(){
		Convey("It should be impossible to cast them to an integer", func(){
			invalid := []interface{}{"asdf", map[string]interface{}{"a": 1}}
			for _, input := range invalid {
				_, err := castToInt(input)
				So(err, ShouldNotBeNil)
			}
		})
	})
}


func TestCastToString(t *testing.T){
	Convey("Given valid destination types", t, func(){
		Convey("It should be possible to correctly cast them to an string", func(){
			tests := map[interface{}]string {1: "1", 2.0: "2.000000", "10": "10"}
			for input, output := range tests {
				actual, err := castToString(input)
				So(actual, ShouldEqual, output)
				So(err, ShouldBeNil) //place this at the bottom otherwise we won't know which case failed
			}
		})
	})
}

func TestCastToTime(t *testing.T){
	Convey("Given valid destination types", t, func(){
		Convey("It should be possible to correctly cast them to an time", func(){
			tests := map[interface{}]time.Time {1: time.Unix(1,0), "1970-01-01T00:00:03Z": time.Unix(3,0)}
			for input, output := range tests {
				actual, err := castToTime(input)
				So(err, ShouldBeNil)
				v := actual.(*time.Time)
				So(v.Year(), ShouldEqual, output.Year())
				So(v.Month(), ShouldEqual, output.Month())
				So(v.Day(), ShouldEqual, output.Day())
				So(v.Hour(), ShouldEqual, output.Hour())
				So(v.Minute(), ShouldEqual, output.Minute())
				So(v.Second(), ShouldEqual, output.Second())
			}
		})
	})
	Convey("Given invalid destination types", t, func(){
		Convey("It should be impossible to cast them to an integer", func(){
			invalid := []interface{}{"asdf", map[string]interface{}{"a": 1}}
			for _, input := range invalid {
				_, err := castToTime(input)
				So(err, ShouldNotBeNil)
			}
		})
	})
}
