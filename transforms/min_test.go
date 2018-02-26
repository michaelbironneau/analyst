package transforms

import (
	"math"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMin(t *testing.T) {
	Convey("Given a min aggregate", t, func() {
		agg := min{}
		agg.SetArgumentMap(DefaultArgMap)
		Convey("It should reduce to nil if there are no messages", func() {
			So(agg.Return(), ShouldBeNil)
		})
		Convey("It should reduce multiple message correctly", func() {
			msgs := [][]interface{}{
				[]interface{}{-0.1},
				[]interface{}{1},
				[]interface{}{nil},
			}
			for _, msg := range msgs {
				err := agg.Reduce(msg)
				So(err, ShouldBeNil)
			}
			f := agg.Return()
			So(f, ShouldNotBeNil)
			So(*f, ShouldEqual, -0.1)
		})
		Convey("It should reduce multiple message with negative numbers correctly", func() {
			msgs := [][]interface{}{
				[]interface{}{-0.1},
				[]interface{}{-1},
				[]interface{}{nil},
			}
			for _, msg := range msgs {
				err := agg.Reduce(msg)
				So(err, ShouldBeNil)
			}
			f := agg.Return()
			So(f, ShouldNotBeNil)
			So(*f, ShouldEqual, -1)
		})
		Convey("It should reduce multiple timestamps correctly", func() {
			expectedMin := "2018-02-13T01:00:00Z"
			msgs := [][]interface{}{
				[]interface{}{"2018-02-14T10:00:00Z"},
				[]interface{}{"2018-02-14T05:00:00Z"},
				[]interface{}{expectedMin},
				[]interface{}{"2018-02-14T03:00:00Z"},
				[]interface{}{"2018-02-14T05:30:00Z"},
				[]interface{}{"2018-02-14T05:00:30Z"},
			}

			// the following makes sure the defaults are as expected,
			// this is waiting for a deeper fix to be performed in the
			// default values for the "min" structure
			agg.result = math.MaxFloat64

			for _, msg := range msgs {
				err := agg.Reduce(msg)
				So(err, ShouldBeNil)
			}
			f := agg.Return()
			So(f, ShouldNotBeNil)
			expectedMinTimestamp, _, err := parseTime(expectedMin)
			So(err, ShouldBeNil)
			So(*f, ShouldEqual, float64(expectedMinTimestamp.Unix()))
		})
		Convey("It should raise an error when a string is not in the expected timestamp formats", func() {
			expectedRejectionTimestampFormat := "FOO_BAR_BAZ"
			err := agg.Reduce([]interface{}{expectedRejectionTimestampFormat})
			So(err, ShouldBeError)
			So(err.Error(), ShouldEqual, "unknown time format FOO_BAR_BAZ: expected RFC3339, RFC3339 with nanoseconds or YYYY-MM-DDTHH:MM:SSZ")
		})
	})
}
