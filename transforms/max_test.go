package transforms

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMax(t *testing.T) {
	Convey("Given a max aggregate", t, func() {
		agg := max{}
		agg.SetArgumentMap(DefaultArgMap)
		Convey("It should reduce to nil if there are no messages", func() {
			So(agg.Return(), ShouldBeNil)
		})
		Convey("It should reduce multiple messages correctly", func() {
			msgs := [][]interface{}{
				[]interface{}{0.0},
				[]interface{}{1},
				[]interface{}{nil},
			}
			for _, msg := range msgs {
				err := agg.Reduce(msg)
				So(err, ShouldBeNil)
			}
			f := agg.Return()
			So(f, ShouldNotBeNil)
			So(*f, ShouldEqual, 1.0)
		})
		Convey("It should reduce multiple timestamps correctly", func() {
			expectedMax := "2018-02-14T11:00:00Z"
			msgs := [][]interface{}{
				[]interface{}{"2018-02-14T10:00:00Z"},
				[]interface{}{"2018-02-14T05:00:00Z"},
				[]interface{}{"2018-02-14T03:00:00Z"},
				[]interface{}{"2018-02-14T05:30:00Z"},
				[]interface{}{expectedMax},
				[]interface{}{"2018-02-14T05:00:30Z"},
			}
			for _, msg := range msgs {
				err := agg.Reduce(msg)
				So(err, ShouldBeNil)
			}
			f := agg.Return()
			So(f, ShouldNotBeNil)
			expectedMaxTimestamp, _, err := parseTime(expectedMax)
			So(err, ShouldBeNil)
			So(*f, ShouldEqual, float64(expectedMaxTimestamp.Unix()))
		})
		Convey("It should raise an error when a string is not in the expected timestamp formats", func() {
			expectedRejectionTimestampFormat := "FOO_BAR_BAZ"
			err := agg.Reduce([]interface{}{expectedRejectionTimestampFormat})
			So(err, ShouldBeError)
			So(err.Error(), ShouldEqual, "unknown time format FOO_BAR_BAZ: expected RFC3339, RFC3339 with nanoseconds or YYYY-MM-DDTHH:MM:SSZ")
			f := agg.Return()
			So(*f, ShouldBeZeroValue)
		})
	})
}
