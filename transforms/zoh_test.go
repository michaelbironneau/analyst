package transforms

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestZoh(t *testing.T) {
	Convey("Given a zoh aggregate", t, func() {
		agg := zoh{}
		agg.SetArgumentMap(DefaultArgMap)
		Convey("It should reduce to nil if there are no messages", func() {
			So(agg.Return(), ShouldBeNil)
		})
		Convey("It should reduce multiple message correctly", func() {
			msgs := [][]interface{}{
				[]interface{}{"2017-01-01T12:00:00Z", 0.0, "2017-01-01T12:00:00Z", "2017-01-01T12:30:00Z"},
				[]interface{}{"2017-01-01T12:20:00Z", 3.0, "2017-01-01T12:00:00Z", "2017-01-01T12:30:00Z"},
			}
			for _, msg := range msgs {
				err := agg.Reduce(msg)
				So(err, ShouldBeNil)
			}
			f := agg.Return()
			So(f, ShouldNotBeNil)
			So(*f, ShouldEqual, 1.0)
		})
	})
}
