package transforms

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestSum(t *testing.T) {
	Convey("Given a sum aggregate", t, func() {
		agg := sum{}
		agg.SetArgumentMap(DefaultArgMap)
		Convey("It should reduce to nil if there are no messages", func() {
			So(agg.Return(), ShouldBeNil)
		})
		Convey("It should reduce multiple message correctly", func() {
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
	})
}
