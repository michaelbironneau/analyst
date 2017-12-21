package transforms

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
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
	})
}
