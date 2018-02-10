package transforms

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestCDF(t *testing.T) {
	Convey("Given a CDF aggregate", t, func() {
		agg := quantile{}
		agg.SetArgumentMap(DefaultArgMap)
		Convey("It should reduce to nil if there are no messages", func() {
			So(agg.Return(), ShouldBeNil)
		})
		Convey("It should reduce multiple message correctly", func() {
			msgs := [][]interface{}{
				[]interface{}{0, 0.5},
				[]interface{}{1, 0.5},
				[]interface{}{nil, 0.5},
			}
			for _, msg := range msgs {
				err := agg.Reduce(msg)
				So(err, ShouldBeNil)
			}
			f := agg.Return()
			So(f, ShouldNotBeNil)
			So(*f, ShouldEqual, 0.5)
		})
	})
}
