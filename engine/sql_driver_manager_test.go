package engine

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestDriverManager(t *testing.T) {
	Convey("Given the driver manager", t, func() {
		Convey("When I connect twice with the same string/driver, I should get back the same connection", func() {
			db, err := SQLDriverManager.DB("sqlite3", ":memory:")
			So(err, ShouldBeNil)
			db2, err2 := SQLDriverManager.DB("sqlite3", ":memory:")
			So(err2, ShouldBeNil)
			So(db, ShouldEqual, db2)
		})

	})
}
