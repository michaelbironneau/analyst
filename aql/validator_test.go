package aql

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestValidate(t *testing.T) {
	Convey("When validating a valid script", t, func() {
		r, err := Load(testScript)
		Convey("No errors should be raised", func() {
			So(err, ShouldBeNil)
		})
		Convey("The correct data should be transferred to the target struct", func() {
			So(err, ShouldBeNil)
			So(r.Name, ShouldEqual, "Report Name")
			So(r.Description, ShouldEqual, "Report description")
			So(r.Connections["g3"], ShouldEqual, "g3.conn")
			So(r.Queries["name2"].Source, ShouldEqual, "azure")
			So(r.Queries["name1"].SourceType, ShouldEqual, FromConnection)
			So(r.Queries["name3"].SourceType, ShouldEqual, FromTempTable)
			So(r.Parameters["Site"].Type, ShouldEqual, "number")
		})
	})
}
