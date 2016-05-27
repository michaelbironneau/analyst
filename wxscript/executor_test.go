package wxscript

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestExecute(t *testing.T) {
	Convey("When executing a report template", t, func() {
		r, err := Load(testScript)
		Convey("The templated fields should be populated", func() {
			So(err, ShouldBeNil)
			So(r, ShouldNotBeNil)
			r.Parameters["Client"] = Parameter{Value: "test"}
			r.Parameters["Site"] = Parameter{Value: 1.23}
			r2, err2 := r.Execute()
			So(err2, ShouldBeNil)
			So(r2, ShouldNotBeNil)
			So(r2.OutputFile, ShouldEqual, "asdf-test.xlsx")
			So(r2.Name, ShouldEqual, r.Name)
			So(r2.Description, ShouldEqual, r.Description)
			So(r2.TemplateFile, ShouldEqual, r.TemplateFile)
			So(r2.Queries, ShouldResemble, r.Queries)
			So(r2.Connections, ShouldResemble, r.Connections)
		})
	})

}
