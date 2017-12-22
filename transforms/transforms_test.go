package transforms

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestParse(t *testing.T) {
	Convey("Given a valid transform body", t, func() {
		s := `AGGREGATE SUM(A) As Val`
		Convey("It should parse and initialize it correctly", func() {
			t, err := Parse(s)
			So(err, ShouldBeNil)
			So(t, ShouldNotBeNil)
		})
	})

}
