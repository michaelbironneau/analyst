package engine

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

const testMandrillKey = "XIrAnHAcpAMpOONkJYjiNg" //this is a test key and can't be used to send any real emails

func TestMandrillDestination(t *testing.T) {
	Convey("Given a Mandrill destination and some test items", t, func() {
		m := MandrillDestination{}
		m.Template = "analyst-test"
		m.Recipients = append(m.Recipients, MandrillPrincipal{Name: "Test User", Email: "test-user@test-user.com"})
		m.SplitByRow = true
		m.Name = "Mandrill Destination"
		m.APIKey = testMandrillKey
		So(m.Ping(), ShouldBeNil)
		msg := [][]interface{}{[]interface{}{"Bob Bobbertson", 123.123}, []interface{}{"Steve Stevenson", 234.234}}
		cols := []string{"Engineer", "Current"}
		l := NewConsoleLogger(Trace)
		st := &stopper{}
		sourceStream := NewStream(cols, DefaultBufferSize)
		source := NewSliceSource(cols, msg)
		source.Open(sourceStream, l, st)
		m.Open(sourceStream, l, st)

		Convey("It should produce two emails and no errors", func() {
			So(m.emailsSent, ShouldEqual, 2)
			So(st.Stopped(), ShouldBeFalse)
		})

	})

}
