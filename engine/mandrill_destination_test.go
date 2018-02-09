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

func TestPrincipalParser(t *testing.T) {
	Convey("Given some valid recipients", t, func() {
		s := `John Johnson <jj@jj.com > `
		s2 := `Adam <  adam@adam.com>, James Jameson <jj@jj.co.uk>`
		Convey("It should parse them to principals correctly", func() {
			r, err := ParseEmailRecipients(s)
			So(err, ShouldBeNil)
			So(len(r), ShouldEqual, 1)
			So(r[0].Name, ShouldEqual, "John Johnson")
			So(r[0].Email, ShouldEqual, "jj@jj.com")
			r, err = ParseEmailRecipients(s2)
			So(err, ShouldBeNil)
			So(len(r), ShouldEqual, 2)
			So(r[1].Name, ShouldEqual, "James Jameson")
			So(r[1].Email, ShouldEqual, "jj@jj.co.uk")
		})
	})

	Convey("Given some invalid email recipients", t, func() {
		s := `John <jj@jj.com>; Bob <bob@bob.com>`
		s2 := `John <john:john.com>`
		Convey("It should return an error when parsing", func() {
			_, err := ParseEmailRecipients(s)
			So(err, ShouldNotBeNil)
			_, err = ParseEmailRecipients(s2)
			So(err, ShouldNotBeNil)
		})
	})
}
