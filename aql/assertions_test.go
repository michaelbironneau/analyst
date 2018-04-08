package aql

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestAssertions(t *testing.T) {
	Convey("When given a table of valid assertions", t, func() {
		assertions := []string{
			"IT OUTPUTS AT LEAST 3 ROWS",
			"IT OUTPUTS AT MOST 4321 ROWS",
			"COLUMN B HAS UNIQUE VALUES",
			"COLUMN A HAS NO NULL VALUES",
		}
		Convey("It should parse correctly and return no error", func() {
			for i := range assertions {
				_, err := NewAssertion(assertions[i])
				So(err, ShouldBeNil)
			}
		})
	})
}
