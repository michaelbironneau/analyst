package models

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestGetRepoName(t *testing.T) {
	Convey("Given some repo URLs", t, func() {
		names := map[string]string{
			"https://github.com/michaelbironneau/analyst": "analyst",
			"https://github.com/src-d/go-git/":            "go-git",
			"asdf": "asdf",
		}
		Convey("It should map them to the repo names", func() {
			for url, name := range names {
				So(repoName(url), ShouldEqual, name)
			}
		})
	})
}
