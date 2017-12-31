package engine

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestJSONParsing(t *testing.T) {
	Convey("Given a JSON string and HTTP source", t, func() {
		Convey("It should correctly parse object arrays", func() {
			h := HTTPSource{
				Name:        "source",
				JSONPath:    "items",
				ColumnNames: []string{"c", "b"},
			}
			b := []byte(`
				{"items": [{"a": 1, "b": 2, "c": "d"}]}
			`)
			i, err := h.parse(b)
			So(err, ShouldBeNil)
			So(i, ShouldHaveLength, 1)
			So(i[0], ShouldHaveLength, 2)
			So(i[0][0], ShouldEqual, "d")
			So(i[0][1], ShouldEqual, 2)
		})
		Convey("It should correctly parse primitive arrays", func() {
			h := HTTPSource{
				Name:          "source",
				JSONPath:      "items",
				NoColumnNames: true,
				ColumnNames:   []string{"a", "c", "b"},
			}
			b := []byte(`
				{"items": [[1, "d", 2]]}
			`)
			i, err := h.parse(b)
			So(err, ShouldBeNil)
			So(i, ShouldHaveLength, 1)
			So(i[0], ShouldHaveLength, 3)
			So(i[0][0], ShouldEqual, 1)
			So(i[0][1], ShouldEqual, "d")
			So(i[0][2], ShouldEqual, 2)
		})
		Convey("It should work with top-level arrays", func() {
			h := HTTPSource{
				Name:          "source",
				JSONPath:      "",
				NoColumnNames: true,
				ColumnNames:   []string{"a", "c", "b"},
			}
			b := []byte(`
				[[1, "d", 2]]
			`)
			i, err := h.parse(b)
			So(err, ShouldBeNil)
			So(i, ShouldHaveLength, 1)
			So(i[0], ShouldHaveLength, 3)
			So(i[0][0], ShouldEqual, 1)
			So(i[0][1], ShouldEqual, "d")
			So(i[0][2], ShouldEqual, 2)
		})
	})

}

func TestPagination(t *testing.T) {
	Convey("Given an HTTP source that is correctly configured", t, func() {
		h := HTTPSource{
			PageSize:             25,
			PaginationLimitName:  "limit",
			PaginationOffsetName: "offset",
			URL:                  "https://api.company.com/v1",
		}
		Convey("It should return first page URL correctly", func() {
			url := h.firstPage()
			So(url, ShouldEqual, h.URL+"?limit=25&offset=0")
		})

	})
}
