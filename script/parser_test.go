package wxscript

import (
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
)

const testScript = `
report 'Report Name'

description 'Report description'

template 'asdf.xlsx'

parameter Client string

parameter (
    Site number    
)

CONNECTION pg 'pg.conn'

connection (
    g3 'g3.conn'
    azure 'azure.conn'
)

output 'asdf-{{.Client}}.xlsx'

usergroup 'commercial'

query 'name1' from azure (
    SELECT 1
) into range [0,0]:[0,1]

QUERY 'name2' FROM g3 (
SELECT 2 FROM
Table WHERE
something
) into range [0,0]:[0,n]
`

func TestGetBlockType(t *testing.T) {
	Convey("When determining the block type", t, func() {
		block := []string{" rEpoRt 'Test Name'"}
		keyword, stop, err := getBlockType(block)
		Convey("The keyword and end of keyword index should be correctly identified", func() {
			So(err, ShouldBeNil)
			So(keyword, ShouldEqual, "report")
			So(stop, ShouldEqual, 7)
		})

	})
}

func TestParse(t *testing.T) {
	Convey("When parsing a valid script", t, func() {
		r, err := Parse(testScript)
		Convey("No error should be returned", func() {
			So(err, ShouldBeNil)
		})
		Convey("The metadata should be correctly parsed", func() {
			So(r, ShouldNotBeNil)
			So(r.metadata, ShouldNotBeNil)
			So(r.metadata, ShouldHaveLength, 5)
			So(r.metadata[0].Type, ShouldEqual, "report")
			So(r.metadata[0].Data, ShouldEqual, "Report Name")
			So(r.metadata[1].Type, ShouldEqual, "description")
			So(r.metadata[1].Data, ShouldEqual, "Report description")
			So(r.metadata[2].Type, ShouldEqual, "template")
			So(r.metadata[2].Data, ShouldEqual, "asdf.xlsx")
			So(r.metadata[3].Type, ShouldEqual, "output")
			So(r.metadata[4].Type, ShouldEqual, "usergroup")
			So(r.metadata[4].Data, ShouldEqual, "commercial")
		})
		Convey("The parameters should be correctly parsed", func() {
			So(r, ShouldNotBeNil)
			So(r.parameters, ShouldHaveLength, 2)
			So(r.parameters[0].Name, ShouldEqual, "Client")
			So(r.parameters[0].Type, ShouldEqual, "string")
			So(r.parameters[1].Name, ShouldEqual, "Site")
			So(r.parameters[1].Type, ShouldEqual, "number")
		})
		Convey("The connections should be correctly parsed", func() {
			So(r, ShouldNotBeNil)
			So(r.connections, ShouldHaveLength, 3)
			So(r.connections[0].Name, ShouldEqual, "pg")
			So(r.connections[0].File, ShouldEqual, "pg.conn")
			So(r.connections[1].Name, ShouldEqual, "g3")
			So(r.connections[1].File, ShouldEqual, "g3.conn")
			So(r.connections[2].Name, ShouldEqual, "azure")
			So(r.connections[2].File, ShouldEqual, "azure.conn")
		})
		Convey("The queries should be correctly parsed", func() {
			So(r, ShouldNotBeNil)
			So(r.queries, ShouldHaveLength, 2)
			So(r.queries[0].Name, ShouldEqual, "name1")
			So(r.queries[0].Source, ShouldEqual, "azure")
			So(strings.TrimSpace(r.queries[0].Statement), ShouldEqual, "SELECT 1")
			So(r.queries[1].Name, ShouldEqual, "name2")
			So(r.queries[1].Source, ShouldEqual, "g3")
			So(r.queries[1].Range, ShouldResemble, QueryRange{X1: 0, Y1: 0, X2: 0, Y2: "n"})
			So(strings.TrimSpace(r.queries[1].Statement), ShouldEqual, strings.TrimSpace(`
SELECT 2 FROM
Table WHERE
something`))
		})
	})
}
