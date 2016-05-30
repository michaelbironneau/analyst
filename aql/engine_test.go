package aql

import (
	. "github.com/smartystreets/goconvey/convey"
	"github.com/tealeg/xlsx"
	"testing"
)

const pipelineTestScript = `
report 'Report Name'

description 'Report description'

template 'asdf.xlsx'

parameter Client string

connection (
    azure 'azure.conn'
)

output '{{.Client}}.xlsx'

query 'something' from azure (
	SELECT 1
) into table tmp (col1 int, col2 string)

query 'name1' from tmp (
    SELECT * FROM tmp
) into sheet 'summary' range [0,0]:[n,1]
`

var testConns map[string]Connection = map[string]Connection{
	"azure": Connection{
		Driver:           "mssql",
		ConnectionString: "something",
	},
}

func testQueryFn(driver, connString, statement string) (result, error) {
	return result([][]interface{}{
		[]interface{}{1, "a"},
		[]interface{}{2, "b"},
		[]interface{}{3, "c"},
	}), nil
}

//getTestResult gets the value of some cells
func getTestResult(f *xlsx.File) (int, string, int, error) {
	s := f.Sheet["summary"]
	c1, err := s.Cell(0, 0).Int()
	if err != nil {
		return 0, "", 0, err
	}
	c2, err := s.Cell(0, 1).String()
	if err != nil {
		return 0, "", 0, err
	}
	c3, err := s.Cell(1, 0).Int()
	if err != nil {
		return 0, "", 0, err
	}
	return c1, c2, c3, nil
}

func TestPipeline(t *testing.T) {
	Convey("When executing a valid script", t, func() {
		s, err := Load(pipelineTestScript)
		So(err, ShouldBeNil)
		err = s.SetParameter("Client", "ABC")
		So(err, ShouldBeNil)
		So(s.Parameters["Client"].Value.(string), ShouldEqual, "ABC")
		task, err := s.ExecuteTemplates()
		So(err, ShouldBeNil)
		f := xlsx.NewFile()
		_, err = f.AddSheet("summary")
		So(err, ShouldBeNil)
		Convey("It should write the correct output to the target spreadsheet", func() {
			progress := make(chan int, 1)
			res, err := task.Execute(testQueryFn, f, testConns, progress)
			close(progress)
			So(err, ShouldBeNil)
			a, b, c, err := getTestResult(res)
			So(err, ShouldBeNil)
			So(a, ShouldEqual, 1)
			So(b, ShouldEqual, "a")
			So(c, ShouldEqual, 2)
		})
	})

}
