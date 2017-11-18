package analyst

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)




func TestCompiler(t *testing.T) {
	script := `
	CONNECTION 'DB' (
		Driver = 'sqlite3',
	    ConnectionString = './engine/testing/test_insert.db'
	)

	CONNECTION 'Workbook' (
		Driver = 'Excel',
		File = './output.xlsx',
		Sheet = 'Test'
	)

	QUERY 'DumpData' FROM DB (
		SELECT 1 AS 'Id', 'Bob' AS 'Name'
	) INTO CONNECTION Workbook
	WITH (Range = '[0,0]:[0,N]')

	`
	SkipConvey("Given a coordinator and an Excel data destination", t, func() {
		So(len(script), ShouldBeGreaterThan, 0)
	})
}
