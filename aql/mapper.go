package aql

import (
	"database/sql"
	"fmt"
	"github.com/tealeg/xlsx"
	"reflect"
	"time"
)

type result [][]interface{}

//DBQuery is a factory of QueryFunc for SQL database
func DBQuery(driver, connection, statement string) (result, error) {
		db, err := sql.Open(driver, connection)
		if err != nil {
			return nil, err
		}
		rows, err := db.Query(statement)
		if err != nil {
			return nil, err
		}
		return rowsToInterface(rows)
	}

//QueryFunc is a func that runs a SQL query and returns
//interface matrix (instead of sql.Rows), and an error (if any).
type QueryFunc func(driver, connection, statement string) (result, error)

//rowsToInterface scans some sql.Rows into an interface{} matrix. The sql.Driver
//that is used for the scanning should preserve type information and not just return []byte.
func rowsToInterface(rows *sql.Rows) ([][]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var ret result
	for rows.Next() {
		row := make([]interface{}, len(cols))
		if err := rows.Scan(row...); err != nil {
			return nil, err
		}
		ret = append(ret, row)
	}
	return ret, nil
}

//Write writes the (i,j)th entry of the result into the given cell.
//It does **not** do bounds checking (it is called inside a loop so
//that would be unecessary).
func (r result) Write(i int, j int, cell *xlsx.Cell) error {
	if r[i][j] == nil {
		return nil //nothing to write
	}
	switch v := r[i][j].(type) {
	case int64:
		cell.SetInt64(v)
	case float64:
		cell.SetFloat(v)
	case bool:
		cell.SetBool(v)
	case []byte:
		cell.SetString(string(v))
	case string:
		cell.SetString(v)
	case time.Time:
		cell.SetDateTime(v)
	default:
		t := reflect.TypeOf(r[i][j])
		return fmt.Errorf("Unsupported SQL type %s", t.Name())
	}
	return nil
}

//WriteToSheet writes the result to the specified range in the sheet
func (r result) WriteToSheet(x1 int, y1 int, transpose bool, sheet *xlsx.Sheet) error {
	if len(r) == 0 {
		panic("WriteToSheet() was called with empty result")
	}

	for i := range r {
		for j := range r[0] {
			var err error

			if transpose {
				err = r.Write(i, j, sheet.Cell(i+x1, j+y1))
			} else {
				err = r.Write(i, j, sheet.Cell(j+x1, i+y1))
			}

			if err != nil {
				return err
			}

		}
	}

	return nil
}

//Map maps the result given the query range parameters
func (r result) Map(qr *QueryRange) (x1 int, x2 int, y1 int, y2 int, transpose bool, err error) {
	if len(r) == 0 {
		panic("Map() was called with empty result") //should not get reached
	}
	//Case 1: Static range [x1,y1]:[x2,y2]
	x1 = qr.X1.(int)
	y1 = qr.Y1.(int)
	_, ok := qr.X2.(int)
	_, ok2 := qr.Y2.(int)

	if ok {
		x2 = qr.X2.(int)
	}

	if ok2 {
		y2 = qr.Y2.(int)
	}

	//Invalid range, both x2 and y2 are 'n' (should have been weeded out by parser)
	if !ok && !ok2 {
		panic("Map() range contained two 'n's") //should not get reached
	}

	switch {
	case ok && !ok2:
		y2 = len(r) + y1
	case ok2 && !ok:
		x2 = len(r) + x1
	}
	transpose, err = r.needsTranspose(x1, x2, y1, y2)
	return

}

//needsTranspose returns true if result should be transposed and false if not
func (r result) needsTranspose(x1, x2, y1, y2 int) (bool, error) {
	width := x2 - x1
	height := y2 - y1
	if width <= 0 || height <= 0 {
		return false, fmt.Errorf("Invalid query range: both height and width must be strictly positive")
	}
	if height == len(r) && width == len(r[0]) {
		return false, nil
	} else if height == len(r[0]) && width == len(r) {
		return true, nil
	}
	return false, fmt.Errorf("Invalid query range: Incorrect number of columns, expected range to have width/height of %d or %d", width, height)
}
