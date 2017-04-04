package aql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/tealeg/xlsx"
	"reflect"
	"time"
)

type result [][]interface{}

//DBQuery is a factory of QueryFunc for SQL database
func DBQuery(driverName, connection, statement string) (result, error) {
	var (
		db      *sql.DB
		err     error
		retry   time.Duration
		retries int
		rows    *sql.Rows
	)
	retry = time.Second
	for {
		if retries > 8 {
			return nil, fmt.Errorf("Gave up retrying following bad connection errors for driver %s", driverName)
		}
		time.Sleep(retry)
		db, err = sql.Open(driverName, connection)
		if err == driver.ErrBadConn {
			retries++
			continue
		}
		if err != nil {
			return nil, err
		}
		defer db.Close()
		rows, err = db.Query(statement)
		if err == driver.ErrBadConn {
			retries++
			continue
		}
		if err != nil {
			return nil, err
		}
		break
	}
	return rowsToInterface(rows)
}

//QueryFunc is a func that runs a SQL query and returns
//interface matrix (instead of sql.Rows), and an error (if any).
type QueryFunc func(driver, connection, statement string) (result, error)

//rowsToInterface scans some sql.Rows into an interface{} matrix. The sql.Driver
//that is used for the scanning should preserve type information and not just return []byte.
func rowsToInterface(rows *sql.Rows) ([][]interface{}, error) {
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var ret result
	for rows.Next() {
		row := make([]interface{}, len(cols))
		rowPointers := makeRowPointers(row)
		if err := rows.Scan(rowPointers...); err != nil {
			return nil, fmt.Errorf("Error running query: %v", err)
		}
		ret = append(ret, row)
	}
	return ret, nil
}

//makeRowPointers creates a slice that points to elements of another slice. The point is that rows.Scan() requires
//the destination types to be pointers but we want interface{} types
func makeRowPointers(row []interface{}) []interface{} {
	ret := make([]interface{}, len(row), len(row))
	for i := range row {
		ret[i] = &row[i]
	}
	return ret
}

//Write writes the (i,j)th entry of the result into the given cell.
//It does **not** do bounds checking (it is called inside a loop so
//that would be unecessary).
func (r result) Write(i int, j int, cell *xlsx.Cell) error {
	if r[i][j] == nil {
		return nil //nothing to write
	}
	switch v := r[i][j].(type) {
	case int:
		cell.SetInt(v)
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
func (r result) WriteToSheet(x1 int, x2 int, y1 int, y2 int, transpose bool, sheet *xlsx.Sheet) error {
	if len(r) == 0 {
		panic("WriteToSheet() was called with empty result")
	}

	for i := range r {
		for j := range r[0] {
			var err error

			if transpose && (i+x1 <= x2 && j+y1 <= y2) {
				err = r.Write(i, j, sheet.Cell(i+x1, j+y1))
			} else if j+x1 <= x2 && i+y1 <= y2 {
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
		y2 = len(r) + y1 - 1
	case ok2 && !ok:
		x2 = len(r) + x1 - 1
	}
	transpose, err = r.needsTranspose(x1, x2, y1, y2)
	if err != nil {
		err = fmt.Errorf("Error with range [%v,%v]:[%v,%v]: %v", qr.X1, qr.Y1, qr.X2, qr.Y2, err)
	}
	return

}

//needsTranspose returns true if result should be transposed and false if not
func (r result) needsTranspose(x1, x2, y1, y2 int) (bool, error) {
	width := 1 + x2 - x1
	height := 1 + y2 - y1
	if width <= 0 || height <= 0 {
		return false, fmt.Errorf("Both height and width must be strictly positive")
	}
	switch {
	case height <= len(r) && width == len(r[0]):
		return false, nil
	case height == len(r[0]) && width <= len(r):
		return true, nil
	default:
		return false, fmt.Errorf("Incorrect number of columns, expected range to have width/height of %d/%d but it had width: %d, height: %d", width, height, len(r[0]), len(r))
	}
}
