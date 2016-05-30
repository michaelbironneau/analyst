package aql

import (
	"database/sql"
	"fmt"
	_ "github.com/cznic/ql/driver" //QL driver
	"strings"
	"time"
)

//NewTempDb creates a new in-memory db from session identifier
func NewTempDb(session string) (*sql.DB, error) {
	return sql.Open("ql-mem", "memory://"+session+".db")
}

//CreateTempTableFromRange creates a new temp table given the query range
func CreateTempTableFromRange(db *sql.DB, qr *QueryRange) error {
	if qr.TempTable == nil {
		panic("Tried to create temp table from range that was not temp table range!")
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	statement := fmt.Sprintf(`
		CREATE TABLE %s %s;			
	`, qr.TempTable.Name, qr.TempTable.Columns)
	_, err = tx.Exec(statement)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (r result) WriteToTempTable(db *sql.DB, table string) error {

	var valueLines []string
	for i := range r {
		valueLines = append(valueLines, valueLine(r[i]))
	}
	statement := fmt.Sprintf(`
		INSERT INTO %s VALUES 
			%s
		;`, table, strings.Join(valueLines, ","))
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(statement)
	if err != nil {
		return err
	}
	return tx.Commit()

}

func valueLine(values []interface{}) string {
	var cols []string
	for i := range values {
		if values[i] == nil {
			cols = append(cols, "NULL")
			continue
		}
		switch values[i].(type) {
		case int, int64:
			cols = append(cols, fmt.Sprintf("%v", values[i]))
		case float64:
			cols = append(cols, fmt.Sprintf("%f", values[i].(float64)))
		case string:
			cols = append(cols, "\""+values[i].(string)+"\"")
		case time.Time:
			cols = append(cols, "\""+values[i].(time.Time).Format(time.RFC3339Nano), "\"")
		default:
			cols = append(cols, fmt.Sprintf("\"%v\"", values[i]))
		}
	}
	return "(" + strings.Join(cols, ",") + ")"
}
