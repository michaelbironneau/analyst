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
	statement := fmt.Sprintf(`
	BEGIN TRANSACTION;
		CREATE TABLE %s %s;			
	COMMIT;
	`, qr.TempTable.Name, qr.TempTable.Columns)
	_, err := db.Exec(statement)
	return err
}

func (r result) WriteToTempTable(db *sql.DB, table string) error {

	var valueLines []string
	for i := range r {
		valueLines = append(valueLines, valueLine(r[i]))
	}
	statement := fmt.Sprintf(`
	BEGIN TRANSACTION;
		INSERT INTO %s VALUES 
			%s	
	COMMIT;`, table, strings.Join(valueLines, ","))
	_, err := db.Exec(statement)
	return err

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
