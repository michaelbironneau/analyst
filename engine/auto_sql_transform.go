package engine

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

//  AutoSQLTransform is a transform that drains the source, sticks the rows in an
//  in-memory SQLite database (not GLOBAL - it doesn't share the cache), and then
//  runs an SQL query on that, returning the result as rows.
//
//  Essentially, this makes it a combination of a SQL destination and a SQL source,
//  where the two are automatically wired up to work together.
//
//  A current limitation is that the source dataset must fit entirely in memory.
//  If this is not possible, it will be necessary to use eg. a GLOBAL destination
//  and to configure SET IN_MEMORY = 'OFF';
type AutoSQLTransform struct {
	db                   *sql.DB
	Name                 string
	Table                string `aql: "STAGING_TABLE, optional"`
	outgoingName         string
	StagingSQLConnString string `aql: "STAGING_CONNECTION_STRING, optional"`
	Query                string
	ParameterTable       *ParameterTable
	ParameterNames       []string
}

func (a *AutoSQLTransform) parameters() ([]interface{}, error) {
	if a.ParameterTable == nil && a.ParameterNames != nil {
		panic("parameter table uninitialized!") //if this gets reached it is a big-time bug
	}
	var params []interface{}
	for _, name := range a.ParameterNames {
		val, ok := a.ParameterTable.Get(name)
		if !ok {
			return nil, fmt.Errorf("parameter not set %s", name)
		}
		params = append(params, val)
	}
	return params, nil
}

func (a *AutoSQLTransform) SetName(name string) {
	a.outgoingName = name

}

func (a *AutoSQLTransform) log(l Logger, level LogLevel, msg string) {
	l.Chan() <- Event{
		Source:  a.Name,
		Level:   level,
		Time:    time.Now(),
		Message: msg,
	}
}

func (a *AutoSQLTransform) fatalerr(err error, st Stream, l Logger) {
	l.Chan() <- Event{
		Level:   Error,
		Source:  a.Name,
		Time:    time.Now(),
		Message: err.Error(),
	}
	close(st.Chan(a.outgoingName))
}

func (a *AutoSQLTransform) connectToStaging(l Logger) error {
	var err error
	if a.StagingSQLConnString != "" {
		a.log(l, Trace, fmt.Sprintf("using provided SQL connection string %s instead of new in-memory cache", a.StagingSQLConnString))
		a.db, err = sql.Open("sqlite3", a.StagingSQLConnString)
	} else {
		a.db, err = sql.Open("sqlite3", ":memory:") //don't use shared cache
	}
	if err != nil {
		return err
	}
	return nil
}

//createTableStatement returns a CREATE TABLE statement by inferring the row types.
func (a *AutoSQLTransform) createTableStatement(row []interface{}, cols []string, l Logger) (string, error) {
	template := `CREATE TABLE %s (
		%s
	)`
	var columns []string
	if len(row) != len(cols) {
		panic("row length did not match provided column length") //this should never get reached as the same piece of code is providing both!
	}
	for i := range row {
		colType, err := inferSQLType(row[i])
		if err != nil {
			return "", err
		}
		columns = append(columns, cols[i]+" "+colType)
	}
	statement := fmt.Sprintf(template, a.Table, strings.Join(columns, ", "))
	a.log(l, Trace, fmt.Sprintf("Create staging table:\n%s", statement))
	return fmt.Sprintf(template, a.Table, strings.Join(columns, ",")), nil
}

//inferSQLType returns a string with the SQLite3 type corresponding to the Go type
//of the interface.
func inferSQLType(i interface{}) (string, error) {
	switch i.(type) {
	case string:
		return "TEXT", nil
	case float64:
		return "REAL", nil
	case int64:
		return "INT", nil
	case int32:
		return "INT", nil
	case int:
		return "INT", nil
	case bool:
		return "BOOLEAN", nil
	default:
		return "", fmt.Errorf("cannot infer SQL type from type %T with value %v", i, i)
	}
}

func printAsSQLValue(i interface{}) (string, error) {
	if i == nil {
		return "null", nil
	}
	switch v := i.(type) {
	case string:
		return fmt.Sprintf("'%s'", v), nil
	case float64:
		//see eg. https://stackoverflow.com/questions/19101419/go-golang-formatfloat-convert-float-number-to-string
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case int64:
		return strconv.Itoa(int(v)), nil
	case int32:
		return strconv.Itoa(int(v)), nil
	case int:
		return strconv.Itoa(v), nil
	case bool:
		if v {
			return "1", nil
		}
		return "0", nil
	default:
		return "", fmt.Errorf("could not print type %T with value %v as SQL value", i, i)
	}
}

func (a *AutoSQLTransform) insertStatement(row []interface{}) (string, error) {
	template := `INSERT INTO %s VALUES (%s)`
	cols := make([]string, len(row), len(row))
	var err error
	for i, col := range row {
		cols[i], err = printAsSQLValue(col)
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf(template, a.Table, strings.Join(cols, ", ")), nil
}

func (a *AutoSQLTransform) Open(source Stream, dest Stream, l Logger, st Stopper) {
	a.log(l, Info, fmt.Sprintf("Auto SQL source opened"))
	if err := a.connectToStaging(l); err != nil {
		a.fatalerr(err, dest, l)
		return
	}
	params, err := a.parameters()
	if err != nil {
		a.fatalerr(err, dest, l)
		return
	}
	a.log(l, Info, fmt.Sprintf("Query: %v", a.Query))
	a.log(l, Info, fmt.Sprintf("Query parameters %v", params))
	inChan := source.Chan(a.outgoingName)
	var firstMessage = true
	var cols []string
	var rowCount int
	//INSERT all rowCount from source into staging table
	for msg := range inChan {
		if st.Stopped() {
			a.log(l, Warning, "Auto SQL source aborted")
			close(dest.Chan(a.outgoingName))
			return
		}
		if firstMessage {
			cols = source.Columns()
			createStagingSQL, err := a.createTableStatement(msg.Data, cols, l)
			if err != nil {
				a.fatalerr(err, dest, l)
				return
			}
			_, err = a.db.Exec(createStagingSQL)
			if err != nil {
				a.fatalerr(err, dest, l)
				return
			}
			a.log(l, Info, fmt.Sprintf("Successfully created staging table %s", a.Table))
			firstMessage = false
		}
		insertStatement, err := a.insertStatement(msg.Data)
		a.log(l, Trace, insertStatement)
		if err != nil {
			a.fatalerr(err, dest, l)
			return
		}
		_, err = a.db.Exec(insertStatement)
		rowCount++
		if err != nil {
			a.fatalerr(err, dest, l)
			return
		}
	}

	a.log(l, Info, fmt.Sprintf("Inserted %d rowCount into staging table", rowCount))

	//Run the user query on the staging table and send the result to the destination stream
	rows, err := a.db.Query(a.Query, params...)

	if err != nil {
		a.fatalerr(err, dest, l)
		return
	}
	defer rows.Close()
	outCols, err := rows.Columns()
	if err != nil {
		a.fatalerr(err, dest, l)
		return
	}
	for rows.Next() {
		if st.Stopped() {
			a.log(l, Warning, fmt.Sprintf("Auto SQL source aborted"))
			close(dest.Chan(a.outgoingName))
			return
		}
		rr := make([]interface{}, len(outCols))
		rrp := makeRowPointers(rr)
		err := rows.Scan(rrp...)
		rr = convertRow(rr)
		if err != nil {
			a.fatalerr(err, dest, l)
			return
		}
		a.log(l, Trace, fmt.Sprintf("Ouput row %v", rr))
		dest.Chan(a.outgoingName) <- Message{Source: a.outgoingName, Data: rr}
	}

	a.log(l, Info, fmt.Sprintf("Auto SQL source closed"))
	close(dest.Chan(a.outgoingName))

}
