package engine

import (
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb" //Microsoft SQL Server driver
	//_ "github.com/go-sql-driver/mysql"   //MySQL (4.1+), MariaDB, Percona Server, Google CloudSQL or Sphinx (2.2.3+)
	"fmt"
	_ "github.com/lib/pq"           //Postgres
	_ "github.com/mattn/go-sqlite3" //SQLite driver
	"time"
)

type SQLSource struct {
	Name             string
	Driver           string
	ConnectionString string
	Query            string
	ParameterTable   *ParameterTable
	Tx               *sql.Tx
	manageTx         bool
	columns          []string
	db               *sql.DB
	outgoingName     string
	ExecOnly         bool
	ParameterNames   []string
}

func (sq *SQLSource) SetName(name string) {
	sq.outgoingName = name
}

func (sq *SQLSource) Columns() []string {
	return sq.columns
}

func (sq *SQLSource) connect() error {
	var err error
	sq.db, err = sql.Open(sq.Driver, sq.ConnectionString)
	if err != nil {
		return fmt.Errorf("SQL destination: %s", err.Error())
	}
	return nil
}

func (sq *SQLSource) Ping() error {
	if sq.db == nil {
		err := sq.connect()
		if err != nil {
			return err
		}
	}
	return sq.db.Ping()
}

func (sq *SQLSource) fatalerr(err error, s Stream, l Logger) {
	l.Chan() <- Event{
		Level:   Error,
		Source:  sq.Name,
		Time:    time.Now(),
		Message: err.Error(),
	}
	close(s.Chan(sq.outgoingName))
}

func (sq *SQLSource) parameters() ([]interface{}, error) {
	if sq.ParameterTable == nil && sq.ParameterNames != nil {
		panic("parameter table uninitialized!") //if this gets reached it is a big-time bug
	}
	var params []interface{}
	for _, name := range sq.ParameterNames {
		val, ok := sq.ParameterTable.Get(name)
		if !ok {
			return nil, fmt.Errorf("parameter not set %s", name)
		}
		params = append(params, val)
	}
	return params, nil
}

func (sq *SQLSource) log(l Logger, level LogLevel, msg string) {
	l.Chan() <- Event{
		Time:    time.Now(),
		Source:  sq.Name,
		Level:   level,
		Message: msg,
	}
}

func (sq *SQLSource) Open(s Stream, l Logger, st Stopper) {
	sq.manageTx = sq.Tx == nil
	if sq.db == nil {
		err := sq.connect()
		if err != nil {
			sq.fatalerr(err, s, l)
			return
		}
	}
	sq.log(l, Info, "SQL source opened")
	var (
		tx  *sql.Tx
		err error
	)
	if sq.Tx == nil {
		tx, err = sq.db.Begin()
		sq.log(l, Trace, "Initiated transaction")
		if err != nil {
			sq.fatalerr(err, s, l)
			return
		}
	}
	params, err := sq.parameters()
	if err != nil {
		sq.fatalerr(err, s, l)
		return
	}
	start := time.Now()
	sq.log(l, Trace, fmt.Sprintf("Query: %s", sq.Query))
	sq.log(l, Trace, fmt.Sprintf("Query Parameters: %v", params))
	var (
		r            *sql.Rows
		res          sql.Result
		rowsAffected int64
	)

	if sq.ExecOnly {
		res, err = tx.Exec(sq.Query, params...)
		if err != nil {
			sq.fatalerr(err, s, l)
			if !sq.manageTx {
				return
			}
			err := tx.Rollback()
			if err == nil {
				sq.log(l, Info, "Transaction rolled back")
			} else {
				sq.log(l, Error, fmt.Sprintf("Failed to roll back transaction: %v", err))
			}
			return
		}
		rowsAffected, err = res.RowsAffected()
		if err != nil {
			sq.log(l, Trace, fmt.Sprintf("Error retrieving rows affected: %v", err))
		}
		sq.log(l, Info, "Done - committing transaction")
		err = tx.Commit()
		if err != nil {
			sq.fatalerr(err, s, l)
		}
		sq.log(l, Info, fmt.Sprintf("Rows affected: %v", rowsAffected))
		close(s.Chan(sq.outgoingName))
		return
	} else {
		r, err = tx.Query(sq.Query, params...)
	}

	sq.log(l, Info, fmt.Sprintf("Query took %7.2f seconds", time.Now().Sub(start).Seconds()))
	if err != nil {
		sq.fatalerr(err, s, l)
		if !sq.manageTx {
			return
		}
		err := tx.Rollback()
		if err == nil {
			sq.log(l, Info, "Transaction rolled back")
		} else {
			sq.log(l, Error, fmt.Sprintf("Failed to roll back transaction: %v", err))
		}
		return
	}
	defer r.Close()
	cols, err := r.Columns()
	if err != nil {
		sq.fatalerr(err, s, l)
		return
	}
	sq.columns = cols
	sq.log(l, Trace, fmt.Sprintf("Found columns %v", cols))
	s.SetColumns(DestinationWildcard, cols)

	for r.Next() {
		if st.Stopped() {
			sq.log(l, Warning, fmt.Sprintf("SQL source aborted"))
			close(s.Chan(sq.outgoingName))
			return
		}
		rr := make([]interface{}, len(cols))
		rrp := makeRowPointers(rr)
		err := r.Scan(rrp...)
		rr = convertRow(rr)
		if err != nil {
			sq.fatalerr(err, s, l)
			return
		}
		sq.log(l, Trace, fmt.Sprintf("Row %v", rr))
		s.Chan(sq.outgoingName) <- Message{Source: sq.outgoingName, Data: rr}
	}
	sq.log(l, Info, "Done - committing transaction")
	err = tx.Commit()
	if err != nil {
		sq.fatalerr(err, s, l)
	}
	sq.log(l, Info, fmt.Sprintf("SQL source closed"))
	close(s.Chan(sq.outgoingName))
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

func convertRow(raw []interface{}) []interface{} {
	ret := make([]interface{}, len(raw))
	for i := range raw {
		switch v := raw[i].(type) {
		case int64:
			ret[i] = int(v)
		case []uint8:
			ret[i] = string(v)
		default:
			ret[i] = v
		}
	}
	return ret
}
