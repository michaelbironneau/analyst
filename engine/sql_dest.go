package engine

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type SQLDestination struct {
	Name             string
	Driver           string
	ConnectionString string
	Table            string
	columns          []string
	db               *sql.DB
	Alias            string
}

const InsertQuery = `INSERT INTO %s (%s) VALUES (%s)`

func (sq *SQLDestination) Columns() []string {
	return sq.columns
}

func (sq *SQLDestination) connect() error {
	var err error
	sq.db, err = sql.Open(sq.Driver, sq.ConnectionString)
	if err != nil {
		return fmt.Errorf("SQL destination: %s", err.Error())
	}
	return nil
}

func (sq *SQLDestination) Ping() error {
	if sq.db == nil {
		err := sq.connect()
		if err != nil {
			return err
		}
	}
	return sq.db.Ping()
}

func (sq *SQLDestination) fatalerr(err error, l Logger) {
	l.Chan() <- Event{
		Level:   Error,
		Source:  sq.Name,
		Time:    time.Now(),
		Message: err.Error(),
	}
}

func (sq *SQLDestination) log(l Logger, level LogLevel, msg string) {
	l.Chan() <- Event{
		Time:    time.Now(),
		Source:  sq.Name,
		Level:   level,
		Message: msg,
	}
}

func (sq *SQLDestination) Open(s Stream, l Logger, st Stopper) {
	if sq.db == nil {
		err := sq.connect()
		if err != nil {
			sq.fatalerr(err, l)
			return
		}
	}
	sq.log(l, Info, "SQL destination opened")

	tx, err := sq.db.Begin()
	sq.log(l, Trace, "Initiated transaction")
	if err != nil {
		sq.fatalerr(err, l)
		return
	}

	var (
		stmt *sql.Stmt
	)
	for msg := range s.Chan(sq.Alias) {
		if st.Stopped() {
			sq.log(l, Warning, "SQL source aborted, rollin back transaction")
			err := tx.Rollback()
			if err == nil {
				sq.log(l, Info, "Transaction rolled back")
			} else {
				sq.log(l, Error, fmt.Sprintf("Failed to roll back transaction: %v", err))
			}
			return
		}
		sq.log(l, Trace, fmt.Sprintf("Row %v", msg.Data))
		if len(s.Columns()) != len(msg.Data) {
			sq.fatalerr(fmt.Errorf("expected %v columns but got %v", len(s.Columns()), len(msg.Data)), l)
			tx.Rollback() //discard error - best effort attempt
			return
		}
		if stmt == nil {
			sq.columns = s.Columns()
			sq.log(l, Trace, fmt.Sprintf("Found columns %v", sq.columns))
			insertQuery := sq.prepare(s, msg.Data)
			stmt, err = tx.Prepare(insertQuery)
			if err != nil {
				sq.fatalerr(err, l)
				tx.Rollback()
				return
			}
		}
		_, err := stmt.Exec(msg.Data...)
		if err != nil {
			sq.fatalerr(err, l)
			tx.Rollback()
			return
		}
	}
	sq.log(l, Info, "Done - committing transaction")
	err = tx.Commit()
	if err != nil {
		sq.fatalerr(err, l)
	}
}

//prepare creates the prepared statement
func (sq *SQLDestination) prepare(s Stream, msg []interface{}) string {
	cols := strings.Join(s.Columns(), ",")
	params := strings.Repeat("?,", len(msg))
	params = params[0 : len(params)-1] //remove trailing comma
	return fmt.Sprintf(InsertQuery, sq.Table, cols, params)
}
