package engine

import (
	"database/sql"
	"time"
	"fmt"
	"strings"
)

type SQLDestination struct {
	Name             string
	Driver           string
	ConnectionString string
	Table            string
	columns          []string
	db               *sql.DB
}

const InsertQuery = `INSERT INTO %s (%s) VALUES (%s)`

func (sq *SQLDestination) Columns() []string {
	return sq.columns
}

func (sq *SQLDestination) connect() error {
	var err error
	sq.db, err = sql.Open(sq.Driver, sq.ConnectionString)
	return err
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

func (sq *SQLDestination) Open(s Stream, l Logger, st Stopper) {
	if sq.db == nil {
		err := sq.connect()
		if err != nil {
			sq.fatalerr(err, l)
			return
		}
	}

	tx, err := sq.db.Begin()
	if err != nil {
		sq.fatalerr(err, l)
		return
	}
	var (
		stmt *sql.Stmt
	)
	for msg := range s.Chan() {
		if st.Stopped(){
			tx.Rollback()
			return
		}
		if len(s.Columns()) != len(msg) {
			sq.fatalerr(fmt.Errorf("expected %v columns but got %v", len(s.Columns()), len(msg)), l)
			tx.Rollback() //discard error - best effort attempt
			return
		}
		if stmt == nil {
			sq.columns = s.Columns()
			insertQuery := sq.prepare(s, msg)
			stmt, err = tx.Prepare(insertQuery)
			if err != nil {
				sq.fatalerr(err, l)
				tx.Rollback()
				return
			}
		}
		_, err := stmt.Exec(msg...)
		if err != nil {
			sq.fatalerr(err, l)
			tx.Rollback()
			return
		}
	}
	err = tx.Commit()
	if err != nil {
		sq.fatalerr(err, l)
	}
}

//prepare creates the prepared statement
func (sq *SQLDestination) prepare(s Stream, msg []interface{}) string {
	cols := strings.Join(s.Columns(), ",")
	params := strings.Repeat("?,", len(msg))
	params = params[0:len(params)-1] //remove trailing comma
	return fmt.Sprintf(InsertQuery, sq.Table, cols, params)
}

