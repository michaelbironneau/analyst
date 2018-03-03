package engine

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
	"errors"
)

type SQLDestination struct {
	Name             string
	Driver           string
	ConnectionString string
	Table            string `aql:"TABLE"`
	Tx               *sql.Tx
	columns          []string
	manageTx         bool `aql:"MANAGED_TRANSACTION,optional"`
	RowsPerBatch     int  `aql:"ROWS_PER_BATCH,optional"`
	db               *sql.DB
	TxUseFunc        func() (*sql.Tx, error)
	TxReleaseFunc    func()
	Alias            string
}

const InsertQuery = `INSERT INTO %s (%s) VALUES (%s)`
var ErrManagedCannotBatch = errors.New("ROWS_PER_BATCH can only be specified if MANAGED_TRANSACTION is false")

func (sq *SQLDestination) Columns() []string {
	return sq.columns
}

func (sq *SQLDestination) connect() error {
	var err error
	sq.db, err = SQLDriverManager.DB(sq.Driver, sq.ConnectionString)
	if err != nil {
		return fmt.Errorf("SQL destination: %s", err.Error())
	}
	return nil
}

func (sq *SQLDestination) Ping() error {
	if sq.RowsPerBatch > 0 && sq.TxUseFunc != nil {
		return ErrManagedCannotBatch
	}
	if sq.db == nil {
		err := sq.connect()
		if err != nil {
			return err
		}
	}
	return sq.db.Ping()
}

func (sq *SQLDestination) fatalerr(err error, l Logger, st Stopper) {
	l.Chan() <- Event{
		Level:   Error,
		Source:  sq.Name,
		Time:    time.Now(),
		Message: err.Error(),
	}
	st.Stop()
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
	if sq.TxReleaseFunc != nil {
		defer sq.TxReleaseFunc()
	}
	sq.manageTx = sq.TxUseFunc == nil
	if sq.db == nil {
		err := sq.connect()
		if err != nil {
			sq.fatalerr(err, l, st)
			return
		}
	}
	sq.log(l, Info, "SQL destination opened")
	var (
		tx  *sql.Tx
		err error
	)
	if sq.manageTx {
		tx, err = sq.db.Begin()
		sq.log(l, Trace, "Initiated transaction")
	} else {
		tx, err = sq.TxUseFunc()
	}
	if err != nil {
		sq.fatalerr(err, l, st)
		return
	}
	var (
		stmt *sql.Stmt
		inserted int
		rowsInBatch int
	)
	for msg := range s.Chan(sq.Alias) {
		if st.Stopped() {
			sq.log(l, Warning, "SQL destination aborted")
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
		if sq.RowsPerBatch > 0 && rowsInBatch == sq.RowsPerBatch {
			//unmanaged transaction commit + reset transaction information
			if err := tx.Commit(); err != nil {
				sq.fatalerr(err, l, st)
				return
			}
			sq.log(l, Info, fmt.Sprintf("Committed batch with %v rows", rowsInBatch))
			tx, err = sq.db.Begin()
			if err != nil {
				sq.fatalerr(err, l, st)
				return
			}
			insertQuery := sq.prepare(s, msg.Data)
			stmt, err = tx.Prepare(insertQuery)
			if err != nil {
				sq.fatalerr(err, l, st)
				return
			}
			rowsInBatch = 0
		}
		sq.log(l, Trace, fmt.Sprintf("Row %v", msg.Data))
		if len(s.Columns()) != len(msg.Data) {
			sq.fatalerr(fmt.Errorf("expected %v columns but got %v", len(s.Columns()), len(msg.Data)), l, st)
			if !sq.manageTx {
				return
			}
			tx.Rollback() //discard error - best effort attempt
			return
		}
		if stmt == nil {
			sq.columns = s.Columns()
			sq.log(l, Trace, fmt.Sprintf("Found columns %v", sq.columns))
			insertQuery := sq.prepare(s, msg.Data)
			stmt, err = tx.Prepare(insertQuery)
			if err != nil {
				sq.fatalerr(err, l, st)
				if !sq.manageTx {
					return
				}
				tx.Rollback()
				return
			}
		}
		_, err := stmt.Exec(msg.Data...)
		if err != nil {
			sq.fatalerr(err, l, st)
			if !sq.manageTx {
				return
			}
			tx.Rollback()
			return
		}
		inserted++
		rowsInBatch++
		if inserted%1000==0{
			sq.log(l, Info, fmt.Sprintf("Inserted %v rows", inserted))
		}
	}
	if !sq.manageTx {
		return
	}
	sq.log(l, Info, "Done - committing transaction")
	err = tx.Commit()
	if err != nil {
		sq.fatalerr(err, l, st)
	}
}

//prepare creates the prepared statement
func (sq *SQLDestination) prepare(s Stream, msg []interface{}) string {
	cols := strings.Join(s.Columns(), ",")
	params := strings.Repeat("?,", len(msg))
	params = params[0 : len(params)-1] //remove trailing comma
	return fmt.Sprintf(InsertQuery, sq.Table, cols, params)
}
