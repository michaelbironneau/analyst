package engine

import (
	"database/sql"
	"github.com/denisenkom/go-mssqldb"
	"fmt"
	"strings"
)

const InsertQuery = `INSERT INTO %s (%s) VALUES (%s)`

// SQLInserter inserts rows into a SQL database. It contains driver-specific optimisations:
//	* MS SQL Server: uses bulk copy
// It does not perform any transaction management.
type SQLInserter interface {

	New() SQLInserter

	//Initialize with connection details and database.
	Initialize(l Logger, tableName string, db *sql.DB, cols []string) error

	//Insert a single batch
	InsertBatch(tx *sql.Tx, msgs []Message) error

	//Hook that is called before the transaction manager/etc commits/rollbacks the transaction
	PreCommit() error
}

var Inserters = map[string]SQLInserter{"mssql": &MSSQLInserter{}}

type DefaultInserter struct {
	tableName string
	template string
	cols []string
}

func (d *DefaultInserter) New() SQLInserter {
	return &DefaultInserter{}
}

func (d *DefaultInserter) Initialize(l Logger, tableName string, db *sql.DB, cols []string) error {
	d.tableName = tableName
	d.cols = cols
	d.template = d.Statement()
	return nil
}

func (d *DefaultInserter) Statement() string {
	cols := strings.Join(d.cols, ",")
	params := strings.Repeat("?,", len(d.cols))
	params = params[0 : len(params)-1] //remove trailing comma
	return fmt.Sprintf(InsertQuery, d.tableName, cols, params)
}

func (d *DefaultInserter) InsertBatch(tx *sql.Tx, msgs []Message) error {
	stmt, err := tx.Prepare(d.template)
	defer stmt.Close()
	if err != nil {
		return fmt.Errorf("error preparing statement: %v", err)
	}

	for _, msg := range msgs {
		_, err := stmt.Exec(msg.Data...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DefaultInserter) PreCommit() error {return nil}

type MSSQLInserter struct {
	l Logger
	tableName string
	cols []string
}

func (m *MSSQLInserter) New() SQLInserter {
	return &MSSQLInserter{}
}

func (m *MSSQLInserter) Initialize(l Logger, tableName string, db *sql.DB, cols []string) error {
	m.l = l
	m.tableName = tableName
	m.cols = cols
	return nil
}

func (m *MSSQLInserter) PreCommit() error {return nil}

func (m *MSSQLInserter) InsertBatch(tx *sql.Tx, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}

	stmt, err := tx.Prepare(mssql.CopyIn(m.tableName, mssql.MssqlBulkOptions{}, m.cols...))
	defer stmt.Close()

	if err != nil {
		return fmt.Errorf("error preparing bulk copy statement: %v", err)
	}

	for _, msg := range msgs {
		_, err = stmt.Exec(msg.Data...)
		if err != nil {
			return err
		}
	}

	_, err = stmt.Exec()

	if err != nil {
		return fmt.Errorf("error with bulk copy: %v", err)
	}


	return nil
}