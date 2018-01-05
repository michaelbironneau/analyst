package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/michaelbironneau/analyst/aql"
	"sync"
	"time"
)

const TxManagerMaxRetries = 32

var ErrTransactionManagerFinished = errors.New("transaction manager is in a committed or rolled back state and can no longer provide new transactions")

//  TransactionManager provides a single transaction per connection, to be used
//  by all components that read or write from the connection. All transactions
//  are then either committed or rolled back together. It is a 2PC Tx manager.
//  Only supported for connections implementing sql.Tx for now.
type TransactionManager interface {
	//  Register makes the connection known to the connection manager. It does
	//  NOT begin a new transaction.
	Register(aql.Connection) error

	//  Use will begin a new transaction (if none exists) or re-use the existing
	//  transaction, applying a func to it.
	Tx(connection string) (*sql.Tx, error)

	//  Commit commits ALL transactions. It is an error to call Use() or Register()
	//  after Commit().
	Commit() error

	//  Rollback rolls back ALL transactions. It is an error to call Use() or Register()
	//  after Commit().
	Rollback() error
}

//transactionManager is the default implementation of TransactionManager.
type transactionManager struct {
	sync.RWMutex
	finished bool
	l        Logger
	txs      map[string]*sql.Tx
	conns    map[string]aql.Connection
	dbs      map[string]*sql.DB
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewTransactionManager(l Logger) TransactionManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &transactionManager{
		txs:    make(map[string]*sql.Tx),
		conns:  make(map[string]aql.Connection),
		dbs:    make(map[string]*sql.DB),
		ctx:    ctx,
		cancel: cancel,
		l:      l,
	}
}

func (tm *transactionManager) log(level LogLevel, msg string, args ...interface{}) {
	tm.l.Chan() <- Event{
		Time:    time.Now(),
		Source:  "Transaction Manager",
		Level:   level,
		Message: fmt.Sprintf(msg, args...),
	}
}

func (tm *transactionManager) Register(conn aql.Connection) error {
	tm.Lock()
	defer tm.Unlock()

	if tm.finished {
		return ErrTransactionManagerFinished
	}

	tm.conns[conn.Name] = conn
	return nil
}

func (tm *transactionManager) Tx(connName string) (*sql.Tx, error) {
	tm.RLock()
	defer tm.RUnlock()

	if tm.finished {
		return nil, ErrTransactionManagerFinished
	}

	var (
		conn aql.Connection
		ok   bool
	)
	conn, ok = tm.conns[connName]

	if !ok {
		return nil, fmt.Errorf("connection not registered with transaction manager %s", connName)
	}

	if tx, ok := tm.txs[connName]; ok {
		return tx, nil
	}

	tm.log(Trace, "opening new db connection for connection %s", connName)
	//new transaction
	db, err := sql.Open(conn.Driver, conn.ConnectionString)

	if err != nil {
		return nil, err
	}

	//upgrade to write lock and update tx/db maps
	tm.RUnlock()
	tm.Lock()
	tx, err := db.BeginTx(tm.ctx, nil)

	if err != nil {
		tm.Unlock()
		tm.RLock()
		return nil, err
	}
	tm.log(Info, "new transaction initiated for connection %s", connName)
	tm.txs[connName] = tx
	tm.Unlock()
	tm.RLock()

	return tx, nil
}

//  Commit commits all transactions. If it encounters an error, eg. network went down after
//  Commit() was called, it will keep retrying TxManagerMaxRetries until Commit() succeeds
//  or TxManagerMaxRetries is exceeded.
func (tm *transactionManager) Commit() error {
	tm.Lock()
	defer tm.Unlock()
	tm.finished = true
	for name, tx := range tm.txs {
		var retries int
		for {
			err := tx.Commit()
			if err == nil {
				break
			}
			if err == sql.ErrTxDone {
				break
			}
			tm.log(Warning, "(retry attempt %d): error committing transaction for connection %s: %v", retries, name, err)
			retries += 1
			if retries > TxManagerMaxRetries {
				tm.log(Error, "exceeded max retries for connection %s", name)
				return err
			}
			time.Sleep(time.Second * time.Duration(retries))
		}
		tm.log(Info, "committed transaction for connection %s", name)
	}
	tm.log(Info, "committed all transactions")
	return nil
}

//  Commit rolls back all transactions. If it encounters an error, eg. network went down after
//  Rollback() was called, it will keep retrying TxManagerMaxRetries until Rollback() succeeds
//  or TxManagerMaxRetries is exceeded.
func (tm *transactionManager) Rollback() error {
	tm.Lock()
	defer tm.Unlock()
	tm.finished = true
	for name, tx := range tm.txs {
		var retries int
		for {
			err := tx.Rollback()
			if err == nil {
				break
			}
			if err == sql.ErrTxDone {
				break
			}
			tm.log(Warning, "(retry attempt %d): error rolling back transaction for connection %s: %v", retries, name, err)
			retries += 1
			if retries > TxManagerMaxRetries {
				tm.log(Error, "exceeded max retries for connection %s", name)
				return err
			}
			time.Sleep(time.Second * time.Duration(retries))
		}
		tm.log(Info, "rolled back transaction for connection %s", name)
	}
	tm.log(Info, "rolled back all transactions")
	return nil
}
