package engine

import (
	"database/sql"
	"sync"
)

type sqlDriverManager struct {
	sync.Mutex
	dbs map[string]*sql.DB
}

//SQLDriverManager is a singleton that makes sure there is only a single DB object per connection, rather than one per
//source/destination
var SQLDriverManager sqlDriverManager

const driverManagerSeparator = "<<>>"

func init() {
	SQLDriverManager = sqlDriverManager{
		dbs: make(map[string]*sql.DB),
	}
}

func (s *sqlDriverManager) DB(driver, connectionString string) (*sql.DB, error) {
	s.Lock()
	defer s.Unlock()
	if db, ok := s.dbs[driver+driverManagerSeparator+connectionString]; ok {
		return db, nil
	}
	db, err := sql.Open(driver, connectionString)
	//db.SetMaxOpenConns(1)
	//db.SetMaxIdleConns(1)
	if err == nil {
		s.dbs[driver+driverManagerSeparator+connectionString] = db
	}
	return db, err
}
