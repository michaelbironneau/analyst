package db

import (
	"github.com/boltdb/bolt"
)

//DB is an interface to a key/value store.
type DB interface {
	Open(connectionString string) (DB, error)
	Put(table string, key []byte, value []byte) error
	Get(table string, key []byte) ([]byte, error)
	Delete(table string, key []byte) error
}

//BoltDb is an implementation of DB with Bolt.
type BoltDb struct {
	db *bolt.DB
}

//Open opens a connection to the given database file, or creates a new one.
func (b *BoltDb) Open(connectionString string) (DB, error) {
	db, err := bolt.Open(connectionString, 0600, nil)

	return &BoltDb{
		db: db,
	}, err
}

//Put sets a key in a table (or bucket, in this case)
func (b *BoltDb) Put(table string, key []byte, value []byte) error {
	b.db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists([]byte(table))
		bu := tx.Bucket([]byte(table))
		return b.Put(key, value)
	})
}

func (b *BoltDB) Get(table string, key []byte, value []byte) error {
	return nil
}
