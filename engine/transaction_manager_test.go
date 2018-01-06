package engine

import (
	"database/sql"
	"github.com/michaelbironneau/analyst/aql"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestSingleConn(t *testing.T) {
	Convey("Given a single connection", t, func() {
		conn := aql.Connection{
			Name:             "Test1",
			Driver:           "sqlite3",
			ConnectionString: "file::memory:?mode=memory&cache=shared",
		}
		l := NewConsoleLogger(Trace)
		Convey("We should be able to register, use and commit", func() {
			tm := NewTransactionManager(l)
			err := tm.Register(conn)
			So(err, ShouldBeNil)
			tx, err := tm.Tx(conn.Name)
			So(err, ShouldBeNil)
			_, err = tx.Exec(`
				CREATE TABLE TxManagerTest (
					id int primary key
				);

				insert into TxManagerTest VALUES (1);
				insert into TxManagerTest VALUES (2);
			`)
			So(err, ShouldBeNil)
			err = tm.Commit()
			So(err, ShouldBeNil)
			db, err := sql.Open(conn.Driver, conn.ConnectionString)
			rows, err := db.Query("SELECT id FROM TxManagerTest")
			So(err, ShouldBeNil)
			var count int
			defer rows.Close()
			for rows.Next() {
				count++
			}
			So(count, ShouldEqual, 2)
		})
		Convey("We should be able to register, use and roll back", func() {
			tm := NewTransactionManager(l)
			err := tm.Register(conn)
			So(err, ShouldBeNil)
			tx, err := tm.Tx(conn.Name)
			So(err, ShouldBeNil)
			_, err = tx.Exec(`
				CREATE TABLE TxManagerTest2 (
					id int primary key
				);

				insert into TxManagerTest2 VALUES (1);
				insert into TxManagerTest2 VALUES (2);
			`)
			So(err, ShouldBeNil)
			err = tm.Rollback()
			So(err, ShouldBeNil)
			db, err := sql.Open(conn.Driver, conn.ConnectionString)
			_, err = db.Query("SELECT id FROM TxManagerTest2")
			So(err, ShouldNotBeNil) //CREATE TABLE is transactional in sqlite3
		})
	})
}
