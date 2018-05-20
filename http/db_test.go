package main

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/michaelbironneau/analyst/http/models"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
	"time"
)

const testDBFile = "_test.db"

func cleanupDB() {
	if err := os.Remove(testDBFile); err != nil {
		fmt.Println(err)
	}
}

func TestModels(t *testing.T) {
	defer cleanupDB()
	Convey("The database should be created without problems", t, func() {
		os.Remove(testDBFile)
		db, err := gorm.Open("sqlite3", testDBFile)
		db.LogMode(true)
		defer db.Close()
		So(MigrateDb(db, testDBFile), ShouldBeNil)

		So(err, ShouldBeNil)
		Convey("It should create a task correctly", func() {
			task := &models.Task{
				Name:      "A task",
				Schedule:  "@daily",
				ScriptURI: "script",
			}
			So(task.Create(db), ShouldBeNil)
			tasks, err := GetTasks(db)
			So(err, ShouldBeNil)
			So(tasks, ShouldHaveLength, 1)
			So(tasks[0].ScriptURI, ShouldEqual, "script")
			s := time.Now()
			s2 := s.Add(time.Second)
			invocation := &models.Invocation{
				TaskID:  task.TaskID,
				Start:   &s,
				Finish:  &s2,
				Success: true,
			}
			So(invocation.Create(db), ShouldBeNil)
			invocations, err := tasks[0].GetInvocations(db)
			So(err, ShouldBeNil)
			So(invocations, ShouldHaveLength, 1)
			So(invocations[0].Success, ShouldBeTrue)
			So(invocations[0].ErrorMessage, ShouldBeBlank)
		})
	})

}
