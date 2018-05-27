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
		db.Exec("PRAGMA foreign_keys = ON")
		db.LogMode(true)
		defer db.Close()
		So(MigrateDb(db, testDBFile), ShouldBeNil)

		So(err, ShouldBeNil)
		Convey("It should create a task correctly", func() {
			task := &models.Task{
				Name:     "A task",
				Schedule: "@daily",
				Command:  "script",
			}
			So(task.Create(db), ShouldBeNil)
			task = &models.Task{
				Name:     "A task",
				Schedule: "@daily",
				Command:  "script",
			}
			So(task.Create(db), ShouldNotBeNil) //violates unique constraint
			task.Name = "Second task"
			So(task.Create(db), ShouldBeNil)
			So(task.ID, ShouldNotEqual, 0)
			tasks, err := GetTasks(db)
			So(err, ShouldBeNil)
			So(tasks, ShouldHaveLength, 2)
			So(tasks[0].Command, ShouldEqual, "script")
			So(tasks[0].ID, ShouldNotEqual, 0)
			So(tasks[0].Enabled, ShouldBeFalse)
			So(tasks[0].Enable(db), ShouldBeNil)
			tasks, err = GetTasks(db)
			So(err, ShouldBeNil)
			So(tasks[0].Enabled || tasks[1].Enabled, ShouldBeTrue)
			s := time.Now()
			s2 := s.Add(time.Second)
			invocation := &models.Invocation{
				TaskID:  task.ID,
				Start:   &s,
				Finish:  &s2,
				Success: true,
			}
			So(invocation.Create(db), ShouldBeNil)
			invocation = &models.Invocation{
				TaskID:  1234, //doesn't exist
				Start:   &s,
				Finish:  &s2,
				Success: true,
			}
			So(invocation.Create(db), ShouldNotBeNil) //violates constraint
			invocations, err := tasks[1].GetInvocations(db)
			So(err, ShouldBeNil)
			So(invocations, ShouldHaveLength, 1)
			So(invocations[0].Success, ShouldBeTrue)
			So(invocations[0].ErrorMessage, ShouldBeBlank)
		})
	})

}
