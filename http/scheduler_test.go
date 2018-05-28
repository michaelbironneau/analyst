package main

import (
	"context"
	"github.com/jinzhu/gorm"
	"github.com/labstack/echo"
	"github.com/labstack/gommon/log"
	"github.com/michaelbironneau/analyst/http/models"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
	"time"
)

func TestScheduler(t *testing.T) {
	defer cleanupDB()
	Convey("Given a scheduler and some tasks", t, func() {
		os.Remove(testDBFile)
		db, err := gorm.Open("sqlite3", testDBFile)
		So(err, ShouldBeNil)
		db.Exec("PRAGMA foreign_keys = ON")
		db.LogMode(true)
		defer db.Close()
		So(MigrateDb(db, testDBFile), ShouldBeNil)
		logger := echo.New().Logger
		logger.SetLevel(log.DEBUG)
		logger.SetOutput(os.Stdout)

		Convey("It should correctly schedule a single invocation, creating invocation", func() {
			task := &models.Task{
				Name:      "A invocation",
				Schedule:  "@daily",
				Command:   "echo",
				Arguments: "hello, world",
				Enabled:   false,
			}
			err = task.Create(db)
			So(err, ShouldBeNil)
			err = task.Enable(db)
			So(err, ShouldBeNil)
			tt, err := GetTasks(db)
			So(err, ShouldBeNil)
			So(tt, ShouldHaveLength, 1)
			s := NewScheduler(db, context.Background(), logger)

			n := time.Now()
			now := n.Add(time.Hour * 48)
			n = n.Add(time.Hour * 24)
			expectedScheduledTime := time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, time.UTC)
			tasks, err := s.Next(now)
			So(err, ShouldBeNil)
			So(tasks, ShouldHaveLength, 1)
			time.Sleep(time.Millisecond * 100)
			So(tasks[0].NextRun.Before(now), ShouldBeTrue)
			//Check that invocation was correctly created
			i, err := tasks[0].GetInvocations(db)
			So(err, ShouldBeNil)
			So(i, ShouldHaveLength, 1)
			So(i[0].TaskID, ShouldEqual, tasks[0].ID)
			So(i[0].ScheduledAt.Year(), ShouldEqual, expectedScheduledTime.Year())
			So(i[0].ScheduledAt.Month(), ShouldEqual, expectedScheduledTime.Month())
			So(i[0].ScheduledAt.Day(), ShouldEqual, expectedScheduledTime.Day())
			So(i[0].ScheduledAt.Hour(), ShouldEqual, expectedScheduledTime.Hour())
			So(i[0].Finish, ShouldNotBeNil)
			So(i[0].Start, ShouldNotBeNil)
			output := <-s.InvocationOutput
			So(output, ShouldStartWith, "hello, world")
		})

		Convey("It should successfully interrupt a long-running invocation", func() {
			task := &models.Task{
				Name:      "A invocation",
				Schedule:  "@daily", //  the @ notation should force it to run straight away
				Command:   "bash",
				Arguments: "-c 'sleep 10; echo hello'",
				Enabled:   false,
			}
			err = task.Create(db)
			So(err, ShouldBeNil)
			err = task.Enable(db)
			So(err, ShouldBeNil)
			s := NewScheduler(db, context.Background(), echo.New().Logger)
			now := time.Now().Add(time.Hour * 48)
			tasks, err := s.Next(now)
			So(err, ShouldBeNil)
			So(tasks, ShouldHaveLength, 1)
			time.Sleep(time.Millisecond * 100)
			So(tasks[0].NextRun.Before(now), ShouldBeTrue)
			//Check that invocation was correctly created
			i, err := tasks[0].GetInvocations(db)
			So(err, ShouldBeNil)
			So(i, ShouldHaveLength, 1)
			So(i[0].TaskID, ShouldEqual, tasks[0].ID)
			So(i[0].Finish, ShouldNotBeNil)
			So(i[0].Start, ShouldNotBeNil)
			So(i[0].ErrorMessage, ShouldNotBeBlank) //cancellation will show up as error message
			s.Shutdown()
			output := <-s.InvocationOutput
			So(output, ShouldBeBlank)
		})

	})
}
