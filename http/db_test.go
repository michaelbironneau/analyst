package main

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/michaelbironneau/analyst/http/models"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"path"
	"testing"
	"time"
)

const (
	testDBFile = "_test.db"
	reposDir   = "./_testrepos"
)

func cleanupDB() {
	if err := os.Remove(testDBFile); err != nil {
		fmt.Println(err)
	}
}

func cleanupRepos() {
	if err := os.RemoveAll(reposDir); err != nil {
		fmt.Println(err)
	}
}

func TestRepositoryModels(t *testing.T) {
	cleanupRepos()
	defer cleanupRepos()
	defer cleanupDB()
	Convey("Given a valid git repository URL", t, func() {
		os.Remove(testDBFile)
		db, err := gorm.Open("sqlite3", testDBFile)
		db.Exec("PRAGMA foreign_keys = ON")
		db.LogMode(true)
		defer db.Close()
		So(MigrateDb(db, testDBFile), ShouldBeNil)
		So(err, ShouldBeNil)
		s := "https://github.com/michaelbironneau/analyst"
		Convey("We should be able to clone it and view commit information", func() {
			r := &models.Repository{
				Name:      "Analyst repository",
				RemoteURL: s,
			}
			So(r.Clone(db, reposDir, ""), ShouldBeNil)
			_, err := os.Stat(path.Join(reposDir, "analyst"))
			So(err, ShouldBeNil)
			repos, err := GetRepositories(db)
			So(err, ShouldBeNil)
			So(repos, ShouldHaveLength, 1)
			So(repos[0].RemoteURL, ShouldEqual, s)
			So(repos[0].Name, ShouldEqual, "Analyst repository")
			So(repos[0].LocalPath, ShouldEqual, path.Join(reposDir, "analyst"))
			//update commit info
			err = repos[0].UpdateStats(db)
			So(err, ShouldBeNil)
			So(repos[0].LastCommitHash, ShouldNotBeBlank)
			So(repos[0].LastCommitAuthor, ShouldNotBeBlank)
			So(repos[0].LastCommitDate, ShouldNotBeNil)
		})
	})
}

func TestSchedulerModels(t *testing.T) {
	defer cleanupDB()
	Convey("The database should be created without problems", t, func() {
		os.Remove(testDBFile)
		db, err := gorm.Open("sqlite3", testDBFile)
		db.Exec("PRAGMA foreign_keys = ON")
		db.LogMode(true)
		defer db.Close()
		So(MigrateDb(db, testDBFile), ShouldBeNil)

		So(err, ShouldBeNil)
		Convey("It should create a invocation correctly", func() {
			task := &models.Task{
				Name:     "A invocation",
				Schedule: "@daily",
				Command:  "script",
			}
			So(task.Create(db), ShouldBeNil)
			task = &models.Task{
				Name:     "A invocation",
				Schedule: "@daily",
				Command:  "script",
			}
			So(task.Create(db), ShouldNotBeNil) //violates unique constraint
			task.Name = "Second invocation"
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
			invocations, err := tasks[1].GetInvocations(db, 100)
			So(err, ShouldBeNil)
			So(invocations, ShouldHaveLength, 1)
			So(invocations[0].Success, ShouldBeTrue)
			So(invocations[0].ErrorMessage, ShouldBeBlank)
		})
	})

}
