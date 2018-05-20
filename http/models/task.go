package models

import (
	"github.com/jinzhu/gorm"
	"github.com/robfig/cron"
	"time"
)

type Task struct {
	gorm.Model
	Name      string        `gorm:"type:varchar(128);UNIQUE;NOT_NULL" json:"name"`
	Schedule  string        `gorm:"type:varchar(128);NOT_NULL" json:"schedule"`
	ScriptURI string        `gorm:"NOT_NULL" json:"script_uri"`
	IsAQL     bool          `json:"is_aql"`
	Enabled   bool          `json:"enabled"`
	Coalesce  bool          `json:"coalesce"`
	NextRun   *time.Time    `json:"next_run"`
	schedule  cron.Schedule `gorm:"-"`
}

func (t *Task) NextInvocation(catchupTime time.Time) (time.Time, error) {
	if t.schedule != nil {
		return t.schedule.Next(catchupTime), nil
	}
	//lazily parse the schedule
	s, err := cron.Parse(t.Schedule)
	if err != nil {
		return time.Now(), err
	}

	t.schedule = s
	return t.schedule.Next(catchupTime), nil
}

func (t *Task) Create(db *gorm.DB) error {
	return db.Create(t).Error
}

func (t *Task) Update(db *gorm.DB) error {
	return db.Save(t).Error
}

func (t *Task) Delete(db *gorm.DB) error {
	return db.Delete(t).Error
}

func (t *Task) GetInvocations(db *gorm.DB) ([]Invocation, error) {
	var invocations []Invocation
	err := db.Model(t).Related(&invocations).Error
	return invocations, err
}
