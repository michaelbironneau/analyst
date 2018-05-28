package models

import (
	"github.com/jinzhu/gorm"
	"github.com/robfig/cron"
	"time"
)

//  Model is base for model, copied from gorm.Model
type Model struct {
	ID        uint       `gorm:"primary_key" json:"id"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	DeletedAt *time.Time `sql:"index" json:"deleted_at"`
}

type Task struct {
	Model
	Name      string        `gorm:"type:varchar(128);UNIQUE;NOT_NULL" json:"name"`
	Schedule  string        `gorm:"type:varchar(128);NOT_NULL" json:"schedule"`
	Command   string        `gorm:"NOT_NULL" json:"command"`
	Arguments string        `json:"args"`
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

func (t *Task) Enable(db *gorm.DB) error {
	t.Enabled = true
	n, err := t.NextInvocation(time.Now())
	if err != nil {
		t.NextRun = &n
	}
	return db.Model(t).Updates(map[string]interface{}{"enabled": true, "next_run": n}).Error
}

func (t *Task) Disable(db *gorm.DB) error {
	t.Enabled = false
	t.NextRun = nil
	return db.Model(t).Updates(map[string]interface{}{"enabled": false, "next_run": nil}).Error
}

func (t *Task) Delete(db *gorm.DB) error {
	return db.Delete(t).Error
}

func (t *Task) GetInvocations(db *gorm.DB, limit int) ([]Invocation, error) {
	var invocations []Invocation
	err := db.Model(t).Order("id desc").Limit(limit).Related(&invocations).Error
	return invocations, err
}

func (t *Task) GetLastInvocation(db *gorm.DB) (Invocation, error) {
	var invocation Invocation
	err := db.Where("task_id = ?", t.ID).Last(&invocation).Error
	return invocation, err
}
