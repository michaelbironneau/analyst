package models

import (
	"github.com/jinzhu/gorm"
	"time"
)

type Invocation struct {
	Model
	TaskID       uint       `gorm:"index:ix_invocation_time;" json:"task_id" sql:"type:integer REFERENCES tasks(id)"`
	ScheduledAt  *time.Time `gorm:"index:ix_invocation_time" json:"scheduled_to_start_at"`
	Start        *time.Time `json:"started_at"`
	Finish       *time.Time `json:"finished_at"`
	Success      bool       `json:"success"`
	ErrorMessage string     `json:"error_message"`
}

func (i *Invocation) Create(db *gorm.DB) error {
	return db.Create(i).Error
}

func (i *Invocation) Update(db *gorm.DB) error {
	return db.Save(i).Error
}

func (i *Invocation) Delete(db *gorm.DB) error {
	return db.Delete(i).Error
}
