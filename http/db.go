package main

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/michaelbironneau/analyst/http/models"
)

func MigrateDb(db *gorm.DB, filename string) error {
	// Migrate the schema
	err := db.AutoMigrate(&models.Task{}, &models.Invocation{}).Error
	if err != nil {
		return err
	}
	/**err = db.Model(&models.Invocation{}).AddForeignKey("TaskId", "Task(TaskId)", "CASCADE", "RESTRICT").Error
	if err != nil {
		return err
	}**/
	return nil
}

func GetTasks(db *gorm.DB) ([]models.Task, error) {
	var tasks []models.Task
	err := db.Find(&tasks).Error
	return tasks, err
}
