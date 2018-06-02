package main

import (
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/michaelbironneau/analyst/http/models"
)

func MigrateDb(db *gorm.DB, filename string) error {
	// Migrate the schema
	err := db.AutoMigrate(&models.Task{}, &models.Invocation{}, &models.Repository{}).Error
	if err != nil {
		return err
	}
	return nil
}

func GetTasks(db *gorm.DB) ([]models.Task, error) {
	var tasks []models.Task
	err := db.Find(&tasks).Error
	return tasks, err
}

func GetInvocations(db *gorm.DB, limit int) ([]models.Invocation, error) {
	var invocations []models.Invocation
	err := db.Order("id desc").Limit(limit).Find(&invocations).Error
	return invocations, err
}

func GetRepositories(db *gorm.DB) ([]models.Repository, error) {
	var repos []models.Repository
	err := db.Find(&repos).Error
	return repos, err
}
