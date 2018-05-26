package main

import (
	"github.com/labstack/echo"
	"github.com/jinzhu/gorm"
	"github.com/michaelbironneau/analyst/http/models"
)

func listTasks(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		tasks, err := GetTasks(db)
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.JSON(200, tasks)
	}
}

func createTask(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		var t models.Task
		if err := c.Bind(&t); err != nil {
			return echo.NewHTTPError(400, err.Error())
		}
		err := t.Create(db)
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.JSON(200, t) //response with the full object containing ID
	}
}

func updateTask(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		var t models.Task
		if err := c.Bind(&t); err != nil {
			return echo.NewHTTPError(400, err.Error())
		}
		if t.ID == 0 {
			return echo.NewHTTPError(400, "ID must be specified") //ID > 0, always
		}
		if err := t.Update(db); err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.NoContent(204)
	}
}

func enableTask(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		var t models.Task
		if err := c.Bind(&t); err != nil {
			return echo.NewHTTPError(400, err.Error())
		}
		if t.ID == 0 {
			return echo.NewHTTPError(400, "ID must be specified") //ID > 0, always
		}
		if err := t.Enable(db); err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.NoContent(204)
	}
}

func disableTask(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		var t models.Task
		if err := c.Bind(&t); err != nil {
			return echo.NewHTTPError(400, err.Error())
		}
		if t.ID == 0 {
			return echo.NewHTTPError(400, "ID must be specified") //ID > 0, always
		}
		if err := t.Disable(db); err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.NoContent(204)
	}
}

func deleteTask(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		var t models.Task
		if err := c.Bind(&t); err != nil {
			return echo.NewHTTPError(400, err.Error())
		}
		if t.ID == 0 {
			return echo.NewHTTPError(400, "ID must be specified") //ID > 0, always
		}
		if err := t.Delete(db); err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.NoContent(204)
	}
}