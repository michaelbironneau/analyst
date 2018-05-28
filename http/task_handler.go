package main

import (
	"github.com/jinzhu/gorm"
	"github.com/labstack/echo"
	"github.com/michaelbironneau/analyst/http/models"
	"strconv"
)

const DefaultLimit = "50"

func listTasks(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		tasks, err := GetTasks(db)
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.JSON(200, tasks)
	}
}

func listInvocations(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		limit := c.QueryParam("limit")
		if limit == "" {
			limit = DefaultLimit
		}
		limitInt, err := strconv.Atoi(limit)
		if err != nil {
			return echo.NewHTTPError(400, "limit parameter should be a number")
		}
		invocations, err := GetInvocations(db, limitInt)
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.JSON(200, invocations)
	}
}

func getInvocations(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		limit := c.QueryParam("limit")
		if limit == "" {
			limit = DefaultLimit
		}
		limitInt, err := strconv.Atoi(limit)
		if err != nil {
			return echo.NewHTTPError(400, "limit parameter should be a number")
		}
		id := c.Param("id")
		var t models.Task
		idNum, err := strconv.Atoi(id)
		if err != nil || idNum < 0 {
			return echo.NewHTTPError(400, "Invalid ID")
		}
		t.ID = uint(idNum)
		invocations, err := t.GetInvocations(db, limitInt)
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.JSON(200, invocations)
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

func getLastInvocation(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		id := c.Param("id")
		var t models.Task
		idNum, err := strconv.Atoi(id)
		if err != nil || idNum < 0 {
			return echo.NewHTTPError(400, "Invalid ID")
		}
		t.ID = uint(idNum)
		i, err := t.GetLastInvocation(db)
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.JSON(200, i)
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
		t.NextRun = nil // prevent this from being overwritten
		if err := t.Update(db); err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.NoContent(204)
	}
}

func enableTask(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		id := c.Param("id")
		var t models.Task
		idNum, err := strconv.Atoi(id)
		if err != nil || idNum < 0 {
			return echo.NewHTTPError(400, "Invalid ID")
		}
		t.ID = uint(idNum)
		if err := t.Enable(db); err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.NoContent(204)
	}
}

func disableTask(db *gorm.DB, s *Scheduler) func(echo.Context) error {
	return func(c echo.Context) error {
		id := c.Param("id")
		var t models.Task
		idNum, err := strconv.Atoi(id)
		if err != nil || idNum < 0 {
			return echo.NewHTTPError(400, "Invalid ID")
		}
		t.ID = uint(idNum)
		s.Cancel(t)
		if err := t.Disable(db); err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.NoContent(204)
	}
}

func deleteTask(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		id := c.Param("id")
		var t models.Task
		idNum, err := strconv.Atoi(id)
		if err != nil || idNum < 0 {
			return echo.NewHTTPError(400, "Invalid ID")
		}
		t.ID = uint(idNum)
		if err := t.Delete(db); err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.NoContent(204)
	}
}
