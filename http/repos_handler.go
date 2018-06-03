package main

import (
	"github.com/jinzhu/gorm"
	"github.com/labstack/echo"
	"github.com/michaelbironneau/analyst/http/models"
	"strconv"
)

func listRepos(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		repos, err := GetRepositories(db)
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.JSON(200, repos)
	}
}

func createRepo(db *gorm.DB, dir string) func(echo.Context) error {
	return func(c echo.Context) error {
		var r struct {
			models.Repository
			AuthPassword string `json:"auth_password"`
		}
		if err := c.Bind(&r); err != nil {
			return echo.NewHTTPError(400, err.Error())
		}
		if err := r.Clone(db, dir, r.AuthPassword); err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		if err := r.UpdateStats(db); err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.JSON(200, r.Repository)

	}
}

func pullRepo(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		var r struct {
			Password string `json:"password"`
		}
		if err := c.Bind(&r); err != nil {
			return echo.NewHTTPError(400, err.Error())
		}
		id := c.Param("id")
		idInt, err := strconv.Atoi(id)
		if err != nil {
			return echo.NewHTTPError(404, "Repository not found")
		}
		var rr models.Repository
		err = db.Where("id = ?", idInt).First(&rr).Error
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		err = rr.Pull(r.Password)
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		err = rr.UpdateStats(db)
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.JSON(200, rr)
	}
}

func deleteRepo(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		id := c.Param("id")
		idInt, err := strconv.Atoi(id)
		if err != nil {
			return echo.NewHTTPError(404, "Repository not found")
		}
		var rr models.Repository
		err = db.Where("id = ?", idInt).First(&rr).Error
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		err = rr.Delete(db)
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.NoContent(204)
	}
}

func listRepoFiles(db *gorm.DB) func(echo.Context) error {
	return func(c echo.Context) error {
		id := c.Param("id")
		idInt, err := strconv.Atoi(id)
		if err != nil {
			return echo.NewHTTPError(404, "Repository not found")
		}
		var rr models.Repository
		err = db.Where("id = ?", idInt).First(&rr).Error
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		files, err := rr.Files()
		if err != nil {
			return echo.NewHTTPError(500, err.Error())
		}
		return c.JSON(200, files)
	}
}
