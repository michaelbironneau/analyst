package main

import (
	"github.com/jinzhu/gorm"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/fasthttp"
	"html/template"
	"net/http"
)

//DataFunc is a function that returns view data
type DataFunc func(c echo.Context) (map[string]interface{}, error)

func createServer() *echo.Echo {
	return echo.New()
}

func addDBToContext(c echo.Context) error {
	db, err := gorm.Open(Config.Database.Driver, Config.Database.ConnectionString)
	db.Set("gorm:insert_option", "ON CONFLICT UPDATE")
	if err != nil {
		return err
	}
	c.Set("db", db)
	return nil
}

//renderView is a helper function to get some data using a DataFunc and render a view using that data
//It short-circuits if admin/analyst privileges are required and they are not present.
func renderView(status int, view string, requireAdmin bool, requireAnalyst bool, get DataFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if err := addDBToContext(c); err != nil {
			return c.Render(http.StatusInternalServerError, "error", map[string]interface{}{"Message": "Error opening connection to database"})
		}
		user, err := getCurrentUser(c)
		if err != nil {
			return c.Render(http.StatusInternalServerError, "error", map[string]interface{}{"Message": err.Error()})
		}
		if !user.IsAdmin && requireAdmin {
			return c.Render(http.StatusUnauthorized, "unauthorized", nil)
		}
		if !(user.IsAnalyst || user.IsAdmin) && requireAnalyst {
			return c.Render(http.StatusUnauthorized, "unauthorized", nil)
		}

		data, err := get(c)
		if err != nil {
			return c.Render(http.StatusInternalServerError, "error", map[string]interface{}{"Message": err.Error()})
		}
		return c.Render(status, view, data)
	}
}

func registerRoutes(e *echo.Echo) {

	v := &View{
		templates: template.Must(template.ParseGlob("views/*.html")),
	}
	e.SetRenderer(v)
	e.Use(validateLogin)
	e.Use(authenticate)

	//WEB UI
	e.Static("/static", "static")
	e.File("/", "index.html")

	//e.POST("/login")

	//GROUPS
	e.GET("/groups", renderView(http.StatusOK, "groups", true, false, Group{}.List))
	e.POST("/groups", Group{}.Save)
	e.GET("/groups/:group_id", renderView(http.StatusOK, "group", true, false, Group{}.Get))
	e.DELETE("/groups/:group_id", Group{}.Delete)
	e.PUT("/groups/:group_id", Group{}.Save)

	//USERS
	e.GET("/users", renderView(http.StatusOK, "users", true, false, User{}.List))
	e.POST("/users", User{}.Save)
	e.GET("/users/:user_id", renderView(http.StatusOK, "user", true, false, User{}.Get))
	e.PUT("/users/:user_id", User{}.Save)
	e.DELETE("/users/:user_id", Group{}.Delete)

	//REPORT TEMPLATES
	e.GET("/templates", renderView(http.StatusOK, "templates", false, false, Template{}.List))
	e.GET("/templates/:template_id", renderView(http.StatusOK, "template", false, false, Template{}.Get))
	//e.POST("/templates")
	//e.PUT("/templates/:template_id")
	e.DELETE("/templates/:template_id", Template{}.Delete)

	//SCRIPTS
	e.GET("/scripts", renderView(http.StatusOK, "templates", false, true, Script{}.List))
	e.GET("/scripts/:script_id", renderView(http.StatusOK, "template", false, true, Script{}.Get))
	e.GET("/scripts/:script_id/download", Script{}.Download)
	//e.POST("/scripts")
	//e.PUT("/scripts/:script_id")
	e.DELETE("/scripts/:script_id", Script{}.Delete)

	//REPORTS
	e.GET("/templates/:template_id/reports", renderView(http.StatusOK, "reports", false, false, Report{}.List))
	//e.POST("/groups/:group_id/templates/:template_id/reports") //create new report
	e.GET("/templates/:template_id/reports/:report_id", renderView(http.StatusOK, "report", false, false, Report{}.Get))
	e.GET("/templates/:template_id/reports/:report_id/download", Report{}.Download)
	e.DELETE("/templates/:template_id/reports/:report_id", Report{}.Delete)

	//CONNECTIONS
	e.GET("/connections", renderView(http.StatusOK, "connections", false, true, Connection{}.List))
	e.GET("/connections/:connection_id", renderView(http.StatusOK, "connection", false, true, Connection{}.Get))
	//e.POST("/connections")
	//e.PUT("/connections/:connection_id")
	e.DELETE("/connections/:connection_id", Connection{}.Delete)

}

//runServer runs the web server and blocks
func runServer(e *echo.Echo) {
	e.Run(fasthttp.New(":8989"))
}
