package main

import (
	"github.com/jinzhu/gorm"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/fasthttp"
	"html/template"
	"net/http"
)

//DataFunc is a function that returns view data
type DataFunc func(c echo.Context) (interface{}, error)

func createServer() *echo.Echo {
	e := echo.New()
	return nil
}

func addDBToContext(c echo.Context) error {
	db, err := gorm.Open(Config.Database.Driver, Config.Database.ConnectionString)
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

	e.POST("/login")

	//GROUPS
	e.GET("/groups", renderView(http.StatusOK, "groups", true, false, Group{}.List))
	//e.POST("/groups")
	e.GET("/groups/:group_id", renderView(http.StatusOK, "group", true, false, Group{}.Get))
	//e.DELETE("/groups/:group_id")
	//e.PUT("/groups/:group_id")

	//USERS
	e.GET("/users", renderView(http.StatusOK, "users", true, false, nil))
	//e.POST("/users")
	e.GET("/users/:user_id", renderView(http.StatusOK, "user", true, false, nil))
	//e.PUT("/users/:user_id")
	//e.DELETE("/users/:user_id")

	//REPORT TEMPLATES
	e.GET("/groups/:group_id/templates", renderView(http.StatusOK, "templates", false, false, nil))
	e.GET("/groups/:group_id/templates/:template_id", renderView(http.StatusOK, "template", false, false, nil))
	//e.POST("/groups/:group_id/templates")
	//e.PUT("/groups/:group_id/templates/:template_id")
	//e.DELETE("/groups/:group_id/templates/:template_id")

	//REPORTS
	e.GET("/groups/:group_id/templates/:template_id/reports", renderView(http.StatusOK, "reports", false, false, Report{}.List))
	//e.POST("/groups/:group_id/templates/:template_id/reports") //create new report
	e.GET("/groups/:group_id/templates/:template_id/reports/:report_id", renderView(http.StatusOK, "report", false, false, nil))
	e.GET("/groups/:group_id/templates/:template_id/reports/:report_id/download")
	//e.DELETE("/groups/:group_id/templates/:template_id/reports/:report_id")

	//CONNECTIONS
	e.GET("/connections", renderView(http.StatusOK, "connections", false, true, Connection{}.List))
	e.GET("/connections/:connection_id", renderView(http.StatusOK, "connection", false, true, Connection{}.Get))
	//e.POST("/connections")
	//e.PUT("/connections/:connection_id")
	//e.DELETE("/connections/:connection_id")

}

//runServer runs the web server and blocks
func runServer(e *echo.Echo) {
	e.Run(fasthttp.New(":8989"))
}
