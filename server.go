package main

import (
	"github.com/jinzhu/gorm"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/fasthttp"
	"html/template"
	"io/ioutil"
	"mime/multipart"
	"net/http"
)

//DataFunc is a function that returns view data
type DataFunc func(c echo.Context) (map[string]interface{}, error)

//converts a file to a byte slice
func readFile(f *multipart.FileHeader) ([]byte, error) {
	src, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer src.Close()
	return ioutil.ReadAll(src)
}

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
func renderView(status int, view string, requireAdmin bool, requireAnalyst bool, gets ...DataFunc) echo.HandlerFunc {
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
		data := make(map[string]interface{})
		for i := range gets {
			d, err := gets[i](c)
			if err != nil {
				return c.Render(http.StatusInternalServerError, "error", map[string]interface{}{"Message": err.Error()})
			}
			mergeMaps(data, d)
		}
		return c.Render(status, view, data)
	}
}

//mergeMaps merges maps m1 and m2 - m2 has precedence over m1 in the event of conflicting keys.
func mergeMaps(m1 map[string]interface{}, m2 map[string]interface{}) map[string]interface{} {
	var ret map[string]interface{}
	for k, v := range m1 {
		ret[k] = v
	}
	for k, v := range m2 {
		ret[k] = v
	}
	return ret
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
	e.GET("/groups", renderView(http.StatusOK, "groups", false, false, Group{}.List))
	e.POST("/groups", renderView(http.StatusOK, "groups", true, false, Group{}.Save))
	e.GET("/groups/:group_id", renderView(http.StatusOK, "group", true, false, Group{}.Get))
	e.POST("/groups/:group_id/delete", renderView(http.StatusOK, "group", true, false, Group{}.Delete))
	e.GET("/groups/new", renderView(http.StatusOK, "group_edit", true, false))
	e.GET("/groups/:group_id/edit", renderView(http.StatusOK, "group_edit", true, false, Group{}.Get))

	//USERS
	e.GET("/users", renderView(http.StatusOK, "users", true, false, User{}.List))
	e.POST("/users", renderView(http.StatusOK, "users", true, false, User{}.Save))
	e.GET("/users/:user_id", renderView(http.StatusOK, "user", true, false, User{}.Get))
	e.POST("/users/:user_id/delete", renderView(http.StatusOK, "user", true, false, User{}.Delete))
	e.GET("/users/new", renderView(http.StatusOK, "user_edit", true, false, Group{}.List))
	e.GET("/users/:user_id/edit", renderView(http.StatusOK, "user_edit", true, false, User{}.Get, Group{}.List))

	//REPORT TEMPLATES
	e.GET("/templates", renderView(http.StatusOK, "templates", false, false, Template{}.List))
	e.GET("/templates/:template_id", renderView(http.StatusOK, "template", false, false, Template{}.Get))
	e.POST("/templates", renderView(http.StatusOK, "templates", false, true, Template{}.List))
	e.POST("/templates/:template_id/delete", renderView(http.StatusOK, "templates", false, false, Template{}.Delete))
	e.GET("/templates/new", renderView(http.StatusOK, "template_edit", false, true))
	e.GET("/templates/:template_id/edit", renderView(http.StatusOK, "template_edit", false, true, Template{}.Get))

	//SCRIPTS
	e.GET("/scripts", renderView(http.StatusOK, "scripts", false, true, Script{}.List))
	e.GET("/scripts/:script_id", renderView(http.StatusOK, "script", false, true, Script{}.Get))
	e.GET("/scripts/:script_id/download", Script{}.Download)
	e.POST("/scripts", renderView(http.StatusOK, "scripts", false, true, Script{}.Save))
	e.POST("/scripts/:script_id/delete", renderView(http.StatusOK, "scripts", false, true, Script{}.Delete))
	e.GET("/scripts/new", renderView(http.StatusOK, "script_edit", false, true, Group{}.List))
	e.GET("/scripts/:script_id/edit", renderView(http.StatusOK, "script_edit", false, true, Script{}.Get, Group{}.List))

	//REPORTS
	e.GET("/templates/:template_id/reports", renderView(http.StatusOK, "reports", false, false, Report{}.List))
	e.POST("/templates/:template_id/reports", renderView(http.StatusOK, "report", false, false, Report{}.Create)) //create new report
	e.GET("/templates/:template_id/reports/:report_id", renderView(http.StatusOK, "report", false, false, Report{}.Get))
	e.GET("/templates/:template_id/reports/:report_id/download", Report{}.Download)
	e.POST("/templates/:template_id/reports/:report_id/delete", renderView(http.StatusOK, "reports", false, false, Report{}.Delete))

	//CONNECTIONS
	e.GET("/connections", renderView(http.StatusOK, "connections", false, true, Connection{}.List))
	e.GET("/connections/:connection_id", renderView(http.StatusOK, "connection", false, true, Connection{}.Get))
	e.POST("/connections", renderView(http.StatusOK, "connections", false, true, Connection{}.List))
	e.POST("/connections/:connection_id/delete", renderView(http.StatusOK, "connections", false, true, Connection{}.Delete))

}

//runServer runs the web server and blocks
func runServer(e *echo.Echo) {
	e.Run(fasthttp.New(":8989"))
}
