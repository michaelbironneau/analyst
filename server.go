package main

import (
	//"net/http"
    "html/template"
	"github.com/labstack/echo"
	"github.com/labstack/echo/engine/fasthttp"
)

func createServer() *echo.Echo {
	e := echo.New()
	return nil
}

func registerRoutes( e *echo.Echo){
    
    v := &View{
        templates: template.Must(template.ParseGlob("views/*.html")),
    }
    e.SetRenderer(v)
    
    //WEB UI
    e.Static("/static", "static")
    e.File("/", "index.html")
   
   e.POST("/login")
    
    //GROUPS
    e.GET("/groups")
    e.POST("/groups")
    e.GET("/groups/:group_id")
    e.DELETE("/groups/:group_id")
    e.PUT("/groups/:group_id")
    
    //USERS
    e.GET("/users")
    e.POST("/users")
    e.GET("/users/:user_id")
    e.PUT("/users/:user_id")
    e.DELETE("/users/:user_id")
    
    //REPORT TEMPLATES
    e.GET("/groups/:group_id/templates")
    e.GET("/groups/:group_id/templates/:template_id")
    e.POST("/groups/:group_id/templates")
    e.PUT("/groups/:group_id/templates/:template_id")
    e.DELETE("/groups/:group_id/templates/:template_id")
    
    //REPORTS
    e.GET("/groups/:group_id/templates/:template_id/reports")
    e.POST("/groups/:group_id/templates/:template_id/reports") //create new report 
    e.GET("/groups/:group_id/templates/:template_id/reports/:report_id")
    e.GET("/groups/:group_id/templates/:template_id/reports/:report_id/download")
    e.DELETE("/groups/:group_id/templates/:template_id/reports/:report_id")
    
    //CONNECTIONS
    e.GET("/connections")
    e.GET("/connections/:connection_id")
    e.POST("/connections")
    e.PUT("/connections/:connection_id")
    e.DELETE("/connections/:connection_id")
    
}



//runServer runs the web server and blocks
func runServer(e *echo.Echo) {
	e.Run(fasthttp.New(":8989"))
}
