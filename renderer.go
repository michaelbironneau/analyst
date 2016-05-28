package main 

import (
	"html/template"
	"io"
	"github.com/labstack/echo"
)

type View struct {
    templates *template.Template
}


func (t *View) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
    return t.templates.ExecuteTemplate(w, name, data)
}