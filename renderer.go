package main

import (
	"github.com/labstack/echo"
	"html/template"
	"io"
)

type View struct {
	templates *template.Template
}

func (t *View) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
	return t.templates.ExecuteTemplate(w, name, data)
}
