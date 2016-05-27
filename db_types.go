package main

import (
	"github.com/asdine/storm"
	"github.com/michaelbironneau/webxcel/wxscript"
)

type ReportTemplate struct {
	ID          int
	Name        string `storm:"unique"`
	Description string
	Parameters  []wxscript.Parameter `storm:"inline"`
	Template    string
}

type User struct {
	ID       int
	Login    string `storm:"unique"`
	PassHash string
	Group    string
}
