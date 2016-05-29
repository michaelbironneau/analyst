package main

import (
	"github.com/urfave/cli"
	"github.com/tealeg/xlsx"
	"fmt"
	"io/ioutil"
	"github.com/michaelbironneau/analyst/aql"
	"gopkg.in/cheggaaa/pb.v1"
)

//Create creates an Excel spreadsheet based on the script
func Create(c *cli.Context) error {
	scriptPath := c.String("script")
	if len(scriptPath) == 0 {
		return fmt.Errorf("Script path not set")
	}
	b, err := ioutil.ReadFile(scriptPath)
	if err != nil {
		return err
	}
	script, err := aql.Load(string(b))
	if err != nil {
		return err
	}
	//Parse and set parameters 
	params, err := parseParameters(c.String("params"))
	if err != nil {
		return err
	}
	for k, v := range params {
		if err := script.SetParameter(k, v); err != nil {
			return err
		}
	}
	//Compile script
	var task *aql.Report
	if task, err = script.ExecuteTemplates(); err != nil {
		return err
	}
	//Get connections
	connections := make(map[string]aql.Connection)
	for _, connection := range task.Connections {
		c, err := parseConn(connection)
		if err != nil {
			return err
		}
		for k, v := range c {
			connections[k] = v
		}
	}
	//Get template 
	templateFile, err := xlsx.OpenFile(task.TemplateFile)
	if err != nil {
		return err
	}
	//Run script 
	progress := make(chan int)
	errs := make(chan bool)
	bar := pb.StartNew(100)
	report, err := task.Execute(aql.DBQuery, templateFile, connections, progress)
	go func(){
		for {
			select {
				case p := <- progress:
					bar.Add(p)
				case <- errs:
					return
			}
		}		
	}()
	return report.Save(task.OutputFile)
}
