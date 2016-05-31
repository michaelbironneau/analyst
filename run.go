package main

import (
	"fmt"
	"github.com/michaelbironneau/analyst/aql"
	"github.com/tealeg/xlsx"
	"github.com/urfave/cli"
	"gopkg.in/cheggaaa/pb.v1"
	"io/ioutil"
	"time"
)

//Run creates an Excel spreadsheet based on the script
func Run(c *cli.Context) error {
	scriptPath := c.String("script")
	if len(scriptPath) == 0 {
		fmt.Println("Script path not set")
		return nil
	}
	b, err := ioutil.ReadFile(scriptPath)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	script, err := aql.Load(string(b))
	if err != nil {
		fmt.Println(err)
		return nil
	}
	//Parse and set parameters
	var params map[string]string
	if c.Bool("i") {
		params, err = promptParameters(script)
	} else {
		params, err = parseParameters(c.String("params"))
	}

	if err != nil {
		fmt.Println(err)
		return nil
	}
	for k, v := range params {
		if err := script.SetParameter(k, v); err != nil {
			fmt.Println(err)
			return nil
		}
	}
	//Compile script
	var task *aql.Report
	if task, err = script.ExecuteTemplates(); err != nil {
		fmt.Println(err)
		return nil
	}
	//Get connections
	connections := make(map[string]aql.Connection)
	for _, connection := range task.Connections {
		c, err := parseConn(connection)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		for k, v := range c {
			connections[k] = v
		}
	}
	//Get template
	templateFile, err := xlsx.OpenFile(task.TemplateFile)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	//Run script
	progress := make(chan int)
	done := make(chan bool, 1)
	bar := pb.StartNew(100)
	var totalProgress int
	go func() {
		for {
			select {
			case p := <-progress:
				totalProgress += p
				bar.Add(p)
				if totalProgress >= 100 {
					return
				}
			case <-done:
				return
			}
		}
	}()
	report, err := task.Execute(aql.DBQuery, templateFile, connections, progress)
	done <- true
	if err != nil {
		fmt.Printf("[ERROR] %v\n", err)
		return nil
	}
	if err := report.Save(task.OutputFile); err != nil {
		fmt.Printf("[ERROR] %v\n", err)
	}
	bar.Add(100 - totalProgress)
	time.Sleep(time.Millisecond * 500) //otherwise the progress bar may not finish rendering
	fmt.Println("\n[SUCCESS] Spreadsheet written to file")
	return nil
}
