package main

import (
	"fmt"
	"github.com/michaelbironneau/analyst/aql"
	"github.com/michaelbironneau/analyst/engine"
	"github.com/urfave/cli"
	"time"
)

func Run(c *cli.Context) error {
	var (
		opts []aql.Option
		err  error
	)
	oString := c.String("params")

	if len(oString) > 0 {
		opts, err = aql.StrToOpts(oString)
	}

	if err != nil {
		fmt.Println("Error reading options", err)
		return err
	}

	scriptFile := c.String("script")

	if len(scriptFile) == 0 {
		fmt.Println("Error - script file not set")
		return fmt.Errorf("script file not set")
	}

	l := engine.ConsoleLogger{
		MinLevel: engine.Warning,
	}

	if c.Bool("v") {
		l.MinLevel = engine.Info
	}

	if c.Bool("vv") {
		l.MinLevel = engine.Trace
	}

	err = ExecuteFile(scriptFile, opts, &l, nil)
	time.Sleep(time.Millisecond * 100) //give loggers time to flush
	if err != nil {
		fmt.Printf("Error: %s\n", err)
	}
	return err
}
