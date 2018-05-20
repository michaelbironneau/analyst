package main

import (
	"fmt"
	"github.com/michaelbironneau/analyst"
	"github.com/michaelbironneau/analyst/aql"
	"github.com/michaelbironneau/analyst/engine"
	"github.com/urfave/cli"
	"path/filepath"
)

func Test(c *cli.Context) error {
	var (
		opts []aql.Option
		err  error
	)
	oString := c.String("params")
	if len(opts) > 0 {
		opts, err = aql.StrToOpts(oString)
	}

	if err != nil {
		return err
	}

	scriptFile := c.String("script")

	if len(scriptFile) == 0 {
		return fmt.Errorf("script file not set")
	}
	var lev engine.LogLevel

	lev = engine.Warning

	if c.Bool("v") {
		lev = engine.Info
	}

	if c.Bool("vv") {
		lev = engine.Trace
	}

	l := engine.NewConsoleLogger(lev)

	err = analyst.TestFile(scriptFile, &analyst.RuntimeOptions{Options: opts, Logger: l, ScriptDirectory: filepath.Dir(scriptFile)})

	if err != nil {
		fmt.Printf("Error: %s\n", err)
	}
	return err
}
