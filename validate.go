package main

import (
	"github.com/urfave/cli"
	"github.com/michaelbironneau/analyst/aql"
	"fmt"
	"github.com/michaelbironneau/analyst/engine"
)

func Validate(c *cli.Context) error {
	var (
		opts []aql.Option
		err error
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

	l := engine.ConsoleLogger{}

	err = ValidateFile(scriptFile, opts, &l)

	if err != nil {
		fmt.Printf("Error: %s\n", err)
	}
	return err
}