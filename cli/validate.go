package main

import (
	"github.com/urfave/cli"
	"io/ioutil"
	"github.com/michaelbironneau/analyst/aql"
	"fmt"
)

//Validate validates the script
func Validate(c *cli.Context) error {
	scriptPath := c.String("script")
	if len(scriptPath) == 0 {
		return fmt.Errorf("Script path not set")
	}
	b, err := ioutil.ReadFile(scriptPath)
	if err != nil {
		return err
	}
	_, err = aql.Load(string(b))
	if err == nil {
		fmt.Println("[OK] Script is valid")
	}
	return err
}
