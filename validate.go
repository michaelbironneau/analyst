package main

import (
	"fmt"
	"github.com/michaelbironneau/analyst/aql"
	"github.com/urfave/cli"
	"io/ioutil"
)

//Validate validates the script
func Validate(c *cli.Context) error {
	scriptPath := c.String("script")
	if len(scriptPath) == 0 {
		fmt.Println("Script path not set")
	}
	b, err := ioutil.ReadFile(scriptPath)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	_, err = aql_old.Load(string(b))
	if err == nil {
		fmt.Println("[OK] Script is valid")
		return nil
	}
	fmt.Printf("[FAIL] %v", err)
	return nil
}
