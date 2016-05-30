package main

import (
	"github.com/urfave/cli"
	"os"
)



func main() {
	app := cli.NewApp()
	app.Name = "analyst"
	app.Commands = []cli.Command{
		{
			Name:    "run",
			Aliases: []string{"r"},
			Usage:   "runs a script",
			Action:  Run,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "script",
					Value: ".analyst",
					Usage: "path to script",
				},
				cli.StringFlag{
					Name:  "params",
					Value: ":",
					Usage: "script parameters, written as \"name:value;name_2:value_2;...\"",
				},
				cli.BoolFlag{
					Name: "i",
					Usage: "interactive mode (enter parameters on STDIN)",
				},
			},
		},
		{
			Name:    "validate",
			Aliases: []string{"v"},
			Usage:   "validates a script",
			Action:  Validate,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "script",
					Value: ".analyst",
					Usage: "path to script",
				},
			},
		},
	}
	app.Run(os.Args)

}