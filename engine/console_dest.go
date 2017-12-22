package engine

import (
	"fmt"
	"github.com/olekukonko/tablewriter"
	"os"
)

const ConsoleDestinationName = "CONSOLE"

type ConsoleDestination struct {
	Name string
	cols []string
	result [][]string
}

func (cd *ConsoleDestination) Ping() error {return nil}

func (cd *ConsoleDestination) Open(s Stream, l Logger, st Stopper) {
	if cd.Name == "" {
		cd.Name = ConsoleDestinationName
	}
	inChan := s.Chan(cd.Name)

	var firstTime = true
	for msg := range inChan {
		if firstTime {
			firstTime = false
			cd.cols = s.Columns()
		}
		var s []string
		for _, i := range msg.Data {
			s = append(s, fmt.Sprintf("%v", i))
		}
		cd.result = append(cd.result, s)
	}

	table := tablewriter.NewWriter(os.Stdout)

	table.SetHeader(cd.cols)
	for _, v := range cd.result {
		table.Append(v)
	}
	table.Render()
}
