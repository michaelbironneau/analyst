package engine

import (
	"fmt"
	"github.com/olekukonko/tablewriter"
	"os"
	"time"
)

const ConsoleDestinationName = "CONSOLE"

type ConsoleDestination struct {
	Name   string
	cols   []string
	result [][]string
}

func (cd *ConsoleDestination) Ping() error { return nil }

func (cd *ConsoleDestination) Open(s Stream, l Logger, st Stopper) {
	if cd.Name == "" {
		cd.Name = ConsoleDestinationName
	}
	inChan := s.Chan(cd.Name)
	l.Chan() <- Event{
		Source:  cd.Name,
		Level:   Trace,
		Time:    time.Now(),
		Message: "Console destination opened",
	}
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

	l.Chan() <- Event{
		Source:  cd.Name,
		Level:   Info,
		Time:    time.Now(),
		Message: fmt.Sprintf("Processed %v rows", len(cd.result)),
	}
	table := tablewriter.NewWriter(os.Stdout)

	table.SetHeader(cd.cols)
	for _, v := range cd.result {
		table.Append(v)
	}
	table.Render()
	l.Chan() <- Event{
		Source:  cd.Name,
		Level:   Info,
		Time:    time.Now(),
		Message: "Console destination closed",
	}
}
