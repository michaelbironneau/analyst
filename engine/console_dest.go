package engine

import (
	"encoding/json"
	"fmt"
	"github.com/olekukonko/tablewriter"
	"io"
	"os"
	"time"
)

const ConsoleDestinationName = "CONSOLE"

type ConsoleDestination struct {
	Name         string
	FormatAsJSON bool
	Writer       io.Writer
	cols         []string
	result       [][]string
}

func (cd *ConsoleDestination) Ping() error { return nil }

func (cd *ConsoleDestination) Open(s Stream, l Logger, st Stopper) {
	if cd.Writer == nil {
		cd.Writer = os.Stdout
	}
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

	if cd.FormatAsJSON {
		s, err := cd.marshal()
		if err != nil {
			l.Chan() <- Event{
				Source:  cd.Name,
				Level:   Error,
				Time:    time.Now(),
				Message: fmt.Sprintf("could not marshal %v", err),
			}
			return
		}
		cd.Writer.Write([]byte(s))
		return
	}
	table := tablewriter.NewWriter(cd.Writer)

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

func (cd *ConsoleDestination) marshal() (string, error) {
	var ret []map[string]interface{}
	for i := range cd.result {
		r := make(map[string]interface{})
		for j, col := range cd.result[i] {
			r[cd.cols[j]] = col
		}
		ret = append(ret, r)
	}
	b, err := json.Marshal(ret)
	return string(b), err
}
