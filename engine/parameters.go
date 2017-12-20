package engine

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	errParameterAlreadyExists = "parameter '%s' already exists"
	errNameNotDeclared        = "parameter '%s' needs to be declared before it can be used"
	ParameterTableName        = "PARAMETERS"
)

type ParameterTable struct {
	sync.Mutex
	allowedNames map[string]bool
	values       map[string]interface{}
}

func NewParameterTable() *ParameterTable {
	return &ParameterTable{
		allowedNames: make(map[string]bool),
		values:       make(map[string]interface{}),
	}
}

func (p *ParameterTable) Declare(name string) error {
	p.Lock()
	defer p.Unlock()
	if p.allowedNames[strings.ToLower(name)] {
		return fmt.Errorf(errParameterAlreadyExists, name)
	}
	p.allowedNames[strings.ToLower(name)] = true
	return nil
}

func (p *ParameterTable) Get(name string) (interface{}, bool) {
	p.Lock()
	defer p.Unlock()
	val, ok := p.values[strings.ToLower(name)]
	return val, ok
}

func (p *ParameterTable) Set(name string, value interface{}) error {
	p.Lock()
	defer p.Unlock()
	if !p.allowedNames[strings.ToLower(name)] {
		return fmt.Errorf(errNameNotDeclared, name)
	}
	p.values[strings.ToLower(name)] = value
	return nil
}

/**
type Destination interface {

	//Ping checks that the destination is available. It is used to verify
	//the destination at runtime.
	Ping() error

	//Open gives the destination a stream to start pulling from and an error stream
	Open(Stream, Logger, Stopper)
}
*/

type ParameterTableDestination struct {
	cols []string
	p    *ParameterTable
}

func NewParameterTableDestination(p *ParameterTable, cols []string) Destination {
	return &ParameterTableDestination{
		p:    p,
		cols: cols,
	}
}

func (p *ParameterTableDestination) Ping() error {
	return nil
}

func (p *ParameterTableDestination) Open(s Stream, l Logger, st Stopper) {
	l.Chan() <- Event{
		Source:  "Parameter Table",
		Level:   Trace,
		Time:    time.Now(),
		Message: "Parameter table opened",
	}
	var cols []string
	for msg := range s.Chan(ParameterTableName) {
		if st.Stopped() {
			return
		}
		if cols == nil {
			cols = s.Columns()
		}

		for i := range msg.Data {
			if len(msg.Data) != len(p.cols) {
				l.Chan() <- Event{
					Source:  "Parameter Table",
					Level:   Error,
					Time:    time.Now(),
					Message: fmt.Sprintf("Expected %v parameters but got %v", len(p.cols), len(msg.Data)),
				}
			}
			err := p.p.Set(p.cols[i], msg.Data[i])
			if err != nil {
				l.Chan() <- Event{
					Source:  "Parameter Table",
					Level:   Error,
					Time:    time.Now(),
					Message: err.Error(),
				}
			}
		}
	}
}
