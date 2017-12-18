package engine

import (
	"sync"
	"fmt"
	"time"
	"strings"
)

const (
	errParameterAlreadyExists = "parameter '%s' already exists"
	errNameNotDeclared = "parameter '%s' needs to be declared before it can be used"
	ParameterTableName = "PARAMETERS"
)


type parameterTable struct {
	sync.Mutex
	allowedNames map[string]bool
	values map[string]interface{}
}

func newParameterTable() *parameterTable {
	return &parameterTable{
		allowedNames: make(map[string]bool),
		values: make(map[string]interface{}),
	}
}

func (p *parameterTable) Declare(name string) error {
	p.Lock()
	defer p.Unlock()
	if p.allowedNames[strings.ToLower(name)] {
		return fmt.Errorf(errParameterAlreadyExists, name)
	}
	p.allowedNames[strings.ToLower(name)] = true
	return nil
}

func (p *parameterTable) Get(name string) (interface{}, bool){
	p.Lock()
	defer p.Unlock()
	val, ok := p.values[strings.ToLower(name)]
	return val, ok
}

func (p *parameterTable) Set(name string, value interface{}) error {
	p.Lock()
	defer p.Unlock()
	if !p.allowedNames[strings.ToLower(name)]{
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

 func (p *parameterTable) Ping() error {
 	return nil
 }

 func (p *parameterTable) Open(s Stream, l Logger, st Stopper){
	 l.Chan() <- Event{
	 	 Source: "Parameter Table",
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
		 	err := p.Set(cols[i], msg.Data[i])
		 	if err != nil {
				l.Chan() <- Event{
					Source: "Parameter Table",
					Level:   Error,
					Time:    time.Now(),
					Message: err.Error(),
				}
			}
		 }
	 }
 }