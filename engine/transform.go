package engine

import (
	"fmt"
	"sync"
	"time"
)

//Transform is a component that is neither a source nor a sink. It is configured with
//one or more sources, and one or more sinks.
type Transform interface {
	//Open gives the transform a stream to start pulling from
	Open(source Stream, dest Stream, logger Logger, stop Stopper)
}

//Condition is a func that returns true if the message passes the test and false otherwise.
type Condition func([]interface{}) bool

type testNode struct {
	names []string
	descs []string
	conds []Condition
}

func (tn *testNode) Add(name string, desc string, cond Condition) {
	tn.names = append(tn.names, name)
	tn.descs = append(tn.descs, desc)
	tn.conds = append(tn.conds, cond)
}

func (tn *testNode) Ping() error {
	return nil
}

func (tn *testNode) Open(s Stream, dest Stream, l Logger, st Stopper) {
	var firstMessage = true
	for msg := range s.Chan() {
		if firstMessage {
			dest.SetColumns(s.Columns())
			firstMessage = false
		}
		for i := range tn.conds {
			if !tn.conds[i](msg) {
				l.Chan() <- Event{
					Source:  tn.names[i],
					Message: fmt.Sprintf("[FAIL] %s", tn.descs[i]),
					Time:    time.Now(),
					Level:   Error,
				}
				st.Stop()
				close(dest.Chan())
				return //a test should stop the job on first failure
			}
		}
		dest.Chan() <- msg
	}
}

type Passthrough struct {
	sync.Mutex
	inputs int
}

func (p *Passthrough) Open(source Stream, dest Stream, logger Logger, stop Stopper) {
	logger.Chan() <- Event{
		Level:   Trace,
		Time:    time.Now(),
		Message: "Passthrough transform opened",
	}
	p.Lock()
	p.inputs++
	p.Unlock()
	destChan := dest.Chan()
	for msg := range source.Chan() {
		destChan <- msg
		if stop.Stopped() {
			close(destChan)
			return
		}
	}

	p.Lock()
	p.inputs--
	if p.inputs == 0 {
		close(destChan)
	}
	p.Unlock()
}
