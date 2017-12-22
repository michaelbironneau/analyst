package engine

import (
	"fmt"
	"sync"
	"time"
)

//Transform is a component that is neither a source nor a sink. It is configured with
//one or more sources, and one or more sinks.
type Transform interface {
	//SetName sets the alias of the transform for outgoing messages
	SetName(name string)

	//Open gives the transform a stream to start pulling from
	Open(source Stream, dest Stream, logger Logger, stop Stopper)
}

type SequenceableTransform interface {
	Transform
	Sequenceable
}

//Condition is a func that returns true if the message passes the test and false otherwise.
type Condition func([]interface{}) bool

type testNode struct {
	names        []string
	descs        []string
	conds        []Condition
	outgoingName string
}

func (tn *testNode) SetName(name string) {
	tn.outgoingName = name
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
	d := dest.Chan(tn.outgoingName)
	for msg := range s.Chan(tn.outgoingName) {
		if firstMessage {
			dest.SetColumns(tn.outgoingName, s.Columns())
			firstMessage = false
		}
		for i := range tn.conds {
			if !tn.conds[i](msg.Data) {
				l.Chan() <- Event{
					Source:  tn.names[i],
					Message: fmt.Sprintf("[FAIL] %s", tn.descs[i]),
					Time:    time.Now(),
					Level:   Error,
				}
				st.Stop()
				close(d)
				return //a test should stop the job on first failure
			}
		}
		d <- msg
	}
}

type Passthrough struct {
	sync.Mutex
	inputs       int
	outgoingName string
}

func (p *Passthrough) SetName(name string) {
	p.outgoingName = name
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
	destChan := dest.Chan(p.outgoingName)
	for msg := range source.Chan(p.outgoingName) {
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
