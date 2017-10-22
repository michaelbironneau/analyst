package engine

import (
	"sync"
	"time"
)

//Transform is a component that is neither a source nor a sink. It is configured with
//one or more sources, and one or more sinks.
type Transform interface {
	//Open gives the transform a stream to start pulling from
	Open(source Stream, dest Stream, logger Logger, stop Stopper)
}

type Passthrough struct {
	sync.Mutex
	inputs int
}

func (p *Passthrough) Stop(){}

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
		if stop.Stopped(){
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
