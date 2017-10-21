package engine

import "sync"

//Transform is a component that is neither a source nor a sink. It is configured with
//one or more sources, and one or more sinks.
type Transform interface {
	//Open gives the transform a stream to start pulling from
	Open(source, dest Stream)
}

type Passthrough struct{
	sync.Mutex
	inputs int
}


func (p *Passthrough) Open(source, dest Stream) {
	p.Lock()
	p.inputs++
	p.Unlock()
	destChan := dest.Chan()
	for msg := range source.Chan() {
		destChan <- msg
	}

	p.Lock()
	p.inputs--
	if p.inputs == 0 {
		close(destChan)
	}
	p.Unlock()
}
