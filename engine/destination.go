package engine

import (
	"sync"
	"time"
)

type Destination interface {

	//Ping checks that the destination is available. It is used to verify
	//the destination at runtime.
	Ping() error

	//Open gives the destination a stream to start pulling from and an error stream
	Open(input Stream, logger Logger)
}

type SliceDestination struct {
	sync.Mutex
	res [][]interface{}
}

func (sd *SliceDestination) Ping() error { return nil }

func (sd *SliceDestination) Open(s Stream, logger Logger) {
	logger.Chan() <- Event{
		Level:   Trace,
		Time:    time.Now(),
		Message: "Slice destination opened",
	}
	for msg := range s.Chan() {
		sd.Lock()
		sd.res = append(sd.res, msg)
		sd.Unlock()
	}

}

func (sd *SliceDestination) Results() [][]interface{} {
	return sd.res
}
