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
	Open(Stream, Logger, Stopper)
}

type DevNull struct {
	Name string
}

func (d *DevNull) Ping() error { return nil }
func (d *DevNull) Open(s Stream, l Logger, st Stopper) {
	c := s.Chan(d.Name)
	for range c {
		if st.Stopped() {
			return
		}
	}
}

type SliceDestination struct {
	Alias string
	sync.Mutex
	res [][]interface{}
}

func (sd *SliceDestination) Ping() error { return nil }

func (sd *SliceDestination) Open(s Stream, logger Logger, stop Stopper) {
	logger.Chan() <- Event{
		Level:   Trace,
		Time:    time.Now(),
		Message: "Slice destination opened",
	}
	for msg := range s.Chan(sd.Alias) {
		if stop.Stopped() {
			return
		}
		sd.Lock()
		sd.res = append(sd.res, msg.Data)
		sd.Unlock()
	}

}

func (sd *SliceDestination) Results() [][]interface{} {
	return sd.res
}
