package engine

import (
	"fmt"
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

//Condition is a func that returns true if the message passes the test and false otherwise.
type Condition func([]interface{}) bool

type test struct {
	name string
	desc string
	t    Condition
}

func NewTest(name string, description string, c Condition) Destination {
	return &test{name, description, c}
}

func (t *test) Ping() error {
	return nil
}

func (t *test) Open(s Stream, l Logger, st Stopper) {
	for msg := range s.Chan() {
		if !t.t(msg) {
			l.Chan() <- Event{
				Source:  t.name,
				Message: fmt.Sprintf("[FAIL] %s", t.desc),
				Time:    time.Now(),
				Level:   Error,
			}
			st.Stop()
			//close(ret.Chan())
			return //a test should stop the job on first failure
		}
	}
}

type SliceDestination struct {
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
	for msg := range s.Chan() {
		if stop.Stopped() {
			return
		}
		sd.Lock()
		sd.res = append(sd.res, msg)
		sd.Unlock()
	}

}

func (sd *SliceDestination) Results() [][]interface{} {
	return sd.res
}
