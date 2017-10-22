package engine

import (
	"fmt"
	"time"
)

//Middleware is a func that transforms a stream.
type Middleware func(Stream) Stream

//Tester is a func that returns true if the message passes the test and false otherwise.
type Tester func([]interface{}) bool

func Test(name string, desc string, l Logger, cond Tester, bufferSize int) Middleware {
	return func(s Stream) Stream {
		ret := NewStream(s.Columns(), bufferSize)
		go func() {
			output := ret.Chan()
			for msg := range s.Chan() {
				if cond(msg) {
					output <- msg
				} else {
					l.Chan() <- Event{
						Source:  name,
						Message: fmt.Sprintf("[FAIL] %s", desc),
						Time:    time.Now(),
						Level:   Error,
					}
					close(ret.Chan())
					return //a test should stop the job on first failure
				}
			}
		}()
		return ret
	}
}
