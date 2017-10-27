package engine

import "time"

//Source represents data inputs into the system, eg. a database query.
type Source interface {
	Columns() []string

	//Ping attempts to connect to the source without creating a stream.
	//This is used to check that the source is valid at run-time.
	Ping() error

	//Get connects to the source and returns a stream of data.
	Open(Stream, Logger, Stopper)
}

type SliceSource struct {
	cols []string
	msg  [][]interface{}
}

func NewSliceSource(cols []string, msg [][]interface{}) Source {
	return &SliceSource{
		cols: cols,
		msg:  msg,
	}
}

func (s *SliceSource) Ping() error {
	return nil
}

func (s *SliceSource) Open(dest Stream, logger Logger, stop Stopper) {
	logger.Chan() <- Event{
		Level:   Trace,
		Time:    time.Now(),
		Message: "Slice source opened",
	}
	dest.SetColumns(s.cols)
	c := dest.Chan()
	for i := range s.msg {
		if stop.Stopped() {
			break
		}
		c <- s.msg[i]
	}
	close(c)
}

func (s *SliceSource) Columns() []string {
	return s.cols
}
