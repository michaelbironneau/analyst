package engine

import "time"

//Source represents data inputs into the system, eg. a database query.
type Source interface {
	//SetName sets the name (or alias) of the source for outgoing messages
	SetName(name string)

	//Ping attempts to connect to the source without creating a stream.
	//This is used to check that the source is valid at run-time.
	Ping() error

	//Get connects to the source and returns a stream of data.
	Open(Stream, Logger, Stopper)
}

type SliceSource struct {
	cols []string
	msg  [][]interface{}
	name string
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
	dest.SetColumns(s.name, s.cols)
	c := dest.Chan(s.name)
	for i := range s.msg {
		if stop.Stopped() {
			break
		}
		c <- Message{Source: s.name, Data: s.msg[i]}
	}
	close(c)
}

func (s *SliceSource) SetName(name string){
	s.name = name
}