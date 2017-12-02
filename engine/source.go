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

type NamedSliceSource struct {
	cols []string
	msg []Message
	name string
}

func NewNamedSliceSource(cols []string, msg []Message) Source {
	return &NamedSliceSource{
		cols: cols,
		msg:  msg,
	}
}

func (ns *NamedSliceSource) SetName(name string){
	ns.name = name
}

func (ns *NamedSliceSource) Ping() error {return nil}

func (ns *NamedSliceSource) Open(dest Stream, logger Logger, stop Stopper){
	logger.Chan() <- Event{
		Level:   Trace,
		Time:    time.Now(),
		Message: "Slice source opened",
	}
	dest.SetColumns(DestinationWildcard, ns.cols)
	c := dest.Chan(ns.name)
	for _, msg := range ns.msg {
		if stop.Stopped() {
			break
		}
		c <- Message{Source: ns.name, Destination: msg.Destination, Data: msg.Data}
	}
	close(c)
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
	dest.SetColumns(DestinationWildcard, s.cols)
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