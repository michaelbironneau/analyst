package engine

import (
	"errors"
)

var ErrEOS = errors.New("end of stream")

const (
	DefaultBufferSize   = 100
	DestinationWildcard = ""
)

//Stream represents a stream of data such as a database resultset
type Stream interface {
	//Columns returns a slice of column names
	Columns() []string

	//SetColumns sets the destination columns. destination can be a wildcard.
	SetColumns(destination string, cols []string) error

	//Chan is the channel for the stream. It will be closed by the sender when the stream is at an end.
	Chan(destination string) chan Message
}

//Message is a named message. Source and/or destination can be blank (i.e. wildcard).
type Message struct {
	Source      string
	Destination string
	Data        []interface{}
}

//default wrapper for a stream
type stream struct {
	cols []string
	msg  chan Message
}

func NewStream(cols []string, bufferSize int) Stream {
	return &stream{
		cols: cols,
		msg:  make(chan Message, bufferSize),
	}
}

func (s *stream) Columns() []string {
	return s.cols
}

func (s *stream) SetColumns(destination string, cols []string) error {
	s.cols = cols
	return nil
}

func (s *stream) Chan(destination string) chan Message {
	return s.msg
}

type sequencedStream struct {
	s   Stream
	seq Sequencer
	in  map[string]chan Message
	out map[string]chan Message
}

func NewSequencedStream(s Stream, sequence []string) Stream {
	return &sequencedStream{s, NewSequencer(sequence), make(map[string]chan Message), make(map[string]chan Message)}
}

func (s *sequencedStream) Columns() []string {
	return s.s.Columns()
}

func (s *sequencedStream) SetColumns(destination string, cols []string) error {
	return s.s.SetColumns(destination, cols)
}

func (s *sequencedStream) Chan(dest string) chan Message {
	s.in[dest] = s.Chan(dest)
	s.out[dest] = make(chan Message, DefaultBufferSize)
	go func(in chan Message, out chan Message) {
		var source string
		for msg := range in {
			if source == "" {
				source = msg.Source
			}
			s.seq.Wait(source)
			out <- msg
		}
		s.seq.Done(source)
	}(s.in[dest], s.out[dest])
	return s.out[dest]
}
