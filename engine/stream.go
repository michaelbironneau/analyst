package engine

import (
	"errors"
)

var ErrEOS = errors.New("end of stream")

const DefaultBufferSize = 100

//Stream represents a stream of data such as a database resultset
type Stream interface {
	//Columns returns a slice of column names
	Columns() []string

	//SetColumns
	SetColumns(cols []string)

	//Chan is the channel for the stream. It will be closed by the sender when the stream is at an end.
	Chan() chan []interface{}
}

//default wrapper for a stream
type stream struct {
	cols []string
	msg  chan []interface{}
}

func NewStream(cols []string, bufferSize int) Stream {
	return &stream{
		cols: cols,
		msg:  make(chan []interface{}, bufferSize),
	}
}

func (s *stream) Columns() []string {
	return s.cols
}

func (s *stream) SetColumns(cols []string) {
	s.cols = cols
}

func (s *stream) Chan() chan []interface{} {
	return s.msg
}
