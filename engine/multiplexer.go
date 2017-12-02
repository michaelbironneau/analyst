package engine

import (
	"sync"
	"fmt"
	"strings"
	"time"
)

type multiplexer struct {
	sync.Mutex
	bufferSize int
	n          int
	s          Stream
	children   map[string]Stream
	name       string
}

func newMultiplexer(name string, aliases []string, bufferSize int) *multiplexer {
	m := multiplexer{
		name: name,
		n: len(aliases),
		bufferSize: bufferSize,
		children: make(map[string]Stream),
	}
	for i := 0; i < m.n; i++ {
		m.children[strings.ToLower(aliases[i])] = NewStream(nil, m.bufferSize)
	}

	return &m
}

func (m *multiplexer) fatalerr(err error, s Stream, l Logger) {

	l.Chan() <- Event{
		Level:   Error,
		Source:  m.name + " multiplexer",
		Time:    time.Now(),
		Message: err.Error(),
	}
	close(s.Chan(DestinationWildcard))
}

func (m *multiplexer) Open(s Stream, l Logger, st Stopper) {
	m.s = s
	//m.SetColumns(s.Columns())
	for msg := range s.Chan(DestinationWildcard) {
		if msg.Destination == DestinationWildcard {
			for alias, ss := range m.children {
				ss.Chan(alias) <- msg
			}
		} else {
			if ss := m.children[strings.ToLower(msg.Destination)]; ss != nil {
				ss.Chan(msg.Destination) <- msg
			} else {
				//stop everything

			}
		}


	}
	for i := range m.children {
		close(m.children[i].Chan(DestinationWildcard))
	}
}

func (m *multiplexer) Columns() []string {
	return m.s.Columns()
}

func (m *multiplexer) SetColumns(destination string, cols []string) error {
	s := m.children[destination]
	if s == nil {
		return fmt.Errorf("unknown destionation alias %s", destination)
	}
	if err := s.SetColumns(destination, cols); err != nil {
		return err
	}
	for i := range m.children {
		m.children[i].SetColumns(destination, cols)
	}
	return nil
}

func (m *multiplexer) Chan(destination string) chan Message {
	m.Lock()
	defer m.Unlock()
	if destination == DestinationWildcard {
		panic("cannot open multiplexed stream anonymously")
	}
	s := m.children[strings.ToLower(destination)]
	if s == nil {
		panic(fmt.Errorf("alias %s not recognised for source/sink", destination))
	}
	return s.Chan(destination)

}
