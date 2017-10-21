package engine

import "sync"

//Multiplexer is a middleware that multiplexes the output
type Multiplexer interface{
	//Columns returns a slice of column names
	Columns() []string

	//SetColumns
	SetColumns(cols []string)

	//Chan returns a new multiplexed channel. If the requested number is greater
	// than the multiplex order, the multiplexer should panic.
	Chan() chan []interface{}
}


type multiplexer struct {
	sync.Mutex
	bufferSize int
	n int
	s Stream
	i int
	children []Stream
}

func newMultiplexer(n int, bufferSize int ) *multiplexer {
	m := multiplexer {n:n,bufferSize:bufferSize,i: 0, children:nil}
	for i := 0; i< m.n; i++ {
		m.children = append(m.children, NewStream(nil, m.bufferSize))
	}
	return &m
}

func (m *multiplexer) Open(s Stream) {
	m.s = s
	m.SetColumns(s.Columns())
	for msg := range s.Chan() {
		for i := range m.children {
			m.children[i].Chan() <- msg
		}
	}
	for i := range m.children {
		close(m.children[i].Chan())
	}
}

func (m *multiplexer) Columns() []string{
	return m.s.Columns()
}

func (m *multiplexer) SetColumns(cols []string){
	m.s.SetColumns(cols)
	for i := range m.children {
		m.children[i].SetColumns(cols)
	}
}

func (m *multiplexer) Chan() chan []interface{}{
	m.Lock()
	defer m.Unlock()
	if m.i >= m.n {
		panic("multiplexer does not have enough outputs")
	}
	m.i++
	return m.children[m.i-1].Chan()

}


