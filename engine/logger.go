package engine

import (
	"fmt"
	"time"
)

type LogLevel int

const (
	Trace LogLevel = iota
	Info
	Warning
	Error
)

type Event struct {
	Time    time.Time
	Source  string
	Level   LogLevel
	Message string
}

type Logger interface {
	Chan() chan<- Event
}

type ConsoleLogger struct{}

func (cl *ConsoleLogger) Chan() chan<- Event {
	ch := make(chan Event, DefaultBufferSize)
	go func() {
		for event := range ch {
			fmt.Println(event)
		}
	}()
	return ch
}
