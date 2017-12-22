package engine

import (
	"fmt"
	"time"
	colors "github.com/logrusorgru/aurora"
)

type LogLevel int

const (
	Trace LogLevel = iota
	Info
	Warning
	Error
)

var eventTypeMap = map[LogLevel]string{
	Trace: "[TRACE]",
	Info: "[INFO]",
	Warning: "[WARNING]",
	Error: "[ERROR]",
}

var eventTypeColors = map[LogLevel]func(interface{}) colors.Value{
	Trace: colors.Gray,
	Info:  colors.Cyan,
	Warning: colors.Brown,
	Error: colors.Red,
}

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
			fmt.Println(eventTypeColors[event.Level](eventTypeMap[event.Level]), "-", event.Time.Format(time.Kitchen), "(" + event.Source + ")", event.Message)
		}
	}()
	return ch
}
