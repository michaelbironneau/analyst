package engine

import (
	"fmt"
	colors "github.com/logrusorgru/aurora"
	"io"
	"time"
	"errors"
)

type LogLevel int

const (
	Trace LogLevel = iota
	Info
	Warning
	Error
)

const timeFormat = "15:04:05"

var eventTypeMap = map[LogLevel]string{
	Trace:   "[TRACE]",
	Info:    "[INFO]",
	Warning: "[WARNING]",
	Error:   "[ERROR]",
}

var htmlTypeMap = map[LogLevel]string{
	Trace:   "<div><small>[TRACE]</small>",
	Info:    "<div><small>[INFO]</small>",
	Warning: "<div class='alert alert-warning'><small>[WARNING]</small>",
	Error:   "<div class='alert alert-danger'><small>[ERROR]</small>",
}

var eventTypeColors = map[LogLevel]func(interface{}) colors.Value{
	Trace:   colors.Gray,
	Info:    colors.Cyan,
	Warning: colors.Brown,
	Error:   colors.Red,
}

type Event struct {
	Time    time.Time
	Source  string
	Level   LogLevel
	Message string
}

type Logger interface {
	//Chan returns a chan that can be used to log events
	Chan() chan<- Event

	//Error returns the latest error that has been logged
	Error() error
}

type ConsoleLogger struct {
	MinLevel LogLevel
	latestError error
	c        chan Event
}

type GenericLogger struct {
	MinLevel LogLevel
	latestError error
	Writer   io.Writer
	c        chan Event
}

func NewGenericLogger(minLevel LogLevel, writer io.Writer) *GenericLogger {
	gl := GenericLogger{
		Writer:   writer,
		MinLevel: minLevel,
		c:        make(chan Event, DefaultBufferSize),
	}

	go func() {
		for event := range gl.c {
			if event.Level == Error {
				gl.latestError = errors.New(event.Message)
			}
			if event.Level >= gl.MinLevel {
				msg := fmt.Sprint(htmlTypeMap[event.Level]+" "+event.Time.Format(timeFormat), "- ("+event.Source+")"+"<p>"+event.Message+"</p></div></p>")
				writer.Write([]byte(msg))
			}
		}
	}()

	return &gl

}

func (gl *GenericLogger) Chan() chan<- Event {
	return gl.c
}

func (gl *GenericLogger) Error() error {
	return gl.latestError
}

func NewConsoleLogger(minLevel LogLevel) *ConsoleLogger {
	cl := ConsoleLogger{
		MinLevel: minLevel,
		c:        make(chan Event, DefaultBufferSize),
	}

	go func() {
		for event := range cl.c {
			if event.Level == Error {
				cl.latestError = errors.New(event.Message)
			}
			if event.Level >= cl.MinLevel {
				fmt.Println(eventTypeColors[event.Level](eventTypeMap[event.Level]), event.Time.Format(timeFormat), "- ("+event.Source+")", event.Message)
			}
		}
	}()

	return &cl

}

func (cl *ConsoleLogger) Chan() chan<- Event {
	return cl.c
}

func (cl *ConsoleLogger) Error() error {
	return cl.latestError
}