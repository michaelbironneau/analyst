package plugins

import (
	"github.com/michaelbironneau/analyst/engine"
	"strings"
)

//InputRow is a row sent from the executor to the plugin.
type InputRow struct {
	Source string `json:"source"`
	Data []interface{} `json:"data"`
}

//OutputRow is a row sent from the plugin to the executor.
type OutputRow struct {
	Destination string `json:"destination"`
	Data []interface{} `json:"data"`
}

//LogEntry is a log entry recorded by the plugin.
type LogEntry struct {
	Level string `json:"level"`
	Message string `json:"message"`
}

//Plugin is the generic interface that all plugins must satisfy.
type Plugin interface {
	//Dial connects to the plugin using whatever RPC. It can hold resources open.
	//These should be released when Close() is called.
	Dial() error

	//SetOption sets the given option name/value pair.
	SetOption(name string, value interface{}) error

	//Close releases any resources associated with the plugin.
	Close() error
}

//Transform is the interface for transforms.
type Transform interface {
	Plugin

	//SetSources sets the names of the input sources.
	SetSources(names []string) error

	//SetDestinations sets the names of the output destinations.
	SetDestinations(names []string) error

	//SetInputColumns sets the names of the input columns for the given source.
	SetInputColumns(source string, columns []string) error

	//GetOutputColumns gets the name of the output columns for the given destination.
	GetOutputColumns(destination string) ([]string, error)

	//Send sends a batch of rows to the plugin, optionally returning output rows and/or
	//log entries.
	Send(row []InputRow) ([]OutputRow, []LogEntry, error)

	//EOS signals the end of the stream and that the plugin should exit.
	EOS() ([]OutputRow, []LogEntry, error)


	//EOG (currently unused) signals the end of the aggregation group. This is reserved
	//for user-defined aggregates in future versions.
	//EOG() error
}

//Source is the interface for sources.
type Source interface {
	Plugin

	//SetDestinations sets the names of the output destinations.
	SetDestinations(names []string) error

	//GetOutputColumns gets the name of the output columns for the given destination.
	GetOutputColumns(destination string) ([]string, error)

	//Receive optionally returns output rows and/or log entries. The boolean parameter
	//is used to indicate whether End of Stream has been reached.
	Receive() ([]OutputRow, []LogEntry, error)
}

//Destination is the interface for destinations.
type Destination interface {
	Plugin

	//SetSources sets the names of the input sources.
	SetSources(names []string) error

	//SetInputColumns sets the names of the input columns for the given source.
	SetInputColumns(source string, columns []string) error

	//Send sends a batch of rows to the plugin, optionally returning output rows and/or
	//log entries.
	Send(row []InputRow) ([]LogEntry, error)

	//EOS signals the end of the stream and that the plugin should exit.
	EOS() ([]LogEntry, error)
}

func logLevel(s string) engine.LogLevel {
	switch strings.ToLower(s){
	case "trace":
		return engine.Trace
	case "warning":
		return engine.Warning
	case "error":
		return engine.Error
	default:
		return engine.Info
	}
}