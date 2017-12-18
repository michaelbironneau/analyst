package plugins

import (
	"github.com/michaelbironneau/analyst/aql"
	"github.com/michaelbironneau/analyst/engine"
	"time"
)

//Source is the default implementation of a SourcePlugin plugin
//that also satisfies the engine.SourcePlugin interface.
type Source struct {
	Plugin SourcePlugin
	alias  string
	opts   []aql.Option
}

func (so *Source) SetName(name string) {
	so.alias = name
}

func (so *Source) fatalerr(err error, s engine.Stream, l engine.Logger) {
	l.Chan() <- engine.Event{
		Level:   engine.Error,
		Source:  so.alias,
		Time:    time.Now(),
		Message: err.Error(),
	}
	close(s.Chan(so.alias))
}

func (so *Source) Ping() error {
	return nil //TODO
}

func (so *Source) Configure(opts []aql.Option) error {
	so.opts = opts
	return nil
}

func (so *Source) configure() error {
	for _, opt := range so.opts {
		var val interface{}
		if opt.Value.Str != nil {
			val = *opt.Value.Str
		} else if opt.Value.Number != nil {
			val = *opt.Value.Number
		}
		if err := so.Plugin.SetOption(opt.Key, val); err != nil {
			return err
		}
	}
	return nil
}

func (so *Source) Open(s engine.Stream, l engine.Logger, st engine.Stopper) {

	if err := so.Plugin.Dial(); err != nil {
		so.fatalerr(err, s, l)
		return
	}

	defer so.Plugin.Close()

	if err := so.configure(); err != nil {
		so.fatalerr(err, s, l)
		return
	}

	logChan := l.Chan()
	msgChan := s.Chan(so.alias)
	logChan <- engine.Event{
		Level:   engine.Trace,
		Source:  so.alias,
		Time:    time.Now(),
		Message: "SourcePlugin plugin opened",
	}

	cols, err := so.Plugin.GetOutputColumns()
	if err != nil {
		so.fatalerr(err, s, l)
		return
	}
	for dest, cs := range cols {
		if err := s.SetColumns(dest, cs); err != nil {
			so.fatalerr(err, s, l)
			return
		}
	}

	for {
		if st.Stopped() {
			return
		}
		msgs, logs, err := so.Plugin.Receive()
		if err != nil {
			so.fatalerr(err, s, l)
			return
		}
		for _, logMsg := range logs {
			logChan <- engine.Event{
				Level:   logLevel(logMsg.Level),
				Message: logMsg.Message,
				Source:  so.alias,
				Time:    time.Now(),
			}
		}
		if len(msgs) == 0 {
			//EOS
			return
		}
		for _, msg := range msgs {
			msgChan <- engine.Message{
				Source:      so.alias,
				Destination: msg.Destination,
				Data:        msg.Data,
			}
		}
	}
}
