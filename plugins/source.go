package plugins

import (
	"github.com/michaelbironneau/analyst/engine"
	"time"
	"github.com/michaelbironneau/analyst/aql"
)

//source is the default implementation of a Source plugin
//that also satisfies the engine.Source interface.
type source struct {
	S Source
	Alias string
	Destinations []string
	opts []aql.Option
}

func (so *source) SetName(name string){
	so.Alias = name
}

func (so *source) fatalerr(err error, s engine.Stream, l engine.Logger) {
	l.Chan() <- engine.Event{
		Level:   engine.Error,
		Source:  so.Alias,
		Time:    time.Now(),
		Message: err.Error(),
	}
	close(s.Chan(so.Alias))
}

func (so *source) Ping() error {
	return nil //TODO
}

func (so *source) Configure(opts []aql.Option) error {
	so.opts = opts
	return nil
}

func (so *source) configure() error {
	for _, opt := range so.opts {
		var val interface{}
		if opt.Value.Str != nil {
			val = *opt.Value.Str
		} else if opt.Value.Number != nil {
			val = *opt.Value.Number
		}
		if err := so.S.SetOption(opt.Key, val); err != nil {
			return err
		}
	}
	return nil
}


func (so *source) Open(s engine.Stream, l engine.Logger, st engine.Stopper){

	if err := so.S.Dial(); err != nil {
		so.fatalerr(err, s, l)
		return
	}

	defer so.S.Close()

	if err := so.configure(); err != nil {
		so.fatalerr(err, s, l)
		return
	}

	logChan := l.Chan()
	msgChan := s.Chan(so.Alias)
	logChan <- engine.Event{
		Level: engine.Trace,
		Source: so.Alias,
		Time: time.Now(),
		Message: "Source plugin opened",
	}

	for _, dest := range so.Destinations {
		cols, err := so.S.GetOutputColumns(dest)
		if err != nil {
			so.fatalerr(err, s, l)
			return
		}
		if err := s.SetColumns(dest, cols); err != nil {
			so.fatalerr(err, s, l)
			return
		}
	}

	for {
		if st.Stopped() {
			return
		}
		msgs, logs, err := so.S.Receive()
		if err != nil {
			so.fatalerr(err, s, l)
			return
		}
		for _, logMsg := range logs {
			logChan <- engine.Event{
				Level: logLevel(logMsg.Level),
				Message: logMsg.Message,
				Source: so.Alias,
				Time: time.Now(),
			}
		}
		if len(msgs) == 0 {
			//EOS
			return
		}
		for _, msg := range msgs {
			msgChan <- engine.Message{
				Source: so.Alias,
				Destination: msg.Destination,
				Data: msg.Data,
			}
		}
	}
}

