package plugins

import (
	"github.com/michaelbironneau/analyst/engine"
	"time"
	"github.com/michaelbironneau/analyst/aql"
	"fmt"
)

//transform is the default implementation of a transform plugin
//that also satisfies the engine.transform interface.
type transform struct {
	T Transform
	Alias string
	opts []aql.Option
	inputColumns map[string][]string
	Destinations []string
}

func (d *transform) fatalerr(err error, s engine.Stream, l engine.Logger) {
	l.Chan() <- engine.Event{
		Level:   engine.Error,
		Source:  d.Alias,
		Time:    time.Now(),
		Message: err.Error(),
	}
	close(s.Chan(d.Alias))
}

func (d *transform) Ping() error {
	return nil //TODO
}

func (d *transform) SetName(name string){
	d.Alias = name
}

func (d *transform) Configure(opts []aql.Option) error {
	d.opts = opts
	return nil
}

func (d *transform) SetInputColumns(source string, columns []string) error {
	if d.inputColumns == nil {
		d.inputColumns = make(map[string][]string)
	}
	d.inputColumns[source] = columns
	return nil
}

func (d *transform) setInputColumns() error {
	if d.inputColumns == nil {
		return nil
	}
	for k, v := range d.inputColumns {
		err := d.T.SetInputColumns(k, v)
		if err != nil {
			return err
		}
	}

	return nil
}


func (d *transform) configure() error {
	for _, opt := range d.opts {
		var val interface{}
		if opt.Value.Str != nil {
			val = *opt.Value.Str
		} else if opt.Value.Number != nil {
			val = *opt.Value.Number
		}
		if err := d.T.SetOption(opt.Key, val); err != nil {
			return err
		}
	}
	return nil
}


func (d *transform) Open(s engine.Stream, dest engine.Stream, l engine.Logger, st engine.Stopper) {

	if err := d.T.Dial(); err != nil {
		d.fatalerr(err, s, l)
		return
	}

	defer d.T.Close()

	if err := d.configure(); err != nil {
		d.fatalerr(err, s, l)
		return
	}

	if err := d.setInputColumns(); err != nil {
		d.fatalerr(err, s, l)
		return
	}

	for _, dest := range d.Destinations {
		cols, err := d.T.GetOutputColumns(dest)
		if err != nil {
			d.fatalerr(err, s, l)
			return
		}
		if err := s.SetColumns(dest, cols); err != nil {
			d.fatalerr(err, s, l)
			return
		}
	}

	logChan := l.Chan()
	msgChan := s.Chan(d.Alias)
	outChan := dest.Chan(d.Alias)
	logChan <- engine.Event{
		Level: engine.Trace,
		Source: d.Alias,
		Message: "Transform plugin opened",
		Time: time.Now(),
	}

	for msg := range msgChan {
		if st.Stopped() {
			return
		}

		//TODO: Buffering
		rows, logs, err := d.T.Send([]InputRow{InputRow{Source: msg.Source, Data: msg.Data}})

		if err != nil {
			d.fatalerr(err, s, l)
			return
		}
		for _, logMsg := range logs {
			logChan <- engine.Event{
				Level: logLevel(logMsg.Level),
				Message: logMsg.Message,
				Source: d.Alias,
			}
		}
		fmt.Println(rows)
		for _, row := range rows {
			outChan <- engine.Message{
				Source: d.Alias,
				Destination: row.Destination,
				Data: row.Data,
			}
		}
	}

	rows, logs, _ := d.T.EOS()

	for _, logMsg := range logs {
		logChan <- engine.Event{
			Level: logLevel(logMsg.Level),
			Message: logMsg.Message,
			Source: d.Alias,
			Time: time.Now(),
		}
	}

	for _, msg := range rows {
		outChan <- engine.Message{
			Source: d.Alias,
			Destination: msg.Destination,
			Data: msg.Data,
		}
	}

	close(outChan)
}

