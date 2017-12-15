package plugins

import (
	"fmt"
	"github.com/michaelbironneau/analyst/aql"
	"github.com/michaelbironneau/analyst/engine"
	"time"
)

//Transform is the default implementation of a Transform plugin
//that also satisfies the engine.Transform interface.
type Transform struct {
	Plugin       TransformPlugin
	Alias        string
	opts         []aql.Option
	inputColumns map[string][]string
}

func (d *Transform) fatalerr(err error, s engine.Stream, l engine.Logger) {
	l.Chan() <- engine.Event{
		Level:   engine.Error,
		Source:  d.Alias,
		Time:    time.Now(),
		Message: err.Error(),
	}
	close(s.Chan(d.Alias))
}

func (d *Transform) Ping() error {
	return nil //TODO
}

func (d *Transform) SetName(name string) {
	d.Alias = name
}

func (d *Transform) Configure(opts []aql.Option) error {
	d.opts = opts
	return nil
}

func (d *Transform) SetInputColumns(source string, columns []string) error {
	if d.inputColumns == nil {
		d.inputColumns = make(map[string][]string)
	}
	d.inputColumns[source] = columns
	return nil
}

func (d *Transform) setInputColumns() error {
	if d.inputColumns == nil {
		return nil
	}
	for k, v := range d.inputColumns {
		err := d.Plugin.SetInputColumns(k, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Transform) configure() error {
	for _, opt := range d.opts {
		var val interface{}
		if opt.Value.Str != nil {
			val = *opt.Value.Str
		} else if opt.Value.Number != nil {
			val = *opt.Value.Number
		}
		if err := d.Plugin.SetOption(opt.Key, val); err != nil {
			return err
		}
	}
	return nil
}

func (d *Transform) Open(s engine.Stream, dest engine.Stream, l engine.Logger, st engine.Stopper) {

	if err := d.Plugin.Dial(); err != nil {
		d.fatalerr(err, s, l)
		return
	}

	defer d.Plugin.Close()

	if err := d.configure(); err != nil {
		d.fatalerr(err, s, l)
		return
	}

	if err := d.setInputColumns(); err != nil {
		d.fatalerr(err, s, l)
		return
	}

	cols, err := d.Plugin.GetOutputColumns()
	if err != nil {
		d.fatalerr(err, s, l)
		return
	}
	for dest, cs := range cols {
		if err := s.SetColumns(dest, cs); err != nil {
			d.fatalerr(err, s, l)
			return
		}
	}

	logChan := l.Chan()
	msgChan := s.Chan(d.Alias)
	outChan := dest.Chan(d.Alias)
	logChan <- engine.Event{
		Level:   engine.Trace,
		Source:  d.Alias,
		Message: "TransformPlugin plugin opened",
		Time:    time.Now(),
	}

	for msg := range msgChan {
		if st.Stopped() {
			return
		}

		//TODO: Buffering
		rows, logs, err := d.Plugin.Send([]InputRow{InputRow{Source: msg.Source, Data: msg.Data}})

		if err != nil {
			d.fatalerr(err, s, l)
			return
		}
		for _, logMsg := range logs {
			logChan <- engine.Event{
				Level:   logLevel(logMsg.Level),
				Message: logMsg.Message,
				Source:  d.Alias,
			}
		}
		fmt.Println(rows)
		for _, row := range rows {
			outChan <- engine.Message{
				Source:      d.Alias,
				Destination: row.Destination,
				Data:        row.Data,
			}
		}
	}

	rows, logs, _ := d.Plugin.EOS()

	for _, logMsg := range logs {
		logChan <- engine.Event{
			Level:   logLevel(logMsg.Level),
			Message: logMsg.Message,
			Source:  d.Alias,
			Time:    time.Now(),
		}
	}

	for _, msg := range rows {
		outChan <- engine.Message{
			Source:      d.Alias,
			Destination: msg.Destination,
			Data:        msg.Data,
		}
	}

	close(outChan)
}
