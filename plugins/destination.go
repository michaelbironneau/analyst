package plugins

import (
	"github.com/michaelbironneau/analyst/engine"
	"time"
	"github.com/michaelbironneau/analyst/aql"
)

//destination is the default implementation of a Destination plugin
//that also satisfies the engine.Destination interface.
type destination struct {
	D Destination
	Alias string
	opts []aql.Option
	inputColumns map[string][]string
}

func (d *destination) SetName(name string){
	d.Alias = name
}

func (d *destination) fatalerr(err error, s engine.Stream, l engine.Logger) {
	l.Chan() <- engine.Event{
		Level:   engine.Error,
		Source:  d.Alias,
		Time:    time.Now(),
		Message: err.Error(),
	}
	close(s.Chan(d.Alias))
}

func (d *destination) Ping() error {
	return nil //TODO
}

func (d *destination) Configure(opts []aql.Option) error {
	d.opts = opts
	return nil
}

func (d *destination) SetInputColumns(source string, columns []string) error {
	if d.inputColumns == nil {
		d.inputColumns = make(map[string][]string)
	}
	d.inputColumns[source] = columns
	return nil
}

func (d *destination) setInputColumns() error {
	if d.inputColumns == nil {
		return nil
	}
	for k, v := range d.inputColumns {
		err := d.D.SetInputColumns(k, v)
		if err != nil {
			return err
		}
	}

	return nil
}


func (d *destination) configure() error {
	for _, opt := range d.opts {
		var val interface{}
		if opt.Value.Str != nil {
			val = *opt.Value.Str
		} else if opt.Value.Number != nil {
			val = *opt.Value.Number
		}
		if err := d.D.SetOption(opt.Key, val); err != nil {
			return err
		}
	}
	return nil
}


func (d *destination) Open(s engine.Stream, l engine.Logger, st engine.Stopper){

	if err := d.D.Dial(); err != nil {
		d.fatalerr(err, s, l)
		return
	}

	defer d.D.Close()


	//  FIXME: It would be nice to have some heads up at compile time if
	//  either configure() or setInputColumns() fails, rather than having
	//  to wait for the whole jig to be up. This however requires adding
	//  some Cleanup() method to source/dest/transform interfaces on
	//  the engine side and having the coordinator call them, otherwise,
	//  we could be left with subprocesses hanging everywhere when running
	//  this code as a library (rather than one-off CLI).

	if err := d.configure(); err != nil {
		d.fatalerr(err, s, l)
		return
	}

	if err := d.setInputColumns(); err != nil {
		d.fatalerr(err, s, l)
		return
	}

	logChan := l.Chan()
	msgChan := s.Chan(d.Alias)
	logChan <- engine.Event{
		Level: engine.Trace,
		Source: d.Alias,
		Message: "Destination plugin opened",
		Time: time.Now(),
	}

	for msg := range msgChan {
		if st.Stopped() {
			return
		}

		//TODO: Buffering
		logs, err := d.D.Send([]InputRow{InputRow{Source: msg.Source, Data: msg.Data}})

		if err != nil {
			d.fatalerr(err, s, l)
			return
		}
		for _, logMsg := range logs {
			logChan <- engine.Event{
				Level: logLevel(logMsg.Level),
				Message: logMsg.Message,
				Source: d.Alias,
				Time: time.Now(),
			}
		}
	}

	logs, _ := d.D.EOS()

	for _, logMsg := range logs {
		logChan <- engine.Event{
			Level: logLevel(logMsg.Level),
			Message: logMsg.Message,
			Source: d.Alias,
		}
	}
}

