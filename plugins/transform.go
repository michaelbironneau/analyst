package plugins

import (
	"fmt"
	"github.com/michaelbironneau/analyst/aql"
	"github.com/michaelbironneau/analyst/engine"
	"sync"
	"time"
)

//Transform is the default implementation of a Transform plugin
//that also satisfies the engine.Transform interface.
type Transform struct {
	Plugin       TransformPlugin
	Alias        string
	opts         []aql.Option
	inputColumns map[string][]string
	open         bool
	l            sync.Mutex
	s            engine.Sequencer
	lastTask     string
	wg           sync.WaitGroup
}

func (d *Transform) fatalerr(err error, s engine.Stream, l engine.Logger) {
	l.Chan() <- engine.Event{
		Level:   engine.Error,
		Source:  d.Alias,
		Time:    time.Now(),
		Message: err.Error(),
	}
	if d.Plugin != nil {
		d.Plugin.Close()
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
	d.l.Lock()
	defer d.l.Unlock()
	if d.inputColumns == nil {
		d.inputColumns = make(map[string][]string)
	}
	d.inputColumns[source] = columns
	return nil
}

func (d *Transform) setInputColumns() error {
	d.l.Lock()
	defer d.l.Unlock()
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

func (d *Transform) Sequence(sourceSeq []string) {
	if len(sourceSeq) == 0 {
		panic("transform cannot be sequenced with 0 tasks") //should be unreachable
	}
	d.l.Lock()
	d.s = engine.NewSequencer(sourceSeq)
	d.lastTask = sourceSeq[len(sourceSeq)-1]
	d.l.Unlock()
}

func (d *Transform) log(l engine.Logger, level engine.LogLevel, msg string) {
	l.Chan() <- engine.Event{
		Source:  d.Alias,
		Level:   level,
		Time:    time.Now(),
		Message: msg,
	}
}

func (d *Transform) Open(s engine.Stream, dest engine.Stream, l engine.Logger, st engine.Stopper) {
	outChan := dest.Chan(d.Alias)
	d.log(l, engine.Info, "Transform plugin open")
	//For later cleanup of the plugin - see note below
	d.wg.Add(1)

	d.l.Lock()

	if !d.open {
		if err := d.Plugin.Dial(); err != nil {
			d.fatalerr(err, s, l)
			return
		}
		d.open = true

		//Cleanup - the invocation of Open() that opens the plugin cleans it up,
		//but only after the others have finished.
		go func() {
			d.wg.Wait()
			d.log(l, engine.Info, "Transform plugin closed")
			close(outChan)
			err := d.Plugin.Close()
			if err != nil {
				d.log(l, engine.Warning, fmt.Sprintf("Failed to close plugin: %v", err))
			}

		}()
	}

	d.l.Unlock()

	defer d.wg.Done()

	if err := d.configure(); err != nil {
		d.fatalerr(err, s, l)
		return
	}

	if err := d.setInputColumns(); err != nil {
		d.fatalerr(err, s, l)
		return
	}
	d.log(l, engine.Trace, "Set input columns")
	cols, err := d.Plugin.GetOutputColumns()

	if err != nil {
		d.fatalerr(err, s, l)
		return
	}
	d.log(l, engine.Trace, fmt.Sprintf("Found output columns %v", cols))
	for destName, cs := range cols {
		if err := dest.SetColumns(destName, cs); err != nil {
			d.fatalerr(err, s, l)
			return
		}
	}

	logChan := l.Chan()
	msgChan := s.Chan(d.Alias)

	var seqTask string
	for msg := range msgChan {
		if st.Stopped() {
			d.log(l, engine.Warning, "Transform plugin aborted")
			return
		}
		if d.s != nil {
			seqTask = msg.Source
			d.log(l, engine.Trace, "Source sequence - waiting")
			d.s.Wait(seqTask)
			d.log(l, engine.Trace, "Source sequence - released")
		}
		d.log(l, engine.Trace, fmt.Sprintf("Row %v", msg.Data))
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
	if d.s != nil {
		d.log(l, engine.Trace, "Source sequence - releasing next source")
		d.s.Done(seqTask)
	}

	d.log(l, engine.Info, "Transform plugin closed")

}
