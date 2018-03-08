package transforms

import (
	"fmt"
	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
	"github.com/michaelbironneau/analyst/engine"
	"strings"
	"time"
)

var (
	applyLexer = lexer.Unquote(lexer.Upper(lexer.Must(lexer.Regexp(`(\s+)`+
		`|(?P<Keyword>(?i)APPLY\s|CAST\(|AS\s)`+
		`|(?P<Ident>[a-zA-Z_][a-zA-Z0-9_]*)`+
		`|(?P<Number>[-+]?\d*\.?\d+([eE][-+]?\d+)?)`+
		`|(?P<String>'[^']*'|"[^"]*")`+
		`|(?P<Operators><>|!=|<=|>=|[-+*/%,.()=<>])`,
	)), "Keyword"), "String")
)

type Column struct {
	Column string  `@Ident`
	Alias  *string `["AS " @Ident]`
}

type CastColumn struct {
	Column   string  `"CAST(" @Ident`
	DestType string  `"AS " @Ident ")"`
	Alias    *string `["AS " @Ident]`
}

type ConversionColumn struct {
	Lookup *Column     `@@`
	Cast   *CastColumn `| @@`
}

type Apply struct {
	Projections []ConversionColumn `"APPLY " @@ {"," @@}`
}

func projectArray(projectionColumns []string, actualColumns []string) (func([]interface{}) []interface{}, error) {
	if projectionColumns == nil {
		return func([]interface{}) []interface{} { return nil }, nil
	}

	var indexes []int
	for _, col := range projectionColumns {
		if ix, ok := find(actualColumns, col); !ok {
			return nil, fmt.Errorf("could not find column %s", col)
		} else {
			indexes = append(indexes, ix)
		}
	}

	return func(input []interface{}) []interface{} {
		ret := make([]interface{}, len(projectionColumns), len(projectionColumns))
		for i := range projectionColumns {
			ret[i] = input[indexes[i]]
		}
		return ret
	}, nil
}

type apply struct {
	outgoingName string
	sourceSeq    []string
	sourceCols   []string
	outputCols   []string
	castFns      []CastFn
	projection   []ConversionColumn
	sequencer    engine.Sequencer
}

//  Sequence is required to satisfy Sequenceable interface, but does nothing for a apply.
//  TODO: Fully implement the interface
func (l *apply) Sequence([]string) {}

func (l *apply) SetName(name string) { l.outgoingName = name }

func (l *apply) Open(s engine.Stream, dest engine.Stream, logger engine.Logger, st engine.Stopper) {

	inChan := s.Chan(l.outgoingName)
	outChan := dest.Chan(l.outgoingName)
	dest.SetColumns(l.outgoingName, l.outputCols)

	var (
		firstMessage = true
		projectOp    func([]interface{}) []interface{}
		err          error
	)

	l.log(logger, engine.Info, "Apply transform opened")
	for msg := range inChan {
		if st.Stopped() {
			l.log(logger, engine.Warning, "Apply transform aborted")
			return
		}
		if firstMessage {
			projectOp, err = projectArray(l.sourceCols, s.Columns())
			if err != nil {
				l.fatalerr(err, s, logger, st)
				return
			}
		}
		l.log(logger, engine.Trace, "Found row %s", msg.Data)
		out := make([]interface{}, len(msg.Data), len(msg.Data))
		projected := projectOp(msg.Data)
		for i := range projected {
			if l.castFns[i] != nil {
				//this is a cast
				out[i], err = l.castFns[i](projected[i])
				if err != nil {
					l.fatalerr(err, s, logger, st)
					return
				}
			} else {
				//this is a simple lookup
				out[i] = projected[i]
			}
		}

		outChan <- engine.Message{
			Source:      l.outgoingName,
			Destination: engine.DestinationWildcard,
			Data:        out,
		}
	}

	close(outChan)

}

func (l *apply) log(logger engine.Logger, level engine.LogLevel, msg string, args ...interface{}) {
	logger.Chan() <- engine.Event{
		Source:  l.outgoingName,
		Level:   level,
		Time:    time.Now(),
		Message: fmt.Sprintf(msg, args...),
	}
}

func (l *apply) fatalerr(err error, s engine.Stream, logger engine.Logger, st engine.Stopper) {
	logger.Chan() <- engine.Event{
		Level:   engine.Error,
		Source:  l.outgoingName,
		Time:    time.Now(),
		Message: err.Error(),
	}
	st.Stop()
	close(s.Chan(l.outgoingName))
}

func newApply(c *Apply) (*apply, error) {
	var ret apply

	ret.castFns = make([]CastFn, len(c.Projections), len(c.Projections))

	//set up source and destination columns
	for i, proj := range c.Projections {
		if proj.Cast != nil {
			if proj.Cast.Alias != nil {
				ret.outputCols = append(ret.outputCols, *proj.Cast.Alias)
			} else {
				ret.outputCols = append(ret.outputCols, proj.Cast.Column)
			}
			var ok bool
			if ret.castFns[i], ok = castFns[strings.ToLower(proj.Cast.DestType)]; !ok {
				return nil, fmt.Errorf("unknown destination type for cast: %s", proj.Cast.DestType)
			}
			ret.sourceCols = append(ret.sourceCols, proj.Cast.Column)
			continue
		}
		if proj.Lookup != nil {
			if proj.Lookup.Alias != nil {
				ret.outputCols = append(ret.outputCols, *proj.Lookup.Alias)
			} else {
				ret.outputCols = append(ret.outputCols, proj.Lookup.Column)
			}
			ret.sourceCols = append(ret.sourceCols, proj.Lookup.Column)
		}
	}

	ret.projection = c.Projections
	return &ret, nil

}

func NewApply(aqlBody string) (*apply, error) {
	p, err := participle.Build(&Apply{}, applyLexer)

	if err != nil {
		panic(err)
	}
	var c Apply
	err = p.ParseString(aqlBody, &c)

	if err != nil {
		return nil, err
	}

	return newApply(&c)
}

func applyInitializer(aqlBody string) (engine.SequenceableTransform, error) {
	return NewApply(aqlBody)
}
