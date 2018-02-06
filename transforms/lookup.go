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
	lookupLexer = lexer.Unquote(lexer.Upper(lexer.Must(lexer.Regexp(`(\s+)`+
		`|(?P<Keyword>(?i)LOOKUP\s|INNER\s|OUTER\s|JOIN\s|ON\s|AND\s|FROM\s|AS\s)`+
		`|(?P<Ident>[a-zA-Z_][a-zA-Z0-9_]*)`+
		`|(?P<Number>[-+]?\d*\.?\d+([eE][-+]?\d+)?)`+
		`|(?P<String>'[^']*'|"[^"]*")`+
		`|(?P<Operators><>|!=|<=|>=|[-+*/%,.()=<>])`,
	)), "Keyword"), "String")
)

type LookupColumn struct {
	Source string  `@Ident "."`
	Column string  `@Ident`
	Alias  *string `["AS " @Ident]`
}

type JoinCondition struct {
	T1Column *LookupColumn `@@`
	T2Column *LookupColumn `"=" @@`
}

type Lookup struct {
	Projection   []LookupColumn  `"LOOKUP " @@ {"," @@}`
	FromSource   string          `"FROM " @Ident`
	InnerJoin    bool            `(@"INNER "`
	OuterJoin    bool            `| @"OUTER ")`
	LookupSource string          `"JOIN " @Ident`
	Conditions   []JoinCondition `"ON " @@ { "AND " @@}`
}

type lookup struct {
	outgoingName      string
	sourceSeq         []string
	baseJoinColumns   []string
	lookupJoinColumns []string
	outerJoin         bool
	projection        []LookupColumn
	sequencer         engine.Sequencer
	lookupTable       map[string]map[string]interface{} //map from join key to row-map (map of col name to value)
}

func project(projectionColumns []string, actualColumns []string) (func([]interface{}) map[string]interface{}, error) {
	if projectionColumns == nil {
		return func([]interface{}) map[string]interface{} { return nil }, nil
	}

	var indexes []int
	for _, col := range projectionColumns {
		if ix, ok := find(actualColumns, col); !ok {
			return nil, fmt.Errorf("could not find column %s", col)
		} else {
			indexes = append(indexes, ix)
		}
	}

	return func(input []interface{}) map[string]interface{} {
		ret := make(map[string]interface{})
		for i, col := range projectionColumns {
			ret[strings.ToLower(col)] = input[indexes[i]]
		}
		return ret
	}, nil
}

func splitProjections(cols []LookupColumn, baseTable string, lookupTable string) (base []string, lookup []string, err error) {
	for _, col := range cols {
		if strings.ToLower(col.Source) == strings.ToLower(baseTable) {
			base = append(base, col.Column)
		} else if strings.ToLower(col.Source) == strings.ToLower(lookupTable) {
			lookup = append(lookup, col.Column)
		} else {
			err = fmt.Errorf("source not found %s", col.Source)
			return
		}
	}
	return
}

func columnNames(cols []LookupColumn) []string {
	var ret []string
	for _, col := range cols {
		if col.Alias != nil {
			ret = append(ret, *col.Alias)
			continue
		}
		ret = append(ret, col.Column)
	}
	return ret
}

//  Sequence is required to satisfy Sequenceable interface, but does nothing for a lookup.
//  A lookup is self-sequencing based on the FROM/JOIN sources as it needs to cache the
//  lookup table first (otherwise it would have to cache both tables).
func (l *lookup) Sequence([]string) {}

func (l *lookup) SetName(name string) { l.outgoingName = name }

func (l *lookup) Open(s engine.Stream, dest engine.Stream, logger engine.Logger, st engine.Stopper) {

	inChan := s.Chan(l.outgoingName)
	outChan := dest.Chan(l.outgoingName)

	var (
		firstMessage      = true
		isLookupSource    bool
		keyMap            func([]interface{}) string
		lookupProjections []string
		baseProjections   []string
		cols              []string
		projectOp         func([]interface{}) map[string]interface{}
		err               error
		source            string
	)

	baseProjections, lookupProjections, err = splitProjections(l.projection, l.sourceSeq[1], l.sourceSeq[0])

	if err != nil {
		l.fatalerr(err, s, logger, st)
		return
	}

	for msg := range inChan {
		if st.Stopped() {
			return
		}

		if firstMessage {
			l.log(logger, engine.Info, "Started processing messages for source %s", msg.Source)
			source = strings.ToLower(msg.Source)
			l.sequencer.Wait(source)

			firstMessage = false

			if source != strings.ToLower(l.sourceSeq[0]) && len(l.lookupTable) == 0 {
				l.fatalerr(fmt.Errorf("expected source %s but got source %s", l.sourceSeq[0], msg.Source), dest, logger, st)
				return
			}
			cols = s.Columns()
			l.log(logger, engine.Trace, "Found columns %v", cols)

			if err := dest.SetColumns(l.outgoingName, columnNames(l.projection)); err != nil {
				l.fatalerr(err, dest, logger, st)
				return
			}

			if source == strings.ToLower(l.sourceSeq[0]) {
				isLookupSource = true
				keyMap, err = groupBy(l.lookupJoinColumns, cols)
			} else {
				keyMap, err = groupBy(l.baseJoinColumns, cols)
			}

			if err != nil {
				l.fatalerr(err, dest, logger, st)
				return
			}

			if isLookupSource {
				projectOp, err = project(lookupProjections, cols)
			} else {
				projectOp, err = project(baseProjections, cols)
			}

			if err != nil {
				l.fatalerr(err, dest, logger, st)
				return
			}

		}

		key := keyMap(msg.Data)

		//cache the lookup table entry
		if isLookupSource {
			projections := projectOp(msg.Data)
			l.lookupTable[key] = projections
			l.log(logger, engine.Trace, "Cached row with key '%s': %v", key, projections)
			continue
		}

		outMsg := l.getMessage(msg, key, projectOp)

		if outMsg != nil {
			outChan <- engine.Message{
				Source:      l.outgoingName,
				Destination: engine.DestinationWildcard,
				Data:        outMsg,
			}
		}
	}
	if source != "" {
		l.sequencer.Done(source)
		l.log(logger, engine.Info, "Finished processing messages for source %s", source)
		if source == l.sourceSeq[1] {
			close(outChan)
		}
	} else {
		l.log(logger, engine.Error, "Opened with empty source")
		st.Stop()
	}

}

func (l *lookup) getMessage(msg engine.Message, key string, projectOp func([]interface{}) map[string]interface{}) []interface{} {
	baseColumnValues := projectOp(msg.Data)

	var outMsg []interface{}
	for _, col := range l.projection {
		if strings.ToLower(col.Source) == strings.ToLower(msg.Source) {
			//base projection
			val, ok := baseColumnValues[strings.ToLower(col.Column)]

			if !ok && !l.outerJoin {
				return nil //ignore as value is nil and this isn't an outer join
			}

			outMsg = append(outMsg, val)

		} else {
			//lookup projection
			if entry, ok := l.lookupTable[key]; !ok && !l.outerJoin {
				return nil //ignore as we don't have a matching row and this isn't an outer join
			} else if !ok && l.outerJoin {
				outMsg = append(outMsg, nil)
				continue
			} else {
				outMsg = append(outMsg, entry[strings.ToLower(col.Column)])
			}
		}
	}
	return outMsg
}

func (l *lookup) log(logger engine.Logger, level engine.LogLevel, msg string, args ...interface{}) {
	logger.Chan() <- engine.Event{
		Source:  l.outgoingName,
		Level:   level,
		Time:    time.Now(),
		Message: fmt.Sprintf(msg, args...),
	}
}

func (l *lookup) fatalerr(err error, s engine.Stream, logger engine.Logger, st engine.Stopper) {
	logger.Chan() <- engine.Event{
		Level:   engine.Error,
		Source:  l.outgoingName,
		Time:    time.Now(),
		Message: err.Error(),
	}
	st.Stop()
	close(s.Chan(l.outgoingName))
}

func newLookup(l *Lookup) (*lookup, error) {
	var ret lookup
	ret.sourceSeq = []string{strings.ToLower(l.LookupSource), strings.ToLower(l.FromSource)}
	ret.sequencer = engine.NewSequencer(ret.sourceSeq)
	ret.projection = l.Projection
	if l.OuterJoin {
		ret.outerJoin = true
	}
	ret.lookupTable = make(map[string]map[string]interface{})
	for _, cond := range l.Conditions {
		if strings.ToLower(cond.T1Column.Source) == strings.ToLower(cond.T2Column.Source) {
			return nil, fmt.Errorf("join condition should be between FROM source and JOIN source, not from a source to itself: %v", cond)
		}

		if strings.ToLower(cond.T1Column.Source) == strings.ToLower(l.LookupSource) {
			ret.lookupJoinColumns = append(ret.lookupJoinColumns, strings.ToLower(cond.T1Column.Column))
		} else if strings.ToLower(cond.T1Column.Source) == strings.ToLower(l.FromSource) {
			ret.baseJoinColumns = append(ret.baseJoinColumns, strings.ToLower(cond.T1Column.Column))
		} else {
			return nil, fmt.Errorf("join condition does not reference either FROM source or JOIN source: %v", cond)
		}

		if strings.ToLower(cond.T2Column.Source) == strings.ToLower(l.LookupSource) {
			ret.lookupJoinColumns = append(ret.lookupJoinColumns, strings.ToLower(cond.T2Column.Column))
		} else if strings.ToLower(cond.T2Column.Source) == strings.ToLower(l.FromSource) {
			ret.baseJoinColumns = append(ret.baseJoinColumns, strings.ToLower(cond.T2Column.Column))
		} else {
			return nil, fmt.Errorf("join condition does not reference either FROM source or JOIN source: %v", cond)
		}

	}

	return &ret, nil

}

func NewLookup(aqlBody string) (*lookup, error) {
	p, err := participle.Build(&Lookup{}, lookupLexer)

	if err != nil {
		panic(err)
	}
	var l Lookup
	err = p.ParseString(aqlBody, &l)

	if err != nil {
		return nil, err
	}

	return newLookup(&l)
}

func lookupInitializer(aqlBody string) (engine.SequenceableTransform, error) {
	return NewLookup(aqlBody)
}
