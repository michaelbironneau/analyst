package transforms

import (
	"fmt"
	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
	"github.com/michaelbironneau/analyst/engine"
	"strings"
	"time"
)

const (
	NoGroupBy        = ""
	groupBySeparator = "||"
)

//The reason that Function's have to include the opening ( is
//because we can't parse these as Ident due the issue in
//https://github.com/alecthomas/participle/issues/3
var (
	aggregateLexer = lexer.Unquote(lexer.Upper(lexer.Must(lexer.Regexp(`(\s+)`+
		`|(?P<Keyword>(?i)AGGREGATE\s|GROUP\s|BY\s|AS\s)`+
		`|(?P<Function>[a-zA-Z0-9_]+\()`+
		`|(?P<Ident>[a-zA-Z_][a-zA-Z0-9_]*)`+
		`|(?P<Number>[-+]?\d*\.?\d+([eE][-+]?\d+)?)`+
		`|(?P<String>'[^']*'|"[^"]*")`+
		`|(?P<Operators><>|!=|<=|>=|[-+*/%,.()=<>])`,
	)), "Keyword"), "String")
	Reducers                  = map[string]Reducer{"count(": &count{}, "sum(": &sum{}, "min(": &min{}, "max(": &max{}, "avg(": &avg{}, "zoh(": &zoh{}}
	DefaultArgMap ArgumentMap = func(i []interface{}) []interface{} { return i }
)

type FunctionArgument struct {
	Column string   `@Ident`
	String *string  `| @String`
	Number *float64 `| @Number`
}

type FunctionApplication struct {
	Function string             `@Function`
	Columns  []FunctionArgument `@@ { "," @@ } ")"`
}

type AggregateTerm struct {
	Column string `(@Ident`

	Function *FunctionApplication `| @@)`
	Alias    string               `["AS " @Ident]`
}

type Aggregate struct {
	Select  []AggregateTerm `"AGGREGATE " @@ { "," @@ }`
	GroupBy []string        `["GROUP " "BY " @Ident { "," @Ident}]`
}

//ArgumentMap is used to map the incoming engine.Message into
//a slice of interface that is correct for a
//Reducer.Reduce() method.
type ArgumentMap func(args []interface{}) []interface{}

func groupBy(groupByColumns []string, actualColumns []string) (func([]interface{}) string, error) {
	if groupByColumns == nil {
		return func(i []interface{}) string { return NoGroupBy }, nil
	}
	var indexes []int
	for _, col := range groupByColumns {
		if ix, ok := find(actualColumns, col); !ok {
			return nil, fmt.Errorf("could not find column %s", col)
		} else {
			indexes = append(indexes, ix)
		}

	}

	return func(input []interface{}) string {
		var s string
		for _, ix := range indexes {
			s += fmt.Sprintf("%s%v", groupBySeparator, input[ix])
		}
		return s
	}, nil

}

type Reducer interface {
	ParameterLen() int //ParameterLen returns the number of parameters the function takes (should be constant)
	SetArgumentMap(ArgumentMap)
	Reduce(arg []interface{}) error
	Copy() Reducer //Returns a reducer with blank state
	Return() *float64
}

type groupByRow struct {
	key        map[string]interface{} //map from column alias to value
	aggregates map[string]Reducer     //map from column alias to value
}

func (gpr *groupByRow) Copy() *groupByRow {
	var copy = groupByRow{
		key:        make(map[string]interface{}),
		aggregates: make(map[string]Reducer),
	}
	for k, v := range gpr.aggregates {
		copy.aggregates[k] = v.Copy()
	}
	return &copy
}

type aggregate struct {
	name       string
	state      map[string]*groupByRow                              //map from row key digest to entries
	blank      groupByRow                                          //blank row to initialize new entries
	aliasOrder []string                                            //order of the columns/alias in the select statement
	keyColumns []string                                            //the input columns that make up the GROUP BY key
	argMaker   map[string]func(cols []string) (ArgumentMap, error) //map from alias to arg maker
	keyMaker   map[string]func(cols []string) (ArgumentMap, error)
	sourceSeq  []string
}

type columnIndex int

func (a *aggregate) SetName(name string) {
	a.name = name
}

func (a *aggregate) fatalerr(err error, s engine.Stream, l engine.Logger, st engine.Stopper) {
	l.Chan() <- engine.Event{
		Level:   engine.Error,
		Source:  a.name,
		Time:    time.Now(),
		Message: err.Error(),
	}
	st.Stop()
	close(s.Chan(a.name))
}

func (a *aggregate) Open(s engine.Stream, dest engine.Stream, l engine.Logger, st engine.Stopper) {
	var (
		argMakers = make(map[string]ArgumentMap)
		cols      []string
		getKey    func([]interface{}) string
		keyMakers = make(map[string]ArgumentMap)
		inChan    chan engine.Message
		outChan   chan engine.Message
		err       error
	)

	if a.sourceSeq != nil {
		seq := engine.NewSequencedStream(s, a.sourceSeq)
		inChan = seq.Chan(a.name)
	} else {
		inChan = s.Chan(a.name)
	}
	outChan = dest.Chan(a.name)

	var firstMessage = true
	for msg := range inChan {
		if st.Stopped() {
			return
		}
		if firstMessage {
			firstMessage = false
			cols = s.Columns()

			if err := dest.SetColumns(a.name, a.aliasOrder); err != nil {
				a.fatalerr(err, dest, l, st)
				return
			}

			for key, maker := range a.argMaker {
				am, err := maker(cols)
				if err != nil {
					a.fatalerr(err, dest, l, st)
					return
				}
				argMakers[key] = am
			}

			for k, red := range a.blank.aggregates {
				red.SetArgumentMap(argMakers[k])
			}

			getKey, err = groupBy(a.keyColumns, cols)

			if err != nil {
				a.fatalerr(err, dest, l, st)
				return
			}

			for key, maker := range a.keyMaker {
				var err error
				keyMakers[key], err = maker(cols)
				if err != nil {
					a.fatalerr(err, dest, l, st)
					return
				}
			}

			if err != nil {
				a.fatalerr(err, dest, l, st)
				return
			}

		}
		key := getKey(msg.Data)
		var gbr *groupByRow
		var ok bool
		//look for existing key
		gbr, ok = a.state[key]
		if !ok {
			a.state[key] = a.blank.Copy()
			gbr = a.state[key]
		}
		for _, red := range gbr.aggregates {
			if err := red.Reduce(msg.Data); err != nil {
				a.fatalerr(err, s, l, st)
				return
			}
		}
		for _, col := range a.keyColumns {
			gbr.key[col] = keyMakers[col](msg.Data)[0]
		}

	}

	//TODO: What if we need to sort the output?
	for _, row := range a.state {
		var msg engine.Message
		var data []interface{}
		msg.Source = a.name
		msg.Destination = engine.DestinationWildcard
		for _, col := range a.aliasOrder {
			if g, ok := row.key[col]; ok {
				data = append(data, g)
				continue
			}
			if g, ok := row.aggregates[col]; ok {
				var f *float64
				f = g.Return()
				if f == nil {
					data = append(data, nil)
				} else {
					data = append(data, *f)
				}
				continue
			}
			panic(fmt.Sprintf("column %s not found", col)) //should be unreachable
		}
		msg.Data = data
		outChan <- msg
	}
	close(outChan)

}

func find(haystack []string, needle string) (int, bool) {
	for i := range haystack {
		if strings.ToLower(haystack[i]) == strings.ToLower(needle) {
			return i, true
		}
	}
	return -1, false
}

func getKeyArgs(key string) func(cols []string) (ArgumentMap, error) {
	return func(cols []string) (ArgumentMap, error) {
		if cols == nil {
			return func([]interface{}) []interface{} {
				return nil
			}, nil
		}
		index, ok := find(cols, key)

		if !ok {
			return nil, fmt.Errorf("column not found %s", key)
		}

		return func(msg []interface{}) []interface{} {
			return []interface{}{msg[index]}
		}, nil
	}
}

//return a function that will generate the argument map at runtime given the columns.
//this is necessary as we want to bind static function parameters at compile time,
//but the ones coming from message columns at run-time.
func getFunctionArgs(a *Aggregate, fIx int) func(cols []string) (ArgumentMap, error) {
	return func(cols []string) (ArgumentMap, error) {
		if a.Select[fIx].Function == nil {
			panic("cannot apply getFunctionArgs to nil Function")
		}

		//static params we work out at Open()-time.
		params := make([]interface{}, len(a.Select[fIx].Function.Columns))

		for i, col := range a.Select[fIx].Function.Columns {
			if col.String != nil {
				params[i] = *col.String
			} else if col.Number != nil {
				params[i] = *col.Number
			} else {
				ix, ok := find(cols, col.Column)
				if !ok {
					return nil, fmt.Errorf("column not found %s", col.Column)
				}
				params[i] = columnIndex(ix)
			}
		}

		//dynamic params we work out at run-time.
		return func(msg []interface{}) []interface{} {
			ret := make([]interface{}, len(params), len(params))
			for i := range params {
				switch v := params[i].(type) {
				case columnIndex:
					ret[i] = msg[v]
				default:
					ret[i] = v
				}
			}
			return ret
		}, nil
	}
}

func (a *aggregate) Sequence(seq []string) {
	a.sourceSeq = seq
}

func newAggregate(a *Aggregate) (*aggregate, error) {

	var columnOrder []string
	var aa aggregate
	aa.argMaker = make(map[string]func(cols []string) (ArgumentMap, error))
	var blank = groupByRow{
		key:        make(map[string]interface{}),
		aggregates: make(map[string]Reducer),
	}

	for i, term := range a.Select {
		var columnAlias string
		if term.Alias == "" {
			if term.Function != nil {
				return nil, fmt.Errorf("must choose alias for column %v of AGGREGATE", i)
			}
			columnOrder = append(columnOrder, term.Column)
			columnAlias = term.Column
		} else {
			columnOrder = append(columnOrder, term.Alias)
			columnAlias = term.Alias
		}
		if term.Function != nil {
			aa.argMaker[term.Alias] = getFunctionArgs(a, i)
			var (
				r  Reducer
				ok bool
			)
			r, ok = Reducers[strings.ToLower(term.Function.Function)]
			if !ok {
				return nil, fmt.Errorf("unknown reducer %s", term.Function.Function)
			}
			if r.ParameterLen() != len(term.Function.Columns) {
				return nil, fmt.Errorf("the reducer %s expects %v parameters but %v were provided", term.Function.Function, r.ParameterLen(), len(term.Function.Columns))
			}
			blank.aggregates[columnAlias] = r.Copy()
		} else {
			//check it is in group by
			if _, ok := find(a.GroupBy, columnAlias); !ok {
				return nil, fmt.Errorf("column %s not found in GROUP BY", columnAlias)
			}
		}
	}

	aa.aliasOrder = columnOrder
	aa.blank = blank
	aa.keyColumns = a.GroupBy
	aa.state = make(map[string]*groupByRow)
	aa.keyMaker = make(map[string]func(cols []string) (ArgumentMap, error))
	for _, col := range aa.keyColumns {
		aa.keyMaker[col] = getKeyArgs(col)
	}

	return &aa, nil
}

func NewAggregate(aqlBody string) (*aggregate, error) {
	p, err := participle.Build(&Aggregate{}, aggregateLexer)

	if err != nil {
		panic(err)
	}
	var a Aggregate
	err = p.ParseString(aqlBody, &a)

	if err != nil {
		return nil, err
	}

	return newAggregate(&a)
}

func aggregateInitializer(aqlBody string) (engine.SequenceableTransform, error) {
	return NewAggregate(aqlBody)
}
