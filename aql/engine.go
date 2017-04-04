package aql

import (
	"fmt"
	_ "github.com/lib/pq"                      //Postgres driver
	_ "github.com/michaelbironneau/go-mssqldb" //MS SQL Server (this fork supports Azure SQL)
	"github.com/tealeg/xlsx"
	_ "github.com/ziutek/mymysql/thrsafe" //Thread-safe MySQL driver
	"math/rand"
	"strconv"
	"sync"
	"time"
)

var lowercaseLetters = []rune("abcdefghijklmnopqrstuvwxyz")

func init() {
	rand.Seed(time.Now().UnixNano())
}

//Connection represents a connection to a database
type Connection struct {
	Driver           string
	ConnectionString string
}

//queryResult is an intermediate result produced by a QueryFunc
type queryResult struct {
	Result      result
	Destination QueryRange
}

//executionStep is a slice of queries that can be executed in parallel.
type executionStep struct {
	Queries []string
}

//sourceSink keeps track of sources/sinks of temp table queries. We need to
//make sure that each temp table has a single source and one or more sinks.
type sourceSink struct {
	Source string   //name of query
	Sinks  []string //names of queries
}

//executionPlan contains a slice of steps. Each step must complete before the next one
//can proceed. Reports that don't have any queries using temp table only require one step.
type executionPlan struct {
	Session     string
	Report      *Report
	Qf          QueryFunc
	Template    *xlsx.File
	Connections map[string]Connection
	TempTables  map[string]bool
	LogChan     chan<- string
	Progress    chan<- int
	Steps       []executionStep
}

func (e *executionPlan) qfFromTempDb(statement string) (result, error) {
	return e.Qf("ql-mem", "memory://"+e.Session+".db", statement)
}

func (e *executionPlan) ExecuteStep(i int) error {
	var wg sync.WaitGroup
	r := e.Report
	rs := make(chan queryResult, len(e.Steps[i].Queries))
	qf := e.Qf
	errs := make(chan error, len(r.Queries))
	progressPerQuery := 100 / len(e.Steps[i].Queries)
	progressPerQuery = int((1.0 / float64(len(e.Steps))) * float64(progressPerQuery))
	wg.Add(len(e.Steps[i].Queries))
	for _, k := range e.Steps[i].Queries {
		go func(qn string) {
			e.LogChan <- fmt.Sprintf("\nRunning query <%s>", qn)
			connection, ok := e.Connections[r.Queries[qn].Source]
			t := time.Now()
			_, ok2 := e.TempTables[r.Queries[qn].Source]
			if !(ok || ok2) {
				errs <- fmt.Errorf("Could not find connection (or temp table) for query <%s>", r.Queries[qn].Source)
				wg.Done()
				return
			}
			var res result
			var err error
			if ok {
				res, err = qf(connection.Driver, connection.ConnectionString, r.Queries[qn].Statement)
			} else if ok2 {
				res, err = e.qfFromTempDb(r.Queries[qn].Statement)
			} else {
				panic("source not found")
			}

			if err != nil {
				errs <- fmt.Errorf("\nQuery <%s> Error: \n %s", qn, err)
				wg.Done()
				return
			}
			e.LogChan <- fmt.Sprintf("\nQuery <%s> took %d seconds", qn, int(time.Now().Sub(t).Seconds()))
			rs <- queryResult{
				Result:      res,
				Destination: r.Queries[qn].Range,
			}
			e.Progress <- progressPerQuery
			wg.Done()
		}(k)
	}
	wg.Wait()
	err := drainErrors(errs)
	if err != nil {
		return err
	}
	var allErrors []error
	results := drainResult(rs)
	for i := range results {
		if results[i].Destination.TempTable == nil {
			if len(results[i].Result) == 0 {
				continue
			}
			if err := writeToSheet(e.Template, results[i], results[i].Destination.Sheet); err != nil {
				allErrors = append(allErrors, err)
				continue
			}
		} else {
			db, err := NewTempDb(e.Session)
			if err != nil {
				return err
			}
			if err := CreateTempTableFromRange(db, &results[i].Destination); err != nil {
				return err
			}
			if err := results[i].Result.WriteToTempTable(db, results[i].Destination.TempTable.Name); err != nil {
				return err
			}
		}

	}
	return concatenateErrors(allErrors)
}

//plan determines dependency between queries
func (r Report) plan(qf QueryFunc, template *xlsx.File, connections map[string]Connection, progress chan<- int, logs chan<- string) (*executionPlan, error) {
	//first, do a quick pass and determine if we need multiple steps
	var usesTempTable bool
	var queries []string
	var independentQueries []string //queries that don't read from or write to temp table
	tempTables := make(map[string]bool)
	for qn, q := range r.Queries {
		queries = append(queries, qn)
		if q.Range.TempTable != nil {
			usesTempTable = true
		}
		if q.Range.TempTable == nil && q.SourceType == FromConnection {
			independentQueries = append(independentQueries, qn)
		}
		if q.SourceType == FromTempTable {
			tempTables[q.Source] = true
		}
	}
	if !usesTempTable {
		//the easiest and probably most common case
		return &executionPlan{
			Session:     makeSession(),
			TempTables:  tempTables,
			Report:      &r,
			Qf:          qf,
			Template:    template,
			Connections: connections,
			Progress:    progress,
			LogChan: logs,
			Steps: []executionStep{{
				Queries: queries,
			},
			},
		}, nil
	}
	if err := r.validateDependencies(); err != nil {
		return nil, err
	}
	//get a map from temp tables to queries. each key is a step.
	mapping, err := r.tempTableMapping()
	if err != nil {
		return nil, err
	}
	return &executionPlan{
		Session:     makeSession(),
		TempTables:  tempTables,
		Report:      &r,
		Qf:          qf,
		Template:    template,
		Connections: connections,
		Progress:    progress,
		Steps:       generatePlan(independentQueries, mapping),
	}, nil
}

//generatePlan converts a slice of independent queries and dependent queries into an
//execution plan
func generatePlan(independent []string, dependent map[string]sourceSink) []executionStep {
	var ret []executionStep
	//step 1: all independent and source queries
	ret = append(ret, executionStep{
		Queries: independent,
	})
	var sinks []string
	for _, q := range dependent {
		ret[0].Queries = append(ret[0].Queries, q.Source)
		sinks = append(sinks, q.Sinks...)
	}
	//step 2: all sink queries
	ret = append(ret, executionStep{
		Queries: sinks,
	})
	return ret
}

//validateDependencies validates whether temp table dependencies exist and errors out
//if any query is reading and writing from temp table (it should do one or the other, but not both).
//This validation ensures there are no cycles in the execution graph.
func (r Report) validateDependencies() error {
	for qn, q := range r.Queries {
		if q.SourceType == FromTempTable && q.Range.TempTable != nil {
			return fmt.Errorf("Query %s should read OR write from temp table, not both", qn)
		}
	}
	return nil
}

//tempTableMapping returns a map of source-to-sink mappings and validates rules, such as:
//	1) Each temp table must have exactly one source
//	2) Each temp table must have one or more sinks (unused temp tables are not allowed)
func (r Report) tempTableMapping() (map[string]sourceSink, error) {
	ret := make(map[string]sourceSink)
	for qn, q := range r.Queries {
		if q.SourceType == FromTempTable {
			for _, tdb := range q.TempDBSourceTables {
				if v, ok := ret[tdb]; ok {
					v.Sinks = append(v.Sinks, qn)
					ret[tdb] = v
				} else {
					ret[tdb] = sourceSink{
						Sinks: []string{qn},
					}
				}
			}
		} else if q.Range.TempTable != nil {
			//make sure we have at most one source
			if v, ok := ret[q.Range.TempTable.Name]; ok && len(v.Source) > 0 {
				return nil, fmt.Errorf("Queries '%s' and '%s' are writing to the same temp table", qn, v.Source)
			} else if ok {
				v.Source = qn
				ret[q.Range.TempTable.Name] = v
			} else {
				ret[q.Range.TempTable.Name] = sourceSink{
					Source: qn,
				}
			}

		}
	}
	//make sure we have at least one sink
	for sn, v := range ret {
		if len(v.Sinks) == 0 {
			return nil, fmt.Errorf("Unused temp table '%s'", sn)
		}
	}
	return ret, nil
}

//Execute executes a report that already has parameters populated and whose templates have been executed.
func (r Report) Execute(qf QueryFunc, template *xlsx.File, connections map[string]Connection, progress chan<- int, logs chan<-string) (*xlsx.File, error) {
	plan, err := r.plan(qf, template, connections, progress, logs)
	if err != nil {
		return nil, err
	}
	for i := range plan.Steps {
		if err := plan.ExecuteStep(i); err != nil {
			return nil, err
		}
	}
	return template, err
}

func writeToSheet(f *xlsx.File, res queryResult, sheet string) error {
	var (
		s   *xlsx.Sheet
		ok  bool
		err error
	)
	s, ok = f.Sheet[sheet]
	if !ok {
		//create sheet
		if s, err = f.AddSheet(sheet); err != nil {
			return err
		}
	}
	x1, x2, y1, y2, tr, err := res.Result.Map(&res.Destination)
	if err != nil {
		return err
	}

	return res.Result.WriteToSheet(x1, x2, y1, y2, tr, s)
}

func drainErrors(errs chan error) error {
	var es []error
	defer close(errs)
	for {
		select {
		case err := <-errs:
			es = append(es, err)
		default:
			return concatenateErrors(es)
		}
	}
}

func drainResult(res chan queryResult) []queryResult {
	var ret []queryResult
	defer close(res)
	for {
		select {
		case r := <-res:
			ret = append(ret, r)
		default:
			return ret
		}
	}
}

//SetParameter sets the report parameter and checks that the type matches what was specified
//in the script. It is a type-safe version of `r.Parameters[k] = Parameter{Value: v}`
func (r Report) SetParameter(k string, v interface{}) error {
	if _, ok := r.Parameters[k]; !ok {
		return fmt.Errorf("Unknown parameter %s", k)
	}
	switch r.Parameters[k].Type {
	case "number":
		switch v.(type) {
		case int, float64:
			break
		case string:
			//attempt string conversion to float
			f, err := strconv.ParseFloat(v.(string), 64)
			if err != nil {
				return err
			}
			r.Parameters[k] = Parameter{Value: f}
			return nil
		default:
			return fmt.Errorf("Incorrect parameter value type: was expecting an int or float64")
		}
	case "string":
		if _, ok := v.(string); !ok {
			return fmt.Errorf("Incorrect parameter value type: was expecting a string")
		}
	case "date":
		switch v.(type) {
		case time.Time:
			break
		case string:
			t, err := time.Parse(time.RFC3339Nano, v.(string))
			if err != nil {
				return fmt.Errorf("Invalid datetime, expecting RFC3339 Nano format")
			}
			r.Parameters[k] = Parameter{Value: t}
		default:
			return fmt.Errorf("Incorrect parameter value type: was expecting a time.Time")
		}

	default:
		return fmt.Errorf("Unknown parameter type %s", r.Parameters[k].Type) //should never get reached as the validator takes care of weeding these out
	}
	r.Parameters[k] = Parameter{Value: v}
	return nil
}

//makeSession returns a string of lowercase letters with length 8. It is prepended
//to db name within session, otherwise different users of the web UI would
//end up trampling each others' data. Once in a blue moon, they'll probably get the
//same session id and they *may* trample each other. TODO: Fix this.
func makeSession() string {
	b := make([]rune, 8)
	for i := range b {
		b[i] = lowercaseLetters[rand.Intn(len(lowercaseLetters))]
	}
	return string(b)
}
