package aql

import (
	"fmt"
	_ "github.com/lib/pq"                      //Postgres driver
	_ "github.com/michaelbironneau/go-mssqldb" //MS SQL Server (this fork supports Azure SQL)
	"github.com/tealeg/xlsx"
	_ "github.com/ziutek/mymysql/thrsafe" //Thread-safe MySQL driver
	"sync"
	"time"
	"strconv"
)

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

//Execute executes a report that already has parameters populated and whose templates have been executed.
func (r Report) Execute(qf QueryFunc, template *xlsx.File, connections map[string]Connection, progress chan<- int) (*xlsx.File, error) {
	var (
		wg sync.WaitGroup
	)
	rs := make(chan queryResult, len(r.Queries))
	errs := make(chan error, len(r.Queries))
	wg.Add(len(r.Queries))
	progressPerQuery := 100 / len(r.Queries)
	for k := range r.Queries {
		go func(qn string) {
			connection, ok := connections[r.Queries[qn].Source]
			if !ok {
				errs <- fmt.Errorf("Connection details not provided for %s", r.Queries[qn].Source)
				wg.Done()
				return
			}
			res, err := qf(connection.Driver, connection.ConnectionString, r.Queries[qn].Statement)
			if err != nil {
				errs <- err
				wg.Done()
				return
			}
			rs <- queryResult{
				Result:      res,
				Destination: r.Queries[qn].Range,
			}
			progress <- progressPerQuery
			wg.Done()
		}(k)
	}
	wg.Wait()
	err := drainErrors(errs)
	if err != nil {
		return nil, err
	}
	results := drainResult(rs)
	for i := range results {
		if err := writeToSheet(template, results[i], results[i].Destination.Sheet); err != nil {
			return nil, err
		}
	}
	return template, err
}

func writeToSheet(f *xlsx.File, res queryResult, sheet string) error {
	s, ok := f.Sheet[sheet]
	if !ok {
		return fmt.Errorf("Sheet not found %s", sheet)
	}
	x1, _, y1, _, tr, err := res.Result.Map(&res.Destination)
	if err != nil {
		return err
	}

	return res.Result.WriteToSheet(x1, y1, tr, s)
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
		switch v.(type){
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
