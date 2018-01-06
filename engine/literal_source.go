package engine

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

/**
type Source interface {
	//SetName sets the name (or alias) of the source for outgoing messages
	SetName(name string)

	//Ping attempts to connect to the source without creating a stream.
	//This is used to check that the source is valid at run-time.
	Ping() error

	//Get connects to the source and returns a stream of data.
	Open(Stream, Logger, Stopper)
}
*/

type LiteralSourceFormat int

const (
	//JSONArray is a flat array eg. [[2,3],[3,4]]
	JSONArray LiteralSourceFormat = iota

	//JSONObjects is an array of objects, eg. [{"a": 1, "b": 2}, {"a": 4, "b": 5}]
	JSONObjects

	//CSVWithoutHeader is a CSV string without headers, eg. 1, 2\n4, 5. Only string
	//types are supported - other types will not be inferred, so eg. the above example
	//will map to strings ["1", "2"], ["4", "5"].
	CSVWithoutHeader
)

//unmarshaller unmarshalls the content from the content string into the slice
//of interface, given the column order (column names not case-sensitive).
type unmarshaller func(string, []string) ([][]interface{}, error)

var unmarshallers = map[LiteralSourceFormat]unmarshaller{
	JSONArray:        unmarshalJSONArray,
	JSONObjects:      unmarshalJSONObjects,
	CSVWithoutHeader: unmarshalCSV,
}

var LiteralSourceFormats = map[string]LiteralSourceFormat{
	"JSON_ARRAY": JSONArray,
	"JSON_OBJECTS": JSONObjects,
	"CSV": CSVWithoutHeader,
}

func unmarshalJSONArray(s string, cols []string) ([][]interface{}, error) {
	var array [][]interface{}
	if err := json.Unmarshal([]byte(s), &array); err != nil {
		return nil, err
	}

	return array, nil
}

func unmarshalJSONObjects(s string, cols []string) ([][]interface{}, error) {
	var array []map[string]interface{}
	if err := json.Unmarshal([]byte(s), &array); err != nil {
		return nil, err
	}

	var ret [][]interface{}
	for i := range array {
		var row []interface{}
		for k, v := range array[i] {
			array[i][strings.ToLower(k)] = v
		}
		for _, col := range cols {
			val, ok := array[i][strings.ToLower(col)]
			if !ok {
				return nil, fmt.Errorf("column not found %s", col)
			}
			row = append(row, val)
		}
		ret = append(ret, row)
	}
	return ret, nil
}

func unmarshalCSV(s string, cols []string) ([][]interface{}, error) {
	reader := strings.NewReader(s)
	r := csv.NewReader(reader)
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	var ret [][]interface{}

	for i := range rows {
		row := make([]interface{}, len(rows[i]), len(rows[i]))
		for j := range rows[i] {
			row[j] = rows[i][j]
		}
		ret = append(ret, row)
	}
	return ret, nil
}

type LiteralSource struct {
	Name         string
	Content      string
	Columns      []string
	Format       LiteralSourceFormat
	outgoingName string
}

func (ls *LiteralSource) log(l Logger, level LogLevel, msg string, args ...interface{}) {
	l.Chan() <- Event{
		Source:  ls.Name,
		Level:   level,
		Time:    time.Now(),
		Message: fmt.Sprintf(msg, args...),
	}
}

func (ls *LiteralSource) fatalerr(err error, st Stream, l Logger, stop Stopper) {
	l.Chan() <- Event{
		Level:   Error,
		Source:  ls.Name,
		Time:    time.Now(),
		Message: err.Error(),
	}
	close(st.Chan(ls.outgoingName))
	stop.Stop()
}

func (ls *LiteralSource) SetName(name string) { ls.outgoingName = name }

func (ls *LiteralSource) Ping() error { return nil }

func (ls *LiteralSource) Open(s Stream, l Logger, st Stopper) {
	outChan := s.Chan(DestinationWildcard)

	m, ok := unmarshallers[ls.Format]

	if !ok || m == nil {
		panic(fmt.Errorf("unmarshaller not found %v", ls.Format)) //shouldn't get reached
	}

	msgs, err := m(ls.Content, ls.Columns)
	ls.log(l, Info, "found %d rows", len(msgs))

	if err != nil {
		ls.fatalerr(err, s, l, st)
	}

	if st.Stopped() {
		ls.log(l, Warning, "literal source interrupted")
		return
	}

	if err := s.SetColumns(DestinationWildcard, ls.Columns); err != nil {
		ls.fatalerr(err, s, l, st)
		return
	}

	for _, msg := range msgs {
		outChan <- Message{
			Source:      ls.outgoingName,
			Destination: DestinationWildcard,
			Data:        msg,
		}
	}

	close(outChan)

}
