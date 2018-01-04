package engine

import (
	"encoding/json"
	"fmt"
	"github.com/Jeffail/gabs"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type HTTPSource struct {
	Name                 string
	outgoingName         string
	URL                  string            //URL of request
	Headers              map[string]string //Headers to add to request, optional
	JSONPath             string            //Path to object containing array of rows, optional
	NoColumnNames        bool              //If response has array of primitive types rather than objects with column names, eg. ["bob",2] instead of {"name": "bob", "age": 2}
	ColumnNames          []string          //if NoColumnNames is true, this should be provided
	PaginationLimitName  string            //query parameter for pagination limit (optional)
	PaginationOffsetName string            //query parameter for pagination offset (optional)
	PageSize             int               //size of page for pagination
	//PaginationTotalResultsProperty string //JSON property containing the total number of results for pagination (optional)
	client http.Client
	limit  int
	offset int
}

func (h *HTTPSource) SetName(name string) {
	h.outgoingName = name
}

func (h *HTTPSource) Ping() error {
	if h.NoColumnNames && h.ColumnNames == nil {
		return fmt.Errorf("if the HTTP response will not contain column names, they must be explicitly specified with COLUMNS option")
	}
	if h.ColumnNames == nil {
		return fmt.Errorf("column name order must be specified as COLUMNS option")
	}
	if _, err := url.Parse(h.URL); err != nil {
		return fmt.Errorf("URL is not parsable: %s: %v", h.URL, err)
	}
	r, err := h.client.Get(h.URL)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("HTTP status %d received from server - failed to read response body", r.StatusCode)
	}
	if r.StatusCode == 404 {
		return fmt.Errorf("HTTP server returned 'not found' response - check URL")
	}
	if r.StatusCode > 499 {
		return fmt.Errorf("error from HTTP server with status code %d: %s", r.StatusCode, string(b))
	}

	return nil
}

func (h *HTTPSource) log(l Logger, level LogLevel, msg string) {
	l.Chan() <- Event{
		Source:  h.Name,
		Level:   level,
		Time:    time.Now(),
		Message: msg,
	}
}

func (h *HTTPSource) fatalerr(err error, st Stream, l Logger) {
	l.Chan() <- Event{
		Level:   Error,
		Source:  h.Name,
		Time:    time.Now(),
		Message: err.Error(),
	}
	close(st.Chan(h.outgoingName))
}

func (h *HTTPSource) firstPage() string {
	h.limit = h.PageSize
	return h.paginatedURL(h.limit, h.offset)
}

func (h *HTTPSource) nextPage() string {
	h.offset += h.PageSize
	return h.paginatedURL(h.limit, h.offset)
}

func (h *HTTPSource) Open(s Stream, l Logger, st Stopper) {
	outChan := s.Chan(h.outgoingName)
	s.SetColumns(DestinationWildcard, h.ColumnNames)

	h.firstPage()

	for {
		url := h.paginatedURL(h.limit, h.offset)

		h.log(l, Trace, fmt.Sprintf("HTTP GET %s", url))

		t1 := time.Now()
		resp, err := h.client.Get(url)
		duration := time.Now().Sub(t1)

		h.log(l, Info, fmt.Sprintf("HTTP request took %7.2f seconds", duration.Seconds()))

		if err != nil {
			h.fatalerr(err, s, l)
			return
		}

		b, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			h.fatalerr(err, s, l)
			return
		}

		rows, err := h.parse(b)

		if err != nil {
			h.fatalerr(err, s, l)
			return
		}

		h.log(l, Info, fmt.Sprintf("Received %d rows", len(rows)))

		for i := range rows {
			h.log(l, Trace, fmt.Sprintf("Found row %v", rows[i]))
			outChan <- Message{
				Source:      h.outgoingName,
				Destination: DestinationWildcard,
				Data:        rows[i],
			}
		}

		if len(rows) == 0 || h.PageSize == 0 {
			//  Either there are no more results or we are not paginating and
			//  will receive all results in one response. In either case, we
			//  should stop making requests immediately.
			break
		}

		h.nextPage()
	}
	close(outChan)

}

func (h *HTTPSource) paginatedURL(limit, offset int) string {
	if h.PaginationOffsetName == "" || h.PaginationLimitName == "" {
		return h.URL
	}

	u, err := url.Parse(h.URL)

	if err != nil {
		panic(fmt.Errorf("unparsable URL %s", h.URL)) //should be unreachable
	}

	vals := u.Query()

	vals.Set(h.PaginationLimitName, strconv.Itoa(limit))
	vals.Set(h.PaginationOffsetName, strconv.Itoa(offset))

	return u.Scheme + "://" + u.Host + u.Path + "?" + vals.Encode()

	//return u.String()
}

func (h *HTTPSource) parseTopLevelArray(b []byte) ([][]interface{}, error) {
	var ret [][]interface{}
	err := json.Unmarshal(b, &ret)
	return ret, err
}

func (h *HTTPSource) parse(b []byte) ([][]interface{}, error) {
	if h.JSONPath == "" {
		return h.parseTopLevelArray(b)
	}
	c, err := gabs.ParseJSON(b)
	if err != nil {
		return nil, err
	}
	rows, err := c.S(h.JSONPath).Children()
	if err != nil {
		return nil, err
	}
	var (
		ret    [][]interface{}
		colMap = columnIndexMap(h.ColumnNames)
	)

	for _, row := range rows {
		var retRow []interface{}
		if h.NoColumnNames {
			//no need to map to the given column names
			//we have an array of arrays
			items, err := row.Children()
			if err != nil {
				return nil, err
			}
			for i := range items {
				retRow = append(retRow, items[i].Data())
			}
			ret = append(ret, retRow)
			continue
		}

		//we have a column name map
		items, err := row.ChildrenMap()
		if err != nil {
			return nil, err
		}
		retRow = make([]interface{}, len(h.ColumnNames), len(h.ColumnNames))
		for key, item := range items {
			ix, ok := colMap[strings.ToLower(key)]
			if !ok {
				//return nil, fmt.Errorf("column not found in JSON object - %s", key)
				continue //projection - ignore this column
			}
			retRow[ix] = item.Data()
		}
		ret = append(ret, retRow)
	}

	return ret, nil
}

//columnIndexMap takes a slice of column names and returns a mapping from the
//column to its index for faster lookups.
func columnIndexMap(cols []string) map[string]int {
	ret := make(map[string]int)
	for i := range cols {
		ret[strings.ToLower(cols[i])] = i
	}
	return ret
}

func (h *HTTPSource) decodeArray(body []byte) ([]interface{}, error) {
	//TODO: use Gabs instead of this nonsense
	return nil, nil
}

func (h *HTTPSource) decodeObject(body []byte) ([]interface{}, error) {
	//TODO: use Gabs instead of this nonsense
	return nil, nil
}
