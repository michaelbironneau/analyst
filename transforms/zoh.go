package transforms

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

type zoh struct {
	items  Timeseries
	start  *time.Time
	finish *time.Time
	format string
	am     ArgumentMap
}

func (s *zoh) ParameterLen() int {
	return 4 //time, value, start, finish
}

func (s *zoh) SetArgumentMap(am ArgumentMap) {
	s.am = am
}

func parseTime(s string) (*time.Time, string, error) {
	var (
		t   time.Time
		err error
	)
	t, err = time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, "", fmt.Errorf("unknown time format %s: expected RFC3339 or RFC3339 with nanoseconds", s)
		}
		return &t, time.RFC3339Nano, nil
	}
	return &t, time.RFC3339, nil
}

func (s *zoh) Reduce(arg []interface{}) error {
	args := s.am(arg)
	if len(args) != 4 {
		return fmt.Errorf("AVG takes exactly 1 argument but %v were provided", len(args))
	}
	if s.start == nil || s.finish == nil {
		var (
			sFormat string
			fFormat string
			start   *time.Time
			finish  *time.Time
			err     error
		)
		if s, ok := args[2].(string); !ok {
			return fmt.Errorf("expected string for third argument but got %T %s", args[2], args[2])
		} else {
			start, sFormat, err = parseTime(s)
		}
		if err != nil {
			return err
		}
		if f, ok := args[3].(string); !ok {
			return fmt.Errorf("expected string for fourth argument but got %T %s", args[3], args[3])
		} else {
			finish, fFormat, err = parseTime(f)
		}
		if err != nil {
			return err
		}
		if sFormat != fFormat {
			return fmt.Errorf("all times need to be formatted the same whereas found %s and %s formats", sFormat, fFormat)
		}
		s.start = start
		s.finish = finish
		s.format = sFormat
	}
	//first argument should be time
	var (
		t   time.Time
		f   float64
		err error
		ok  bool
	)

	if ss, ok := args[0].(string); !ok {
		return fmt.Errorf("expected string for first argument but got %T %s", args[0], args[0])
	} else {
		t, err = time.Parse(s.format, ss)
	}
	if err != nil {
		return fmt.Errorf("failed to parse time %v: %v", args[0], err)
	}

	//second argument should be value, and it should be float64
	f, ok = args[1].(float64)

	if !ok {
		return fmt.Errorf("expected floating-point number for second argument but got %T: %v", args[1], args[1])
	}

	s.items = append(s.items, TimeseriesItem{
		Time:  t,
		Value: f,
	})

	return nil
}

func (s *zoh) Return() *float64 {
	if s.start == nil || s.finish == nil {
		return nil
	}
	sort.Sort(s.items)
	return s.items.Mean(*s.start, *s.finish)
}

func (s *zoh) Copy() Reducer {
	var newStart *time.Time
	var newFinish *time.Time
	if s.start != nil {
		newStart = &(*s.start)
	}
	if s.finish != nil {
		newFinish = &(*s.finish)
	}
	return &zoh{am: s.am, start: newStart, finish: newFinish, format: s.format}
}

//ErrTimeseriesNoEarlierData is returned when trimming or resampling a timeseries when the desired start time is later than the
//earliest point in the timeseries.
var ErrTimeseriesNoEarlierData = errors.New("could not trim timeseries because its start time is later than the requested start time")

//TimeseriesItem is a point in a timeseries
type TimeseriesItem struct {
	Time  time.Time
	Value float64
}

//Timeseries represents a data point changing in time. It is assumed to be ordered by Time, ascending.
type Timeseries []TimeseriesItem

func (t Timeseries) Len() int { return len(t) }

func (t Timeseries) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

func (t Timeseries) Less(i, j int) bool { return t[i].Time.Before(t[j].Time) }

//Equals checks for equality with "something". This is to make TimeseriesItem satisfy the Value interface.
func (ti TimeseriesItem) Equals(w interface{}) bool {
	var (
		ti2 TimeseriesItem
		ok  bool
	)
	if ti2, ok = w.(TimeseriesItem); !ok {
		return false
	}
	return ti2.Value == ti.Value && ti2.Time.Equal(ti.Time)
}

//Start returns the first time where the series is defined.
func (t Timeseries) Start() *time.Time {
	if t == nil || len(t) == 0 {
		return nil
	}
	return &t[0].Time
}

//Equals check for equality with "something". This is to make Timeseries satisfy the Value interface.
func (t Timeseries) Equals(w interface{}) bool {
	var (
		t2 Timeseries
		ok bool
	)
	if t2, ok = w.(Timeseries); !ok {
		return false
	}
	if len(t) != len(t2) {
		return false
	}
	for i := range t {
		if t[i].Value != t2[i].Value || !t[i].Time.Equal(t2[i].Time) {
			return false
		}
	}
	return true
}

//Mean computes the ZOH average of the timeseries between `start` and `end`. It returns NaN if start is before the
//earliest point of the timeseries or if `end` is not after `start`.
func (t Timeseries) Mean(start time.Time, end time.Time) *float64 {
	if !(end.After(start)) {
		return nil
	}
	if !(t[0].Time.Before(start) || t[0].Time.Equal(start)) {
		return nil
	}

	//short circuit if there's only one point or the second is out of range
	if len(t) == 1 {
		return &t[0].Value
	} else if t[1].Time.After(end) {
		return &t[0].Value
	}

	//loop through until we find first point greater or equal to `start`
	var first int
	for first = range t {
		if !(t[first].Time.Before(start) || t[first].Time.Equal(start)) {
			break
		}
	}

	//loop through until we find the first point greater or equal to `end`
	var finish int
	for finish = first; finish < len(t); finish++ {
		if !(t[finish].Time.Before(end) || t[finish].Time.Equal(end)) {
			break
		}
	}

	finish = minInt(finish, len(t))
	var weightedSum, totalLength, currLength float64

	//First interval (could be zero length). We know from above that the series
	//contains at least two points and that t[0] is before start.
	currLength = float64(t[first].Time.Sub(start))
	weightedSum += t[first-1].Value * currLength
	totalLength += currLength

	//Middle intervals
	//Because we didn't short circuit above we know that there must be at least one point
	//in the timeseries between start and finish.
	for i := first + 1; i < finish; i++ {
		currLength = float64(t[i].Time.Sub(t[i-1].Time))
		weightedSum += t[i-1].Value * currLength
		totalLength += currLength

	}

	//Last interval (could be zero length)
	currLength = float64(end.Sub(t[finish-1].Time))
	weightedSum += t[finish-1].Value * currLength
	totalLength += currLength

	if totalLength <= 0 {
		//shouldn't reach this if timeseries if ordered correctly
		return nil
	}
	f := weightedSum / totalLength
	return &f
}

func minInt(i, j int) int {
	if i < j {
		return i
	} else if j < i {
		return j
	} else {
		return i
	}
}
