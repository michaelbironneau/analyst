package transforms

import (
	"fmt"
	"math"
)

type max struct {
	result  float64
	notNull bool
	am      ArgumentMap
}

func (s *max) ParameterLen() int {
	return 1
}

func (s *max) SetArgumentMap(am ArgumentMap) {
	s.am = am
}

func (s *max) Reduce(arg []interface{}) error {
	args := s.am(arg)
	if len(args) != 1 {
		return fmt.Errorf("max takes exactly 1 argument but %v were provided", len(args))
	}
	if args[0] == nil {
		return nil
	}
	s.notNull = true
	if args[0] == nil {
		return nil //ignore
	}
	switch v := args[0].(type) {
	case float64:
		s.result = math.Max(s.result, v)
	case int:
		s.result = math.Max(s.result, float64(v))
	case int64:
		s.result = math.Max(s.result, float64(v))
	case int32:
		s.result = math.Max(s.result, float64(v))
	case string:
		value, _, err := parseTime(v)
		if err != nil {
			return err
		}
		s.result = math.Max(s.result, float64(value.Unix()))
	default:
		return fmt.Errorf("max takes a single numerical argument, but %v was provided", args[0])
	}
	return nil
}

func (s *max) Return() *float64 {
	if !s.notNull {
		return nil
	}
	return &s.result
}

func (s *max) Copy() Reducer {
	return &max{am: s.am}
}
