package transforms

import (
	"fmt"
	"math"
)

type min struct {
	result  float64
	notNull bool
	am      ArgumentMap
}

func (s *min) ParameterLen() int {
	return 1
}

func (s *min) SetArgumentMap(am ArgumentMap) {
	s.am = am
}

func (s *min) Reduce(arg []interface{}) error {
	args := s.am(arg)
	if len(args) != 1 {
		return fmt.Errorf("MIN takes exactly 1 argument but %v were provided", len(args))
	}
	if args[0] == nil {
		return nil
	}
	if !s.notNull {
		//initialize result
		s.result = math.MaxFloat64
	}
	s.notNull = true
	switch v := args[0].(type) {
	case float64:
		s.result = math.Min(s.result, v)
	case int:
		s.result = math.Min(s.result, float64(v))
	case int64:
		s.result = math.Min(s.result, float64(v))
	case int32:
		s.result = math.Min(s.result, float64(v))
	case string:
		value, _, err := parseTime(v)
		if err != nil {
			return err
		}
		s.result = math.Min(s.result, float64(value.Unix()))
	default:
		return fmt.Errorf("MIN takes a single numerical argument, but %v was provided", args[0])
	}
	return nil
}

func (s *min) Return() *float64 {
	if !s.notNull {
		return nil
	}
	return &s.result
}

func (s *min) Copy() Reducer {
	return &min{am: s.am}
}
