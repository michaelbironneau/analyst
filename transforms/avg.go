package transforms

import "fmt"

type avg struct {
	result  float64
	notNull bool
	count   float64
	am      ArgumentMap
}

func (s *avg) ParameterLen() int {
	return 1
}

func (s *avg) SetArgumentMap(am ArgumentMap) {
	s.am = am
}

func (s *avg) Reduce(arg []interface{}) error {
	args := s.am(arg)
	if len(args) != 1 {
		return fmt.Errorf("AVG takes exactly 1 argument but %v were provided", len(args))
	}
	s.notNull = true
	switch v := args[0].(type) {
	case float64:
		s.result = (s.count*s.result + v) / (s.count + 1)
	case int:
		s.result += (s.count*s.result + float64(v)) / (s.count + 1)
	case int64:
		s.result += (s.count*s.result + float64(v)) / (s.count + 1)
	case int32:
		s.result += (s.count*s.result + float64(v)) / (s.count + 1)
	default:
		return fmt.Errorf("AVG takes a single numerical argument, but %v was provided", args[0])
	}
	s.count += 1
	return nil
}

func (s *avg) Return() *float64 {
	if !s.notNull {
		return nil
	}
	return &s.result
}

func (s *avg) Copy() Reducer {
	return &avg{am: s.am}
}
