package transforms

import "fmt"

type sum struct {
	result  float64
	notNull bool
	am      ArgumentMap
}

func (s *sum) ParameterLen() int {
	return 1
}

func (s *sum) SetArgumentMap(am ArgumentMap) {
	s.am = am
}

func (s *sum) Reduce(arg []interface{}) error {
	args := s.am(arg)

	if len(args) != 1 {
		return fmt.Errorf("SUM takes exactly 1 argument but %v were provided", len(args))
	}
	if args[0] == nil {
		return nil
	}
	s.notNull = true
	switch v := args[0].(type) {
	case float64:
		s.result += v
	case int:
		s.result += float64(v)
	case int64:
		s.result += float64(v)
	case int32:
		s.result += float64(v)
	default:
		return fmt.Errorf("SUM takes a single numerical argument, but %v was provided", args[0])
	}
	return nil
}

func (s *sum) Return() *float64 {
	if !s.notNull {
		return nil
	}
	return &s.result
}

func (s *sum) Copy() Reducer {
	return &sum{am: s.am}
}
