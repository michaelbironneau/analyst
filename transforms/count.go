package transforms

type count struct {
	result  float64
	notNull bool
	am      ArgumentMap
}

func (s *count) ParameterLen() int {
	return 1
}

func (s *count) SetArgumentMap(am ArgumentMap) {
	s.am = am
}

func (s *count) Reduce(arg []interface{}) error {
	s.result += 1
	return nil
}

func (s *count) Return() *float64 {
	if !s.notNull {
		return nil
	}
	return &s.result
}

func (s *count) Copy() Reducer {
	return &sum{am: s.am}
}
