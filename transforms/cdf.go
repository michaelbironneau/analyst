package transforms

import (
	"fmt"
	"github.com/influxdata/tdigest"
)

type cdf struct {
	td       *tdigest.TDigest
	notNull  bool
	notFirst bool
	val      float64
	am       ArgumentMap
}

func (q *cdf) ParameterLen() int {
	return 2
}

func (q *cdf) SetArgumentMap(am ArgumentMap) {
	q.am = am
}

func (q *cdf) Reduce(arg []interface{}) error {
	if q.td == nil {
		q.td = tdigest.New()
	}

	args := q.am(arg)

	if len(args) != 2 {
		return fmt.Errorf("CDF takes exactly 2 arguments but %v were provided", len(args))
	}
	f, ok := args[1].(float64)

	if !ok {
		return fmt.Errorf("CDF expects second argument to be a float representing the value")
	}

	if q.notFirst && (f != q.val) {
		return fmt.Errorf("CDF expects the value for each group to be constant")
	}

	q.val = f

	q.notFirst = true
	if args[0] == nil {
		return nil
	}
	q.notNull = true
	switch v := args[0].(type) {
	case float64:
		q.td.Add(v, 1)
	case int:
		q.td.Add(float64(v), 1)
	case int64:
		q.td.Add(float64(v), 1)
	case int32:
		q.td.Add(float64(v), 1)
	default:
		return fmt.Errorf("QUANTILE wants its first argument to be numeric but %v was provided", args[0])
	}
	return nil
}

func (q *cdf) Return() *float64 {
	if !q.notNull || !q.notFirst {
		return nil
	}
	f := q.td.CDF(q.val)
	return &f
}

func (q *cdf) Copy() Reducer {
	return &quantile{am: q.am}
}
