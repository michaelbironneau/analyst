package transforms

import (
	"fmt"
	"github.com/influxdata/tdigest"
)

type quantile struct {
	td       *tdigest.TDigest
	notNull  bool
	notFirst bool
	quantile float64
	am       ArgumentMap
}

func (q *quantile) ParameterLen() int {
	return 2
}

func (q *quantile) SetArgumentMap(am ArgumentMap) {
	q.am = am
}

func (q *quantile) Reduce(arg []interface{}) error {
	if q.td == nil {
		q.td = tdigest.New()
	}

	args := q.am(arg)

	if len(args) != 2 {
		return fmt.Errorf("QUANTILE takes exactly 2 arguments but %v were provided", len(args))
	}
	f, ok := args[1].(float64)

	if !ok {
		return fmt.Errorf("QUANTILE expects second argument to be a float between 0 and 1 representing the quantile")
	}

	if q.notFirst && (f != q.quantile) {
		return fmt.Errorf("QUANTILE expects the quantile for each group to be constant")
	}

	q.quantile = f

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

func (q *quantile) Return() *float64 {
	if !q.notNull || !q.notFirst {
		return nil
	}
	f := q.td.Quantile(q.quantile)
	return &f
}

func (q *quantile) Copy() Reducer {
	return &quantile{am: q.am}
}
