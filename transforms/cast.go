package transforms

import (
	"time"
	"strconv"
	"fmt"
)

// CastFn casts a source interface into a destination interface. A nil interface maps to a nil interface regardless of the destination type.
type CastFn func(src interface{}) (interface{}, error)

// castFns is a map from the destination type to the cast function
var castFns = map[string]CastFn{"int": castToInt, "varchar": castToString, "datetime": castToTime}

func castToInt(src interface{}) (interface{}, error){
	if src == nil {
		return nil, nil
	}
	switch v := src.(type){
	case int:
		return v, nil
	case float64:
		return int(v), nil
	case int64:
		return int(v), nil
	case string:
		return strconv.Atoi(v)
	case *time.Time:
		return int(v.Unix()), nil
	case time.Time:
		return int(v.Unix()), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return nil, fmt.Errorf("unsupported type for casting to integer: %T", src)
	}
}

func castToString(src interface{}) (interface{}, error){
	if src == nil {
		return nil, nil
	}
	switch v := src.(type){
	case string:
		return v, nil
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.Itoa(int(v)), nil
	case float64:
		return fmt.Sprintf("%f", v), nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case *time.Time:
		return v.Format(time.RFC3339Nano), nil
	case time.Time:
		return v.Format(time.RFC3339Nano), nil
	default:
		return fmt.Sprintf("%v", src), nil //TODO: Better type checking here or it will lead to unpredictable results for someone
	}
}

func castToTime(src interface{}) (interface{}, error){
	if src == nil {
		return nil, nil
	}
	switch v := src.(type){
	case string:
		t, _, err := parseTime(v)
		return t, err
	case int:
		t := time.Unix(int64(v),0)
		return &t, nil
	default:
		return nil, fmt.Errorf("unsupported type for casting to integer: %T", src)
	}

}