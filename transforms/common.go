package transforms

import (
	"fmt"
	"time"
)

const (
	DefaultDatabaseDateFormat   = "YYYY-MM-DDTHH:MM:SSZ"
	defaultDatabaseGoDateFormat = "2006-01-02T15:04:05Z"
)

func parseTime(s string) (*time.Time, string, error) {
	var (
		t   time.Time
		err error
	)
	t, err = time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t, err = time.Parse(defaultDatabaseGoDateFormat, s)
			if err != nil {
				return nil, "", fmt.Errorf("unknown time format %s: expected RFC3339, RFC3339 with nanoseconds or %s", s, DefaultDatabaseDateFormat)
			}
			return &t, DefaultDatabaseDateFormat, nil
		}
		return &t, time.RFC3339Nano, nil
	}
	return &t, time.RFC3339, nil
}
