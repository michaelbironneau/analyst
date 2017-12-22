package transforms

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

var testSeries = map[string]Timeseries{
	"single": Timeseries([]TimeseriesItem{
		TimeseriesItem{
			Time:  time.Unix(0, 0),
			Value: 1,
		},
	}),
	"outside interval": Timeseries([]TimeseriesItem{
		TimeseriesItem{
			Time:  time.Unix(1, 0),
			Value: 1,
		},
		TimeseriesItem{
			Time:  time.Unix(10, 0),
			Value: 0,
		},
	}),
	"one inside": Timeseries([]TimeseriesItem{
		TimeseriesItem{
			Time:  time.Unix(0, 0),
			Value: 1,
		},
		TimeseriesItem{
			Time:  time.Unix(2, 0),
			Value: 3,
		},
		TimeseriesItem{
			Time:  time.Unix(10, 0),
			Value: 4,
		},
	}),
	"two inside": Timeseries([]TimeseriesItem{
		TimeseriesItem{
			Time:  time.Unix(0, 0),
			Value: 1,
		},
		TimeseriesItem{
			Time:  time.Unix(1, 0),
			Value: 2,
		},
		TimeseriesItem{
			Time:  time.Unix(2, 0),
			Value: 3,
		},
		TimeseriesItem{
			Time:  time.Unix(10, 0),
			Value: 4,
		},
	}),
	"all inside but one": Timeseries([]TimeseriesItem{
		TimeseriesItem{
			Time:  time.Unix(0, 0),
			Value: 1,
		},
		TimeseriesItem{
			Time:  time.Unix(1, 0),
			Value: 2,
		},
		TimeseriesItem{
			Time:  time.Unix(2, 0),
			Value: 3,
		},
		TimeseriesItem{
			Time:  time.Unix(3, 0),
			Value: 4,
		},
	}),
}

func TestTimeseriesResampling(t *testing.T) {
	start := time.Unix(1, 0)
	finish := time.Unix(5, 0)
	expected := map[string]float64{
		"single":             1,
		"outside interval":   1,
		"one inside":         (1./4.)*1. + (3./4.)*3.,
		"two inside":         (1./4.)*2. + (3./4.)*3.,
		"all inside but one": (1./4.)*2. + (1./4.)*3. + (2./4.)*4.,
	}
	Convey("Given the timeseries resampling algorithm", t, func() {

		for key, ts := range testSeries {
			Convey(fmt.Sprintf("It should resample '%s' correctly", key), func() {
				So(*ts.Mean(start, finish), ShouldEqual, expected[key])
			})

		}
	})
}

func BenchmarkMeanSmallSeries(b *testing.B) {
	b.StopTimer()
	start := time.Unix(1, 0)
	finish := time.Unix(5, 0)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		testSeries["two inside"].Mean(start, finish)
	}
}
