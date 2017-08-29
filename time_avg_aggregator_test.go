package estream

import (
	"math"
	"testing"
)

func TestTimeAvgAggregator(t *testing.T) {
	c := NewTimeAvgAggregator(func(i interface{}) int64 { return i.(int64) })

	type opt int
	const (
		opEnter opt = iota
		opLeave
		opTimeChange
		opReset
	)

	type cTest struct {
		op        opt
		startTime int64
		endTime   int64
		value     int64
		mean      float64
	}
	cTests := []cTest{
		{opTimeChange, 0, 1, 0, math.NaN()},
		{opEnter, 0, 1, 3, 3},
		{opTimeChange, 0, 2, 0, 3},
		{opEnter, 0, 2, 2, 3},
		{opEnter, 0, 2, 1, 3},
		{opTimeChange, 0, 3, 0, 2},
		{opTimeChange, 1, 5, 0, 1.5},
		{opLeave, 1, 5, 3, 1.5},
		{opTimeChange, 2, 6, 0, 1},
		{opEnter, 2, 6, 2, 1},
		{opTimeChange, 2, 8, 0, 4.0 / 3.0},
		{opLeave, 2, 8, 2, 4.0 / 3.0},
		{opLeave, 2, 8, 1, 4.0 / 3.0},
		{opReset, 8, 8, 0, 2},
		{opTimeChange, 8, 10, 0, 2},
	}
	events := make([]Event, 0)
	for i, test := range cTests {
		w := Window{
			StartTime: test.startTime,
			EndTime:   test.endTime,
			Events:    events,
		}
		switch test.op {
		case opTimeChange:
			c.TimeChange(w)
		case opEnter:
			ev := Event{
				Timestamp: test.endTime,
				Value:     test.value,
			}
			events = append(events, ev)
			w.Events = events
			c.Enter(ev, w)
		case opLeave:
			ev := events[0]
			events = events[1:]
			w.Events = events
			c.Leave(ev, w)
		case opReset:
			c.Reset(w)
		}
		if (math.IsNaN(test.mean) && !math.IsNaN(c.Mean())) || math.Abs(c.Mean()-test.mean) > 1e-6 {
			t.Errorf("In test %d expected mean to be %f, but got %f", i, test.mean, c.Mean())
		}
	}
}
