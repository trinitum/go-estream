package estream

import (
	"math"
)

// TimeAvgAggregator struct represents aggregator calculating average value of
// the process over the time
type TimeAvgAggregator struct {
	getValue    func(interface{}) int64
	initialized bool
	startTime   int64
	startValue  int64
	endTime     int64
	endValue    int64
	integral    int64
}

// NewTimeAvgAggregator creates a new TimeAvgAggregator and returns a
// reference to it. Argument is a function that accepts an event as the
// argument and returns the numeric value for this event.
func NewTimeAvgAggregator(getValue func(interface{}) int64) *TimeAvgAggregator {
	return &TimeAvgAggregator{getValue: getValue}
}

// Enter is called when a new event enters window
func (c *TimeAvgAggregator) Enter(e Event, w Window) {
	val := c.getValue(e.Value)
	if !c.initialized {
		c.startTime = e.Timestamp
		c.startValue = val
		c.initialized = true
	}
	c.endTime = e.Timestamp
	c.endValue = val
}

// Leave is called when an event leaves window
func (c *TimeAvgAggregator) Leave(e Event, w Window) {
	if len(w.Events) > 0 && w.Events[0].Timestamp == w.StartTime {
		c.startValue = c.getValue(w.Events[0].Value)
	} else {
		c.startValue = c.getValue(e.Value)
	}
}

// Reset is called for batch aggregators when the window is full
func (c *TimeAvgAggregator) Reset(w Window) {
	c.integral = 0
	if c.initialized {
		c.startTime = w.StartTime
		c.startValue = c.endValue
		c.endTime = w.EndTime
	}
}

// TimeChange is called when time for the window has changed
func (c *TimeAvgAggregator) TimeChange(w Window) {
	if !c.initialized {
		return
	}
	if c.startTime < w.StartTime {
		c.integral -= c.startValue * (w.StartTime - c.startTime)
		c.startTime = w.StartTime
	}
	if c.endTime < w.EndTime {
		c.integral += c.endValue * (w.EndTime - c.endTime)
		c.endTime = w.EndTime
	}
}

// Mean returns average value of the process in the window
func (c *TimeAvgAggregator) Mean() float64 {
	if !c.initialized {
		return math.NaN()
	}
	if c.startTime == c.endTime {
		return float64(c.endValue)
	}
	return float64(c.integral) / float64(c.endTime-c.startTime)
}
