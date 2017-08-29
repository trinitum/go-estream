package estream

import (
	"math"
)

// CountAvgAggregator struct represents aggregator calculating simple average
// value over the number of events.
type CountAvgAggregator struct {
	getValue func(interface{}) int64
	sum      int64
	sqSum    int64
	count    uint64
}

// NewCountAvgAggregator creates a new CountAvgAggregator and returns a
// reference to it. Argument is a function that accepts an event as the
// argument and returns the numeric value for this event.
func NewCountAvgAggregator(getValue func(interface{}) int64) *CountAvgAggregator {
	return &CountAvgAggregator{getValue: getValue}
}

// Enter is called when a new event enters window
func (d *CountAvgAggregator) Enter(e Event, w Window) {
	val := d.getValue(e.Value)
	d.sum += val
	d.sqSum += val * val
	d.count++
}

// Leave is called when an event leaves window
func (d *CountAvgAggregator) Leave(e Event, w Window) {
	val := d.getValue(e.Value)
	d.sum -= val
	d.sqSum -= val * val
	d.count--
}

// Reset is called for batch aggregators when the window is full
func (d *CountAvgAggregator) Reset(w Window) {
	d.sum = 0
	d.sqSum = 0
	d.count = 0
}

// TimeChange is called when time for the window has changed
func (d *CountAvgAggregator) TimeChange(w Window) {}

// Mean returns current sample averate of values of all events in the window
func (d *CountAvgAggregator) Mean() float64 {
	if d.count == 0 {
		return math.NaN()
	}
	return float64(d.sum) / float64(d.count)
}

// Variance returns sample variance of all events in the window
func (d *CountAvgAggregator) Variance() float64 {
	if d.count == 0 {
		return math.NaN()
	} else if d.count == 1 {
		return float64(0)
	} else {
		mean := d.Mean()
		vari := (float64(d.sqSum) - float64(d.count)*mean*mean) / float64(d.count-1)
		if vari < 0 {
			vari = 0
		}
		return vari
	}
}
