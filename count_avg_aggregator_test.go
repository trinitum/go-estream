package estream

import (
	"math"
	"testing"
)

func TestCountAvgAggregator(t *testing.T) {
	d := NewCountAvgAggregator(func(i interface{}) int64 { return i.(int64) })

	type dTest struct {
		op    int
		value int64
		mean  float64
		vari  float64
	}
	dTests := []dTest{
		{1, 10, 10, 0},
		{1, 20, 15, 50},
		{1, 0, 10, 100},
		{-1, 10, 10, 200},
		{0, 0, math.NaN(), math.NaN()},
	}
	for i, test := range dTests {
		ev := Event{0, test.value}
		w := Window{}
		if test.op > 0 {
			d.Enter(ev, w)
		} else if test.op < 0 {
			d.Leave(ev, w)
		} else {
			d.Reset(w)
		}
		if (math.IsNaN(test.mean) && !math.IsNaN(d.Mean())) || math.Abs(d.Mean()-test.mean) > 1e-6 {
			t.Errorf("In test %d expected mean to be %f, but it's %f", i, test.mean, d.Mean())
		}
		if (math.IsNaN(test.vari) && !math.IsNaN(d.Variance())) || math.Abs(d.Variance()-test.vari) > 1e-6 {
			t.Errorf("In test %d expected variance to be %f, but it's %f", i, test.vari, d.Variance())
		}
	}
}
