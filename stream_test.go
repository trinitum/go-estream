package estream

import (
	"fmt"
	"math"
	"testing"
	"time"
)

const (
	onEnter int64 = iota
	onLeave
	onReset
)

func eventName(ev int64) string {
	switch ev {
	case onEnter:
		return "onEnter"
	case onLeave:
		return "onLeave"
	case onReset:
		return "onReset"
	default:
		panic(fmt.Sprintf("Unknown event type %d", ev))
	}
}

type testOut struct {
	cb    int64
	time  int64
	value float64
}

type averager interface {
	Mean() float64
}

type testAggregator struct {
	name     string
	aggr     Aggregator
	opts     []AggregatorOption
	expected []testOut
}

func testStream(t *testing.T, stream []Event, tal ...testAggregator) {
	p := NewStream(0)
	exp := make(map[string][]testOut)
	for _, ta := range tal {
		name := ta.name
		exp[name] = ta.expected
		aggr := ta.aggr.(averager)
		report := func(gcb, gtime int64) {
			gvalue := aggr.Mean()
			if len(exp[name]) == 0 {
				t.Errorf("%s: got %s, %d, %f but expected nothing", name, eventName(gcb), gtime, gvalue)
				return
			}
			e := exp[name][0]
			exp[name] = exp[name][1:]
			if e.cb != gcb || e.time != gtime || math.Abs(e.value-gvalue) > 0.00001 {
				t.Errorf("%s: got %s, %d, %f, but expected %s, %d, %f", name, eventName(gcb), gtime, gvalue, eventName(e.cb), e.time, e.value)
				return
			}
			// t.Logf("%s: got %s, %d, %f as expected", name, eventName(gcb), gtime, gvalue)
		}
		var opts []AggregatorOption
		for _, o := range ta.opts {
			opts = append(opts, o)
		}
		opts = append(opts, WithOnEnter(func(ts int64) { report(onEnter, ts) }))
		opts = append(opts, WithOnReset(func(ts int64) { report(onReset, ts) }))
		opts = append(opts, WithOnLeave(func(ts int64) { report(onLeave, ts) }))
		if err := p.AddAggregator(ta.aggr, opts...); err != nil {
			t.Errorf("Couldn't add aggregator %s: %v", name, err)
		}
	}
	go func() {
		time.Sleep(25 * time.Second)
		panic("test timed out")
	}()
	for _, ev := range stream {
		ev.Value = int64(ev.Value.(int))
		p.AddEvent(ev)
	}
	if err := p.SetTime(0); err == nil {
		t.Error("Didn't expect to be able to set time to 0")
	}
	for name, out := range exp {
		if len(out) > 0 {
			t.Errorf("%s: expected but didn't get %v", name, out)
		}
	}
}

func TestTimeStream(t *testing.T) {
	stream := []Event{
		{0, 1},
		{1, 2},
		// two events enter at the same time
		{2, 3},
		{2, 6},
		{5, 1},
		// long time no events
		{19, 5},
		{22, 7},
	}
	timeTest1 := []testOut{
		{onEnter, 0, 1},
		{onEnter, 1, 1},
		{onEnter, 2, 1.5},
		{onEnter, 2, 1.5},
		{onLeave, 3, 3},
		{onLeave, 4, 4.66666},
		{onLeave, 5, 6},
		{onLeave, 5, 6},
		{onEnter, 5, 6},
		{onLeave, 8, 1},
		{onEnter, 19, 1},
		{onLeave, 22, 5},
		{onEnter, 22, 5},
	}
	timeTest1Agg := testAggregator{
		name:     "Test 1",
		aggr:     NewTimeAvgAggregator(func(i interface{}) int64 { return i.(int64) }),
		opts:     []AggregatorOption{WithDuration(3)},
		expected: timeTest1,
	}
	timeTest2 := []testOut{
		{onEnter, 0, 1},
		{onEnter, 1, 1},
		{onEnter, 2, 1.5},
		{onEnter, 2, 1.5},
		{onReset, 3, 3},
		{onEnter, 5, 6},
		{onReset, 6, 4.33333},
		{onReset, 9, 1},
		{onReset, 12, 1},
		{onReset, 15, 1},
		{onReset, 18, 1},
		{onEnter, 19, 1},
		{onReset, 21, 3.66666},
		{onEnter, 22, 5},
	}
	timeTest2Agg := testAggregator{
		name: "Test 2",
		aggr: NewTimeAvgAggregator(func(i interface{}) int64 { return i.(int64) }),
		opts: []AggregatorOption{
			WithDuration(3),
			WithBatch(),
		},
		expected: timeTest2,
	}
	timeTest3 := []testOut{
		{onEnter, 5, 1},
		{onReset, 10, 1},
	}
	timeTest3Agg := testAggregator{
		name: "Test 3",
		aggr: NewTimeAvgAggregator(func(i interface{}) int64 { return i.(int64) }),
		opts: []AggregatorOption{
			WithDuration(7),
			WithStartTime(3),
			WithDisposable(),
		},
		expected: timeTest3,
	}
	testStream(t, stream, timeTest1Agg, timeTest2Agg, timeTest3Agg)
}

func TestCountStream(t *testing.T) {
	stream := []Event{
		{0, 1},
		{1, 2},
		{1, 3},
		{1, 4},
		{1, 5},
		{2, 6},
		{3, 7},
		{4, 1},
		{5, 1},
	}
	countTest1 := []testOut{
		{onEnter, 0, 1},
		{onEnter, 1, 1.5},
		{onEnter, 1, 2},
		{onEnter, 1, 2.5},
		{onEnter, 1, 3},
		{onLeave, 2, 3},
		{onEnter, 2, 4},
		{onLeave, 3, 4},
		{onEnter, 3, 5},
		{onLeave, 4, 5},
		{onEnter, 4, 4.6},
		{onLeave, 5, 4.6},
		{onEnter, 5, 4},
	}
	countTest1Agg := testAggregator{
		name: "Test 1",
		aggr: NewCountAvgAggregator(func(i interface{}) int64 { return i.(int64) }),
		opts: []AggregatorOption{
			WithCount(5),
		},
		expected: countTest1,
	}
	countTest2 := []testOut{
		{onEnter, 0, 1},
		{onEnter, 1, 1.5},
		{onEnter, 1, 2},
		{onReset, 1, 2},
		{onEnter, 1, 4},
		{onEnter, 1, 4.5},
		{onEnter, 2, 5},
		{onReset, 2, 5},
		{onEnter, 3, 7},
		{onEnter, 4, 4},
		{onEnter, 5, 3},
		{onReset, 5, 3},
	}
	countTest2Agg := testAggregator{
		name: "Test 2",
		aggr: NewCountAvgAggregator(func(i interface{}) int64 { return i.(int64) }),
		opts: []AggregatorOption{
			WithCount(3),
			WithBatch(),
		},
		expected: countTest2,
	}
	countTest3 := []testOut{
		{onEnter, 0, 1},
		{onEnter, 1, 1},
		{onEnter, 1, 1},
		{onLeave, 1, 1},
		// win.StartTime should move to 1 after first event left
		{onEnter, 1, 4},
		{onLeave, 1, 4},
		{onEnter, 1, 5},
		{onLeave, 2, 5},
		{onEnter, 2, 5},
		{onLeave, 3, 5.5},
		{onEnter, 3, 5.5},
		{onLeave, 4, 6},
		// win.StartTime should move to 2 here
		{onEnter, 4, 6.5},
		{onLeave, 5, 4.66666},
		// win.StartTime should move to 3 here
		{onEnter, 5, 4},
	}
	countTest3Agg := testAggregator{
		name: "Test 3",
		aggr: NewTimeAvgAggregator(func(i interface{}) int64 {
			return i.(int64)
		}),
		opts:     []AggregatorOption{WithCount(3)},
		expected: countTest3,
	}
	testStream(t, stream, countTest1Agg, countTest2Agg, countTest3Agg)
}

func TestMixedStream(t *testing.T) {
	stream := []Event{
		{5, 1},
		{6, 2},
		{7, 3},
		{8, 4},
		{11, 5},
		{15, 6},
		{25, 1},
		{26, 2},
		{27, 3},
		{28, 4},
	}
	mxTest1 := []testOut{
		{onEnter, 5, 1},
		{onEnter, 6, 1},
		{onEnter, 7, 1.5},
		{onLeave, 8, 2},
		{onEnter, 8, 2.5},
		{onLeave, 11, 3.4},
		{onEnter, 11, 3.75},
		{onLeave, 12, 4},
		{onLeave, 13, 4.4},
		{onEnter, 15, 5},
		{onLeave, 16, 5.2},
		{onLeave, 20, 6},
		{onEnter, 25, 1},
		{onEnter, 26, 1},
		{onEnter, 27, 1.5},
		{onLeave, 28, 2},
		{onEnter, 28, 2.5},
	}
	mxTest1Agg := testAggregator{
		name: "Test 1",
		aggr: NewTimeAvgAggregator(func(i interface{}) int64 { return i.(int64) }),
		opts: []AggregatorOption{
			WithDuration(5),
			WithCount(3),
		},
		expected: mxTest1,
	}
	mxTest2 := []testOut{
		{onEnter, 11, 5},
		{onEnter, 15, 5},
		{onReset, 17, 5.33333},
		{onReset, 23, 6},
		{onEnter, 25, 1},
		{onEnter, 26, 1},
		{onEnter, 27, 1.5},
		{onReset, 27, 1.5},
		{onEnter, 28, 4},
	}
	mxTest2Agg := testAggregator{
		name: "Test 2",
		aggr: NewTimeAvgAggregator(func(i interface{}) int64 { return i.(int64) }),
		opts: []AggregatorOption{
			WithStartTime(10),
			WithDuration(6),
			WithCount(3),
			WithBatch(),
		},
		expected: mxTest2,
	}
	testStream(t, stream, mxTest1Agg, mxTest2Agg)
}
