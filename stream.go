package estream

import (
	"fmt"
	"math"
)

// AggregatorConfig struct describes how aggregator should be used by the
// Stream. It includes aggregator itself, parameters of aggregator's
// window, and callback functions that should be called when events enter or
// leave aggregator's window
type AggregatorConfig struct {
	// Aggregator is an instance of Aggregator interface
	Aggregator Aggregator
	// Count is the size of the aggregator's window in the number of
	// events. Should be zero if aggregator uses only time window.
	Count int64
	// Duration is the length of the aggregator's window in time. Should
	// be zero if window is only limited by the number of events.
	Duration int64
	// Batch. If set every time the window reaches its full size,
	// aggregator is reset and window size is reduced to zero.
	Batch bool
	// Disposable can be used with batch aggregators. Once window reached
	// its full size aggregator is reset and deleted.
	Disposable bool
	// StartTime should be set if aggregator should be activated at some
	// point in time
	StartTime int64
	// OnEnter is a function that is called every time after the
	// aggregator's Enter method. Argument is a current model time.
	OnEnter func(int64)
	// OnLeave is a function that is called every time before the
	// aggregator's Leave method. Argument is a current model time.
	OnLeave func(int64)
	// OnReset is a function that is called every time before the
	// aggregator's Reset method. Argument is a current model time.
	OnReset func(int64)
}

type aggregator struct {
	win        Window
	obj        Aggregator
	count      int64
	duration   int64
	batch      bool
	disposable bool
	deleted    bool
	startTime  int64
	onEnter    func(int64)
	onLeave    func(int64)
	onReset    func(int64)
}

// Stream struct represents stream, it keeps track of events and aggregators
// and invokes appropriate methods of aggregators when time changes or when
// events enter or leave aggregators' windows.
type Stream struct {
	events      []Event
	aggregators []*aggregator
	deleted     []int
	time        int64
	nextLeave   int64
	timeLength  int64
	eventLength int64
	in          chan<- interface{}
}

// NewStream creates a new Stream with model time set to the specified
// value
func NewStream(procTime int64) *Stream {
	in := make(chan interface{}, 100)
	p := &Stream{
		events:      make([]Event, 2),
		aggregators: make([]*aggregator, 0),
		time:        procTime,
		in:          in,
	}
	go p.listen(in)
	return p
}

func (s *Stream) addEvent(ev Event) error {
	if ev.Timestamp < s.time {
		return fmt.Errorf("Can't add events that are in the past")
	}
	if ev.Timestamp > s.time {
		s.setTime(ev.Timestamp)
	}
	s.events = append(s.events, ev)
	s.nextLeave = math.MaxInt64
	for n, aggr := range s.aggregators {
		del := s.addAggregatorEvent(aggr, ev)
		if del {
			s.deleted = append(s.deleted, n)
		}
	}
	s.removeOldEvents()
	return nil
}

func (s *Stream) removeOldEvents() {
	limit := s.time - s.timeLength
	for int64(len(s.events)) > s.eventLength && s.events[0].Timestamp <= limit {
		s.events = s.events[1:]
	}
}

func (s *Stream) addAggregatorEvent(aggr *aggregator, ev Event) bool {
	if aggr.startTime > s.time || aggr.deleted {
		return false
	}
	if aggr.count > 0 && int64(len(aggr.win.Events)) == aggr.count {
		if aggr.onLeave != nil {
			aggr.onLeave(s.time)
		}
		lev := aggr.win.shiftEvent()
		aggr.obj.Leave(lev, aggr.win)
	}
	startIdx := len(s.events) - (len(aggr.win.Events) + 1)
	aggr.win.Events = s.events[startIdx:]
	if aggr.count > 0 {
		aggr.win.StartTime = aggr.win.Events[0].Timestamp
		aggr.win.EndTime = s.time
		aggr.obj.TimeChange(aggr.win)
		aggr.obj.Enter(ev, aggr.win)
		if aggr.onEnter != nil {
			aggr.onEnter(s.time)
		}
		if aggr.batch && int64(len(aggr.win.Events)) == aggr.count {
			if aggr.onReset != nil {
				aggr.onReset(s.time)
			}
			aggr.win.StartTime = s.time
			aggr.win.Events = nil
			aggr.obj.Reset(aggr.win)
			if aggr.disposable {
				aggr.deleted = true
				return true
			}
		}
	} else {
		aggr.obj.Enter(ev, aggr.win)
		if aggr.onEnter != nil {
			aggr.onEnter(s.time)
		}
	}
	if aggr.duration > 0 {
		var nl int64
		if aggr.batch {
			nl = aggr.win.StartTime + aggr.duration
		} else {
			nl = aggr.win.Events[0].Timestamp + aggr.duration
		}
		if nl < s.nextLeave {
			s.nextLeave = nl
		}
	}
	return false
}

func (s *Stream) setTime(t int64) error {
	if t < s.time {
		return fmt.Errorf("Cannot set time to %d because it is less than the current time %d", t, s.time)
	}
	for t > s.nextLeave {
		nl := s.nextLeave
		s.nextLeave = math.MaxInt64
		err := s.setTime(nl)
		if err != nil {
			return err
		}
	}
	if t > s.time {
		// update time for every aggregator
		s.time = t
		s.nextLeave = math.MaxInt64
		for n, aggr := range s.aggregators {
			del := s.setAggregatorTime(aggr)
			if del {
				s.deleted = append(s.deleted, n)
			}
		}
	}
	s.removeOldEvents()
	return nil
}

func (s *Stream) setAggregatorTime(aggr *aggregator) bool {
	if aggr.startTime > s.time && aggr.startTime+aggr.duration < s.nextLeave {
		s.nextLeave = aggr.startTime + aggr.duration
	}
	if aggr.startTime > s.time || aggr.deleted {
		return false
	}
	aggr.win.EndTime = s.time
	if aggr.duration == 0 {
		aggr.obj.TimeChange(aggr.win)
		return false
	}
	if aggr.batch {
		aggr.obj.TimeChange(aggr.win)
		if aggr.win.TimeLength() == aggr.duration {
			if aggr.onReset != nil {
				aggr.onReset(s.time)
			}
			aggr.win.StartTime = aggr.win.EndTime
			aggr.win.Events = nil
			aggr.obj.Reset(aggr.win)
			if aggr.disposable {
				aggr.deleted = true
				return true
			}
		}
		nl := aggr.win.StartTime + aggr.duration
		if nl < s.nextLeave {
			s.nextLeave = nl
		}
	} else {
		if aggr.win.TimeLength() >= aggr.duration {
			aggr.win.StartTime = aggr.win.EndTime - aggr.duration
		}
		aggr.obj.TimeChange(aggr.win)
		if len(aggr.win.Events) != 0 && aggr.win.Events[0].Timestamp == aggr.win.StartTime && aggr.win.TimeLength() == aggr.duration {
			for len(aggr.win.Events) > 0 && aggr.win.Events[0].Timestamp == aggr.win.StartTime {
				if aggr.onLeave != nil {
					aggr.onLeave(s.time)
				}
				ev := aggr.win.shiftEvent()
				aggr.obj.Leave(ev, aggr.win)
			}
		}
		if len(aggr.win.Events) > 0 {
			nl := aggr.win.Events[0].Timestamp + aggr.duration
			if nl < s.nextLeave {
				s.nextLeave = nl
			}
		}
	}
	return false
}

func (s *Stream) addAggregator(aggrConf AggregatorConfig) error {
	if aggrConf.Count == 0 && aggrConf.Duration == 0 {
		return fmt.Errorf("At least one of Count and Duration must be set")
	}
	if aggrConf.Count < 0 || aggrConf.Duration < 0 {
		return fmt.Errorf("Count and Duration must not be negative")
	}
	if aggrConf.Disposable && !aggrConf.Batch {
		return fmt.Errorf("Only Batch aggregators can be Disposable")
	}
	if aggrConf.OnReset != nil && !aggrConf.Batch {
		return fmt.Errorf("OnReset is only makes sense for batch aggregators")
	}
	if aggrConf.OnLeave != nil && aggrConf.Batch {
		return fmt.Errorf("OnLeave is never called for batch aggregators")
	}
	if aggrConf.StartTime < s.time && aggrConf.StartTime != 0 {
		return fmt.Errorf("StartTime must be more or equal to current model time or zero")
	}
	winTime := s.time
	if aggrConf.StartTime > s.time {
		winTime = aggrConf.StartTime
	}
	aggr := &aggregator{
		win:        Window{StartTime: winTime, EndTime: winTime},
		obj:        aggrConf.Aggregator,
		count:      aggrConf.Count,
		duration:   aggrConf.Duration,
		batch:      aggrConf.Batch,
		disposable: aggrConf.Disposable,
		startTime:  aggrConf.StartTime,
		onEnter:    aggrConf.OnEnter,
		onLeave:    aggrConf.OnLeave,
		onReset:    aggrConf.OnReset,
	}
	if len(s.deleted) > 0 {
		s.aggregators[s.deleted[0]] = aggr
		s.deleted = s.deleted[1:]
	} else {
		s.aggregators = append(s.aggregators, aggr)
	}
	if aggr.count > s.eventLength {
		s.eventLength = aggr.count
	}
	if aggr.duration > s.timeLength {
		s.timeLength = aggr.duration
	}
	return nil
}

type msgSetTime struct {
	t   int64
	res chan<- error
}

// SetTime sets stream time to the specified value
func (s *Stream) SetTime(t int64) error {
	res := make(chan error)
	s.in <- &msgSetTime{t: t, res: res}
	return <-res
}

type msgAddAggregator struct {
	aggr AggregatorConfig
	res  chan<- error
}

// AddAggregator adds new aggregator object to the stream
func (s *Stream) AddAggregator(aggr AggregatorConfig) error {
	res := make(chan error)
	s.in <- &msgAddAggregator{aggr: aggr, res: res}
	return <-res
}

// AddEvent pushes new event to the stream
func (s *Stream) AddEvent(ev Event) {
	s.in <- ev
}

func (s *Stream) listen(in <-chan interface{}) {
	for m := range in {
		switch msg := m.(type) {
		case Event:
			s.addEvent(msg)
		case *msgSetTime:
			err := s.setTime(msg.t)
			msg.res <- err
			close(msg.res)
		case *msgAddAggregator:
			err := s.addAggregator(msg.aggr)
			msg.res <- err
			close(msg.res)
		}
	}
}
