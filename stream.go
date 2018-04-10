package estream

import (
	"fmt"
	"math"
)

// AggregatorOption configures aggregator
type AggregatorOption func(*aggregator) error

// WithCount specifies the window size in the number of events
func WithCount(count int64) AggregatorOption {
	return func(agg *aggregator) error {
		if count < 0 {
			return fmt.Errorf("WithCount argument must not be negative")
		}
		agg.count = count
		return nil
	}
}

// WithDuration specifies the time span of the window
func WithDuration(duration int64) AggregatorOption {
	return func(agg *aggregator) error {
		if duration < 0 {
			return fmt.Errorf("WithDuration argument must not be negative")
		}
		agg.duration = duration
		return nil
	}
}

// WithBatch specifies that when window reaches it's maximum size all events
// are removed from it at once and window is reset to a zero size
func WithBatch() AggregatorOption {
	return func(agg *aggregator) error {
		agg.batch = true
		return nil
	}
}

// WithDisposable specifies that when window reaches it's maximum size, all
// events are removed from it at once, and aggregator itself is dropped
func WithDisposable() AggregatorOption {
	return func(agg *aggregator) error {
		agg.disposable = true
		agg.batch = true
		return nil
	}
}

// WithStartTime specifies the time from which aggregator should start
// aggregating events, events arriving before the specified time are not
// passed to the aggregator. The time specified must not be from the past.
func WithStartTime(t int64) AggregatorOption {
	return func(agg *aggregator) error {
		if agg.win.StartTime > t {
			return fmt.Errorf("The specified start time %d is less than current model time %d", t, agg.win.StartTime)
		}
		agg.startTime = t
		agg.win.StartTime = t
		agg.win.EndTime = t
		return nil
	}
}

// WithOnEnter specifies the function that should be invoked every time
// immidiately after the aggregator's Enter method. The only argument is the
// current model time.
func WithOnEnter(f func(int64)) AggregatorOption {
	return func(agg *aggregator) error {
		agg.onEnter = f
		return nil
	}
}

// WithOnLeave specifies the function that should be invoked every time just
// before the aggregator's Leave method. The only argument is the current
// model time.
func WithOnLeave(f func(int64)) AggregatorOption {
	return func(agg *aggregator) error {
		agg.onLeave = f
		return nil
	}
}

// WithOnReset specifies the function that should be invoked every time just
// before the aggregator's Reset method. The only argument is the current
// model time.
func WithOnReset(f func(int64)) AggregatorOption {
	return func(agg *aggregator) error {
		agg.onReset = f
		return nil
	}
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

func (s *Stream) addAggregator(aggrObj Aggregator, aggrOptions ...AggregatorOption) error {
	winTime := s.time
	aggr := &aggregator{
		win: Window{StartTime: winTime, EndTime: winTime},
		obj: aggrObj,
	}
	for _, opt := range aggrOptions {
		if err := opt(aggr); err != nil {
			return err
		}
	}
	if aggr.count == 0 && aggr.duration == 0 {
		return fmt.Errorf("At least one of Count or Duration must be set")
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
	aggr Aggregator
	opts []AggregatorOption
	res  chan<- error
}

// AddAggregator adds new aggregator object to the stream
func (s *Stream) AddAggregator(aggr Aggregator, opts ...AggregatorOption) error {
	res := make(chan error)
	s.in <- &msgAddAggregator{aggr: aggr, opts: opts, res: res}
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
			err := s.addAggregator(msg.aggr, msg.opts...)
			msg.res <- err
			close(msg.res)
		}
	}
}
