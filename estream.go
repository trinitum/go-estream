package estream

// Event struct represents an event. Event consists of timestamp, which is an
// abstract 64bit integer value, and abstract payload.
type Event struct {
	// Timestamp of the event. Abstract integer value.
	Timestamp int64
	// Value. This is payload of the event.
	Value interface{}
}

// Window struct represents sliding window attached to aggregator.
type Window struct {
	// Window start time
	StartTime int64
	// Window end time
	EndTime int64
	// Events that are in the window
	Events []Event
}

// TimeLength returns the length of the time interval that is included in
// window
func (w *Window) TimeLength() int64 {
	return w.EndTime - w.StartTime
}

func (w *Window) shiftEvent() Event {
	ev := w.Events[0]
	w.Events = w.Events[1:]
	return ev
}

// Aggregator interface represents aggregate function. The aggregator is
// attached to the Stream with window parameters specified and stream invokes
// its methods when time changes or events enter or leave aggregator's window.
type Aggregator interface {
	// Enter is called when event enters aggregator's window. Event's
	// timestamp is always equal to window's end time, and event is
	// already in the Window
	Enter(Event, Window)
	// Leave is called when event is leaving aggregator's window.
	// Window's start time is always equal to event's timestamp, and
	// event is already removed from the Window. For batch aggregators
	// Reset method is used instead.
	Leave(Event, Window)
	// Reset is called for batch aggregators when the window is full, and
	// all events are leaving window. Window is already empty and start
	// time is equal to end time.
	Reset(Window)
	// TimeChange is called when start or end time of the window has
	// changed.
	TimeChange(Window)
}
