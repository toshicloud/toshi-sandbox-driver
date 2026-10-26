package driver

import (
	"time"

	workspacev1 "go.toshicloud.io/api/workspacev1"
)

// EventType is the type of one event of the driver's sandboxes.
type EventType int

// The event types the driver reports. An event of a type not listed here is
// dropped.
const (
	// EventUnspecified is no event type the driver reports.
	EventUnspecified EventType = iota
	// EventWorkloadCreated is a sandbox created.
	EventWorkloadCreated
	// EventWorkloadStateChanged is a sandbox's state changed.
	EventWorkloadStateChanged
	// EventWorkloadUpdated is a sandbox's labels changed.
	EventWorkloadUpdated
	// EventWorkloadDeleted is a sandbox removed.
	EventWorkloadDeleted
	// EventExecutionStarted is an execution started.
	EventExecutionStarted
	// EventExecutionEnded is an execution ended.
	EventExecutionEnded
	// EventEmitted is an event a guest emitted.
	EventEmitted
)

// String returns the event type's name, as the API names it without the
// EVENT_TYPE_ prefix.
func (t EventType) String() string {
	switch t {
	case EventWorkloadCreated:
		return "workload_created"
	case EventWorkloadStateChanged:
		return "workload_state_changed"
	case EventWorkloadUpdated:
		return "workload_updated"
	case EventWorkloadDeleted:
		return "workload_deleted"
	case EventExecutionStarted:
		return "execution_started"
	case EventExecutionEnded:
		return "execution_ended"
	case EventEmitted:
		return "emitted"
	}
	return "unspecified"
}

// eventType returns the driver's type of the API's event type t.
func eventType(t workspacev1.EventType) EventType {
	switch t {
	case workspacev1.EventType_EVENT_TYPE_WORKLOAD_CREATED:
		return EventWorkloadCreated
	case workspacev1.EventType_EVENT_TYPE_WORKLOAD_STATE_CHANGED:
		return EventWorkloadStateChanged
	case workspacev1.EventType_EVENT_TYPE_WORKLOAD_UPDATED:
		return EventWorkloadUpdated
	case workspacev1.EventType_EVENT_TYPE_WORKLOAD_DELETED:
		return EventWorkloadDeleted
	case workspacev1.EventType_EVENT_TYPE_EXECUTION_STARTED:
		return EventExecutionStarted
	case workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED:
		return EventExecutionEnded
	case workspacev1.EventType_EVENT_TYPE_EMITTED:
		return EventEmitted
	}
	return EventUnspecified
}

// Event is one event of one of the driver's sandboxes, as Recover and Watch
// report it.
type Event struct {
	// Type is the event's type.
	Type EventType
	// SandboxID is the ID of the sandbox the event belongs to.
	SandboxID string
	// Time is when the server wrote the event.
	Time time.Time
	// Cursor is the position after the event, to store once it is handled.
	// The server delivers at least once: a repeat carries the same cursor.
	Cursor string
	// Execution is the execution an execution event starts or ends, and nil
	// for another event.
	Execution *ExecutionEvent
	// Emitted is what a guest emitted in an emitted event, and nil for
	// another event.
	Emitted *EmittedEvent

	event *workspacev1.Event
}

// Proto returns the event as the API's message, for code that needs a field
// the driver does not carry.
func (e *Event) Proto() *workspacev1.Event { return e.event }

// ExecutionEvent is an execution starting or ending.
type ExecutionEvent struct {
	// ExecutionID is the execution's ID.
	ExecutionID string
	// Argv is the program and its arguments the execution ran.
	Argv []string
	// ExitCode is the command's exit status, or -1 when it ended without
	// one: cancelled, or not reported.
	ExitCode int
	// Cancelled reports an execution the server cancelled.
	Cancelled bool
}

// EmittedEvent is an event a guest emitted.
type EmittedEvent struct {
	// Data is the event's payload, as the guest sent it.
	Data []byte
}

// eventFrom returns the driver's event for the API's event e.
func eventFrom(e *workspacev1.Event) *Event {
	ev := &Event{
		Type:      eventType(e.GetType()),
		SandboxID: e.GetWorkloadId(),
		Cursor:    e.GetCursor(),
		event:     e,
	}
	if ts := e.GetTime(); ts != nil {
		ev.Time = ts.AsTime()
	}
	if x := e.GetExecution(); x != nil {
		execution := &ExecutionEvent{ExecutionID: x.GetExecutionId(), Argv: x.GetArgv(), ExitCode: -1}
		if exit := x.GetExit(); exit != nil {
			execution.Cancelled = exit.GetCancelled()
			if exit.ExitCode != nil {
				execution.ExitCode = int(exit.GetExitCode())
			}
		}
		ev.Execution = execution
	}
	if x := e.GetEmitted(); x != nil {
		ev.Emitted = &EmittedEvent{Data: x.GetData()}
	}
	return ev
}
