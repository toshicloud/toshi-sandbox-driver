package driver

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"connectrpc.com/connect"
	workspacev1 "go.toshicloud.io/api/workspacev1"
)

// Recovery is what Recover found.
type Recovery struct {
	// Sandboxes are the driver's sandboxes at Cursor: the sandboxes
	// labelled with its name.
	Sandboxes []*Sandbox
	// Events are the execution and emitted events of the driver's
	// sandboxes after the cursor Recover was given, and the delete events of
	// those deleted since, oldest first.
	Events []*Event
	// Cursor is where Watch starts, and the position to store for the next
	// Recover until Watch reports a later one.
	Cursor string
}

// Recover finds the driver's sandboxes when it starts. Sandboxes.List lists
// the sandboxes labelled with the driver's name. Given the cursor stored by
// an earlier run, ListEvents then reads the events after it, and Events
// keeps the execution and emitted events of the driver's sandboxes, and the
// delete events of those deleted since. Each sandbox event carries the
// sandbox after its change, so Recover applies them in order to the list: a
// sandbox created after the list was read is added, one deleted is removed,
// and Sandboxes holds each as it was at Cursor, the position after the last
// event. Given an empty cursor, on a first start, Events is empty and Cursor
// is the list's position. A cursor the event log no longer keeps fails with
// OUT_OF_RANGE; Recover with an empty cursor then starts afresh.
func (d *Driver) Recover(ctx context.Context, cursor string) (*Recovery, error) {
	rec := &Recovery{}
	list := &workspacev1.ListSandboxesRequest{
		Labels: map[string]string{LabelDriver: d.cfg.Name},
	}
	for {
		resp, err := d.client.Workload().Sandboxes().List(ctx, list)
		if err != nil {
			return nil, fmt.Errorf("driver: list sandboxes: %w", err)
		}
		// The first page's cursor is the log's position when the list
		// was read.
		if list.PageToken == "" {
			rec.Cursor = resp.GetCursor()
		}
		for _, sb := range resp.GetSandboxes() {
			if d.mine(sb) {
				d.know(sb.GetId())
				rec.Sandboxes = append(rec.Sandboxes, sandboxFrom(sb))
			}
		}
		if resp.GetNextPageToken() == "" {
			break
		}
		list.PageToken = resp.GetNextPageToken()
	}
	if cursor == "" {
		return rec, nil
	}

	var events []*workspacev1.Event
	req := &workspacev1.ListEventsRequest{PageToken: cursor}
	for {
		resp, err := d.client.Workload().ListEvents(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("driver: list events after the stored cursor: %w", err)
		}
		events = append(events, resp.GetEvents()...)
		if resp.GetNextPageToken() == "" {
			rec.Cursor = resp.GetCursor()
			break
		}
		req.PageToken = resp.GetNextPageToken()
	}
	// A sandbox deleted before the list was read is not listed; its later
	// sandbox events make it known before its executions' events are kept.
	for _, e := range events {
		if d.mine(e.GetSandbox()) {
			d.know(e.GetWorkloadId())
		}
	}
	for _, e := range events {
		sb := e.GetSandbox()
		if sb == nil {
			if d.passes(e) {
				rec.Events = append(rec.Events, eventFrom(e))
			}
			continue
		}
		deleted := e.GetType() == workspacev1.EventType_EVENT_TYPE_WORKLOAD_DELETED
		if deleted && d.mine(sb) {
			rec.Events = append(rec.Events, eventFrom(e))
		}
		i := slices.IndexFunc(rec.Sandboxes, func(s *Sandbox) bool { return s.ID() == e.GetWorkloadId() })
		switch {
		case deleted || !d.mine(sb):
			if i >= 0 {
				rec.Sandboxes = slices.Delete(rec.Sandboxes, i, i+1)
			}
		case i >= 0:
			rec.Sandboxes[i] = sandboxFrom(sb)
		default:
			rec.Sandboxes = append(rec.Sandboxes, sandboxFrom(sb))
		}
	}
	return rec, nil
}

// sandboxFrom returns the sandbox sb, named by its label.
func sandboxFrom(sb *workspacev1.Sandbox) *Sandbox {
	return &Sandbox{Name: sb.GetLabels()[LabelSandbox], sandbox: sb}
}

// deadlineGrace is how long Watch waits for ctx's deadline after a watch
// failed with DEADLINE_EXCEEDED before it returns the watch's error.
const deadlineGrace = time.Second

// Update is one change Watch reports: a sandbox's change or an event.
type Update struct {
	// Type is the change's or the event's type.
	Type EventType
	// Sandbox is one of the driver's sandboxes after a change
	// Sandboxes.Watch reported, or nil for an event.
	Sandbox *Sandbox
	// Event is an execution or emitted event of one of the driver's
	// sandboxes that WatchEvents reported, or nil for a sandbox's change.
	Event *Event
	// Cursor is set with Event: the position after it, to store once the
	// update is handled. It is empty on a sandbox's change, which leaves the
	// stored cursor as it was.
	Cursor string
}

// Watch follows the driver's sandboxes from cursor, as Recover returned it,
// and calls fn with each update, one at a time: Sandboxes.Watch reports the
// sandboxes' changes, WatchEvents their executions' and emitted events, and
// each resumes after a dropped connection. Neither call filters by label, so
// Watch passes on the changes of sandboxes labelled with the driver's name,
// and the events of the sandboxes Recover returned or whose sandbox events,
// which come before their executions' in the log, show them as the driver's.
// The two watches are not ordered with each other: an execution's event can
// come before the change that shows its sandbox. An event can come again
// after a restart from a cursor stored before it was handled: the server
// delivers at least once, with the same event ID. It returns when ctx ends, a
// watch ends or fn returns an error, with why; a watch ends once toshi-go's
// reconnect attempts run out, and Recover from the stored cursor and Watch
// again carry on from there.
func (d *Driver) Watch(ctx context.Context, cursor string, fn func(Update) error) error {
	if cursor == "" {
		return errors.New("driver: Watch needs the cursor Recover returned")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sandboxes, err := d.client.Workload().Sandboxes().Watch(ctx, &workspacev1.WatchSandboxesRequest{Cursor: cursor})
	if err != nil {
		return endErr(ctx, fmt.Errorf("driver: watch sandboxes: %w", err))
	}
	defer func() { _ = sandboxes.Close() }()
	events, err := d.client.Workload().WatchEvents(ctx, &workspacev1.WatchEventsRequest{Cursor: cursor})
	if err != nil {
		return endErr(ctx, fmt.Errorf("driver: watch events: %w", err))
	}
	defer func() { _ = events.Close() }()

	updates := make(chan Update)
	ended := make(chan error, 2)
	send := func(u Update) bool {
		select {
		case updates <- u:
			return true
		case <-ctx.Done():
			return false
		}
	}
	go func() {
		for sandboxes.Receive() {
			msg := sandboxes.Msg()
			sb := msg.GetSandbox()
			if !d.mine(sb) {
				continue
			}
			if !send(Update{Type: eventType(msg.GetType()), Sandbox: sandboxFrom(sb)}) {
				return
			}
		}
		ended <- fmt.Errorf("driver: watch sandboxes: %w", sandboxes.Err())
	}()
	go func() {
		for events.Receive() {
			e := events.Msg().GetEvent()
			if e == nil {
				continue
			}
			if d.mine(e.GetSandbox()) {
				d.know(e.GetWorkloadId())
			}
			if !d.passes(e) {
				continue
			}
			if !send(Update{Type: eventType(e.GetType()), Event: eventFrom(e), Cursor: e.GetCursor()}) {
				return
			}
		}
		ended <- fmt.Errorf("driver: watch events: %w", events.Err())
	}()

	for {
		select {
		case u := <-updates:
			if err := fn(u); err != nil {
				return err
			}
		case err := <-ended:
			return endErr(ctx, err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// endErr returns ctx's error once ctx ended, and err otherwise. The server
// ends a call at the deadline it was sent, which can come a moment before
// ctx's own, so after DEADLINE_EXCEEDED it waits up to deadlineGrace for
// ctx's.
func endErr(ctx context.Context, err error) error {
	if _, ok := ctx.Deadline(); ok && ctx.Err() == nil && connect.CodeOf(err) == connect.CodeDeadlineExceeded {
		select {
		case <-ctx.Done():
		case <-time.After(deadlineGrace):
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// know records id as one of the driver's sandboxes.
func (d *Driver) know(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.known[id] = true
}

// mine reports whether sb is a sandbox labelled with the driver's name.
func (d *Driver) mine(sb *workspacev1.Sandbox) bool {
	return sb != nil && sb.GetLabels()[LabelDriver] == d.cfg.Name
}

// passes reports whether e is an execution or emitted event of one of the
// driver's sandboxes. Sandbox events are left out: Recover applies them to
// the sandboxes it returns and Watch's sandbox watch reports them.
func (d *Driver) passes(e *workspacev1.Event) bool {
	switch e.GetType() {
	case workspacev1.EventType_EVENT_TYPE_EXECUTION_STARTED,
		workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED,
		workspacev1.EventType_EVENT_TYPE_EMITTED:
	default:
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.known[e.GetWorkloadId()]
}
