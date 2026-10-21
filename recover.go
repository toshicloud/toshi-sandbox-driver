package driver

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"connectrpc.com/connect"
	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

// Recovery is what Recover found.
type Recovery struct {
	// Sandboxes are the driver's sandboxes at Cursor: the sandbox workloads
	// labelled with its name.
	Sandboxes []*Sandbox
	// Events are the execution and emitted events of the driver's
	// sandboxes after the cursor Recover was given, and the delete events of
	// those deleted since, oldest first.
	Events []*workspacev1.Event
	// Cursor is where Follow starts, and the position to store for the next
	// Recover until Follow reports a later one.
	Cursor string
}

// Recover finds the driver's sandboxes when it starts. ListWorkloads lists
// the sandboxes labelled with the driver's name. Given the cursor stored by
// an earlier run, ListEvents then reads the events after it, and Events
// keeps the execution and emitted events of the driver's sandboxes, and the
// delete events of those deleted since. Each workload event carries the
// workload after its change, so Recover applies them in order to the list:
// a sandbox created after the list was read is added, one deleted is
// removed, and Sandboxes holds each as it was at Cursor, the position after
// the last event. Given an empty cursor, on a first start, Events is empty
// and Cursor is the list's position. A cursor the event log no longer keeps
// fails with OUT_OF_RANGE; Recover with an empty cursor then starts afresh.
func (d *Driver) Recover(ctx context.Context, cursor string) (*Recovery, error) {
	rec := &Recovery{}
	list := &workspacev1.ListWorkloadsRequest{
		Kinds:  []workspacev1.WorkloadKind{workspacev1.WorkloadKind_WORKLOAD_KIND_SANDBOX},
		Labels: map[string]string{LabelDriver: d.cfg.Name},
	}
	for {
		resp, err := d.client.Workspace.ListWorkloads(ctx, connect.NewRequest(list))
		if err != nil {
			return nil, fmt.Errorf("driver: list sandboxes: %w", err)
		}
		// The first page's cursor is the log's position when the list
		// was read.
		if list.PageToken == "" {
			rec.Cursor = resp.Msg.GetCursor()
		}
		for _, wl := range resp.Msg.GetWorkloads() {
			if d.mine(wl) {
				d.know(wl.GetId())
				rec.Sandboxes = append(rec.Sandboxes, sandboxFrom(wl))
			}
		}
		if resp.Msg.GetNextPageToken() == "" {
			break
		}
		list.PageToken = resp.Msg.GetNextPageToken()
	}
	if cursor == "" {
		return rec, nil
	}

	var events []*workspacev1.Event
	req := &workspacev1.ListEventsRequest{Cursor: cursor}
	for {
		resp, err := d.client.Workspace.ListEvents(ctx, connect.NewRequest(req))
		if err != nil {
			return nil, fmt.Errorf("driver: list events after the stored cursor: %w", err)
		}
		if c := resp.Msg.GetCursor(); c != "" {
			req.Cursor = c
		}
		if len(resp.Msg.GetEvents()) == 0 {
			break
		}
		events = append(events, resp.Msg.GetEvents()...)
	}
	rec.Cursor = req.Cursor
	// A sandbox deleted before the list was read is not listed; its later
	// workload events make it known before its executions' events are kept.
	for _, e := range events {
		if d.mine(e.GetWorkload()) {
			d.know(e.GetWorkloadId())
		}
	}
	for _, e := range events {
		wl := e.GetWorkload()
		if wl == nil {
			if d.passes(e) {
				rec.Events = append(rec.Events, e)
			}
			continue
		}
		deleted := e.GetType() == workspacev1.EventType_EVENT_TYPE_WORKLOAD_DELETED
		if deleted && d.mine(wl) {
			rec.Events = append(rec.Events, e)
		}
		i := slices.IndexFunc(rec.Sandboxes, func(sb *Sandbox) bool { return sb.ID() == e.GetWorkloadId() })
		switch {
		case deleted || !d.mine(wl):
			if i >= 0 {
				rec.Sandboxes = slices.Delete(rec.Sandboxes, i, i+1)
			}
		case i >= 0:
			rec.Sandboxes[i] = sandboxFrom(wl)
		default:
			rec.Sandboxes = append(rec.Sandboxes, sandboxFrom(wl))
		}
	}
	return rec, nil
}

// sandboxFrom returns the sandbox wl, named by its label.
func sandboxFrom(wl *workspacev1.Workload) *Sandbox {
	return &Sandbox{Name: wl.GetLabels()[LabelSandbox], Workload: wl}
}

// deadlineGrace is how long Follow waits for ctx's deadline after a watch
// failed with DEADLINE_EXCEEDED before it returns the watch's error.
const deadlineGrace = time.Second

// Update is one change Follow reports: a sandbox's change or an event.
type Update struct {
	// Type is the change's or the event's type.
	Type workspacev1.EventType
	// Sandbox is one of the driver's sandboxes after a change WatchWorkloads
	// reported, or nil for an event.
	Sandbox *Sandbox
	// Event is an execution or emitted event of one of the driver's
	// sandboxes that WatchEvents reported, or nil for a sandbox's change.
	Event *workspacev1.Event
	// Cursor is set with Event: the position after it, to store once the
	// update is handled. It is empty on a sandbox's change, which leaves the
	// stored cursor as it was.
	Cursor string
}

// Follow follows the driver's sandboxes from cursor, as Recover returned
// it, and calls fn with each update, one at a time: WatchWorkloads reports
// the sandboxes' changes, WatchEvents their executions' and emitted events,
// and each resumes after a dropped connection. Neither call filters by
// label, so Follow passes on the changes of sandbox workloads labelled with
// the driver's name, and the events of the sandboxes Recover returned or
// whose workload events, which come before their executions' in the log,
// show them as the driver's. The two watches are not ordered with each
// other: an execution's event can come before the change that shows its
// sandbox. An event can come again after a restart from a cursor
// stored before it was handled: the server delivers at least once, with the
// same event ID. It returns when ctx ends, a watch ends or fn returns an
// error, with why; a watch ends once toshi-go's reconnect attempts run out,
// and Recover from the stored cursor and Follow again carry on from there.
func (d *Driver) Follow(ctx context.Context, cursor string, fn func(Update) error) error {
	if cursor == "" {
		return errors.New("driver: Follow needs the cursor Recover returned")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	workloads, err := d.client.WatchWorkloads(ctx, &workspacev1.WatchWorkloadsRequest{
		Kinds:  []workspacev1.WorkloadKind{workspacev1.WorkloadKind_WORKLOAD_KIND_SANDBOX},
		Cursor: cursor,
	})
	if err != nil {
		return endErr(ctx, fmt.Errorf("driver: watch sandboxes: %w", err))
	}
	defer func() { _ = workloads.Close() }()
	events, err := d.client.WatchEvents(ctx, &workspacev1.WatchEventsRequest{Cursor: cursor})
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
		for workloads.Receive() {
			msg := workloads.Msg()
			wl := msg.GetWorkload()
			if !d.mine(wl) {
				continue
			}
			if !send(Update{Type: msg.GetType(), Sandbox: sandboxFrom(wl)}) {
				return
			}
		}
		ended <- fmt.Errorf("driver: watch sandboxes: %w", workloads.Err())
	}()
	go func() {
		for events.Receive() {
			e := events.Event()
			if d.mine(e.GetWorkload()) {
				d.know(e.GetWorkloadId())
			}
			if !d.passes(e) {
				continue
			}
			if !send(Update{Type: e.GetType(), Event: e, Cursor: e.GetCursor()}) {
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

// mine reports whether wl is a sandbox labelled with the driver's name.
func (d *Driver) mine(wl *workspacev1.Workload) bool {
	return wl.GetKind() == workspacev1.WorkloadKind_WORKLOAD_KIND_SANDBOX && wl.GetLabels()[LabelDriver] == d.cfg.Name
}

// passes reports whether e is an execution or emitted event of one of the
// driver's sandboxes. Workload events are left out: Recover applies them to
// the sandboxes it returns and Follow's WatchWorkloads reports them.
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
