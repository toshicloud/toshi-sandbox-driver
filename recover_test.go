package driver

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	workspacev1 "go.toshicloud.io/api/workspacev1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// sandboxOf returns a sandbox of driver, called name.
func sandboxOf(id, driver, name string) *workspacev1.Sandbox {
	return &workspacev1.Sandbox{
		Id:     id,
		Labels: map[string]string{LabelDriver: driver, LabelSandbox: name, LabelState: StateReady},
	}
}

func executionEvent(cursor, workload string, t workspacev1.EventType) *workspacev1.Event {
	return &workspacev1.Event{
		Id: "ev-" + cursor, Type: t, WorkloadId: workload, Cursor: cursor,
		Payload: &workspacev1.Event_Execution{Execution: &workspacev1.ExecutionEvent{ExecutionId: "ex-" + cursor}},
	}
}

func sandboxEvent(cursor string, t workspacev1.EventType, sb *workspacev1.Sandbox) *workspacev1.Event {
	return &workspacev1.Event{
		Id: "ev-" + cursor, Type: t, WorkloadId: sb.GetId(), Cursor: cursor,
		Payload: &workspacev1.Event_Sandbox{Sandbox: sb},
	}
}

// cursors returns the events' cursors.
func cursors(events []*Event) []string {
	var out []string
	for _, e := range events {
		out = append(out, e.Cursor)
	}
	return out
}

func TestRecover(t *testing.T) {
	ours, gone, theirs := sandboxOf("wl-1", "orchestrator-1", "a"), sandboxOf("wl-2", "orchestrator-1", "b"), sandboxOf("wl-3", "orchestrator-2", "c")
	f := newFake()
	f.sandboxes = []*workspacev1.Sandbox{ours, theirs, sandboxOf("wl-4", "orchestrator-1", "d")}
	f.listCursor = "c2"
	f.events = []*workspacev1.Event{
		executionEvent("c1", "wl-1", workspacev1.EventType_EVENT_TYPE_EXECUTION_STARTED),
		executionEvent("c2", "wl-1", workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED),
		executionEvent("c3", "wl-2", workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED),
		executionEvent("c4", "wl-3", workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED),
		sandboxEvent("c5", workspacev1.EventType_EVENT_TYPE_WORKLOAD_DELETED, gone),
		sandboxEvent("c6", workspacev1.EventType_EVENT_TYPE_WORKLOAD_UPDATED, ours),
		{Id: "ev-c7", Type: workspacev1.EventType_EVENT_TYPE_EMITTED, WorkloadId: "wl-4", Cursor: "c7"},
		sandboxEvent("c8", workspacev1.EventType_EVENT_TYPE_WORKLOAD_DELETED, theirs),
	}
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()

	// A first start lists the driver's sandboxes and reads no events.
	rec, err := d.Recover(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, sb := range rec.Sandboxes {
		names = append(names, sb.ID()+"/"+sb.Name)
	}
	if !slices.Equal(names, []string{"wl-1/a", "wl-4/d"}) || rec.Cursor != "c2" || rec.Events != nil {
		t.Fatalf("first start: sandboxes %v, cursor %q, events %v", names, rec.Cursor, cursors(rec.Events))
	}
	if len(f.eventLists) != 0 {
		t.Fatalf("first start read events: %v", f.eventLists)
	}
	list := f.lists[0]
	if list.GetLabels()[LabelDriver] != "orchestrator-1" || len(list.GetLabels()) != 1 ||
		list.GetProject() != "default" || list.GetEnvironment() != "production" {
		t.Fatalf("list request = %v", list)
	}

	// From a stored cursor it pages the events after it and keeps the
	// executions and emits of its sandboxes, the deleted one's too, and that
	// one's delete; another driver's delete is dropped.
	d = newTestDriver(t, f, testConfig)
	rec, err = d.Recover(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if got := cursors(rec.Events); !slices.Equal(got, []string{"c2", "c3", "c5", "c7"}) || rec.Cursor != "c8" {
		t.Fatalf("events %v, cursor %q; want c2 c3 c5 c7 and c8", got, rec.Cursor)
	}
	var asked []string
	for _, r := range f.eventLists {
		asked = append(asked, r.GetPageToken())
	}
	if !slices.Equal(asked, []string{"c1", "c3", "c5", "c7"}) {
		t.Fatalf("ListEvents page tokens %v", asked)
	}
}

// labelled returns sb with the driver label set to driver and the state
// label to state.
func labelled(sb *workspacev1.Sandbox, driver, state string) *workspacev1.Sandbox {
	c := proto.Clone(sb).(*workspacev1.Sandbox)
	c.Labels[LabelDriver], c.Labels[LabelState] = driver, state
	return c
}

// jobOf returns a job labelled as a sandbox of driver.
func jobOf(id, driver string) *workspacev1.Job {
	return &workspacev1.Job{Id: id, Labels: map[string]string{LabelDriver: driver, LabelSandbox: "job"}}
}

// jobEvent is the event of a job after a change.
func jobEvent(cursor string, t workspacev1.EventType, job *workspacev1.Job) *workspacev1.Event {
	return &workspacev1.Event{
		Id: "ev-" + cursor, Type: t, WorkloadId: job.GetId(), Cursor: cursor,
		Payload: &workspacev1.Event_Job{Job: job},
	}
}

func TestRecoverAppliesSandboxEvents(t *testing.T) {
	kept, moved, gone := sandboxOf("wl-1", "orchestrator-1", "a"), sandboxOf("wl-2", "orchestrator-1", "b"), sandboxOf("wl-3", "orchestrator-1", "c")
	added := sandboxOf("wl-4", "orchestrator-1", "d")
	f := newFake()
	// The list is read at c1, after the stored cursor c0.
	f.sandboxes = []*workspacev1.Sandbox{kept, moved, gone}
	f.listCursor = "c1"
	f.events = []*workspacev1.Event{
		sandboxEvent("c1", workspacev1.EventType_EVENT_TYPE_WORKLOAD_CREATED, labelled(kept, "orchestrator-1", StateStarting)),
		sandboxEvent("c2", workspacev1.EventType_EVENT_TYPE_WORKLOAD_CREATED, added),
		executionEvent("c3", "wl-4", workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED),
		sandboxEvent("c4", workspacev1.EventType_EVENT_TYPE_WORKLOAD_STATE_CHANGED, labelled(kept, "orchestrator-1", StateBusy)),
		sandboxEvent("c5", workspacev1.EventType_EVENT_TYPE_WORKLOAD_UPDATED, labelled(moved, "orchestrator-2", StateReady)),
		sandboxEvent("c6", workspacev1.EventType_EVENT_TYPE_WORKLOAD_DELETED, gone),
		jobEvent("c7", workspacev1.EventType_EVENT_TYPE_WORKLOAD_CREATED, jobOf("wl-6", "orchestrator-1")),
		executionEvent("c8", "wl-6", workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED),
		executionEvent("c9", "wl-5", workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED),
	}
	d := newTestDriver(t, f, testConfig)

	// A sandbox created after the list is added and its execution kept, a
	// changed one is as its last event shows it, one relabelled for another
	// driver or deleted is removed, and a job carrying the label, created
	// after the list, is left out with its executions.
	rec, err := d.Recover(context.Background(), "c0")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, sb := range rec.Sandboxes {
		got = append(got, sb.ID()+"/"+sb.Name+" "+sb.State())
	}
	if !slices.Equal(got, []string{"wl-1/a busy", "wl-4/d ready"}) {
		t.Fatalf("sandboxes %v", got)
	}
	if got := cursors(rec.Events); !slices.Equal(got, []string{"c3", "c6"}) || rec.Cursor != "c9" {
		t.Fatalf("events %v, cursor %q; want c3 c6 and c9", got, rec.Cursor)
	}
}

func TestWatch(t *testing.T) {
	f := newFake()
	ours, theirs := sandboxOf("wl-1", "orchestrator-1", "a"), sandboxOf("wl-3", "orchestrator-2", "c")
	f.sandboxes = []*workspacev1.Sandbox{ours}
	f.changes = []*workspacev1.WatchSandboxesResponse{
		{Type: workspacev1.EventType_EVENT_TYPE_WORKLOAD_UPDATED, Sandbox: theirs, Cursor: "c3"},
		{Type: workspacev1.EventType_EVENT_TYPE_WORKLOAD_UPDATED, Sandbox: ours, Cursor: "c4"},
	}
	f.events = []*workspacev1.Event{
		executionEvent("c1", "wl-1", workspacev1.EventType_EVENT_TYPE_EXECUTION_STARTED),
		executionEvent("c3", "wl-3", workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED),
		sandboxEvent("c4", workspacev1.EventType_EVENT_TYPE_WORKLOAD_UPDATED, ours),
		executionEvent("c5", "wl-1", workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED),
	}
	d := newTestDriver(t, f, testConfig)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := d.Watch(ctx, "", func(Update) error { return nil }); err == nil {
		t.Fatal("Watch without a cursor: no error")
	}
	if _, err := d.Recover(ctx, ""); err != nil {
		t.Fatal(err)
	}
	stop := errors.New("stop")
	var sandboxes, events []string
	err := d.Watch(ctx, "c2", func(u Update) error {
		switch {
		case u.Sandbox != nil:
			sandboxes = append(sandboxes, u.Sandbox.ID()+"/"+u.Sandbox.Name+" "+u.Type.String())
		case u.Event != nil:
			if u.Cursor != u.Event.Cursor {
				t.Errorf("update cursor %q for event %q", u.Cursor, u.Event.Cursor)
			}
			events = append(events, u.Cursor)
		}
		if len(sandboxes) == 1 && len(events) == 1 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("Watch = %v, want fn's error", err)
	}
	if !slices.Equal(sandboxes, []string{"wl-1/a workload_updated"}) || !slices.Equal(events, []string{"c5"}) {
		t.Fatalf("sandbox updates %v, event updates %v", sandboxes, events)
	}
	f.mu.Lock()
	watches := slices.Clone(f.watches)
	f.mu.Unlock()
	slices.Sort(watches)
	if !slices.Equal(watches, []string{"events c2", "sandboxes c2"}) {
		t.Fatalf("watches %v", watches)
	}

	// A context that ends ends Watch with its error.
	short, cancelShort := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancelShort()
	if err := d.Watch(short, "c9", func(Update) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Watch past its deadline = %v", err)
	}
}

func TestWatchLearnsSandboxesFromTheEventLog(t *testing.T) {
	old, added := sandboxOf("wl-1", "orchestrator-1", "a"), sandboxOf("wl-2", "orchestrator-1", "b")
	job := jobOf("wl-3", "orchestrator-1")
	f := newFake()
	f.sandboxes = []*workspacev1.Sandbox{old}
	f.listCursor = "c1"
	f.changes = []*workspacev1.WatchSandboxesResponse{
		{Type: workspacev1.EventType_EVENT_TYPE_WORKLOAD_CREATED, Sandbox: added, Cursor: "c2"},
		{Type: workspacev1.EventType_EVENT_TYPE_WORKLOAD_UPDATED, Sandbox: old, Cursor: "c6"},
	}
	f.events = []*workspacev1.Event{
		sandboxEvent("c2", workspacev1.EventType_EVENT_TYPE_WORKLOAD_CREATED, added),
		executionEvent("c3", "wl-2", workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED),
		jobEvent("c4", workspacev1.EventType_EVENT_TYPE_WORKLOAD_CREATED, job),
		executionEvent("c5", "wl-3", workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED),
		executionEvent("c6", "wl-1", workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED),
	}
	// The sandbox watch sends nothing until the event watch's last event is
	// handled.
	f.holdChanges = make(chan struct{})
	d := newTestDriver(t, f, testConfig)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := d.Recover(ctx, ""); err != nil {
		t.Fatal(err)
	}

	// The sandbox created at c2 is known from its event in WatchEvents, so
	// its execution at c3 is passed on before the sandbox watch shows it; the
	// job carrying the label is left out on both watches.
	stop := errors.New("stop")
	var sandboxes, events []string
	err := d.Watch(ctx, "c1", func(u Update) error {
		switch {
		case u.Sandbox != nil:
			sandboxes = append(sandboxes, u.Sandbox.ID()+" "+u.Type.String())
			if u.Sandbox.ID() == "wl-1" {
				return stop
			}
		case u.Event != nil:
			events = append(events, u.Cursor)
			if u.Cursor == "c6" {
				close(f.holdChanges)
			}
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("Watch = %v, want fn's error", err)
	}
	if !slices.Equal(events, []string{"c3", "c6"}) ||
		!slices.Equal(sandboxes, []string{"wl-2 workload_created", "wl-1 workload_updated"}) {
		t.Fatalf("event updates %v, sandbox updates %v", events, sandboxes)
	}
}

func TestDeleteAndWait(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb := sandboxFrom(sandboxOf("wl-1", "orchestrator-1", "a"))

	op, err := d.Delete(ctx, sb)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Delete(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if k := f.deletes[0].GetRequestKey(); f.deletes[0].GetId() != "wl-1" || !strings.HasPrefix(k, "delete-") || f.deletes[1].GetRequestKey() != k {
		t.Fatalf("deletes %v", f.deletes)
	}
	done, err := d.WaitOperation(ctx, op.ID)
	if err != nil || !done.Done {
		t.Fatalf("WaitOperation = %v, %v", done, err)
	}
	// A done operation is only read.
	if _, err := d.WaitOperation(ctx, op.ID); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.opCalls, []string{"get op-1", "wait op-1", "get op-1"}) {
		t.Fatalf("operation calls %v", f.opCalls)
	}

	f = newFake()
	f.opError = &workspacev1.Error{Code: "internal", Message: "the host failed"}
	d = newTestDriver(t, f, testConfig)
	op, err = d.Delete(ctx, sb)
	if err != nil {
		t.Fatal(err)
	}
	failed, err := d.WaitOperation(ctx, op.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Err == nil || !strings.Contains(failed.Err.Error(), "internal: the host failed") {
		t.Fatalf("a failed operation: Err = %v", failed.Err)
	}
}

// TestRecoverEventPayloads covers the driver's own event types: the type,
// the sandbox, the time, the cursor and the typed payload of each family.
func TestRecoverEventPayloads(t *testing.T) {
	ours := sandboxOf("wl-1", "orchestrator-1", "a")
	at := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	code := int32(7)
	gone := sandboxOf("wl-2", "orchestrator-1", "b")
	f := newFake()
	f.sandboxes = []*workspacev1.Sandbox{ours}
	f.listCursor = "c0"
	f.events = []*workspacev1.Event{
		{
			Id: "ev-c1", Type: workspacev1.EventType_EVENT_TYPE_EXECUTION_STARTED, WorkloadId: "wl-1", Cursor: "c1",
			Time:    timestamppb.New(at),
			Payload: &workspacev1.Event_Execution{Execution: &workspacev1.ExecutionEvent{ExecutionId: "ex-1", Argv: []string{"sh", "-c", "true"}}},
		},
		{
			Id: "ev-c2", Type: workspacev1.EventType_EVENT_TYPE_EXECUTION_ENDED, WorkloadId: "wl-1", Cursor: "c2",
			Time:    timestamppb.New(at.Add(time.Second)),
			Payload: &workspacev1.Event_Execution{Execution: &workspacev1.ExecutionEvent{ExecutionId: "ex-1", Exit: &workspacev1.ExecutionExit{ExitCode: &code}}},
		},
		{
			Id: "ev-c3", Type: workspacev1.EventType_EVENT_TYPE_EMITTED, WorkloadId: "wl-1", Cursor: "c3",
			Time:    timestamppb.New(at.Add(2 * time.Second)),
			Payload: &workspacev1.Event_Emitted{Emitted: &workspacev1.EmittedEvent{Data: []byte("hi")}},
		},
		sandboxEvent("c4", workspacev1.EventType_EVENT_TYPE_WORKLOAD_DELETED, gone),
	}
	d := newTestDriver(t, f, testConfig)
	rec, err := d.Recover(context.Background(), "c0")
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.Events) != 4 {
		t.Fatalf("%d events", len(rec.Events))
	}
	started, ended, emitted, deleted := rec.Events[0], rec.Events[1], rec.Events[2], rec.Events[3]
	if started.Type != EventExecutionStarted || started.SandboxID != "wl-1" || started.Cursor != "c1" || !started.Time.Equal(at) {
		t.Fatalf("started = %+v", started)
	}
	if x := started.Execution; x == nil || x.ExecutionID != "ex-1" || !slices.Equal(x.Argv, []string{"sh", "-c", "true"}) || x.ExitCode != -1 || x.Cancelled {
		t.Fatalf("started execution = %+v", started.Execution)
	}
	if x := ended.Execution; x == nil || x.ExitCode != 7 || x.Cancelled || ended.Type != EventExecutionEnded {
		t.Fatalf("ended = %+v, %+v", ended, ended.Execution)
	}
	if x := emitted.Emitted; x == nil || string(x.Data) != "hi" || emitted.Execution != nil {
		t.Fatalf("emitted = %+v", emitted)
	}
	if deleted.Type != EventWorkloadDeleted || deleted.SandboxID != "wl-2" || deleted.Execution != nil || deleted.Emitted != nil {
		t.Fatalf("deleted = %+v", deleted)
	}
	if started.Proto().GetId() != "ev-c1" || deleted.Proto().GetId() != "ev-c4" {
		t.Fatalf("Proto ids %q and %q", started.Proto().GetId(), deleted.Proto().GetId())
	}
}

func TestEndErrTakesTheDeadline(t *testing.T) {
	server := connect.NewError(connect.CodeDeadlineExceeded, errors.New("server deadline"))
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	// The server's deadline came first; ctx's follows within the grace.
	if err := endErr(ctx, server); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("endErr before ctx's deadline = %v", err)
	}
	// Without a deadline of ctx's, the server's error is the reason.
	if err := endErr(context.Background(), server); !errors.Is(err, server) {
		t.Fatalf("endErr without a deadline = %v", err)
	}
}
