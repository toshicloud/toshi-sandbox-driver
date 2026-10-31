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
)

// startKeptSandbox creates a sandbox with KeepVM and waits for it to run.
func startKeptSandbox(t *testing.T, d *Driver) *Sandbox {
	t.Helper()
	ctx := context.Background()
	sb, err := d.Create(ctx, "s", Spec{Size: "toshi-linux-2x", MaxDuration: time.Minute, KeepVM: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.WaitRunning(ctx, sb); err != nil {
		t.Fatal(err)
	}
	return sb
}

func TestStopAndStart(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb, err := d.Create(ctx, "s", Spec{Size: "toshi-linux-2x", MaxDuration: time.Minute, KeepVM: true})
	if err != nil {
		t.Fatal(err)
	}
	if creates, _, _, _ := f.calls(); !creates[0].GetSpec().GetKeepVm() {
		t.Fatal("Create sent no KeepVm")
	}
	if err := d.WaitRunning(ctx, sb); err != nil {
		t.Fatal(err)
	}

	if err := d.Stop(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if got := sb.State(); got != StateStopped {
		t.Fatalf("after Stop: state %q, want %q", got, StateStopped)
	}
	if err := d.Start(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if got := sb.State(); got != StateStarting {
		t.Fatalf("after Start: state %q, want %q", got, StateStarting)
	}
	if err := d.WaitRunning(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if got := sb.State(); got != StateReady {
		t.Fatalf("after WaitRunning: state %q, want %q", got, StateReady)
	}

	stops, starts := f.transitions()
	if len(stops) != 1 || stops[0].GetId() != "wl-1" || stops[0].GetRequestKey() == "" {
		t.Fatalf("stops %v", stops)
	}
	if len(starts) != 1 || starts[0].GetId() != "wl-1" || starts[0].GetRequestKey() == "" {
		t.Fatalf("starts %v", starts)
	}
}

func TestExecuteOnStoppedSandbox(t *testing.T) {
	f := newFake()
	ran := false
	f.execute = func(context.Context, *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
		ran = true
		return errors.New("unexpected execute")
	}
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb := startKeptSandbox(t, d)
	if err := d.Stop(ctx, sb); err != nil {
		t.Fatal(err)
	}
	_, _, updates, _ := f.calls()

	_, err := d.Execute(ctx, sb, Command{Argv: []string{"true"}})
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("Execute = %v, want ErrStopped", err)
	}
	if !strings.Contains(err.Error(), "wl-1") {
		t.Fatalf("Execute error %q names no sandbox", err)
	}
	if ran {
		t.Fatal("Execute sent a command to a stopped sandbox")
	}
	if _, _, after, _ := f.calls(); len(after) != len(updates) {
		t.Fatalf("Execute patched the stopped sandbox: %v", f.states())
	}
}

// TestStopWaitsForTheServer covers a server that answers the stop while the
// sandbox still runs: Stop returns once it is stopped, and Execute right after
// it sends nothing.
func TestStopWaitsForTheServer(t *testing.T) {
	f := newFake()
	f.stopLag = true
	ran := false
	f.execute = func(context.Context, *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
		ran = true
		return errors.New("unexpected execute")
	}
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb := startKeptSandbox(t, d)
	_, before, _, _ := f.calls()

	if err := d.Stop(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if got := sb.State(); got != StateStopped {
		t.Fatalf("after Stop: state %q, want %q", got, StateStopped)
	}
	_, waits, _, _ := f.calls()
	if len(waits) != len(before)+1 {
		t.Fatalf("Stop made %d WaitSandbox calls, want 1", len(waits)-len(before))
	}
	want := []workspacev1.SandboxState{workspacev1.SandboxState_SANDBOX_STATE_CREATED, workspacev1.SandboxState_SANDBOX_STATE_ENDED}
	if w := waits[len(waits)-1]; w.GetId() != "wl-1" || !slices.Equal(w.GetStates(), want) || w.GetTimeout().AsDuration() != waitBound {
		t.Fatalf("Stop's wait %v", w)
	}
	if got := f.states(); got[len(got)-1] != StateStopped {
		t.Fatalf("labels recorded %v, want %q last", got, StateStopped)
	}

	_, err := d.Execute(ctx, sb, Command{Argv: []string{"true"}})
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("Execute right after Stop = %v, want ErrStopped", err)
	}
	if ran {
		t.Fatal("Execute sent a command to a stopped sandbox")
	}
}

// TestStopEndsWhileWaiting covers a sandbox that ends instead of stopping:
// Stop fails naming its state and does not record it as stopped.
func TestStopEndsWhileWaiting(t *testing.T) {
	f := newFake()
	f.stopLag = true
	f.stopEnds = true
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb := startKeptSandbox(t, d)

	err := d.Stop(ctx, sb)
	if err == nil || !strings.Contains(err.Error(), "SANDBOX_STATE_ENDED") {
		t.Fatalf("Stop of a sandbox that ended = %v, want an error naming its state", err)
	}
	if got := f.states(); got[len(got)-1] == StateStopped {
		t.Fatalf("labels recorded %v for an ended sandbox", got)
	}
}

func TestStopRequestKeys(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb := startKeptSandbox(t, d)

	// Two calls from the same sandbox as last read send one key.
	stale := proto.Clone(sb.Proto()).(*workspacev1.Sandbox)
	first := &Sandbox{Name: sb.Name, sandbox: proto.Clone(stale).(*workspacev1.Sandbox)}
	replay := &Sandbox{Name: sb.Name, sandbox: proto.Clone(stale).(*workspacev1.Sandbox)}
	if err := d.Stop(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := d.Stop(ctx, replay); err != nil {
		t.Fatal(err)
	}

	// A second cycle, after the sandbox changed, sends a key of its own.
	if err := d.Start(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if err := d.WaitRunning(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if err := d.Stop(ctx, sb); err != nil {
		t.Fatal(err)
	}
	stops, _ := f.transitions()
	if len(stops) != 3 {
		t.Fatalf("%d stops", len(stops))
	}
	key := stops[0].GetRequestKey()
	if key == "" || stops[1].GetRequestKey() != key {
		t.Fatalf("repeated stop keys %q and %q", key, stops[1].GetRequestKey())
	}
	if stops[2].GetRequestKey() == key {
		t.Fatalf("a later stop reused key %q", key)
	}
}

func TestStopNeedsKeepVM(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb, err := d.Create(ctx, "s", Spec{Size: "toshi-linux-2x", MaxDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.WaitRunning(ctx, sb); err != nil {
		t.Fatal(err)
	}
	err = d.Stop(ctx, sb)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("Stop without KeepVM = %v, want FAILED_PRECONDITION", err)
	}
}

// TestCreateRequestKeyUnchanged pins a create key: requestKey gained parts for
// Stop and Start, and Create and Delete keep the one-part keys they sent
// before.
func TestCreateRequestKeyUnchanged(t *testing.T) {
	d := &Driver{cfg: testConfig}
	const want = "sandbox-3bcac37e21244d59a7fc9bf516c1162f"
	if got := d.requestKey("sandbox", "task-7"); got != want {
		t.Fatalf("create key = %q, want %q", got, want)
	}
}
