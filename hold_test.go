package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	workspacev1 "go.toshicloud.io/api/workspacev1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// heldAt is when the hold began in these tests, and heldContact is where to
// write for its release.
var heldAt = time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

const heldContact = "ops@example.com"

// refusalMessage is the server's message in a refusal of a held sandbox.
const refusalMessage = "sandbox wl-1 is on hold: suspected abuse; an operator can release it"

// asHeld fails unless err matches ErrHeld and is a HeldError, and returns it.
func asHeld(t *testing.T, err error) *HeldError {
	t.Helper()
	var held *HeldError
	if !errors.As(err, &held) {
		t.Fatalf("errors.As(%v, *HeldError) = false", err)
	}
	if !errors.Is(err, ErrHeld) {
		t.Fatalf("errors.Is(%v, ErrHeld) = false", err)
	}
	return held
}

// wantHeld fails unless err's text is want and it is a HeldError with the
// fields named.
func wantHeld(t *testing.T, err error, want string, hold workspacev1.OperatorHold, at time.Time, contact string, refused bool) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %q", want)
	}
	if got := err.Error(); got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
	held := asHeld(t, err)
	if held.SandboxID != "wl-1" || held.Hold != hold || !held.HeldAt.Equal(at) || held.Contact != contact {
		t.Fatalf("HeldError = %+v, want wl-1 %v %v %q", held, hold, at, contact)
	}
	switch {
	case refused && held.Err == nil:
		t.Fatalf("HeldError.Err = nil, want the server's refusal")
	case !refused && held.Err != nil:
		t.Fatalf("HeldError.Err = %v, want nil", held.Err)
	}
	if refused {
		var connectErr *connect.Error
		if !errors.As(held.Unwrap(), &connectErr) || connectErr.Code() != connect.CodeFailedPrecondition || connectErr.Message() != refusalMessage {
			t.Fatalf("Unwrap = %v, want the server's FAILED_PRECONDITION %q", held.Unwrap(), refusalMessage)
		}
	}
}

// TestHeldWaitRunningNotRunning covers WaitRunning reading a hold from a
// sandbox that is not running.
func TestHeldWaitRunningNotRunning(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb, err := d.Create(ctx, "s", Spec{Size: "toshi-linux-2x", MaxDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	f.hold(workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, timestamppb.New(heldAt), heldContact)
	f.waitState = workspacev1.SandboxState_SANDBOX_STATE_ENDED

	err = d.WaitRunning(ctx, sb)
	wantHeld(t, err,
		"driver: sandbox wl-1 is held by the operator for suspected abuse since 2026-03-04T05:06:07Z; write to ops@example.com",
		workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, heldAt, heldContact, false)
	if _, _, updates, _ := f.calls(); len(updates) != 0 {
		t.Fatalf("a held sandbox was patched: %v", f.states())
	}
}

// TestHeldWaitRunningStopped covers WaitRunning on a held sandbox it last read
// as stopped: no call is made.
func TestHeldWaitRunningStopped(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb := startKeptSandbox(t, d)
	f.hold(workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, timestamppb.New(heldAt), heldContact)
	if err := d.Stop(ctx, sb); err != nil {
		t.Fatal(err)
	}
	_, waits, updates, _ := f.calls()
	gets := f.getCount()

	err := d.WaitRunning(ctx, sb)
	wantHeld(t, err,
		"driver: sandbox wl-1 is held by the operator for suspected abuse since 2026-03-04T05:06:07Z; write to ops@example.com",
		workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, heldAt, heldContact, false)
	if _, after, afterUpdates, _ := f.calls(); len(after) != len(waits) || len(afterUpdates) != len(updates) {
		t.Fatalf("WaitRunning called the server: %d waits, %d updates", len(after), len(afterUpdates))
	}
	if got := f.getCount(); got != gets {
		t.Fatalf("WaitRunning made %d GetSandbox calls", got-gets)
	}
}

// TestHeldExecuteStopped covers Execute on a held sandbox it last read as
// stopped: no call is made.
func TestHeldExecuteStopped(t *testing.T) {
	f := newFake()
	ran := false
	f.execute = func(context.Context, *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
		ran = true
		return errors.New("unexpected execute")
	}
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb := startKeptSandbox(t, d)
	f.hold(workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, timestamppb.New(heldAt), heldContact)
	if err := d.Stop(ctx, sb); err != nil {
		t.Fatal(err)
	}
	_, _, updates, _ := f.calls()
	gets := f.getCount()

	_, err := d.Execute(ctx, sb, Command{Argv: []string{"true"}})
	wantHeld(t, err,
		"driver: sandbox wl-1 is held by the operator for suspected abuse since 2026-03-04T05:06:07Z; write to ops@example.com",
		workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, heldAt, heldContact, false)
	if ran {
		t.Fatal("Execute sent a command to a held sandbox")
	}
	if _, _, after, _ := f.calls(); len(after) != len(updates) {
		t.Fatalf("Execute patched the held sandbox: %v", f.states())
	}
	if got := f.getCount(); got != gets {
		t.Fatalf("Execute made %d GetSandbox calls", got-gets)
	}
}

// TestHeldStartRefused covers Start refused by the server, with the hold read
// back from GetSandbox.
func TestHeldStartRefused(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb := startKeptSandbox(t, d)
	if err := d.Stop(ctx, sb); err != nil {
		t.Fatal(err)
	}
	f.hold(workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, timestamppb.New(heldAt), heldContact)
	f.refused = "abuse_suspected"
	gets := f.getCount()

	err := d.Start(ctx, sb)
	wantHeld(t, err,
		"driver: start sandbox wl-1: held by the operator for suspected abuse since 2026-03-04T05:06:07Z; write to ops@example.com",
		workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, heldAt, heldContact, true)
	if got := f.getCount(); got != gets+1 {
		t.Fatalf("Start made %d GetSandbox calls, want 1", got-gets)
	}
}

// TestHeldStopRefused covers Stop refused by the server.
func TestHeldStopRefused(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb := startKeptSandbox(t, d)
	f.hold(workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, timestamppb.New(heldAt), heldContact)
	f.refused = "abuse_suspected"

	err := d.Stop(ctx, sb)
	wantHeld(t, err,
		"driver: stop sandbox wl-1: held by the operator for suspected abuse since 2026-03-04T05:06:07Z; write to ops@example.com",
		workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, heldAt, heldContact, true)
	if got := sb.State(); got != StateReady {
		t.Fatalf("a refused Stop left the sandbox %q", got)
	}
}

// TestHeldDeleteRefused covers Delete refused by the server.
func TestHeldDeleteRefused(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb := startKeptSandbox(t, d)
	f.hold(workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, timestamppb.New(heldAt), heldContact)
	f.refused = "abuse_suspected"

	op, err := d.Delete(ctx, sb)
	wantHeld(t, err,
		"driver: delete sandbox wl-1: held by the operator for suspected abuse since 2026-03-04T05:06:07Z; write to ops@example.com",
		workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, heldAt, heldContact, true)
	if op != nil {
		t.Fatalf("Delete = %+v, want no operation", op)
	}
}

// TestHeldRefusalGetFails covers a refusal whose GetSandbox fails: the reason
// comes from the refusal's metadata, with no time and no contact.
func TestHeldRefusalGetFails(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb := startKeptSandbox(t, d)
	if err := d.Stop(ctx, sb); err != nil {
		t.Fatal(err)
	}
	f.refused = "abuse_suspected"
	f.getErr = errors.New("the sandbox is gone")

	err := d.Start(ctx, sb)
	wantHeld(t, err,
		"driver: start sandbox wl-1: held by the operator for suspected abuse",
		workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, time.Time{}, "", true)
}

// TestHeldConfirmedAbuse covers the wording of confirmed abuse.
func TestHeldConfirmedAbuse(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb, err := d.Create(ctx, "s", Spec{Size: "toshi-linux-2x", MaxDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	f.hold(workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_CONFIRMED, timestamppb.New(heldAt), heldContact)
	f.waitState = workspacev1.SandboxState_SANDBOX_STATE_ENDED

	err = d.WaitRunning(ctx, sb)
	wantHeld(t, err,
		"driver: sandbox wl-1 is held by the operator for confirmed abuse since 2026-03-04T05:06:07Z; write to ops@example.com",
		workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_CONFIRMED, heldAt, heldContact, false)
}

// TestHeldUnknownValue covers a hold value the API does not name: the error
// says only that the sandbox is held.
func TestHeldUnknownValue(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb, err := d.Create(ctx, "s", Spec{Size: "toshi-linux-2x", MaxDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	f.hold(workspacev1.OperatorHold(99), nil, "")
	f.waitState = workspacev1.SandboxState_SANDBOX_STATE_ENDED

	err = d.WaitRunning(ctx, sb)
	wantHeld(t, err, "driver: sandbox wl-1 is held by the operator",
		workspacev1.OperatorHold(99), time.Time{}, "", false)
}

// TestHeldRefusalWithoutReason covers another FAILED_PRECONDITION: it stays
// the server's error and is no HeldError.
func TestHeldRefusalWithoutReason(t *testing.T) {
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
	gets := f.getCount()

	err = d.Start(ctx, sb)
	if err == nil || err.Error() != "driver: start sandbox wl-1: failed_precondition" {
		t.Fatalf("Start = %v, want the server's FAILED_PRECONDITION", err)
	}
	if errors.Is(err, ErrHeld) {
		t.Fatalf("errors.Is(%v, ErrHeld) = true", err)
	}
	var held *HeldError
	if errors.As(err, &held) {
		t.Fatalf("errors.As(%v, *HeldError) = true", err)
	}
	if got := f.getCount(); got != gets {
		t.Fatalf("Start made %d GetSandbox calls for another error", got-gets)
	}
}

// TestHeldWithoutTimeOrContact covers a hold whose status names neither when
// it began nor where to write.
func TestHeldWithoutTimeOrContact(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()
	sb := startKeptSandbox(t, d)
	if err := d.Stop(ctx, sb); err != nil {
		t.Fatal(err)
	}
	f.hold(workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, nil, "")
	f.refused = "abuse_suspected"

	err := d.Start(ctx, sb)
	wantHeld(t, err,
		"driver: start sandbox wl-1: held by the operator for suspected abuse",
		workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED, time.Time{}, "", true)
}
