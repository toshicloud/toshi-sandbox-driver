package driver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
)

func TestCheck(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{name: "project and environment exist", cfg: testConfig},
		{name: "no such environment", cfg: Config{Project: "default", Environment: "staging", Name: "o"}, wantErr: `project "default" has no environment "staging"`},
		{name: "no such project", cfg: Config{Project: "web", Environment: "production", Name: "o"}, wantErr: `the token sees no project "web"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDriver(t, newFake(), tt.cfg)
			err := d.Check(context.Background())
			if tt.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestCreateAndWaitRunning(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	ctx := context.Background()

	sb, err := d.Create(ctx, "task-7", Spec{Size: "toshi-linux-2x", MaxDuration: 30 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if sb.ID() != "wl-1" || sb.Name != "task-7" || sb.State() != StateStarting {
		t.Fatalf("Create = %s %s %s", sb.ID(), sb.Name, sb.State())
	}
	creates, _, _, _ := f.calls()
	req := creates[0]
	if req.GetProject() != "default" || req.GetEnvironment() != "production" {
		t.Fatalf("create scope = %s/%s", req.GetProject(), req.GetEnvironment())
	}
	if spec := req.GetSpec(); spec.GetSize() != "toshi-linux-2x" || spec.GetMaxDuration().AsDuration() != 30*time.Minute {
		t.Fatalf("create spec = %v", spec)
	}
	wantLabels := map[string]string{LabelDriver: "orchestrator-1", LabelSandbox: "task-7", LabelState: StateStarting}
	if !maps.Equal(req.GetLabels(), wantLabels) {
		t.Fatalf("create labels = %v, want %v", req.GetLabels(), wantLabels)
	}

	// The key is the same for the same driver and name, and only then.
	if _, err := d.Create(ctx, "task-7", Spec{Size: "toshi-linux-2x", MaxDuration: 30 * time.Minute}); err != nil {
		t.Fatal(err)
	}
	other := newTestDriver(t, newFake(), Config{Project: "default", Environment: "production", Name: "orchestrator-2"})
	creates, _, _, _ = f.calls()
	if creates[0].GetRequestKey() == "" || creates[1].GetRequestKey() != creates[0].GetRequestKey() {
		t.Fatalf("repeated create keys %q and %q", creates[0].GetRequestKey(), creates[1].GetRequestKey())
	}
	staging := newTestDriver(t, newFake(), Config{Project: "default", Environment: "staging", Name: "orchestrator-1"})
	key := d.requestKey("sandbox", "task-7")
	if d.requestKey("sandbox", "task-8") == key || other.requestKey("sandbox", "task-7") == key || staging.requestKey("sandbox", "task-7") == key {
		t.Fatal("a create key does not depend on the names and the environment")
	}
	// Names may hold any bytes, and the parts still do not run together.
	joined := newTestDriver(t, newFake(), Config{Project: "default", Environment: "production", Name: "a\x00b"})
	split := newTestDriver(t, newFake(), Config{Project: "default", Environment: "production", Name: "a"})
	if joined.requestKey("sandbox", "c") == split.requestKey("sandbox", "b\x00c") {
		t.Fatal("the same bytes split into other names share a create key")
	}

	if err := d.WaitRunning(ctx, sb); err != nil {
		t.Fatal(err)
	}
	wantStates := []workspacev1.SandboxState{workspacev1.SandboxState_SANDBOX_STATE_RUNNING, workspacev1.SandboxState_SANDBOX_STATE_ENDED}
	_, waits, updates, _ := f.calls()
	if w := waits[0]; w.GetId() != "wl-1" || !slices.Equal(w.GetStates(), wantStates) || w.GetTimeout().AsDuration() != waitBound {
		t.Fatalf("wait = %v", w)
	}
	if sb.State() != StateReady || sb.Sandbox.GetStatus().GetState() != workspacev1.SandboxState_SANDBOX_STATE_RUNNING {
		t.Fatalf("after WaitRunning: %s %s", sb.State(), sb.Sandbox.GetStatus().GetState())
	}
	patch := updates[0]
	if !slices.Equal(patch.GetUpdateMask().GetPaths(), []string{"labels"}) {
		t.Fatalf("patch mask = %v", patch.GetUpdateMask().GetPaths())
	}
	wantLabels[LabelState] = StateReady
	if !maps.Equal(patch.GetSandbox().GetLabels(), wantLabels) {
		t.Fatalf("patch labels = %v, want %v", patch.GetSandbox().GetLabels(), wantLabels)
	}
}

func TestCreateNeedsWholeMinutes(t *testing.T) {
	f := newFake()
	d := newTestDriver(t, f, testConfig)
	for _, max := range []time.Duration{0, 30 * time.Second, 90 * time.Second} {
		if _, err := d.Create(context.Background(), "s", Spec{Size: "toshi-linux-2x", MaxDuration: max}); err == nil {
			t.Fatalf("Create with %s: no error", max)
		}
	}
	if creates, _, _, _ := f.calls(); len(creates) != 0 {
		t.Fatalf("%d creates sent", len(creates))
	}
}

func TestWaitRunningEnded(t *testing.T) {
	f := newFake()
	f.waitState = workspacev1.SandboxState_SANDBOX_STATE_ENDED
	d := newTestDriver(t, f, testConfig)
	sb, err := d.Create(context.Background(), "s", Spec{Size: "toshi-linux-2x", MaxDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	err = d.WaitRunning(context.Background(), sb)
	if err == nil || !strings.Contains(err.Error(), "not running") {
		t.Fatalf("err = %v", err)
	}
	if _, _, updates, _ := f.calls(); len(updates) != 0 {
		t.Fatalf("an ended sandbox was patched: %v", f.states())
	}
}

// startSandbox creates a sandbox on f and waits for it to run.
func startSandbox(t *testing.T, d *Driver) *Sandbox {
	t.Helper()
	sb, err := d.Create(context.Background(), "s", Spec{Size: "toshi-linux-2x", MaxDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.WaitRunning(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	return sb
}

func exitCode(code int32) *workspacev1.ExecuteResponse {
	return &workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Exit{Exit: &workspacev1.ExecutionExit{ExitCode: &code}}}
}

func TestExecuteStreams(t *testing.T) {
	f := newFake()
	var mu sync.Mutex
	var start *workspacev1.ExecuteStart
	f.execute = func(_ context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
		first, err := stream.Receive()
		if err != nil {
			return err
		}
		mu.Lock()
		start = first.GetStart()
		mu.Unlock()
		if err := stream.Send(&workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Started{Started: &workspacev1.ExecutionStarted{ExecutionId: "ex-1"}}}); err != nil {
			return err
		}
		// Echo standard input to stdout until it is closed, then exit 3.
		for {
			req, err := stream.Receive()
			if err != nil {
				return err
			}
			if data := req.GetStdin(); data != nil {
				if err := stream.Send(&workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Stdout{Stdout: data}}); err != nil {
					return err
				}
			}
			if req.GetCloseStdin() {
				if err := stream.Send(&workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Stderr{Stderr: []byte("done\n")}}); err != nil {
					return err
				}
				return stream.Send(exitCode(3))
			}
		}
	}
	d := newTestDriver(t, f, testConfig)
	sb := startSandbox(t, d)

	input := strings.Repeat("line\n", 50000)
	var stdout, stderr bytes.Buffer
	res, err := d.Execute(context.Background(), sb, Command{
		Argv:   []string{"cat"},
		Env:    map[string]string{"A": "1"},
		Cwd:    "/work",
		Stdin:  strings.NewReader(input),
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res != (Result{ExecutionID: "ex-1", ExitCode: 3}) {
		t.Fatalf("Result = %+v", res)
	}
	if stdout.String() != input || stderr.String() != "done\n" {
		t.Fatalf("stdout %d bytes, stderr %q", stdout.Len(), stderr.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if start.GetSandboxId() != "wl-1" || !slices.Equal(start.GetCommand().GetArgv(), []string{"cat"}) ||
		start.GetCommand().GetEnv()["A"] != "1" || start.GetCommand().GetCwd() != "/work" {
		t.Fatalf("start = %v", start)
	}
	if got, want := f.states(), []string{StateReady, StateBusy, StateReady}; !slices.Equal(got, want) {
		t.Fatalf("states = %v, want %v", got, want)
	}
}

func TestExecuteTimeoutCancels(t *testing.T) {
	f := newFake()
	f.execute = func(ctx context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
		if _, err := stream.Receive(); err != nil {
			return err
		}
		if err := stream.Send(&workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Started{Started: &workspacev1.ExecutionStarted{ExecutionId: "ex-2"}}}); err != nil {
			return err
		}
		select {
		case <-f.cancelled:
		case <-ctx.Done():
			return ctx.Err()
		}
		return stream.Send(&workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Exit{Exit: &workspacev1.ExecutionExit{Cancelled: true}}})
	}
	d := newTestDriver(t, f, testConfig)
	sb := startSandbox(t, d)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := d.Execute(ctx, sb, Command{Argv: []string{"sleep", "600"}, Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if res != (Result{ExecutionID: "ex-2", ExitCode: -1, Cancelled: true, TimedOut: true}) {
		t.Fatalf("Result = %+v", res)
	}
	if _, _, _, cancels := f.calls(); len(cancels) != 1 || cancels[0].GetExecutionId() != "ex-2" {
		t.Fatalf("cancels = %v", cancels)
	}
	if sb.State() != StateReady {
		t.Fatalf("state = %s", sb.State())
	}
}

func TestExecuteStreamLostLeavesBusy(t *testing.T) {
	f := newFake()
	f.execute = func(_ context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
		if _, err := stream.Receive(); err != nil {
			return err
		}
		if err := stream.Send(&workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Started{Started: &workspacev1.ExecutionStarted{ExecutionId: "ex-3"}}}); err != nil {
			return err
		}
		return connect.NewError(connect.CodeUnavailable, errors.New("agent gone"))
	}
	d := newTestDriver(t, f, testConfig)
	sb := startSandbox(t, d)

	if _, err := d.Execute(context.Background(), sb, Command{Argv: []string{"true"}}); err == nil {
		t.Fatal("Execute over a lost stream: no error")
	}
	if sb.State() != StateBusy {
		t.Fatalf("state = %s, want %s", sb.State(), StateBusy)
	}
	// The command may still run, so a wait for the sandbox keeps it busy.
	if err := d.WaitRunning(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	if sb.State() != StateBusy {
		t.Fatalf("state after WaitRunning = %s, want %s", sb.State(), StateBusy)
	}
}

var errInput = errors.New("input failed")

// brokenInput reads its data, then fails.
type brokenInput struct{ data string }

func (b *brokenInput) Read(p []byte) (int, error) {
	if b.data == "" {
		return 0, errInput
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

func TestExecuteStdinReadError(t *testing.T) {
	f := newFake()
	var got []byte
	f.execute = func(_ context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
		if _, err := stream.Receive(); err != nil {
			return err
		}
		if err := stream.Send(&workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Started{Started: &workspacev1.ExecutionStarted{ExecutionId: "ex-5"}}}); err != nil {
			return err
		}
		for {
			req, err := stream.Receive()
			if err != nil {
				return err
			}
			got = append(got, req.GetStdin()...)
			if req.GetCloseStdin() {
				return stream.Send(exitCode(0))
			}
		}
	}
	d := newTestDriver(t, f, testConfig)
	sb := startSandbox(t, d)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := d.Execute(ctx, sb, Command{Argv: []string{"cat"}, Stdin: &brokenInput{data: "partial"}})
	if !errors.Is(err, errInput) {
		t.Fatalf("err = %v, want the read error", err)
	}
	// The command saw the input up to the failure, then its end, and ended.
	if string(got) != "partial" || res.ExitCode != 0 || sb.State() != StateReady {
		t.Fatalf("input %q, Result %+v, state %s", got, res, sb.State())
	}
}

func TestExecuteDoesNotWaitForBlockedInput(t *testing.T) {
	f := newFake()
	f.execute = func(_ context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
		if _, err := stream.Receive(); err != nil {
			return err
		}
		if err := stream.Send(&workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Started{Started: &workspacev1.ExecutionStarted{ExecutionId: "ex-6"}}}); err != nil {
			return err
		}
		return stream.Send(exitCode(0))
	}
	d := newTestDriver(t, f, testConfig)
	sb := startSandbox(t, d)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stdin, never := io.Pipe()
	defer func() { _ = never.Close() }()
	if res, err := d.Execute(ctx, sb, Command{Argv: []string{"true"}, Stdin: stdin}); err != nil || res.ExitCode != 0 {
		t.Fatalf("Execute with input that never comes: %+v, %v", res, err)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestExecuteOutputWriteError(t *testing.T) {
	f := newFake()
	f.execute = func(_ context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
		if _, err := stream.Receive(); err != nil {
			return err
		}
		if err := stream.Send(&workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Started{Started: &workspacev1.ExecutionStarted{ExecutionId: "ex-4"}}}); err != nil {
			return err
		}
		chunk := bytes.Repeat([]byte("x"), 32<<10)
		for range 64 {
			if err := stream.Send(&workspacev1.ExecuteResponse{Message: &workspacev1.ExecuteResponse_Stdout{Stdout: chunk}}); err != nil {
				return err
			}
		}
		return stream.Send(exitCode(0))
	}
	d := newTestDriver(t, f, testConfig)
	sb := startSandbox(t, d)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := d.Execute(ctx, sb, Command{Argv: []string{"yes"}, Stdout: failingWriter{}})
	if err == nil || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("err = %v, want the write error", err)
	}
	if res.ExitCode != 0 || sb.State() != StateReady {
		t.Fatalf("Result = %+v, state %s", res, sb.State())
	}
}
