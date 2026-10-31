// Package e2e_test runs the driver against a served Toshi: toshi serve with
// its all-in-one agent, running sandboxes in qemu VMs.
package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	workspacev1 "go.toshicloud.io/api/workspacev1"
	toshi "go.toshicloud.io/toshi"
	"google.golang.org/protobuf/types/known/durationpb"

	driver "github.com/toshicloud/toshi-sandbox-driver"
)

// TestSandboxFlow runs steps 1 to 9 against a served stack and checks that
// the driver made every call of the flow. It is skipped
// unless TOSHI_DRIVER_E2E_BIN names a toshi binary; it then needs
// TOSHI_DRIVER_E2E_IMAGE_DIR and TOSHI_DRIVER_E2E_IMAGE, a guest image
// <dir>/<image>.qcow2 the binary's agent can run, and KVM unless
// TOSHI_DRIVER_E2E_ACCEL says otherwise.
func TestSandboxFlow(t *testing.T) {
	bin := os.Getenv("TOSHI_DRIVER_E2E_BIN")
	if bin == "" {
		t.Skip("set TOSHI_DRIVER_E2E_BIN to a toshi binary, or run scripts/e2e.sh")
	}
	bin, err := filepath.Abs(bin)
	if err != nil {
		t.Fatal(err)
	}
	cfg := stackConfig{
		bin:       bin,
		imageDir:  os.Getenv("TOSHI_DRIVER_E2E_IMAGE_DIR"),
		imageName: os.Getenv("TOSHI_DRIVER_E2E_IMAGE"),
		accel:     os.Getenv("TOSHI_DRIVER_E2E_ACCEL"),
		logDir:    os.Getenv("TOSHI_DRIVER_E2E_LOG_DIR"),
	}
	if cfg.imageDir == "" || cfg.imageName == "" {
		t.Fatal("TOSHI_DRIVER_E2E_IMAGE_DIR and TOSHI_DRIVER_E2E_IMAGE must be set with TOSHI_DRIVER_E2E_BIN")
	}
	if cfg.accel == "" {
		cfg.accel = "kvm"
	}
	if cfg.accel == "kvm" {
		if _, err := os.Stat("/dev/kvm"); err != nil {
			t.Fatalf("accel kvm: %v", err)
		}
	}
	if want := os.Getenv("TOSHI_DRIVER_E2E_EXPECT_COMMIT"); want != "" {
		if got := servedCommit(t, bin); got != want {
			t.Fatalf("served Toshi commit %s, want %s", got, want)
		}
	}

	// With Toshi's network helper on the host, the agent writes each VM's
	// network rules through it, in the VM's own cgroup; the stack's VMs have
	// none, so their jobs would fail.
	if info, err := os.Stat(networkHelperSocket); err == nil && info.Mode()&os.ModeSocket != 0 {
		t.Fatalf("this host runs Toshi's network helper (%s), and the stack's VMs have no cgroup of their own for its rules: run scripts/e2e.sh with TOSHI_DRIVER_E2E_HIDE_NETWORK_HELPER=1", networkHelperSocket)
	}

	st := startStack(t, cfg)
	t.Logf("stack: host %s of workspace %s, keep_vm %t, network enforced %t (%s)", st.host.ID, st.host.OwnerTenant, st.host.KeepVM, st.host.NetworkEnforced, st.host.NetworkReason)
	// The driver's calls are counted; the test's own checks go through a
	// client of their own.
	t.Setenv("TOSHI_CA_FILE", "")
	httpClient, counted := countingClient(t, st.caPath)
	opts := []toshi.Option{toshi.WithServer(st.url), toshi.WithToken(st.token), toshi.WithHTTPClient(httpClient)}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	driverCfg := driver.Config{Project: projectName, Environment: environment, Name: "e2e"}

	// Step 1: the scope is checked, and a wrong one refused.
	d, err := driver.New(driverCfg, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Check(ctx); err != nil {
		t.Fatalf("Check: %v", err)
	}
	wrong, err := driver.New(driver.Config{Project: projectName, Environment: "staging", Name: "e2e"}, opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := wrong.Check(ctx); err == nil {
		t.Fatal("Check with an environment the project lacks: no error")
	}
	t.Log("step 1: Check passed for default/production and refused default/staging")

	// Step 2: created with its labels; created again, the same sandbox.
	spec := driver.Spec{Size: sandboxSize, MaxDuration: 10 * time.Minute}
	sb, err := d.Create(ctx, "e2e-sandbox", spec)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	client, err := toshi.New(toshi.WithServer(st.url), toshi.WithToken(st.token), toshi.WithCAFile(st.caPath), toshi.WithScope(projectName, environment))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { deleteSandbox(t, client, sb.ID()) })
	again, err := d.Create(ctx, "e2e-sandbox", spec)
	if err != nil {
		t.Fatalf("Create again: %v", err)
	}
	if again.ID() != sb.ID() {
		t.Fatalf("Create again made %s, want %s", again.ID(), sb.ID())
	}
	t.Logf("step 2: created %s with labels %v; a second Create returned the same sandbox", sb.ID(), sb.Labels())

	// Step 3: running, and read.
	started := time.Now()
	if err := d.WaitRunning(ctx, sb); err != nil {
		t.Fatalf("WaitRunning: %v", err)
	}
	t.Logf("step 3: %s running after %s", sb.ID(), time.Since(started).Round(time.Second))

	// The driver stores the event log's position, as it would before a
	// restart.
	first, err := d.Recover(ctx, "")
	if err != nil {
		t.Fatalf("Recover on a first start: %v", err)
	}
	cursorFile := filepath.Join(t.TempDir(), "cursor")
	if err := os.WriteFile(cursorFile, []byte(first.Cursor), 0o600); err != nil {
		t.Fatal(err)
	}

	// Step 4: standard input fed, output streamed, exit statuses returned.
	input := "hello from the driver\n"
	var stdout, stderr bytes.Buffer
	res, err := d.Execute(ctx, sb, driver.Command{Argv: []string{"cat"}, Stdin: strings.NewReader(input), Stdout: &stdout, Stderr: &stderr})
	if err != nil {
		t.Fatalf("Execute cat: %v", err)
	}
	if res.ExitCode != 0 || stdout.String() != input {
		t.Fatalf("cat: %+v, stdout %q, stderr %q", res, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	res, err = d.Execute(ctx, sb, driver.Command{
		Argv:   []string{"sh", "-c", `echo "out $GREETING"; pwd; echo err >&2; exit 7`},
		Env:    map[string]string{"GREETING": "hi"},
		Cwd:    "/tmp",
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		t.Fatalf("Execute sh: %v", err)
	}
	if res.ExitCode != 7 || stdout.String() != "out hi\n/tmp\n" || stderr.String() != "err\n" {
		t.Fatalf("sh: %+v, stdout %q, stderr %q", res, stdout.String(), stderr.String())
	}
	t.Logf("step 4: cat echoed %d bytes of standard input; sh exited 7 with its stdout and stderr apart", len(input))

	// Step 5: cancelled at its timeout.
	started = time.Now()
	res, err = d.Execute(ctx, sb, driver.Command{Argv: []string{"sleep", "600"}, Timeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("Execute sleep: %v", err)
	}
	took := time.Since(started)
	if !res.TimedOut || !res.Cancelled || took > time.Minute {
		t.Fatalf("sleep: %+v after %s", res, took)
	}
	t.Logf("step 5: sleep 600 cancelled at its 3s timeout, ended after %s", took.Round(100*time.Millisecond))

	// Step 6: the state label as the server holds it.
	got, err := client.Workload().Sandboxes().Get(ctx, &workspacev1.GetSandboxRequest{Id: sb.ID()})
	if err != nil {
		t.Fatal(err)
	}
	labels := got.GetSandbox().GetLabels()
	if labels[driver.LabelState] != driver.StateReady || labels[driver.LabelDriver] != "e2e" || labels[driver.LabelSandbox] != "e2e-sandbox" {
		t.Fatalf("labels = %v", labels)
	}
	t.Logf("step 6: the server holds %s=%s", driver.LabelState, labels[driver.LabelState])

	// Step 7: a restarted driver finds its sandbox, catches up from the
	// stored cursor, and follows from there.
	stored, err := os.ReadFile(cursorFile)
	if err != nil {
		t.Fatal(err)
	}
	d, err = driver.New(driverCfg, opts...)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := d.Recover(ctx, string(stored))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(rec.Sandboxes) != 1 || rec.Sandboxes[0].ID() != sb.ID() || rec.Sandboxes[0].Name != "e2e-sandbox" || rec.Sandboxes[0].State() != driver.StateReady {
		t.Fatalf("Recover found %v", rec.Sandboxes)
	}
	if ended := countType(rec.Events, driver.EventExecutionEnded); ended != 3 {
		t.Fatalf("Recover caught up %d ended executions, want 3 (cat, sh, sleep)", ended)
	}
	t.Logf("step 7: Recover found %s (%s) and %d events after the stored cursor", sb.ID(), rec.Sandboxes[0].State(), len(rec.Events))

	sb = rec.Sandboxes[0]
	watchCtx, stopWatch := context.WithCancel(ctx)
	updates := make(chan driver.Update, 64)
	var followErr error
	var following sync.WaitGroup
	following.Add(1)
	go func() {
		defer following.Done()
		followErr = d.Watch(watchCtx, rec.Cursor, func(u driver.Update) error {
			updates <- u
			return nil
		})
	}()
	res, err = d.Execute(ctx, sb, driver.Command{Argv: []string{"true"}})
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("Execute true: %+v, %v", res, err)
	}
	await(t, updates, "the execution's end and the busy label", func(seen []driver.Update) bool {
		var ended, busy bool
		for _, u := range seen {
			ended = ended || u.Event != nil && u.Type == driver.EventExecutionEnded && u.Event.Execution != nil && u.Event.Execution.ExecutionID == res.ExecutionID
			busy = busy || u.Sandbox != nil && u.Sandbox.State() == driver.StateBusy
		}
		return ended && busy
	})
	t.Logf("step 7: Watch reported execution %s ending and %s turning busy", res.ExecutionID, sb.ID())

	// Step 9: deleted, and the delete's operation followed to its end.
	op, err := d.Delete(ctx, sb)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if op, err = d.WaitOperation(ctx, op.ID); err != nil || !op.Done {
		t.Fatalf("WaitOperation: %v, %v", op, err)
	}
	if _, err := client.Workload().Sandboxes().Get(ctx, &workspacev1.GetSandboxRequest{Id: sb.ID()}); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("GetSandbox after the delete: %v, want NOT_FOUND", err)
	}
	await(t, updates, "the delete", func(seen []driver.Update) bool {
		for _, u := range seen {
			if u.Sandbox != nil && u.Type == driver.EventWorkloadDeleted {
				return true
			}
		}
		return false
	})
	stopWatch()
	following.Wait()
	if !errors.Is(followErr, context.Canceled) {
		t.Fatalf("Watch ended with %v, want its context's end", followErr)
	}
	t.Logf("step 9: %s deleted, operation %s done, and Watch reported the delete", sb.ID(), op.ID)

	// Step 8, once the first sandbox is gone and the host has room: a sandbox
	// whose VM is kept is stopped, refuses a command while stopped, and starts
	// again on the same disk.
	kept, err := d.Create(ctx, "e2e-kept", driver.Spec{Size: sandboxSize, MaxDuration: 10 * time.Minute, KeepVM: true})
	if err != nil {
		t.Fatalf("Create with KeepVM: %v", err)
	}
	keptID := kept.ID()
	t.Cleanup(func() { deleteSandbox(t, client, keptID) })
	if err := d.WaitRunning(ctx, kept); err != nil {
		t.Fatalf("WaitRunning the kept sandbox: %v", err)
	}
	const keptFile = "/var/tmp/sandbox-driver-e2e"
	if res, err := d.Execute(ctx, kept, driver.Command{Argv: []string{"sh", "-c", "echo kept > " + keptFile}}); err != nil || res.ExitCode != 0 {
		t.Fatalf("Execute before the stop: %+v, %v", res, err)
	}
	if err := d.Stop(ctx, kept); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// The server answers the stop before the VM is off; the sandbox is
	// stopped once it is back in CREATED, and the driver reads it again then.
	answered := kept.Proto().GetStatus().GetState()
	if _, err := client.Workload().Sandboxes().Wait(ctx, &workspacev1.WaitSandboxRequest{
		Id:      kept.ID(),
		States:  []workspacev1.SandboxState{workspacev1.SandboxState_SANDBOX_STATE_CREATED},
		Timeout: durationpb.New(time.Minute),
	}); err != nil {
		t.Fatalf("wait for the stop: %v", err)
	}
	afterStop, err := d.Recover(ctx, "")
	if err != nil {
		t.Fatalf("Recover after the stop: %v", err)
	}
	kept = nil
	for _, s := range afterStop.Sandboxes {
		if s.Name == "e2e-kept" {
			kept = s
		}
	}
	if kept == nil {
		t.Fatalf("Recover after the stop found %v, not e2e-kept", afterStop.Sandboxes)
	}
	if state := kept.State(); state != driver.StateStopped {
		t.Fatalf("after the stop: state %q, want %q", state, driver.StateStopped)
	}
	if _, err := d.Execute(ctx, kept, driver.Command{Argv: []string{"true"}}); !errors.Is(err, driver.ErrStopped) || !strings.Contains(err.Error(), kept.ID()) {
		t.Fatalf("Execute on the stopped sandbox = %v, want ErrStopped naming %s", err, kept.ID())
	}
	started = time.Now()
	if err := d.Start(ctx, kept); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := d.WaitRunning(ctx, kept); err != nil {
		t.Fatalf("WaitRunning after Start: %v", err)
	}
	restarted := time.Since(started)
	stdout.Reset()
	if res, err := d.Execute(ctx, kept, driver.Command{Argv: []string{"cat", keptFile}, Stdout: &stdout}); err != nil || res.ExitCode != 0 || stdout.String() != "kept\n" {
		t.Fatalf("Execute after the start: %+v, %v, stdout %q", res, err, stdout.String())
	}
	if op, err = d.Delete(ctx, kept); err != nil {
		t.Fatalf("Delete the kept sandbox: %v", err)
	}
	if op, err = d.WaitOperation(ctx, op.ID); err != nil || !op.Done {
		t.Fatalf("WaitOperation for the kept sandbox: %v, %v", op, err)
	}
	t.Logf("step 8: %s stopped (the stop answered in %s), refused a command while stopped, started again in %s with its file kept, and deleted", kept.ID(), answered, restarted.Round(time.Second))

	// The calls of the flow.
	procedures, counts := counted.list()
	for i, p := range procedures {
		t.Logf("call %s x%d", p, counts[i])
	}
	var missing []string
	for _, p := range flowCalls {
		if !counted.called(p) {
			missing = append(missing, p)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("the driver never called %v", missing)
	}
	t.Logf("calls: the driver made all %d calls of the flow", len(flowCalls))
}

// networkHelperSocket is where Toshi's network helper listens.
const networkHelperSocket = "/run/toshi-agent-netsets.sock"

// flowCalls are the calls the flow makes: ProjectService's ListProjects and
// the WorkloadService calls the driver uses.
var flowCalls = []string{
	"toshi.workspace.v1.ProjectService/ListProjects",
	"toshi.workspace.v1.WorkloadService/CreateSandbox",
	"toshi.workspace.v1.WorkloadService/WaitSandbox",
	"toshi.workspace.v1.WorkloadService/GetSandbox",
	"toshi.workspace.v1.WorkloadService/Execute",
	"toshi.workspace.v1.WorkloadService/CancelExecution",
	"toshi.workspace.v1.WorkloadService/UpdateSandbox",
	"toshi.workspace.v1.WorkloadService/ListSandboxes",
	"toshi.workspace.v1.WorkloadService/ListEvents",
	"toshi.workspace.v1.WorkloadService/WatchSandboxes",
	"toshi.workspace.v1.WorkloadService/WatchEvents",
	"toshi.workspace.v1.WorkloadService/StopSandbox",
	"toshi.workspace.v1.WorkloadService/StartSandbox",
	"toshi.workspace.v1.WorkloadService/DeleteSandbox",
	"toshi.workspace.v1.WorkloadService/GetOperation",
	"toshi.workspace.v1.WorkloadService/WaitOperation",
}

// countType counts the events of type t.
func countType(events []*driver.Event, t driver.EventType) int {
	n := 0
	for _, e := range events {
		if e.Type == t {
			n++
		}
	}
	return n
}

// await reads updates until done holds for those seen, failing after a
// minute.
func await(t *testing.T, updates <-chan driver.Update, what string, done func([]driver.Update) bool) {
	t.Helper()
	var seen []driver.Update
	timeout := time.After(time.Minute)
	for !done(seen) {
		select {
		case u := <-updates:
			seen = append(seen, u)
		case <-timeout:
			t.Fatalf("Watch did not report %s in a minute; it reported %d updates", what, len(seen))
		}
	}
}

// servedCommit returns the commit toshi version prints.
func servedCommit(t *testing.T, bin string) string {
	t.Helper()
	out, err := exec.Command(bin, "version").Output()
	if err != nil {
		t.Fatalf("toshi version: %v", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if rest, ok := strings.CutPrefix(line, "commit: "); ok {
			return strings.TrimSpace(rest)
		}
	}
	t.Fatalf("toshi version printed no commit:\n%s", out)
	return ""
}

// deleteSandbox deletes the sandbox, unless step 9 did, and waits for the
// delete, so its VM is gone before the stack stops.
func deleteSandbox(t *testing.T, client *toshi.Client, id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	resp, err := client.Workload().Sandboxes().Delete(ctx, &workspacev1.DeleteSandboxRequest{Id: id})
	if connect.CodeOf(err) == connect.CodeNotFound {
		return
	}
	if err != nil {
		t.Errorf("delete %s: %v", id, err)
		return
	}
	if _, err := client.Workload().WaitOperation(ctx, &workspacev1.WaitOperationRequest{
		Id:      resp.GetOperation().GetId(),
		Timeout: durationpb.New(30 * time.Second),
	}); err != nil {
		t.Errorf("wait for the delete of %s: %v", id, err)
	}
}
