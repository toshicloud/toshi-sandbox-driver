package driver

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"connectrpc.com/connect"
	workspacev1 "go.toshicloud.io/api/workspacev1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// The labels the driver puts on each sandbox it creates.
const (
	// LabelDriver holds the driver's name.
	LabelDriver = "toshi-sandbox-driver"
	// LabelSandbox holds the sandbox's name, which the driver's caller
	// chooses and which names one sandbox per driver.
	LabelSandbox = "toshi-sandbox-driver/sandbox"
	// LabelState holds the driver's own state for the sandbox: StateStarting,
	// StateReady, StateBusy or StateStopped.
	LabelState = "toshi-sandbox-driver/state"
)

// The driver's states for a sandbox, kept in its LabelState label.
const (
	// StateStarting is a sandbox created and not yet seen running.
	StateStarting = "starting"
	// StateReady is a running sandbox with no command of the driver's in it.
	StateReady = "ready"
	// StateBusy is a running sandbox the driver runs a command in.
	StateBusy = "busy"
	// StateStopped is a kept sandbox powered off, by Stop or by Toshi.
	StateStopped = "stopped"
)

// ErrStopped is what Execute and WaitRunning return for a stopped sandbox:
// its VM is powered off until Start runs it again.
var ErrStopped = errors.New("driver: the sandbox is stopped")

// ErrHeld is what the driver returns for a sandbox an operator holds:
// WaitRunning and Execute in place of their stopped or not running errors, and
// Start, Stop and Delete in place of the server's refusal. The hold names its
// reason in a HeldError, with when it began and where to write.
var ErrHeld = errors.New("driver: the sandbox is held by the operator")

// HeldError is an operator's hold on a sandbox: the reason, when it began and
// where to write for its release. The driver learns of a hold from the
// sandbox's status or from the server's refusal of Start, Stop or Delete. Is
// matches ErrHeld, and Unwrap returns the server's refusal, nil when the hold
// was read from the status.
type HeldError struct {
	// SandboxID is the sandbox's workload ID.
	SandboxID string
	// Hold is the operator's hold. A value other than the API's suspected or
	// confirmed abuse names no reason.
	Hold workspacev1.OperatorHold
	// HeldAt is when the operator held the sandbox; the zero time when the
	// driver does not know.
	HeldAt time.Time
	// Contact is the email address or URL to write to for the hold's
	// release; empty when the driver does not know.
	Contact string
	// Err is the server's refusal that named the hold, nil when the driver
	// read the hold from the sandbox's status.
	Err error

	// verb is the call the server refused: "start", "stop" or "delete".
	// Empty for a hold read from the status.
	verb string
}

// Error names the sandbox, the reason, when the hold began and where to
// write: after the sandbox when the hold came from its status, and after the
// refused call when it came from the server.
func (e *HeldError) Error() string {
	var b strings.Builder
	if e.verb == "" {
		fmt.Fprintf(&b, "driver: sandbox %s is ", e.SandboxID)
	} else {
		fmt.Fprintf(&b, "driver: %s sandbox %s: ", e.verb, e.SandboxID)
	}
	b.WriteString(e.phrase())
	return b.String()
}

// phrase is the hold in words: its reason when the API names one, then when it
// began and where to write, each when the driver knows it.
func (e *HeldError) phrase() string {
	var b strings.Builder
	b.WriteString("held by the operator")
	switch e.Hold {
	case workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED:
		b.WriteString(" for suspected abuse")
	case workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_CONFIRMED:
		b.WriteString(" for confirmed abuse")
	}
	if !e.HeldAt.IsZero() {
		fmt.Fprintf(&b, " since %s", e.HeldAt.UTC().Format(time.RFC3339))
	}
	if e.Contact != "" {
		fmt.Fprintf(&b, "; write to %s", e.Contact)
	}
	return b.String()
}

// Is reports that the error is ErrHeld.
func (e *HeldError) Is(target error) bool { return target == ErrHeld }

// Unwrap returns the server's refusal, nil when the driver read the hold from
// the sandbox's status.
func (e *HeldError) Unwrap() error { return e.Err }

// heldStatus returns the hold on the sandbox as it was last read, if its
// status carries one.
func heldStatus(sb *Sandbox) (*HeldError, bool) {
	status := sb.sandbox.GetStatus()
	if status.GetOperatorHold() == workspacev1.OperatorHold_OPERATOR_HOLD_UNSPECIFIED {
		return nil, false
	}
	held := &HeldError{SandboxID: sb.ID(), Hold: status.GetOperatorHold(), Contact: status.GetHoldContact()}
	if at := status.GetHeldAt(); at != nil {
		held.HeldAt = at.AsTime()
	}
	return held, true
}

// heldRefusal returns the hold the server named when it refused err, if it
// refused because an operator holds the sandbox: FAILED_PRECONDITION with an
// ErrorInfo detail whose reason is workload_held. A hold the metadata does not
// name is unspecified.
func heldRefusal(err error) (workspacev1.OperatorHold, bool) {
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) || connectErr.Code() != connect.CodeFailedPrecondition {
		return workspacev1.OperatorHold_OPERATOR_HOLD_UNSPECIFIED, false
	}
	for _, detail := range connectErr.Details() {
		msg, valueErr := detail.Value()
		if valueErr != nil {
			continue
		}
		info, ok := msg.(*errdetails.ErrorInfo)
		if !ok || info.GetReason() != "workload_held" {
			continue
		}
		return holdFromMetadata(info.GetMetadata()["hold"]), true
	}
	return workspacev1.OperatorHold_OPERATOR_HOLD_UNSPECIFIED, false
}

// holdFromMetadata returns the hold the refusal's metadata names. Only the two
// reasons the API defines are named; another value is unspecified.
func holdFromMetadata(hold string) workspacev1.OperatorHold {
	switch hold {
	case "abuse_suspected":
		return workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_SUSPECTED
	case "abuse_confirmed":
		return workspacev1.OperatorHold_OPERATOR_HOLD_ABUSE_CONFIRMED
	}
	return workspacev1.OperatorHold_OPERATOR_HOLD_UNSPECIFIED
}

// refused returns the error for a call the server refused because an operator
// holds the sandbox, and false for another error. It makes one GetSandbox to
// read the hold, when it began and where to write; when that call fails, the
// error names the hold the refusal's metadata gave, with no time and no
// contact.
func (d *Driver) refused(ctx context.Context, verb string, sb *Sandbox, refusal error) (*HeldError, bool) {
	hold, ok := heldRefusal(refusal)
	if !ok {
		return nil, false
	}
	held := &HeldError{SandboxID: sb.ID(), Hold: hold, Err: refusal, verb: verb}
	resp, err := d.client.Workload().Sandboxes().Get(ctx, &workspacev1.GetSandboxRequest{Id: sb.ID()})
	if err != nil {
		return held, true
	}
	status := resp.GetSandbox().GetStatus()
	if hold := status.GetOperatorHold(); hold != workspacev1.OperatorHold_OPERATOR_HOLD_UNSPECIFIED {
		held.Hold = hold
	}
	if at := status.GetHeldAt(); at != nil {
		held.HeldAt = at.AsTime()
	}
	held.Contact = status.GetHoldContact()
	return held, true
}

// waitBound is how long one WaitSandbox call asks the server to wait.
// toshi-go asks again until the sandbox reaches a state or the context ends.
const waitBound = 30 * time.Second

// Spec is what a sandbox is created with.
type Spec struct {
	// Size names the machine size, for example "toshi-linux-2x".
	Size string
	// MaxDuration is how long the sandbox may run: required, at least one
	// minute and a whole number of minutes. The server stops it then.
	MaxDuration time.Duration
	// KeepVM keeps the sandbox's VM, powered off on its disk, while the
	// sandbox is stopped, so Stop and Start can be used. The workspace must
	// allow it; otherwise Create fails with FAILED_PRECONDITION.
	KeepVM bool
}

// Sandbox is one sandbox of the driver's, as the driver last read it.
type Sandbox struct {
	// Name is the name the driver's caller gave it.
	Name string

	sandbox *workspacev1.Sandbox
}

// ID returns the sandbox's workload ID.
func (s *Sandbox) ID() string { return s.sandbox.GetId() }

// State returns the driver's state for the sandbox, from the sandbox as it
// last read it: a sandbox in CREATED with a stop time is StateStopped; a
// sandbox labelled StateStopped otherwise, left by a Start that was answered
// before the label was rewritten or by a crash in between, is StateStarting;
// otherwise State is the label's.
func (s *Sandbox) State() string {
	if stoppedStatus(s.sandbox.GetStatus()) {
		return StateStopped
	}
	if state := s.sandbox.GetLabels()[LabelState]; state != StateStopped {
		return state
	}
	return StateStarting
}

// stoppedStatus reports whether status is a stopped sandbox's: in CREATED,
// with a stop time.
func stoppedStatus(status *workspacev1.SandboxStatus) bool {
	return status.GetState() == workspacev1.SandboxState_SANDBOX_STATE_CREATED && status.GetStoppedAt() != nil
}

// Labels returns a copy of the sandbox's labels.
func (s *Sandbox) Labels() map[string]string { return maps.Clone(s.sandbox.GetLabels()) }

// Proto returns the sandbox as the API's message, for code that needs a field
// the driver does not carry.
func (s *Sandbox) Proto() *workspacev1.Sandbox { return s.sandbox }

// Create creates the sandbox called name with Sandboxes.Create, labelled
// with the driver's name, the sandbox's name and StateStarting. Its request
// key is derived from the scope and the two names, so a call repeated with
// the same spec after a lost answer or a restart returns the sandbox the
// first call created instead of creating another; a name is therefore used
// for one sandbox only. The same name with another spec fails with
// FAILED_PRECONDITION.
func (d *Driver) Create(ctx context.Context, name string, spec Spec) (*Sandbox, error) {
	if name == "" {
		return nil, errors.New("driver: a sandbox needs a name")
	}
	switch {
	case spec.MaxDuration < time.Minute:
		return nil, fmt.Errorf("driver: sandbox %q: the maximum duration %s is under a minute", name, spec.MaxDuration)
	case spec.MaxDuration%time.Minute != 0:
		return nil, fmt.Errorf("driver: sandbox %q: the maximum duration %s is not a whole number of minutes", name, spec.MaxDuration)
	}
	resp, err := d.client.Workload().Sandboxes().Create(ctx, &workspacev1.CreateSandboxRequest{
		Spec: &workspacev1.SandboxSpec{
			Size:        spec.Size,
			MaxDuration: durationpb.New(spec.MaxDuration),
			KeepVm:      spec.KeepVM,
		},
		Labels: map[string]string{
			LabelDriver:  d.cfg.Name,
			LabelSandbox: name,
			LabelState:   StateStarting,
		},
		RequestKey: d.requestKey("sandbox", name),
	})
	if err != nil {
		return nil, fmt.Errorf("driver: create sandbox %q: %w", name, err)
	}
	return &Sandbox{Name: name, sandbox: resp.GetSandbox()}, nil
}

// requestKey is the request key of a call on a sandbox: the call's prefix and
// a hash of the project, the environment, the driver's name and the parts the
// call names. The server keeps one key space per workspace, so the scope
// keeps one driver name apart across environments. Each part is hashed after
// its length, so names may hold any bytes and no two sets of parts share a
// key.
func (d *Driver) requestKey(prefix string, parts ...string) string {
	var b []byte
	all := make([]string, 0, 3+len(parts))
	all = append(all, d.cfg.Project, d.cfg.Environment, d.cfg.Name)
	all = append(all, parts...)
	for _, part := range all {
		b = binary.BigEndian.AppendUint64(b, uint64(len(part)))
		b = append(b, part...)
	}
	sum := sha256.Sum256(b)
	return prefix + "-" + hex.EncodeToString(sum[:16])
}

// WaitRunning waits with WaitSandbox until the sandbox runs or ends, reads
// it with GetSandbox, and records it as StateReady. A stopped sandbox fails
// with ErrStopped without waiting, and a held one with ErrHeld. A sandbox
// recorded as StateBusy stays so: a command Execute lost track of may still
// run in it. It fails when the sandbox is not running, or with ErrHeld when an
// operator holds it. It has no bound of its own: ctx ends a wait for a sandbox
// that stays queued.
func (d *Driver) WaitRunning(ctx context.Context, sb *Sandbox) error {
	if sb.State() == StateStopped {
		if held, ok := heldStatus(sb); ok {
			return held
		}
		return fmt.Errorf("driver: wait for sandbox %s: %w", sb.ID(), ErrStopped)
	}
	if _, err := d.client.Workload().Sandboxes().Wait(ctx, &workspacev1.WaitSandboxRequest{
		Id:      sb.ID(),
		States:  []workspacev1.SandboxState{workspacev1.SandboxState_SANDBOX_STATE_RUNNING, workspacev1.SandboxState_SANDBOX_STATE_ENDED},
		Timeout: durationpb.New(waitBound),
	}); err != nil {
		return fmt.Errorf("driver: wait for sandbox %s: %w", sb.ID(), err)
	}
	resp, err := d.client.Workload().Sandboxes().Get(ctx, &workspacev1.GetSandboxRequest{Id: sb.ID()})
	if err != nil {
		return fmt.Errorf("driver: get sandbox %s: %w", sb.ID(), err)
	}
	sb.sandbox = resp.GetSandbox()
	if status := sb.sandbox.GetStatus(); status.GetState() != workspacev1.SandboxState_SANDBOX_STATE_RUNNING {
		if held, ok := heldStatus(sb); ok {
			return held
		}
		return fmt.Errorf("driver: sandbox %s is %s, not running (end reason %s, stop reason %s)",
			sb.ID(), status.GetState(), status.GetEndReason(), status.GetStopReason())
	}
	if sb.State() == StateBusy {
		return nil
	}
	return d.setState(ctx, sb, StateReady)
}

// setState records state in the sandbox's LabelState label with
// UpdateSandbox. A patch replaces all labels, so it sends the sandbox's
// labels as last read with that one changed.
func (d *Driver) setState(ctx context.Context, sb *Sandbox, state string) error {
	labels := maps.Clone(sb.sandbox.GetLabels())
	if labels == nil {
		labels = make(map[string]string, 1)
	}
	labels[LabelState] = state
	resp, err := d.client.Workload().Sandboxes().Update(ctx, &workspacev1.UpdateSandboxRequest{
		Id:         sb.ID(),
		Sandbox:    &workspacev1.Sandbox{Labels: labels},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("driver: record sandbox %s as %s: %w", sb.ID(), state, err)
	}
	sb.sandbox = resp.GetSandbox()
	return nil
}
