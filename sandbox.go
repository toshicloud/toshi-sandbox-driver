package driver

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"time"

	workspacev1 "go.toshicloud.io/api/workspacev1"
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
	// StateReady or StateBusy.
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
)

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
}

// Sandbox is one sandbox of the driver's, as the driver last read it.
type Sandbox struct {
	// Name is the name the driver's caller gave it.
	Name string
	// Sandbox is the sandbox as the latest call returned it.
	Sandbox *workspacev1.Sandbox
}

// ID returns the sandbox's workload ID.
func (s *Sandbox) ID() string { return s.Sandbox.GetId() }

// State returns the driver's state for the sandbox, from its label.
func (s *Sandbox) State() string { return s.Sandbox.GetLabels()[LabelState] }

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
	return &Sandbox{Name: name, Sandbox: resp.GetSandbox()}, nil
}

// requestKey is the request key of a call on the sandbox called name: the
// call's prefix and a hash of the project, the environment and the driver's
// and the sandbox's names. The server keeps one key space per workspace, so
// the scope keeps one driver name apart across environments. Each part is
// hashed after its length, so names may hold any bytes and no two sets of
// parts share a key.
func (d *Driver) requestKey(prefix, name string) string {
	var b []byte
	for _, part := range []string{d.cfg.Project, d.cfg.Environment, d.cfg.Name, name} {
		b = binary.BigEndian.AppendUint64(b, uint64(len(part)))
		b = append(b, part...)
	}
	sum := sha256.Sum256(b)
	return prefix + "-" + hex.EncodeToString(sum[:16])
}

// WaitRunning waits with WaitSandbox until the sandbox runs or ends, reads
// it with GetSandbox, and records it as StateReady. A sandbox recorded as
// StateBusy stays so: a command Execute lost track of may still run in it.
// It fails when the sandbox is not running. It has no bound of its own: ctx
// ends a wait for a sandbox that stays queued.
func (d *Driver) WaitRunning(ctx context.Context, sb *Sandbox) error {
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
	sb.Sandbox = resp.GetSandbox()
	if status := sb.Sandbox.GetStatus(); status.GetState() != workspacev1.SandboxState_SANDBOX_STATE_RUNNING {
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
	labels := maps.Clone(sb.Sandbox.GetLabels())
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
	sb.Sandbox = resp.GetSandbox()
	return nil
}
