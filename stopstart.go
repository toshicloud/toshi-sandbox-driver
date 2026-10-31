package driver

import (
	"context"
	"fmt"
	"time"

	workspacev1 "go.toshicloud.io/api/workspacev1"
)

// Stop stops the sandbox with Sandboxes.Stop, which ends its run and every
// execution in it, keeps its VM, and records the sandbox as StateStopped. The
// commands in it are cancelled. A sandbox created without KeepVM, or one that
// has ended, fails with FAILED_PRECONDITION. Its request key names the
// transition from the sandbox as last read: a call repeated after a lost
// answer, with the same *Sandbox, gets the first call's answer, while a later
// stop of the sandbox, after it changed, gets a key of its own.
func (d *Driver) Stop(ctx context.Context, sb *Sandbox) error {
	resp, err := d.client.Workload().Sandboxes().Stop(ctx, &workspacev1.StopSandboxRequest{
		Id:         sb.ID(),
		RequestKey: d.transitionKey("stop", sb),
	})
	if err != nil {
		return fmt.Errorf("driver: stop sandbox %s: %w", sb.ID(), err)
	}
	sb.sandbox = resp.GetSandbox()
	return d.setState(ctx, sb, StateStopped)
}

// Start starts the sandbox with Sandboxes.Start, which queues a run on its
// kept VM, and records the sandbox as StateStarting. WaitRunning then waits
// until it runs and records it as StateReady. A sandbox created without
// KeepVM, or one that has ended, fails with FAILED_PRECONDITION. Its request
// key names the transition from the sandbox as last read: a call repeated
// after a lost answer, with the same *Sandbox, gets the first call's answer,
// while a later start of the sandbox, after it changed, gets a key of its
// own.
func (d *Driver) Start(ctx context.Context, sb *Sandbox) error {
	resp, err := d.client.Workload().Sandboxes().Start(ctx, &workspacev1.StartSandboxRequest{
		Id:         sb.ID(),
		RequestKey: d.transitionKey("start", sb),
	})
	if err != nil {
		return fmt.Errorf("driver: start sandbox %s: %w", sb.ID(), err)
	}
	sb.sandbox = resp.GetSandbox()
	return d.setState(ctx, sb, StateStarting)
}

// transitionKey is the request key of a stop or start of sb, from the sandbox
// as last read: the prefix, the sandbox's name and the time it last changed.
// A call repeated after a lost answer, with the same *Sandbox, sends the same
// key, so the server answers with the first call's sandbox. Every change of
// the sandbox, and so every stop or start, moves its update time on, so a
// later cycle sends a key of its own. A call made from a sandbox read again,
// after a restart or from Recover, sends a new key too, and the server then
// answers by state: a stopped sandbox stays stopped, and a queued or running
// one comes back as it is. Without an update time, transitionKey is empty.
func (d *Driver) transitionKey(prefix string, sb *Sandbox) string {
	updated := sb.Proto().GetStatus().GetUpdatedAt()
	if updated == nil {
		return ""
	}
	return d.requestKey(prefix, sb.Name, updated.AsTime().UTC().Format(time.RFC3339Nano))
}
