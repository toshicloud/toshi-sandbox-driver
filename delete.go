package driver

import (
	"context"
	"fmt"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Delete deletes the sandbox with Sandboxes.Delete, which stops it and
// cancels the commands in it, and returns the delete's operation; the
// sandbox is gone once the operation is done. A sandbox an operator holds
// fails with ErrHeld. Its request key is derived from the scope and the
// driver's and the sandbox's names, so a call repeated after a lost answer
// returns the first call's operation.
func (d *Driver) Delete(ctx context.Context, sb *Sandbox) (*Operation, error) {
	resp, err := d.client.Workload().Sandboxes().Delete(ctx, &workspacev1.DeleteSandboxRequest{
		Id:         sb.ID(),
		RequestKey: d.requestKey("delete", sb.Name),
	})
	if err != nil {
		if held, ok := d.refused(ctx, "delete", sb, err); ok {
			return nil, held
		}
		return nil, fmt.Errorf("driver: delete sandbox %s: %w", sb.ID(), err)
	}
	return operationFrom(resp.GetOperation()), nil
}

// WaitOperation reads the operation with GetOperation and, unless it is
// done, waits for it with WaitOperation. It returns the done operation; an
// operation the server reports as failed comes back with Err set, and the
// returned error is the call's own.
func (d *Driver) WaitOperation(ctx context.Context, id string) (*Operation, error) {
	resp, err := d.client.Workload().GetOperation(ctx, &workspacev1.GetOperationRequest{Id: id})
	if err != nil {
		return nil, fmt.Errorf("driver: get operation %s: %w", id, err)
	}
	op := resp.GetOperation()
	if !op.GetDone() {
		waited, err := d.client.Workload().WaitOperation(ctx, &workspacev1.WaitOperationRequest{Id: id, Timeout: durationpb.New(waitBound)})
		if err != nil {
			return nil, fmt.Errorf("driver: wait for operation %s: %w", id, err)
		}
		op = waited
	}
	out := operationFrom(op)
	if e := op.GetError(); e != nil {
		out.Err = fmt.Errorf("driver: operation %s failed: %s: %s", id, e.GetCode(), e.GetMessage())
	}
	return out, nil
}
