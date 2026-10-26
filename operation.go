package driver

import (
	workspacev1 "go.toshicloud.io/api/workspacev1"
)

// Operation is a long-running action of the server's, such as a sandbox's
// delete, as the driver last read it.
type Operation struct {
	// ID is the operation's ID.
	ID string
	// Done reports whether the operation finished.
	Done bool
	// Err is nil unless the operation failed, and holds the server's error
	// message then.
	Err error

	operation *workspacev1.Operation
}

// Proto returns the operation as the API's message, for code that needs a
// field the driver does not carry.
func (o *Operation) Proto() *workspacev1.Operation { return o.operation }

// operationFrom returns the driver's operation for the API's operation op.
func operationFrom(op *workspacev1.Operation) *Operation {
	return &Operation{ID: op.GetId(), Done: op.GetDone(), operation: op}
}
