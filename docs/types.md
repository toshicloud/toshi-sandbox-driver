# Types


The exported types carry the API's data without naming its generated
messages, so a caller needs the API module only for a field the driver does
not carry.

- `Sandbox` is one sandbox as the driver last read it: `Name`, as the
  caller gave it; `ID`; `State`, the driver's own state from its label
  and status;
  `Labels`, a copy; and `Proto` for the whole message.
- `Operation` is a long-running action such as a delete: `ID`, `Done`, and
  `Err`, which holds the server's message when the operation failed.
- `Event` is one event of the driver's sandboxes: `Type`, `SandboxID`,
  `Time`, `Cursor`, the position after it, and the payload of its family:
  `Execution` for an execution event and `Emitted` for one a guest
  emitted, nil for another event.
- `ExecutionEvent` is an execution starting or ending: `ExecutionID`,
  `Argv`, `ExitCode` (-1 when it ended without one) and `Cancelled`.
- `EmittedEvent` is an event a guest emitted: `Data`.
- `EventType` is the driver's own type, with one constant per event type
  the driver reports: `EventWorkloadCreated`,
  `EventWorkloadStateChanged`, `EventWorkloadUpdated`,
  `EventWorkloadDeleted`, `EventExecutionStarted`, `EventExecutionEnded`
  and `EventEmitted`. `EventUnspecified`, the zero value, is no event the
  driver reports; an event of another type is dropped.
- `Proto` on `Sandbox`, `Operation` and `Event` returns the generated
  message, for code that needs a field the driver does not carry.

