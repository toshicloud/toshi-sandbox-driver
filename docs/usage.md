# Using the driver

A Go package that runs sandboxes on Toshi for an agent orchestrator. It
uses only the public Workspace API, `toshi.workspace.v1`, through the
[toshi-go](https://pkg.go.dev/go.toshicloud.io/toshi) client,
`go.toshicloud.io/toshi`, and imports no other Toshi module but `go.toshicloud.io/api`.
Before v0.2.0 that client was the module
[github.com/toshicloud/toshi-go](https://github.com/toshicloud/toshi-go).

A sandbox is a VM that runs until its owner stops it or its maximum
duration passes. The driver:

1. reads its token, project and environment from the environment, and
   checks them with `ListProjects`;
2. creates a sandbox with labels and a request key;
3. waits until it runs and reads it;
4. runs commands in it, feeding standard input and streaming output;
5. cancels a command that runs too long;
6. records its own state for the sandbox in a label;
7. after a restart, lists its sandboxes by that label, catches up on the
   events since its stored cursor, and follows from there;
8. stops a sandbox whose VM is kept, powering the VM off on its disk, and
   starts it again;
9. deletes the sandbox and follows the delete operation.

## Use

<!-- ExampleNew -->
```go
cfg, err := driver.ConfigFromEnv()
if err != nil {
	log.Fatal(err)
}
d, err := driver.New(cfg)
if err != nil {
	log.Fatal(err)
}
if err := d.Check(ctx); err != nil {
	log.Fatal(err) // unreachable, refused, or the wrong scope
}
```

<!-- ExampleDriver_Create -->
```go
sb, err := d.Create(ctx, "task-42", driver.Spec{Size: "toshi-linux-2x", MaxDuration: 30 * time.Minute})
if err != nil {
	log.Fatal(err)
}
if err := d.WaitRunning(ctx, sb); err != nil {
	log.Fatal(err) // it ended instead, or ctx ended
}

res, err := d.Execute(ctx, sb, driver.Command{
	Argv:    []string{"sh", "-c", "make test"},
	Stdin:   os.Stdin,
	Stdout:  os.Stdout,
	Stderr:  os.Stderr,
	Timeout: 10 * time.Minute,
})
if err != nil {
	log.Fatal(err)
}
// res.ExitCode, res.Cancelled, res.TimedOut
```

A sandbox created with `KeepVM` can be stopped and started again:

<!-- ExampleDriver_Stop -->
```go
if err := d.Stop(ctx, sb); err != nil {
	log.Fatal(err) // created without KeepVM, or ended
}
// sb.State() is driver.StateStopped; Execute fails with driver.ErrStopped.

if err := d.Start(ctx, sb); err != nil {
	log.Fatal(err)
}
if err := d.WaitRunning(ctx, sb); err != nil {
	log.Fatal(err)
}
```

<!-- ExampleDriver_Delete -->
```go
op, err := d.Delete(ctx, sb)
if err != nil {
	log.Fatal(err)
}
op, err = d.WaitOperation(ctx, op.ID)
if err != nil {
	log.Fatal(err)
}
```

After a restart:

<!-- ExampleDriver_Recover -->
```go
rec, err := d.Recover(ctx, stored) // stored: the last cursor saved, or ""
if err != nil {
	log.Fatal(err)
}
// rec.Sandboxes, rec.Events, rec.Cursor
err = d.Watch(ctx, rec.Cursor, func(u driver.Update) error {
	// u.Sandbox changed, or u.Event happened. Handle it, then save
	// u.Cursor if it is set: only events carry one.
	return nil
})
if err != nil {
	log.Fatal(err)
}
```

- `New` passes its options to toshi-go's `toshi.New`, so an option such as
  `toshi.WithServer`, `toshi.WithToken`, `toshi.WithCAFile`,
  `toshi.WithHTTPClient` or `toshi.WithRetryPolicy` replaces what the
  environment or toshi-go's default would give. The scope is always the
  configured project and environment, whatever the options say.
- `Check` lists the projects the token sees and fails unless the
  configured project has the configured environment.
- `Create` creates a sandbox with three labels: `toshi-sandbox-driver`
  (the driver's name), `toshi-sandbox-driver/sandbox` (the sandbox's
  name) and `toshi-sandbox-driver/state`, exported as `LabelDriver`,
  `LabelSandbox` and `LabelState`. `Spec.Size` names the machine size, for
  example `toshi-linux-2x`. `MaxDuration` is required: a
  whole number of minutes, at least one. The request key comes from the
  project, the environment and the two names, so creating the same name
  again with the same spec, after a lost answer or a restart, returns the
  first sandbox: a name is for one sandbox. The same name with another
  spec fails with `FAILED_PRECONDITION`. `Spec.KeepVM` keeps the
  sandbox's VM, powered off on its disk, while it is stopped; the
  workspace must allow kept VMs, or `Create` fails with
  `FAILED_PRECONDITION`.
- `WaitRunning` waits until the sandbox runs and reads it; it fails if
  the sandbox ended instead, and with `ErrStopped` at once if it is
  stopped. A sandbox an operator holds fails with `ErrHeld` instead: here,
  in `Execute`, and in a refused `Start`, `Stop` or `Delete`, its
  `HeldError` names the hold's reason, when it began and where to write,
  and `Unwrap` gives the server's refusal. It has no bound of its own, so
  give `ctx` a deadline.
- `Execute` runs a command, feeding `Stdin` and streaming its output, and
  cancels it with `CancelExecution` when `Timeout` passes; a zero
  `Timeout` sets no limit. `Argv[0]` is looked up on the guest's `PATH`,
  `Env` is added to the command's environment and `Cwd` is its working
  directory (empty: the guest's default). A nil `Stdin` closes standard
  input at once, and a nil `Stdout` or `Stderr` discards that stream. A
  non-zero exit is a result, not an error: `Result` holds `ExecutionID`,
  `ExitCode` (-1 when the command ended without one), `Cancelled` and
  `TimedOut`. A read of `Stdin` that fails before the command ended closes
  its input and fails `Execute`. On a stopped sandbox `Execute` fails
  with `ErrStopped` and runs nothing.
- `Stop` stops a sandbox created with `KeepVM` with `StopSandbox`: its
  run and every command in it end, and its VM is powered off with its
  disk kept. The server answers before the VM is off, so `Stop` then
  waits with `WaitSandbox`, bounded by its context, until the sandbox is
  stopped: once it returns, `Execute` fails with `ErrStopped`. `Start`
  queues a run on that VM with `StartSandbox`; then
  `WaitRunning` waits until it runs. Either fails with
  `FAILED_PRECONDITION` on a sandbox created without `KeepVM` or one that
  has ended. Each call's request key comes from the scope, the two names
  and the sandbox as last read, so a call repeated with the same `Sandbox`
  after a lost answer gets the first call's answer, and the next stop or
  start gets a key of its own. A call made after the sandbox was read
  again, as after a restart, is answered by its state: stopping a stopped
  sandbox, or starting a queued or running one, returns it as it is.
- The state label (`StateStarting`, `StateReady`, `StateBusy` and
  `StateStopped`) is `starting` after `Create` and `Start`, `ready` once
  `WaitRunning` has seen the sandbox run and between commands, `busy`
  while a command runs, and `stopped` after `Stop`. When `Execute` fails
  or its stream is lost, the driver cannot always tell whether the
  command started or still runs, so the label stays `busy`, and
  `WaitRunning` leaves it so. `State` reads the sandbox's status as well
  as its label: a kept sandbox Toshi stopped itself, for its spending cap
  or a suspension, is `stopped`, and one whose label still says `stopped`
  after a start is `starting`.
- `Recover` lists the sandboxes labelled with the driver's name with
  `Sandboxes.List`. Given the cursor saved by an earlier run, it reads the
  events after it with `ListEvents` and keeps the execution and emitted
  events of the driver's sandboxes, and the delete events of those deleted
  since. Each sandbox event carries the sandbox after its change, and
  `Recover` applies them in order to the list, so `rec.Sandboxes` holds
  the sandboxes as they are at `rec.Cursor`, including one created after
  the list was read and one stopped or started since. A sandbox is the driver's when it carries the
  driver's label. Given `""`, on a first start, it reads no events and
  returns the list's position. A cursor the server no longer keeps fails
  with `OUT_OF_RANGE`; start again with `""`.
- `Watch` follows from `rec.Cursor` with `Sandboxes.Watch` and
  `WatchEvents`, each resuming after a dropped connection, and calls the
  function with one update at a time. Neither call filters by label, so
  the driver keeps the changes of its sandboxes, and the events of those
  `Recover` returned or whose sandbox events in `WatchEvents`, which come
  before their executions', show them as its own. The two watches are not
  ordered with each other: an execution's event can come before the change
  that shows its sandbox. Only event updates carry a cursor; save it
  after the event is handled. The server delivers at least once, so after
  a restart an event can come again with the same `Event.Cursor`: handle it
  idempotently or drop the repeat. `Watch` returns when `ctx` ends, a
  watch ends or the function returns an error. A watch ends when
  toshi-go's reconnect attempts run out (its retry policy's `MaxAttempts`,
  five by default);
  then `Recover` from the saved cursor and `Watch` again.
- `Delete` deletes the sandbox with `Sandboxes.Delete`, under a request key
  from the scope and the two names, and returns the delete's operation;
  `WaitOperation` reads it with `GetOperation` and waits with
  `WaitOperation` until it is done, and sets its `Err` if the operation
  failed.

