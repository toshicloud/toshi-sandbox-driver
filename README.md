# toshi-sandbox-driver

A Go program that runs sandboxes on Toshi for an agent orchestrator. It
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
8. deletes the sandbox and follows the delete operation.

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

<!-- ExampleDriver_Delete -->
```go
op, err := d.Delete(ctx, sb)
if err != nil {
	log.Fatal(err)
}
op, err = d.WaitOperation(ctx, op.GetId())
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

- `Check` lists the projects the token sees and fails unless the
  configured project has the configured environment.
- `Create` creates a sandbox with three labels: `toshi-sandbox-driver`
  (the driver's name), `toshi-sandbox-driver/sandbox` (the sandbox's
  name) and `toshi-sandbox-driver/state`. `MaxDuration` is required: a
  whole number of minutes, at least one. The request key comes from the
  project, the environment and the two names, so creating the same name
  again with the same spec, after a lost answer or a restart, returns the
  first sandbox: a name is for one sandbox. The same name with another
  spec fails with `FAILED_PRECONDITION`.
- `WaitRunning` waits until the sandbox runs and reads it; it fails if
  the sandbox ended instead. It has no bound of its own, so give `ctx` a
  deadline.
- `Execute` runs a command, feeding `Stdin` and streaming its output, and
  cancels it with `CancelExecution` when `Timeout` passes. A non-zero
  exit is a result, not an error. A read of `Stdin` that fails before the
  command ended closes its input and fails `Execute`.
- The state label is `starting` after `Create`, `ready` once
  `WaitRunning` has seen the sandbox run and between commands, and `busy`
  while a command runs. When `Execute` fails or its stream is lost, the
  driver cannot always tell whether the command started or still runs, so
  the label stays `busy`, and `WaitRunning` leaves it so.
- `Recover` lists the sandboxes labelled with the driver's name with
  `Sandboxes.List`. Given the cursor saved by an earlier run, it reads the
  events after it with `ListEvents` and keeps the execution and emitted
  events of the driver's sandboxes, and the delete events of those deleted
  since. Each sandbox event carries the sandbox after its change, and
  `Recover` applies them in order to the list, so `rec.Sandboxes` holds
  the sandboxes as they are at `rec.Cursor`, including one created after
  the list was read. A sandbox is the driver's when it carries the
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
  a restart an event can come again with the same `Event.Id`: handle it
  idempotently or drop the repeat. `Watch` returns when `ctx` ends, a
  watch ends or the function returns an error. A watch ends when
  toshi-go's reconnect attempts run out (its retry policy's `MaxAttempts`,
  five by default);
  then `Recover` from the saved cursor and `Watch` again.
- `Delete` deletes the sandbox with `Sandboxes.Delete`, under a request key
  from the scope and the two names, and returns the delete's operation;
  `WaitOperation` reads it with `GetOperation` and waits with
  `WaitOperation` until it is done, and fails if the operation failed.

## Configuration

Everything comes from the environment.

| Variable | Meaning |
| --- | --- |
| `TOSHI_SERVER` | The control plane's base URL, for example `https://toshi.example.com`. |
| `TOSHI_TOKEN` | A workspace API token. |
| `TOSHI_TOKEN_FILE` | A file holding the token, read when `TOSHI_TOKEN` is not set. |
| `TOSHI_CA_FILE` | Optional. A PEM bundle to trust besides the system's roots. |
| `TOSHI_PROJECT` | The project the sandboxes run in, for example `default`. Required. |
| `TOSHI_ENV` | One of the project's environments, for example `production`. Required. |
| `TOSHI_DRIVER_NAME` | This driver's name, unique among the drivers of the environment. Required. |

The server, the token and the CA bundle are read by toshi-go. The API
has no default project or environment, and a default driver name would
let two drivers take each other's sandboxes, so the driver refuses to
start without all three. Running commands needs HTTP/2, which the server
offers over https.

## Checks

```sh
scripts/check.sh
```

It fails if the module's packages or their tests depend on a
`github.com/toshicloud` or `go.toshicloud.io` module other than
`go.toshicloud.io/toshi` and `go.toshicloud.io/api` when built for Linux or for
Windows, or if they cannot be listed
(`scripts/module-guard.sh`, itself tested by
`scripts/module-guard-test.sh`). Then it builds the module for the host
and for Windows, runs `go vet`, golangci-lint (version pinned in the
script, configuration in `.golangci.yml`) and the unit tests.

## End-to-end test

`e2e/` runs steps 1 to 8 against a served Toshi, a second driver
recovering the first one's sandbox from a saved cursor, and fails unless
the driver made `ListProjects` and every `WorkloadService` call but
`WatchOutput`. The served Toshi is `toshi serve` with its
all-in-one agent, which runs the sandbox in a qemu VM. It needs KVM and
a guest image, so it skips itself unless `TOSHI_DRIVER_E2E_BIN` is set,
and CI does not set it.

```sh
TOSHI_SERVER_REPOSITORY=<repository URL> \
TOSHI_DRIVER_E2E_IMAGE_DIR=/path/to/images \
TOSHI_DRIVER_E2E_IMAGE=ubuntu-24.04-<version> \
  scripts/e2e.sh
```

The script clones the server repository that `TOSHI_SERVER_REPOSITORY`
names at the commit `e2e/pin.env` names, so it needs read access to that
repository, then builds toshi and runs
`go test ./e2e/...` against it. The image is `<dir>/<image>.qcow2`, built
by that commit's `images/` tooling. To serve a toshi binary of your own,
set `TOSHI_DRIVER_E2E_BIN` with the two image variables and run
`go test -v ./e2e/...`. `TOSHI_DRIVER_E2E_ACCEL` changes the qemu
accelerator (default `kvm`), and `TOSHI_DRIVER_E2E_LOG_DIR` keeps the
stack's logs.

## Versions

`go.mod` pins the client and `go.toshicloud.io/api` to one version each. Moving either
to another version is one pull request that changes `go.mod` and
`go.sum`.

## Licence

Apache License 2.0, see LICENSE.
