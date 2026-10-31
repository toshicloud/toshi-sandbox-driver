<p align="center">
  <img src="docs/assets/toshi-logo.svg" alt="Toshi" width="120">
</p>

<h1 align="center">toshi-sandbox-driver</h1>

<p align="center">Run sandboxes on Toshi from an agent orchestrator.</p>

<p align="center">
  <img alt="licence" src="https://img.shields.io/badge/licence-Apache--2.0-blue">
  <img alt="go" src="https://img.shields.io/badge/go-1.26%2B-00ADD8">
  <img alt="status" src="https://img.shields.io/badge/status-early-orange">
</p>

<p align="center">
  <a href="docs/usage.md">Get started</a> ·
  <a href="docs/usage.md">Docs</a> ·
  <a href="https://github.com/toshicloud/toshi-sandbox-driver/issues">Issues</a>
</p>

A Go package for an agent orchestrator that needs sandboxes: it creates a Toshi sandbox, runs commands in it with standard input and streamed output, stops and starts it when its VM is kept, recovers its sandboxes after a restart, and deletes it. It uses only the public Workspace API, through the [Toshi Go client](https://pkg.go.dev/go.toshicloud.io/toshi).

> **Early stage.** v0.1.0 is tagged; `main` is ahead of it with stop and start of a sandbox whose VM is kept.
> Interfaces may change before 1.0.
> Read the [release notes](https://github.com/toshicloud/toshi-sandbox-driver/releases) when upgrading.

## Quick start

Set `TOSHI_SERVER`, `TOSHI_TOKEN`, `TOSHI_PROJECT`, `TOSHI_ENV` and `TOSHI_DRIVER_NAME`, then create a driver, run a command in a sandbox and delete it:

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
op, err = d.WaitOperation(ctx, op.ID)
if err != nil {
	log.Fatal(err)
}
```

The [usage guide](docs/usage.md) continues with stop and start, recovery after a restart and watching.

## Documentation

- [Usage](docs/usage.md): every call, in the order a sandbox lives
- [Types](docs/types.md): `Sandbox`, `Operation`, `Event` and the rest
- [Configuration](docs/configuration.md): the environment variables
- [Upgrading and versions](docs/upgrading.md)
- [End-to-end test](docs/e2e.md) and [development checks](docs/development.md)
- [Package reference](https://pkg.go.dev/github.com/toshicloud/toshi-sandbox-driver) on pkg.go.dev
- [The Toshi API](https://github.com/toshicloud/toshi-api) the driver calls
- [Security](SECURITY.md) · [Licence](LICENSE)

## Status and support

Issues and questions go to [GitHub issues](https://github.com/toshicloud/toshi-sandbox-driver/issues).
Security reports follow [SECURITY.md](SECURITY.md).

Apache-2.0
