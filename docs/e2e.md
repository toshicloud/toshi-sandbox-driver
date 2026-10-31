# End-to-end test


`e2e/` runs steps 1 to 9 against a served Toshi, a second driver
recovering the first one's sandbox from a saved cursor, and fails unless
the driver made `ListProjects` and the 15 `WorkloadService` calls it
uses: the sandbox calls `CreateSandbox`, `WaitSandbox`, `GetSandbox`,
`UpdateSandbox`, `ListSandboxes`, `WatchSandboxes`, `StopSandbox`,
`StartSandbox` and `DeleteSandbox`; the execution calls `Execute` and
`CancelExecution`; the event calls `ListEvents` and `WatchEvents`; and
the operation calls `GetOperation` and `WaitOperation`. The served Toshi
is `toshi serve` with its all-in-one agent, which runs each sandbox in a
qemu VM. The operator grants the workspace one kept VM and turns its
kept-VM setting on, and the agent enrolls with the workspace's own
enrollment token, so it is a host of the workspace's own that keeps VMs.
Step 8 creates a sandbox with `KeepVM`, writes a file in it, stops it,
gets `ErrStopped` from `Execute`, starts it again and reads the file
back. It needs KVM and a guest image, so it skips itself unless
`TOSHI_DRIVER_E2E_BIN` is set, and CI does not set it.

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
with `ubuntu-24.04/build.sh` in the
[toshi-images](https://github.com/toshicloud/toshi-images) repository and
installed with its `install.sh`. To serve a toshi binary of your own,
set `TOSHI_DRIVER_E2E_BIN` with the two image variables and run
`go test -v ./e2e/...`. `TOSHI_DRIVER_E2E_ACCEL` changes the qemu
accelerator (default `kvm`), and `TOSHI_DRIVER_E2E_LOG_DIR` keeps the
stack's logs.

On a host that runs Toshi's network helper (its socket is
`/run/toshi-agent-netsets.sock`), the agent writes each VM's network
rules in the VM's own cgroup, and the stack gives its VMs none, so the
test stops at once. With `TOSHI_DRIVER_E2E_HIDE_NETWORK_HELPER=1`, the
script runs the test in a user and mount namespace in which that socket
is `/dev/null`, and the agent writes no network rules.

