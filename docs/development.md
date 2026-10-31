# Development checks


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

