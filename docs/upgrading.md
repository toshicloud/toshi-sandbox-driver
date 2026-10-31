# Upgrading and versions


The exported types no longer name the API's generated messages.
`Sandbox`'s `Sandbox` field is gone: read a sandbox with `ID`, `State`,
`Labels` and `Proto`. `Delete` and `WaitOperation` return an `Operation`,
so the ID is `Operation.ID` and a failed operation sets `Operation.Err`
instead of returning an error. `Recovery.Events` is `[]*Event` and
`Update.Type` is an `EventType`; an event's generated fields are the
driver's own, with `Execution` and `Emitted` holding the typed payload.
`Proto()` returns the generated message where a field the driver does not
carry is needed.



`go.mod` pins the client and `go.toshicloud.io/api` to one version each:
`go.toshicloud.io/toshi` v0.6.0 and `go.toshicloud.io/api` v0.5.0. Moving
either to another version is one pull request that changes `go.mod` and
`go.sum`.

