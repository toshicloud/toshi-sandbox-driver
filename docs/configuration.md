# Configuration


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

