#!/usr/bin/env bash
# Fails when the module in the given directory (default: the current one),
# its tests included, depends on a github.com/toshicloud or go.toshicloud.io
# module other than itself, go.toshicloud.io/toshi and go.toshicloud.io/api when built
# for Linux or for Windows, or when its packages cannot be listed.
set -euo pipefail

cd "${1:-.}"

module="$(go list -m)"
allowed="^(${module//./\\.}(_test)?|go\.toshicloud\.io/toshi|go\.toshicloud\.io/api)(/|\.test( |\$)| |\$)"

for goos in linux windows; do
  if ! deps="$(GOOS="$goos" go list -deps -test ./...)"; then
    echo "module guard: listing the packages for GOOS=$goos failed" >&2
    exit 1
  fi
  if found="$(grep -E '^(github\.com/toshicloud/|go\.toshicloud\.io/)' <<<"$deps" | grep -vE "$allowed")"; then
    echo "module guard: for GOOS=$goos the module depends on a github.com/toshicloud or go.toshicloud.io module other than go.toshicloud.io/toshi and go.toshicloud.io/api; it must reach Toshi through go.toshicloud.io/toshi only:" >&2
    echo "$found" >&2
    exit 1
  fi
done
