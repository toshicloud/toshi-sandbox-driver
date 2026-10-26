#!/usr/bin/env bash
# Runs scripts/module-guard.sh on small modules that import a stand-in for
# another Toshi module: it must pass a module without the import and a module
# that imports the client, and fail one that imports any other github or
# vanity Toshi module from a test file or a Windows-only file, or that
# cannot be listed.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
guard="$root/scripts/module-guard.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
export GOWORK=off GOFLAGS=

# fixture creates a module named $1 that replaces the module $2 (default
# github.com/toshicloud/other) with a local stand-in, and prints its
# directory. $3 names the fixture's own module (default example.com/fixture).
fixture() {
  local dir="$tmp/$1" other="${2:-github.com/toshicloud/other}" self="${3:-example.com/fixture}"
  mkdir -p "$dir/other/probe"
  printf 'module %s\n\ngo 1.25.0\n' "$other" >"$dir/other/go.mod"
  printf 'package probe\n' >"$dir/other/probe/probe.go"
  printf 'module %s\n\ngo 1.25.0\n\nrequire %s v0.0.0\n\nreplace %s => ./other\n' "$self" "$other" "$other" >"$dir/go.mod"
  printf 'package fixture\n' >"$dir/fixture.go"
  echo "$dir"
}

# expect runs the guard on $2 and checks that it exits with status $1 and,
# when $3 is given, that its output names it.
expect() {
  local want="$1" dir="$2" text="${3:-}" got=0 out
  out="$("$guard" "$dir" 2>&1)" || got=$?
  if [ "$want" = 0 ] && [ "$got" != 0 ]; then
    echo "module guard test: $(basename "$dir"): want a pass, got status $got: $out" >&2
    exit 1
  fi
  if [ "$want" != 0 ] && [ "$got" = 0 ]; then
    echo "module guard test: $(basename "$dir"): want a failure, got a pass" >&2
    exit 1
  fi
  if [ -n "$text" ] && ! grep -qF "$text" <<<"$out"; then
    echo "module guard test: $(basename "$dir"): output lacks \"$text\": $out" >&2
    exit 1
  fi
}

refused='the module depends on a github.com/toshicloud or go.toshicloud.io module other than go.toshicloud.io/toshi and go.toshicloud.io/api'
import='import _ "github.com/toshicloud/other/probe"'

dir="$(fixture clean)"
expect 0 "$dir"

dir="$(fixture own-test-package github.com/toshicloud/other github.com/toshicloud/self)"
printf 'package fixture_test\n' >"$dir/fixture_test.go"
expect 0 "$dir"

dir="$(fixture test-import)"
printf 'package fixture\n\n%s\n' "$import" >"$dir/fixture_test.go"
expect 1 "$dir" "GOOS=linux $refused"

dir="$(fixture windows-import)"
printf 'package fixture\n\n%s\n' "$import" >"$dir/fixture_windows.go"
expect 1 "$dir" "GOOS=windows $refused"

dir="$(fixture test-suffix-module go.toshicloud.io/api.test-helper)"
printf 'package fixture\n\nimport _ "go.toshicloud.io/api.test-helper/probe"\n' >"$dir/fixture.go"
expect 1 "$dir" "GOOS=linux $refused"

dir="$(fixture vanity-import go.toshicloud.io/other)"
printf 'package fixture\n\nimport _ "go.toshicloud.io/other/probe"\n' >"$dir/fixture_test.go"
expect 1 "$dir" "GOOS=linux $refused"

dir="$(fixture stale-client-import github.com/toshicloud/toshi-go)"
printf 'package fixture\n\nimport _ "github.com/toshicloud/toshi-go/probe"\n' >"$dir/fixture.go"
expect 1 "$dir" "GOOS=linux $refused"

dir="$(fixture client-import go.toshicloud.io/toshi)"
printf 'package fixture\n\nimport _ "go.toshicloud.io/toshi/probe"\n' >"$dir/fixture.go"
expect 0 "$dir"

dir="$(fixture listing-error)"
printf 'package fixture\n\nimport _ "example.com/missing"\n' >"$dir/missing.go"
expect 1 "$dir" "listing the packages for GOOS=linux failed"

echo "module guard test: ok"
