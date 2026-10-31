#!/usr/bin/env bash
# Builds toshi at the commit e2e/pin.env names and runs the end-to-end test
# against it. TOSHI_DRIVER_E2E_IMAGE_DIR and TOSHI_DRIVER_E2E_IMAGE must name
# a guest image that commit's agent runs; the host needs KVM.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=e2e/pin.env
source "$root/e2e/pin.env"

: "${TOSHI_DRIVER_E2E_IMAGE_DIR:?set TOSHI_DRIVER_E2E_IMAGE_DIR to the directory holding the guest image}"
: "${TOSHI_DRIVER_E2E_IMAGE:?set TOSHI_DRIVER_E2E_IMAGE to the image name, without .qcow2}"

: "${TOSHI_SERVER_REPOSITORY:?set TOSHI_SERVER_REPOSITORY to the Toshi server repository}"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

git clone --quiet "$TOSHI_SERVER_REPOSITORY" "$tmp/server"
git -C "$tmp/server" checkout --quiet --detach "$TOSHI_SERVER_REF"
(
  cd "$tmp/server"
  go build -o "$tmp/toshi" ./cmd/toshi
)

test=(go test -count=1 -v -timeout 15m ./e2e/...)
# The stack's VMs have no cgroup of their own, so on a host that runs Toshi's
# network helper the agent refuses their network rules. With
# TOSHI_DRIVER_E2E_HIDE_NETWORK_HELPER=1 the test runs in a user and mount
# namespace in which the helper's socket is /dev/null, and the agent writes no
# network rules.
if [ "${TOSHI_DRIVER_E2E_HIDE_NETWORK_HELPER:-}" = 1 ]; then
  test=(unshare --map-root-user --mount sh -c \
    'mount --bind /dev/null /run/toshi-agent-netsets.sock && exec "$@"' sh "${test[@]}")
fi

cd "$root"
echo "serving Toshi $TOSHI_SERVER_REF from $tmp/toshi"
TOSHI_DRIVER_E2E_BIN="$tmp/toshi" \
TOSHI_DRIVER_E2E_EXPECT_COMMIT="$TOSHI_SERVER_REF" \
  "${test[@]}"
