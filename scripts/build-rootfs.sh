#!/usr/bin/env bash
# Builds the python sandbox root filesystem into /var/lib/gss/rootfs/python (Linux only).
# The build ID in /var/lib/gss/rootfs/python.id is a sha256 over the Dockerfile, zygote.py
# and the pyfork binary; the build is skipped when it already matches, unless FORCE=1.
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CLI=${CONTAINER_CLI:-nerdctl}
GO=${GO:-go}
image=gss-python:latest
root=/var/lib/gss/rootfs
dest=$root/python
idfile=$root/python.id

if [[ "$(uname -s)" != Linux ]]; then
	echo "build-rootfs.sh: run this on Linux (make rootfs does that from macOS)" >&2
	exit 1
fi

cd "$repo"
mkdir -p bin
# -trimpath and -buildvcs=false keep the binary, and so the build ID, independent of
# where the repo is checked out and of uncommitted changes elsewhere in it.
CGO_ENABLED=0 "$GO" build -trimpath -buildvcs=false -o bin/pyfork ./cmd/pyfork

ctx=$(mktemp -d "$repo/bin/rootfs-context.XXXXXX")
container=""
new=""
cleanup() {
	if [[ -n "$container" ]]; then "$CLI" rm -f "$container" >/dev/null 2>&1 || true; fi
	if [[ -n "$new" ]]; then sudo rm -rf "$new"; fi # a partial export
	rm -rf "$ctx"
}
trap cleanup EXIT

cp images/python/Dockerfile images/python/zygote.py bin/pyfork "$ctx/"
# This script is hashed too: it shapes the tree (see the fixups below).
id=$(cat "$ctx/Dockerfile" "$ctx/zygote.py" "$ctx/pyfork" scripts/build-rootfs.sh | sha256sum | cut -d' ' -f1)

if [[ "${FORCE:-0}" != 1 ]] && sudo test -d "$dest" && [[ "$(sudo cat "$idfile" 2>/dev/null || true)" == "$id" ]]; then
	echo "rootfs $dest is up to date ($id)"
	exit 0
fi

if [[ "$(basename "$CLI")" == nerdctl ]]; then
	systemctl --user start buildkit || true
fi
"$CLI" build -t "$image" "$ctx"

sudo mkdir -p "$root"
new=$(sudo mktemp -d "$root/.python.new.XXXXXX")
container=$("$CLI" create "$image")
"$CLI" export "$container" | sudo tar -x -C "$new"
sudo chmod 0755 "$new"

# Mount points the sandboxes need must exist in the shared tree, so that starting
# a sandbox never has to create them (gVisor would create a missing bind-mount
# target inside the rootfs). The export leaves /etc/hosts as an empty placeholder.
sudo rm -rf "$new/etc/hosts"
printf '127.0.0.1 localhost\n' | sudo tee "$new/etc/hosts" >/dev/null
sudo chown root:root "$new/etc/hosts"
sudo chmod 0644 "$new/etc/hosts"
sudo mkdir -p "$new/work"
sudo chown root:root "$new/work"
sudo chmod 0755 "$new/work"

# Swap in the new tree: rename the old one aside, rename the new one into place.
# mv -T never moves a directory into an existing one.
old=""
if sudo test -e "$dest"; then
	old="$root/.python.old.$$"
	sudo mv -T "$dest" "$old"
fi
sudo mv -T "$new" "$dest"
new=""
echo "$id" | sudo tee "$idfile.tmp" >/dev/null
sudo mv -T "$idfile.tmp" "$idfile"
if [[ -n "$old" ]]; then sudo rm -rf "$old"; fi
echo "rootfs $dest built ($id)"
