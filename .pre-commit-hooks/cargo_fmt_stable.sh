#!/usr/bin/env bash
set -euo pipefail

# Run cargo fmt while forcing rustfmt to read an empty config
# to avoid nightly-only options in the repository rustfmt.toml.
# (Same pattern as nautilus_trader.)

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.99.1}"

tmpdir="$(mktemp -d)"
cleanup() { rm -rf "$tmpdir"; }
trap cleanup EXIT

touch "$tmpdir/rustfmt.toml"

has_dd=false
for arg in "$@"; do
  if [[ "$arg" == "--" ]]; then
    has_dd=true
    break
  fi
done

if $has_dd; then
  "$ROOT/scripts/cargo.sh" fmt "$@" --config-path "$tmpdir"
else
  "$ROOT/scripts/cargo.sh" fmt "$@" -- --config-path "$tmpdir"
fi
