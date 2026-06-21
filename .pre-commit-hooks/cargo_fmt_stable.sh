#!/usr/bin/env bash
set -euo pipefail

# Run cargo fmt while forcing rustfmt to read an empty config
# to avoid nightly-only options in the repository rustfmt.toml.

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
  cargo fmt "$@" --config-path "$tmpdir"
else
  cargo fmt "$@" -- --config-path "$tmpdir"
fi