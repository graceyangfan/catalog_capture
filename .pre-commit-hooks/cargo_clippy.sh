#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v cargo > /dev/null 2>&1; then
  echo "cargo not found; skipping clippy"
  exit 0
fi

if [[ ! -d ../nautilus_trader ]]; then
  echo "Skipping clippy: sibling dependency checkout not found at ../nautilus_trader"
  exit 0
fi

export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.97.1}"

# Product crates only (matches Makefile / CI).
cargo clippy \
  -p catalog-capture-core \
  -p catalog-capture-runtime-adapter \
  -p catalog-capture-cli \
  --all-targets \
  -- -D warnings
