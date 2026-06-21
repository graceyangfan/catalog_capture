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

TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.96.0}"
cargo "+${TOOLCHAIN}" clippy --workspace --all-targets -- -D warnings
