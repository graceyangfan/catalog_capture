#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo not found; skipping fmt check"
  exit 0
fi

TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.96.0}"
cargo "+${TOOLCHAIN}" fmt --all -- --check