#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v cargo > /dev/null 2>&1; then
  echo "cargo not found; skipping clippy"
  exit 0
fi

if [[ ! -d ../nautilus_trader_quantity_commission ]]; then
  echo "Skipping clippy: sibling dependency checkout not found at ../nautilus_trader_quantity_commission"
  exit 0
fi

export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.99.0}"

# Product crates and the Predict adapter (matches Makefile / CI).
cargo clippy \
  -p nautilus-predict \
  -p catalog-capture-core \
  -p catalog-capture-runtime-adapter \
  -p catalog-capture-cli \
  --all-targets \
  -- -D warnings
