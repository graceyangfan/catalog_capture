#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -d ../nautilus_trader_quantity_commission ]]; then
  echo "Skipping clippy: sibling dependency checkout not found at ../nautilus_trader_quantity_commission"
  exit 0
fi

export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.99.1}"

# Product crates and local adapters (matches Makefile / CI).
"$ROOT/scripts/cargo.sh" clippy \
  -p nautilus-lighter-ext \
  -p nautilus-predict \
  -p catalog-capture-core \
  -p catalog-capture-runtime-adapter \
  -p catalog-capture-cli \
  --all-targets \
  --locked \
  -- -D warnings
