#!/usr/bin/env bash
# Compatibility wrapper; prefer the local `fmt` hook entrypoint.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.97.1}"
exec bash .pre-commit-hooks/cargo_fmt_stable.sh --all -- --check
