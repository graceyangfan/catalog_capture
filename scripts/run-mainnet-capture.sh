#!/usr/bin/env bash
# Run a long-lived mainnet capture from the repo root (cloud / bare metal).
# Usage:
#   ./scripts/run-mainnet-capture.sh
#   ./scripts/run-mainnet-capture.sh examples/capture.hyperliquid-hip4-btc-daily.toml
#   CAPTURE_SECONDS=120 ./scripts/run-mainnet-capture.sh   # short verify run via temp config
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CONFIG="${1:-examples/capture.multi-venue-mainnet.toml}"
LOG_DIR="${CATALOG_CAPTURE_LOG_DIR:-$ROOT/logs}"
RUSTUP_TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.97.1}"
export RUSTUP_TOOLCHAIN

if [[ ! -f "$CONFIG" ]]; then
  echo "config not found: $CONFIG" >&2
  exit 1
fi

mkdir -p "$LOG_DIR" data

if [[ ! -d ../nautilus_trader ]]; then
  echo "bootstrap sibling nautilus_trader..."
  make bootstrap-deps
fi

echo "building release catalog-capture-cli..."
cargo build --release -p catalog-capture-cli

BIN="$ROOT/target/release/catalog-capture-cli"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BASENAME="$(basename "$CONFIG" .toml)"
LOG_FILE="$LOG_DIR/${BASENAME}-${STAMP}.log"

echo "validate: $CONFIG"
"$BIN" validate --config "$CONFIG"

# Optional short run: CAPTURE_SECONDS=N rewrites a temp config with finite duration.
RUN_CONFIG="$CONFIG"
if [[ -n "${CAPTURE_SECONDS:-}" ]]; then
  RUN_CONFIG="$(mktemp "${TMPDIR:-/tmp}/capture-mainnet.XXXXXX.toml")"
  # shellcheck disable=SC2016
  sed -E "s/^capture_seconds = .*/capture_seconds = ${CAPTURE_SECONDS}/" "$CONFIG" >"$RUN_CONFIG"
  echo "temporary config (capture_seconds=${CAPTURE_SECONDS}): $RUN_CONFIG"
fi

echo "catalog data under: $ROOT/data/"
echo "log: $LOG_FILE"
echo "metrics (if enabled in TOML): http://127.0.0.1:9108/metrics"
echo "stop: Ctrl+C or kill -TERM <pid>"

set +e
"$BIN" run --config "$RUN_CONFIG" 2>&1 | tee -a "$LOG_FILE"
RC=${PIPESTATUS[0]}
set -e

if [[ "$RUN_CONFIG" != "$CONFIG" ]]; then
  rm -f "$RUN_CONFIG"
fi

exit "$RC"
