#!/usr/bin/env bash
# Run a long-lived mainnet capture from the repo root (cloud / bare metal).
# Usage:
#   ./scripts/run-mainnet-capture.sh
#   CAPTURE_FEATURES=venue-hyperliquid \
#     ./scripts/run-mainnet-capture.sh examples/capture.hyperliquid-hip4-btc-daily.toml
#   CAPTURE_SECONDS=120 ./scripts/run-mainnet-capture.sh   # short verify run via temp config
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CONFIG="${1:-examples/capture.multi-venue-mainnet.toml}"
RUN_CONFIG="$CONFIG"

if [[ ! -f "$CONFIG" ]]; then
  echo "config not found: $CONFIG" >&2
  exit 1
fi
if [[ ! -d ../nautilus_trader_quantity_commission ]]; then
  make bootstrap-deps
fi

cleanup() {
  if [[ "$RUN_CONFIG" != "$CONFIG" ]]; then
    rm -f "$RUN_CONFIG"
  fi
}
trap cleanup EXIT

# Keep one build/validation/logging implementation in the foreground service helper.
if [[ -n "${CAPTURE_SECONDS:-}" ]]; then
  if [[ ! "$CAPTURE_SECONDS" =~ ^[0-9]+$ ]]; then
    echo "CAPTURE_SECONDS must be a non-negative integer" >&2
    exit 2
  fi
  RUN_CONFIG="$(mktemp "${TMPDIR:-/tmp}/capture-mainnet.XXXXXX.toml")"
  sed -E "s/^capture_seconds = .*/capture_seconds = ${CAPTURE_SECONDS}/" "$CONFIG" > "$RUN_CONFIG"
fi
"$ROOT/scripts/run-capture-service.sh" --config "$RUN_CONFIG" --release
