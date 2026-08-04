#!/usr/bin/env bash
# Long-lived capture with logging. Run from anywhere; resolves repo root.
#
# Usage:
#   ./scripts/run-capture-service.sh --config examples/capture.multi-venue-mainnet.toml --release
#
# Env:
#   CAPTURE_FEATURES  --no-default-features --features … (release only)
#                     default when --release: venue-binance,venue-deribit,venue-hyperliquid
#   RUSTUP_TOOLCHAIN  default 1.97.1
#   CATALOG_CAPTURE_LOG_DIR  default <repo>/logs
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CONFIG=""
RELEASE=0
VALIDATE=0
LOG_DIR="${CATALOG_CAPTURE_LOG_DIR:-$ROOT/logs}"
CARGO="${CARGO:-cargo}"
TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.97.1}"
CAPTURE_FEATURES="${CAPTURE_FEATURES:-venue-binance,venue-deribit,venue-hyperliquid}"

usage() {
  cat << 'EOF'
Usage: scripts/run-capture-service.sh --config <path> [options]

Options:
  --config <path>     Required TOML config (capture_seconds=0 for daemons)
  --release           Build/run release binary (slim features via CAPTURE_FEATURES)
  --all-venues        With --release: build default all-venues features
  --validate          Run validate-option-universe after a successful capture
  --log-dir <path>    Log directory (default: ./logs)
  -h, --help          Show this help

Environment:
  CAPTURE_FEATURES    Comma features for slim release (default multi-venue set)
  CARGO               Cargo executable
  RUSTUP_TOOLCHAIN    Rust toolchain (default 1.97.1)
  CATALOG_CAPTURE_LOG_DIR
EOF
}

ALL_VENUES=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG="$2"
      shift 2
      ;;
    --release)
      RELEASE=1
      shift
      ;;
    --all-venues)
      ALL_VENUES=1
      shift
      ;;
    --validate)
      VALIDATE=1
      shift
      ;;
    --log-dir)
      LOG_DIR="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$CONFIG" ]]; then
  echo "--config is required" >&2
  usage >&2
  exit 2
fi

if [[ ! -f "$CONFIG" ]]; then
  echo "Config not found: $CONFIG" >&2
  exit 1
fi

mkdir -p "$LOG_DIR" data
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
CONFIG_BASENAME="$(basename "$CONFIG" .toml)"
LOG_FILE="$LOG_DIR/${CONFIG_BASENAME}-${STAMP}.log"

if [[ $RELEASE -eq 1 ]]; then
  if [[ $ALL_VENUES -eq 1 ]]; then
    echo "Building release (all-venues)..." | tee -a "$LOG_FILE"
    "$CARGO" "+$TOOLCHAIN" build --release -p catalog-capture-cli
  else
    echo "Building release (features=${CAPTURE_FEATURES})..." | tee -a "$LOG_FILE"
    "$CARGO" "+$TOOLCHAIN" build --release -p catalog-capture-cli \
      --no-default-features \
      --features "${CAPTURE_FEATURES}"
  fi
  BIN="$ROOT/target/release/catalog-capture-cli"
else
  BIN="$ROOT/target/debug/catalog-capture-cli"
  if [[ ! -x "$BIN" ]]; then
    "$CARGO" "+$TOOLCHAIN" build -p catalog-capture-cli
  fi
fi

echo "Starting capture: config=$CONFIG log=$LOG_FILE" | tee -a "$LOG_FILE"
set +e
"$BIN" run --config "$CONFIG" 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}
set -e

if [[ $EXIT_CODE -ne 0 ]]; then
  echo "Capture failed with exit code $EXIT_CODE" | tee -a "$LOG_FILE"
  exit "$EXIT_CODE"
fi

if [[ $VALIDATE -eq 1 ]]; then
  CATALOG_URI="$(
    awk -F'"' '/^catalog_uri = / { print $2; exit }' "$CONFIG"
  )"
  if [[ -z "$CATALOG_URI" ]]; then
    echo "Could not read catalog_uri from config; skipping validation" | tee -a "$LOG_FILE"
    exit 0
  fi
  echo "Running validate-option-universe for $CATALOG_URI" | tee -a "$LOG_FILE"
  set +e
  "$BIN" validate-option-universe \
    --catalog-uri "$CATALOG_URI" \
    --config "$CONFIG" \
    --option-universe-format text 2>&1 | tee -a "$LOG_FILE"
  VALIDATE_EXIT=${PIPESTATUS[0]}
  set -e
  if [[ $VALIDATE_EXIT -ne 0 ]]; then
    echo "Post-run validation failed with exit code $VALIDATE_EXIT" | tee -a "$LOG_FILE"
    exit "$VALIDATE_EXIT"
  fi
fi

echo "Capture service run completed successfully" | tee -a "$LOG_FILE"
exit 0
