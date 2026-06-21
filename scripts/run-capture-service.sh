#!/usr/bin/env bash
# Run a long-lived capture session with logging and optional post-run validation.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG=""
RELEASE=0
VALIDATE=0
LOG_DIR="${CATALOG_CAPTURE_LOG_DIR:-$ROOT/logs}"
CARGO="${CARGO:-cargo}"
TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.96.0}"

usage() {
  cat <<'EOF'
Usage: scripts/run-capture-service.sh --config <path> [options]

Options:
  --config <path>     Required TOML config (use capture_seconds=0 for daemons)
  --release           Build and run the release binary
  --validate          Run validate-option-universe after a successful capture
  --log-dir <path>    Log directory (default: ./logs or $CATALOG_CAPTURE_LOG_DIR)
  -h, --help          Show this help

Environment:
  CARGO               Cargo executable (default: cargo)
  RUSTUP_TOOLCHAIN    Rust toolchain channel (default: 1.96.0)
  CATALOG_CAPTURE_LOG_DIR  Override default log directory
EOF
}

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
    --validate)
      VALIDATE=1
      shift
      ;;
    --log-dir)
      LOG_DIR="$2"
      shift 2
      ;;
    -h|--help)
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

mkdir -p "$LOG_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
CONFIG_BASENAME="$(basename "$CONFIG" .toml)"
LOG_FILE="$LOG_DIR/${CONFIG_BASENAME}-${STAMP}.log"

if [[ $RELEASE -eq 1 ]]; then
  "$CARGO" "+$TOOLCHAIN" build --release -p catalog-capture-cli
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