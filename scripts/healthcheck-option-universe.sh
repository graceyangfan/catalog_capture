#!/usr/bin/env bash
# Lightweight health check for a running or recently stopped option-universe catalog.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG=""
CATALOG_URI=""
CARGO="${CARGO:-cargo}"
TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.96.0}"

usage() {
  cat <<'EOF'
Usage: scripts/healthcheck-option-universe.sh --config <path> [--catalog-uri <uri>]

Runs validate-option-universe-metadata against the catalog referenced by the config.
Use from cron or a process supervisor to detect lineage/metadata regressions.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG="$2"
      shift 2
      ;;
    --catalog-uri)
      CATALOG_URI="$2"
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
  exit 2
fi

if [[ -z "$CATALOG_URI" ]]; then
  CATALOG_URI="$(awk -F'"' '/^catalog_uri = / { print $2; exit }' "$CONFIG")"
fi

if [[ -z "$CATALOG_URI" ]]; then
  echo "catalog_uri is required" >&2
  exit 2
fi

BIN="$ROOT/target/release/catalog-capture-cli"
if [[ ! -x "$BIN" ]]; then
  BIN="$ROOT/target/debug/catalog-capture-cli"
fi
if [[ ! -x "$BIN" ]]; then
  "$CARGO" "+$TOOLCHAIN" build -p catalog-capture-cli
  BIN="$ROOT/target/debug/catalog-capture-cli"
fi

exec "$BIN" validate-option-universe-metadata \
  --catalog-uri "$CATALOG_URI" \
  --config "$CONFIG" \
  --option-universe-format text