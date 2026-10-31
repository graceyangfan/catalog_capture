#!/usr/bin/env bash
# Remove local capture artifacts created by examples / smokes.
# Portable (no mapfile; works on macOS bash 3.x).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DRY_RUN=0

usage() {
  cat << 'EOF'
Usage: scripts/cleanup-tmp-captures.sh [path] [--dry-run]

Default path: <repo>/data

Also accepts an explicit directory (e.g. /tmp for leftover smokes).

Deletes under the target:
  - everything when target is the repo data/ dir (careful)
  - or, when target is /tmp: catalog-capture-*, verify-*, legacy names, capture.*.toml smokes
EOF
}

ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      usage
      exit 0
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

DIR="${ARGS[0]:-$ROOT/data}"

if [[ ! -d "$DIR" ]]; then
  echo "Nothing to clean (missing): $DIR"
  exit 0
fi

TARGETS=()
if [[ "$(cd "$DIR" && pwd)" == "$ROOT/data" ]]; then
  while IFS= read -r line; do
    [[ -n "$line" ]] && TARGETS+=("$line")
  done < <(find "$DIR" -mindepth 1 -maxdepth 1 -print 2> /dev/null | sort)
else
  while IFS= read -r line; do
    [[ -n "$line" ]] && TARGETS+=("$line")
  done < <(
    find "$DIR" -maxdepth 1 \( \
      -name 'catalog-capture-*' -o \
      -name 'nautilus-catalog-capture-*' -o \
      -name 'verify-*' -o \
      -name 'capture.verify-*' -o \
      -name 'capture.*-universe-smoke.*.toml' -o \
      -name 'capture.hyperliquid-hip4-smoke.*.toml' \
      \) -print 2> /dev/null | sort
  )
fi

if [[ ${#TARGETS[@]} -eq 0 ]]; then
  echo "No capture artifacts under $DIR"
  exit 0
fi

echo "Found ${#TARGETS[@]} under $DIR:"
printf '  %s\n' "${TARGETS[@]}"
if [[ $DRY_RUN -eq 1 ]]; then
  exit 0
fi
# A part can be open in a running recorder or await crash recovery. Never unlink it
# through the smoke cleanup helper; seal/recover it before deleting its parent.
for t in "${TARGETS[@]}"; do
  if [[ -n "$(find "$t" -type f -name '*.part' -print -quit 2> /dev/null)" ]]; then
    echo "Refusing cleanup: unsealed part under $t; stop and seal/recover the recorder first." >&2
    exit 1
  fi
done
for t in "${TARGETS[@]}"; do
  rm -rf "$t"
done
echo "Cleanup complete."
