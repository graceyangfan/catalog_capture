#!/usr/bin/env bash
# Remove local capture artifacts created by examples / smokes.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${1:-}"
DRY_RUN=0

usage() {
  cat << 'EOF'
Usage: scripts/cleanup-tmp-captures.sh [path] [--dry-run]

Default path: <repo>/data

Also accepts an explicit directory (e.g. /tmp for leftover legacy smokes).

Deletes under the target:
  - everything when target is the repo data/ dir (careful)
  - or, when target is /tmp: catalog-capture-* and legacy nautilus-catalog-capture-*
EOF
}

ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) ARGS+=("$1"); shift ;;
  esac
done

DIR="${ARGS[0]:-$ROOT/data}"

if [[ ! -d "$DIR" ]]; then
  echo "Nothing to clean (missing): $DIR"
  exit 0
fi

if [[ "$(cd "$DIR" && pwd)" == "$ROOT/data" ]]; then
  mapfile -t TARGETS < <(find "$DIR" -mindepth 1 -maxdepth 1 -print 2>/dev/null | sort)
else
  mapfile -t TARGETS < <(
    find "$DIR" -maxdepth 1 \( \
      -name 'catalog-capture-*' -o \
      -name 'nautilus-catalog-capture-*' -o \
      -name 'capture.*-universe-smoke.*.toml' \
      \) -print 2>/dev/null | sort
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
for t in "${TARGETS[@]}"; do
  rm -rf "$t"
done
echo "Cleanup complete."
