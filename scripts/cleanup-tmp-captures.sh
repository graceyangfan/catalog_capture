#!/usr/bin/env bash
# Remove smoke/soak capture artifacts from a temp directory.
set -euo pipefail

TMP_ROOT="/tmp"
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage: scripts/cleanup-tmp-captures.sh [tmp_root] [--dry-run]

Deletes:
  - nautilus-catalog-capture-* directories
  - capture.*-universe-smoke.*.toml files

Defaults to /tmp. Pass --dry-run to list targets without deleting.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    *)
      TMP_ROOT="$1"
      shift
      ;;
  esac
done

if [[ ! -d "$TMP_ROOT" ]]; then
  echo "tmp root does not exist: $TMP_ROOT" >&2
  exit 1
fi

mapfile -t TARGETS < <(
  find "$TMP_ROOT" -maxdepth 1 \( \
    -name 'nautilus-catalog-capture-*' -o \
    -name 'capture.*-universe-smoke.*.toml' -o \
    -name 'capture.*-autorefresh-btc-universe-smoke.*.toml' \
  \) -print 2>/dev/null | sort
)

if [[ ${#TARGETS[@]} -eq 0 ]]; then
  echo "No capture artifacts found under $TMP_ROOT"
  exit 0
fi

echo "Found ${#TARGETS[@]} artifact(s) under $TMP_ROOT:"
printf '  %s\n' "${TARGETS[@]}"

if [[ $DRY_RUN -eq 1 ]]; then
  exit 0
fi

for target in "${TARGETS[@]}"; do
  rm -rf "$target"
done

echo "Cleanup complete."