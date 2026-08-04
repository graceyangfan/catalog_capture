#!/usr/bin/env bash
# -------------------------------------------------------------------------------------------------
#  Bootstrap the sibling nautilus_trader path dependency for this workspace.
#
#  Policy:
#    1. Prefer an existing local checkout (NAUTILUS_TRADER_PATH or ../nautilus_trader).
#    2. If missing, clone https://github.com/nautechsystems/nautilus_trader (default: develop).
#
#  Cargo.toml path deps always expect ../nautilus_trader relative to this repo. If you point
#  NAUTILUS_TRADER_PATH elsewhere, this script creates/updates a sibling symlink so builds work.
#
#  Usage:
#    ./scripts/bootstrap-deps.sh
#    ./scripts/bootstrap-deps.sh --pin-ci          # after resolve, checkout CI pin rev
#    NAUTILUS_TRADER_PATH=/other/path ./scripts/bootstrap-deps.sh
#    NAUTILUS_TRADER_REF=<sha> ./scripts/bootstrap-deps.sh --pin-ci
# -------------------------------------------------------------------------------------------------
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SIBLING_PATH="$(cd "${REPO_ROOT}/.." && pwd)/nautilus_trader"

UPSTREAM_URL="${NAUTILUS_TRADER_URL:-https://github.com/nautechsystems/nautilus_trader.git}"
CLONE_BRANCH="${NAUTILUS_TRADER_BRANCH:-develop}"

# Keep default pin in sync with .github/workflows/ci.yml (NAUTILUS_TRADER_REF).
CI_PIN_DEFAULT="a7159b484e816a8b73388ff58db71de454253222"
NAUTILUS_TRADER_REF="${NAUTILUS_TRADER_REF:-${CI_PIN_DEFAULT}}"

PIN_CI=0
SKIP_VERIFY=0
FORCE_CLONE=0

usage() {
  cat <<'EOF'
bootstrap-deps.sh — prepare sibling nautilus_trader for catalog-capture builds

  Prefer local checkout; clone upstream develop only when missing.

Options:
  --pin-ci       Checkout NAUTILUS_TRADER_REF (default: CI pin) after resolve
  --force-clone  Remove empty/broken target and clone fresh (never deletes a valid git repo
                 unless it is only a broken symlink)
  --skip-verify  Do not run cargo check -p catalog-capture-core
  -h, --help     Show this help

Environment:
  NAUTILUS_TRADER_PATH    Existing checkout to use (symlink to sibling if needed)
  NAUTILUS_TRADER_URL     Clone URL (default: nautechsystems/nautilus_trader)
  NAUTILUS_TRADER_BRANCH  Branch when cloning (default: develop)
  NAUTILUS_TRADER_REF     Git rev for --pin-ci (default: CI pin)
EOF
}

log() { printf '==> %s\n' "$*"; }
warn() { printf 'warning: %s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pin-ci) PIN_CI=1; shift ;;
    --force-clone) FORCE_CLONE=1; shift ;;
    --skip-verify) SKIP_VERIFY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1 (see --help)" ;;
  esac
done

is_usable_checkout() {
  local path="$1"
  [[ -d "${path}/crates" ]] || [[ -d "${path}/.git" ]] || [[ -f "${path}/Cargo.toml" ]]
}

resolve_source_path() {
  if [[ -n "${NAUTILUS_TRADER_PATH:-}" ]]; then
    local p
    p="$(cd "${NAUTILUS_TRADER_PATH}" 2>/dev/null && pwd)" || die "NAUTILUS_TRADER_PATH is not a directory: ${NAUTILUS_TRADER_PATH}"
    printf '%s\n' "${p}"
    return
  fi
  if is_usable_checkout "${SIBLING_PATH}"; then
    printf '%s\n' "${SIBLING_PATH}"
    return
  fi
  # Empty string means "need clone"
  printf '\n'
}

ensure_sibling_link_or_path() {
  local source="$1"
  if [[ "${source}" == "${SIBLING_PATH}" ]]; then
    return 0
  fi
  if [[ -e "${SIBLING_PATH}" || -L "${SIBLING_PATH}" ]]; then
    if [[ -L "${SIBLING_PATH}" ]]; then
      local current
      current="$(readlink "${SIBLING_PATH}")"
      if [[ "${current}" == "${source}" ]]; then
        log "sibling symlink already points at ${source}"
        return 0
      fi
      warn "replacing sibling symlink ${SIBLING_PATH} -> ${current}"
      rm -f "${SIBLING_PATH}"
    elif is_usable_checkout "${SIBLING_PATH}"; then
      die "sibling path ${SIBLING_PATH} already exists and differs from NAUTILUS_TRADER_PATH=${source}; move or remove it first"
    else
      warn "removing non-checkout path at sibling location: ${SIBLING_PATH}"
      rm -rf "${SIBLING_PATH}"
    fi
  fi
  log "linking sibling ${SIBLING_PATH} -> ${source}"
  ln -s "${source}" "${SIBLING_PATH}"
}

clone_upstream() {
  local dest="$1"
  if [[ -e "${dest}" || -L "${dest}" ]]; then
    if [[ -L "${dest}" ]]; then
      rm -f "${dest}"
    elif is_usable_checkout "${dest}"; then
      die "refusing to overwrite existing checkout: ${dest}"
    elif [[ "${FORCE_CLONE}" -eq 1 ]]; then
      warn "removing incomplete path before clone: ${dest}"
      rm -rf "${dest}"
    else
      die "path exists but is not a usable nautilus_trader checkout: ${dest} (use --force-clone if safe)"
    fi
  fi
  log "cloning ${UPSTREAM_URL} (branch ${CLONE_BRANCH}) -> ${dest}"
  git clone --branch "${CLONE_BRANCH}" --single-branch --recurse-submodules "${UPSTREAM_URL}" "${dest}"
}

maybe_pin() {
  local path="$1"
  if [[ "${PIN_CI}" -ne 1 ]]; then
    return 0
  fi
  [[ -d "${path}/.git" ]] || die "cannot --pin-ci: not a git checkout: ${path}"
  log "checking out pin NAUTILUS_TRADER_REF=${NAUTILUS_TRADER_REF}"
  (
    cd "${path}"
    git fetch --tags --recurse-submodules origin "${NAUTILUS_TRADER_REF}" 2>/dev/null \
      || git fetch --tags --recurse-submodules origin
    git checkout --recurse-submodules "${NAUTILUS_TRADER_REF}"
  )
}

print_status() {
  local path="$1"
  log "nautilus_trader ready at: ${path}"
  if [[ -d "${path}/.git" ]]; then
    (
      cd "${path}"
      local rev branch
      rev="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
      branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo detached)"
      printf '    rev:    %s\n' "${rev}"
      printf '    branch: %s\n' "${branch}"
      if [[ "${rev}" != "${NAUTILUS_TRADER_REF}" ]]; then
        printf '    note:   CI pin is %s (use --pin-ci to match CI)\n' "${NAUTILUS_TRADER_REF}"
      fi
    )
  fi
  log "Cargo path deps expect: ${SIBLING_PATH}"
}

verify_core() {
  if [[ "${SKIP_VERIFY}" -eq 1 ]]; then
    log "skipping cargo verify (--skip-verify)"
    return 0
  fi
  if ! command -v cargo >/dev/null 2>&1; then
    warn "cargo not found; skip verify. Install Rust 1.97.1 then: cargo test -p catalog-capture-core --lib"
    return 0
  fi
  log "verifying: cargo check -p catalog-capture-core --lib"
  (
    cd "${REPO_ROOT}"
    cargo check -p catalog-capture-core --lib
  )
  log "verify ok"
}

main() {
  log "repo root: ${REPO_ROOT}"
  log "sibling path (Cargo): ${SIBLING_PATH}"

  local source
  source="$(resolve_source_path)"

  if [[ -n "${source}" ]]; then
    log "using existing local checkout: ${source}"
    ensure_sibling_link_or_path "${source}"
  else
    log "no local checkout found; cloning upstream"
    clone_upstream "${SIBLING_PATH}"
    source="${SIBLING_PATH}"
  fi

  maybe_pin "${source}"
  print_status "${source}"
  verify_core
  log "done. Next: make build   # or cargo build -p catalog-capture-cli"
}

main
