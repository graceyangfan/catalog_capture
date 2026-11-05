#!/usr/bin/env bash
# Project Cargo entry point; version and release checksums are pinned here.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="0.102.0-9"
FORK_BIN="$ROOT/.tools/cargo/$VERSION/cargo"
export CARGO_FORK_AUTO_UPDATE=0
cd "$ROOT"

install_fork() (
  local platform arch checksum work asset
  case "$(uname -s)" in
    Darwin) platform=apple-darwin ;;
    Linux) platform=unknown-linux-gnu ;;
    *)
      echo "Only macOS and Linux are supported by this wrapper." >&2
      exit 1
      ;;
  esac
  case "$(uname -m)" in
    arm64 | aarch64) arch=aarch64 ;;
    x86_64 | amd64) arch=x86_64 ;;
    *)
      echo "Unsupported CPU architecture." >&2
      exit 1
      ;;
  esac
  # Pinned release checksums; update together with VERSION.
  case "$arch-$platform" in
    aarch64-apple-darwin) checksum=6a6371f2c8988b8ee2965867145a03f0481881c702c05fb429a5f67d1e0998bb ;;
    aarch64-unknown-linux-gnu) checksum=d521070d114d03d483f53b68c3fb8845e457c91776a164cebdee799b887f6703 ;;
    x86_64-apple-darwin) checksum=4c0e87b3258c5d76a9a0fa89f43b3736b9f6e50adb4bae90474721c0afa71e57 ;;
    x86_64-unknown-linux-gnu) checksum=dc45535da4830f416949fdf739bcb655ef799bd2206e7813757261477f96bb20 ;;
  esac
  work="$(mktemp -d)"
  trap 'rm -rf "$work"' EXIT
  asset="cargo-$VERSION-$arch-$platform.tar.gz"
  curl --proto '=https' --tlsv1.2 -fsSL --retry 2 --connect-timeout 15 --max-time 180 \
    "https://github.com/huacnlee/cargo/releases/download/v$VERSION/$asset" -o "$work/$asset"
  if command -v sha256sum > /dev/null; then
    (cd "$work" && printf '%s  %s\n' "$checksum" "$asset" | sha256sum -c -)
  else
    (cd "$work" && printf '%s  %s\n' "$checksum" "$asset" | shasum -a 256 -c -)
  fi
  tar -xzf "$work/$asset" -C "$work" cargo
  CARGO_FORK_AUTO_UPDATE=0 "$work/cargo" --version
  mkdir -p "$(dirname "$FORK_BIN")"
  install -m 755 "$work/cargo" "$FORK_BIN"
  rm -rf "$work"
  trap - EXIT
)

case "${1:-}" in
  --install)
    if [[ -x "$FORK_BIN" ]] && "$FORK_BIN" --version | grep -Fq "huacnlee/cargo $VERSION,"; then
      exit 0
    fi
    install_fork
    exit 0
    ;;
  --cache-size)
    for cache in "$ROOT/target" "${CARGO_HOME:-$HOME/.cargo}/targets"; do
      if [[ -d "$cache" ]]; then du -sh "$cache"; fi
    done
    exit 0
    ;;
esac

if [[ ! -x "$FORK_BIN" ]]; then
  echo "Run make install-cargo first." >&2
  exit 1
fi

# External Cargo subcommands must also resolve this version of Cargo.
PATH="$(dirname "$FORK_BIN"):$PATH"
export PATH
exec "$FORK_BIN" "$@"
