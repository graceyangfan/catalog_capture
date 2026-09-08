#!/usr/bin/env bash
# Build a native release binary and create a self-contained cloud package.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CONFIG="${CONFIG:-examples/capture.multi-venue-mainnet.toml}"
OUTPUT_DIR="${CATALOG_CAPTURE_PACKAGE_DIR:-$ROOT/dist}"
CAPTURE_FEATURES="${CAPTURE_FEATURES:-venue-binance,venue-deribit,venue-hyperliquid,venue-lighter}"
CARGO="${CARGO:-cargo}"
TOOLCHAIN="${RUSTUP_TOOLCHAIN:-1.98.0}"
SKIP_BUILD=0
PACKAGE_NAME=""

usage() {
  cat << 'EOF'
Usage: scripts/package-cloud.sh [options]

Builds a native release binary and writes a deployable .tar.gz plus SHA-256
checksum under ./dist (or CATALOG_CAPTURE_PACKAGE_DIR).

Options:
  --config <path>       TOML config to include as config/capture.toml
  --output-dir <path>   Package output directory (default: ./dist)
  --name <name>         Package directory/archive name
  --features <list>     Release features (default: cloud capture venues)
  --skip-build          Package the existing bin/catalog-capture-cli
  -h, --help            Show this help

Run this on the target cloud OS/architecture. Cross-compilation is not
attempted; the package contains a native binary for the build host.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --name)
      PACKAGE_NAME="$2"
      shift 2
      ;;
    --features)
      CAPTURE_FEATURES="$2"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
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

if [[ "$CONFIG" != /* ]]; then
  CONFIG="$ROOT/$CONFIG"
fi
if [[ ! -f "$CONFIG" ]]; then
  echo "Config not found: $CONFIG" >&2
  exit 1
fi

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -z "$PACKAGE_NAME" ]]; then
  PACKAGE_NAME="catalog-capture-cloud-${OS}-${ARCH}-${STAMP}"
fi
if [[ "$PACKAGE_NAME" == */* || "$PACKAGE_NAME" == .* ]]; then
  echo "Package name must be a simple directory name: $PACKAGE_NAME" >&2
  exit 2
fi

BIN="$ROOT/bin/catalog-capture-cli"
if [[ $SKIP_BUILD -eq 0 ]]; then
  echo "Building release binary (features=${CAPTURE_FEATURES})..."
  "$CARGO" "+$TOOLCHAIN" build --release -p catalog-capture-cli \
    --no-default-features \
    --features "$CAPTURE_FEATURES"
  mkdir -p "$ROOT/bin"
  install -m 755 "$ROOT/target/release/catalog-capture-cli" "$BIN"
elif [[ ! -x "$BIN" ]]; then
  echo "Prebuilt binary not found: $BIN" >&2
  exit 1
fi

echo "Validating config: $CONFIG"
"$BIN" validate --config "$CONFIG"

mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
STAGE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/catalog-capture-cloud.XXXXXX")"
STAGE="$STAGE_ROOT/$PACKAGE_NAME"
trap 'rm -rf "$STAGE_ROOT"' EXIT

mkdir -p "$STAGE/bin" "$STAGE/config" "$STAGE/scripts"
install -m 755 "$BIN" "$STAGE/bin/catalog-capture-cli"
install -m 644 "$CONFIG" "$STAGE/config/capture.toml"
install -m 755 \
  scripts/run-capture-service.sh \
  scripts/healthcheck-option-universe.sh \
  scripts/optional-user-service.sh \
  "$STAGE/scripts/"

COMMIT="$(git rev-parse --short HEAD 2>/dev/null || printf 'unknown')"
cat > "$STAGE/MANIFEST" << EOF
package=$PACKAGE_NAME
build_host=${OS}-${ARCH}
git_commit=$COMMIT
rust_toolchain=$TOOLCHAIN
capture_features=$CAPTURE_FEATURES
config=config/capture.toml
EOF

cat > "$STAGE/README.md" << 'EOF'
# Catalog Capture Cloud Package

This package contains the native `catalog-capture-cli` product binary and one
capture configuration. It does not require Rust, Cargo, the source checkout,
or the large `target/` build cache.

## Run

```bash
./scripts/run-capture-service.sh \
  --config config/capture.toml \
  --prebuilt
```

The service writes logs to `./logs/` and catalog data according to
`config/capture.toml`. Use `Ctrl+C` or `SIGTERM` for a clean Parquet flush.

## Optional user service

```bash
./scripts/optional-user-service.sh \
  --platform systemd \
  --config config/capture.toml \
  --prebuilt
```

Credentials should be supplied through the environment or the deployment
secret manager; do not commit secret values into the packaged TOML.
EOF

ARCHIVE="$OUTPUT_DIR/${PACKAGE_NAME}.tar.gz"
CHECKSUM="$ARCHIVE.sha256"
tar -C "$OUTPUT_DIR" -czf "$ARCHIVE" -C "$STAGE_ROOT" "$PACKAGE_NAME"
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$ARCHIVE" > "$CHECKSUM"
else
  shasum -a 256 "$ARCHIVE" > "$CHECKSUM"
fi

echo "Cloud package created: $ARCHIVE"
echo "Checksum created:      $CHECKSUM"
