# Long-running market data capture (cloud / bare metal)

Run from the **repository root**. Data under `./data/` (gitignored). No system install.

## 0) Prerequisites

```bash
# Ubuntu/Debian example
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libssl-dev curl git clang

curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"
rustup toolchain install 1.98.0
rustup default 1.98.0
```

Outbound mainnet access to the configured venues (HTTPS/WSS).
Public capture needs **no API keys**.

## 1) Clone and bootstrap

```bash
mkdir -p ~/work && cd ~/work
git clone https://github.com/graceyangfan/catalog_capture.git
cd catalog_capture

# Needs sibling ../nautilus_trader
make bootstrap-deps
# optional CI pin: ./scripts/bootstrap-deps.sh --pin-ci
```

## 2) Build the product binary

```bash
# Default cloud feature set; override CAPTURE_FEATURES for a smaller venue set.
make build-release-capture

# Free disk if needed (this repo + sibling NT target/)
# make clean-all-targets
```

See [build size](build_size.md).

## 2a) Create a deployment package

Run this on the target cloud OS and architecture. The script builds the
native release binary, includes one TOML config and the runtime service
helpers, then writes a small package without `target/` or source code:

```bash
./scripts/package-cloud.sh \
  --config examples/capture.multi-venue-mainnet.toml
```

Transfer the generated `dist/catalog-capture-cloud-*.tar.gz` and its `.sha256`
file to the server, verify and extract it:

```bash
sha256sum -c catalog-capture-cloud-*.tar.gz.sha256
tar -xzf catalog-capture-cloud-*.tar.gz
cd catalog-capture-cloud-*/
./scripts/run-capture-service.sh --config config/capture.toml --prebuilt
```

The packaged binary is native to the machine where the package was built;
cross-compilation is intentionally not attempted.

## 3) Validate

```bash
./bin/catalog-capture-cli validate \
  --config examples/capture.multi-venue-mainnet.toml
```

## 4) Short smoke

```bash
CAPTURE_SECONDS=120 ./scripts/run-mainnet-capture.sh \
  examples/capture.multi-venue-mainnet.toml

find data -type f \( -name '*.parquet' -o -name '*.jsonl' -o -name '*.json' \) | head
du -sh data/*
```

## 5) Unattended

```bash
# Foreground (default config = multi-venue mainnet)
./scripts/run-mainnet-capture.sh

# Background
mkdir -p logs
nohup ./scripts/run-mainnet-capture.sh \
  examples/capture.multi-venue-mainnet.toml \
  > logs/nohup-multi-venue.out 2>&1 &
echo $! > logs/capture.pid

# Stop (flush / seal)
kill -TERM "$(cat logs/capture.pid)"
```

Generic service wrapper (supports `CAPTURE_FEATURES`):

```bash
CAPTURE_FEATURES=venue-binance,venue-deribit,venue-hyperliquid \
  ./scripts/run-capture-service.sh \
  --config examples/capture.multi-venue-mainnet.toml \
  --release
```

Optional user unit (still this clone):

```bash
./scripts/optional-user-service.sh --platform systemd \
  --config examples/capture.multi-venue-mainnet.toml
```

## 6) Monitor

```bash
# multi-venue example: runtime.metrics on 127.0.0.1:9108
curl -s http://127.0.0.1:9108/metrics | egrep 'rss|dropped|active_partitions|flush'
tail -f logs/*.log
```

## 7) Catalog layout (Nautilus)

```text
./data/<catalog>/data/{instruments,quotes,trades,order_book_deltas,…}/…
./data/<catalog>/data/custom/<CustomDataType>/…
```

Optional `metadata/` is operator lineage only. See
[catalog layout](../concepts/catalog_layout.md).

| Clock | Behavior |
|-------|----------|
| Dynamic universe refresh | Unsub old / sub new when enabled by the config |
| Segment seal | Configured rotation boundary (the example uses **06:00 UTC**) |

## 8) Cleanup

```bash
./scripts/cleanup-tmp-captures.sh
./scripts/cleanup-tmp-captures.sh /tmp
```
