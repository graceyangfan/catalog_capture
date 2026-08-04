# Cloud / bare-metal capture (mainnet)

Run from the **repository root**. Data under `./data/` (gitignored). No system install.

## 0) Prerequisites

```bash
# Ubuntu/Debian example
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libssl-dev curl git clang

curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"
rustup toolchain install 1.97.1
rustup default 1.97.1
```

Outbound mainnet access: Hyperliquid, Binance Futures, Deribit (HTTPS/WSS).
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

## 2) Build (slim multi-venue graph)

```bash
# Binance + Deribit + Hyperliquid only (preferred)
make build-release-capture

# Free disk if needed (this repo + sibling NT target/)
# make clean-all-targets
```

See [build size](build_size.md).

## 3) Validate

```bash
./target/release/catalog-capture-cli validate \
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
./data/multi-venue-mainnet/data/{quotes,trades,order_book_deltas,mark_prices,instruments}/…
./data/multi-venue-mainnet/data/custom/DeribitBookSummary/…
```

Optional `metadata/` is operator lineage only. See
[catalog layout](../concepts/catalog_layout.md).

| Clock | Behavior |
|-------|----------|
| HIP-4 universe refresh | Unsub old / sub new on discovery |
| Segment seal | Day files at **06:00 UTC** |

## 8) Cleanup

```bash
./scripts/cleanup-tmp-captures.sh
./scripts/cleanup-tmp-captures.sh /tmp
```
