# Cloud server capture (mainnet)

Run everything from the **repository root**. Data lands under `./data/` (gitignored).
No system install required.

## 0) Server prerequisites

```bash
# Ubuntu/Debian example
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libssl-dev curl git clang

# Rust 1.97.1
curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"
rustup toolchain install 1.97.1
rustup default 1.97.1
rustup component add rustfmt clippy
```

Outbound access needed: Hyperliquid, Binance Futures, Deribit **mainnet** HTTPS/WSS.

Public capture needs **no API keys**. Authenticated venues only if you set env keys.

## 1) Clone and bootstrap

```bash
# Layout (sibling path dependency):
#   ~/work/nautilus_trader
#   ~/work/catalog_capture

mkdir -p ~/work && cd ~/work
git clone https://github.com/graceyangfan/catalog_capture.git
cd catalog_capture

# Prefer existing ../nautilus_trader; else clone develop (or pin CI rev)
make bootstrap-deps
# reproducible pin (optional):
# ./scripts/bootstrap-deps.sh --pin-ci

make build-release
```

## 2) Validate configs

```bash
# Full multi-venue (HL + Binance L2 d20 + Deribit book summary)
./target/release/catalog-capture-cli validate \
  --config examples/capture.multi-venue-mainnet.toml

# HL universe only
./target/release/catalog-capture-cli validate \
  --config examples/capture.hyperliquid-hip4-btc-daily.toml

# Deribit book summary only
./target/release/catalog-capture-cli validate \
  --config examples/capture.deribit-btc-book-summary.toml
```

## 3) Short smoke (before unattended)

```bash
# ~2 minutes multi-venue (temp override)
CAPTURE_SECONDS=120 ./scripts/run-mainnet-capture.sh \
  examples/capture.multi-venue-mainnet.toml

# Or HL-only smoke example (~75s baked in smoke TOML)
./target/release/catalog-capture-cli run \
  --config examples/capture.hyperliquid-hip4-btc-smoke.toml
```

Check output:

```bash
find data -type f \( -name '*.parquet' -o -name '*.json' -o -name '*.jsonl' \) | head -50
du -sh data/*
```

## 4) Unattended mainnet capture (recommended)

```bash
cd ~/work/catalog_capture

# multi-venue (default config in script)
./scripts/run-mainnet-capture.sh

# equivalent explicit:
./scripts/run-capture-service.sh \
  --config examples/capture.multi-venue-mainnet.toml \
  --release

# HL-only long run
./scripts/run-mainnet-capture.sh examples/capture.hyperliquid-hip4-btc-daily.toml
```

Background with nohup:

```bash
nohup ./scripts/run-mainnet-capture.sh \
  examples/capture.multi-venue-mainnet.toml \
  > logs/nohup-multi-venue.out 2>&1 &
echo $! > logs/capture.pid
```

Stop cleanly (flush/seal):

```bash
kill -TERM "$(cat logs/capture.pid)"
# or: pkill -TERM -f catalog-capture-cli
```

Optional user-level service (still points at this clone):

```bash
./scripts/optional-user-service.sh --platform systemd \
  --config examples/capture.multi-venue-mainnet.toml
# then: systemctl --user enable --now catalog-capture.service
```

## 5) Monitor

```bash
# if runtime.metrics.enabled in TOML (multi-venue example uses 127.0.0.1:9108)
curl -s http://127.0.0.1:9108/metrics | egrep 'rss|dropped|active_partitions|flush'

# logs
tail -f logs/*.log
```

## 6) What gets written (Nautilus Rust catalog layout)

Same as `ParquetDataCatalog`:

```text
./data/multi-venue-mainnet/data/{quotes,trades,order_book_deltas,mark_prices,instruments,...}/{id}/…parquet
./data/multi-venue-mainnet/data/custom/DeribitBookSummary/…parquet
```

Optional `metadata/` next to `data/` is operator lineage only (not a catalog type folder).

- **HIP-4 roll:** unsubscribe old / subscribe new on discovery.  
- **File day cut (segment mode):** seal sealed names with Nautilus `timestamps_to_filename` at **06:00 UTC**.

## 7) Disk / process notes

```bash
# free disk check
df -h .

# cleanup local capture trees (careful)
# make cleanup-tmp   # defaults to ./data
```

Recommend: several GB free for multi-day L2 + outcomes; use `flush_*` already tuned in the multi-venue TOML.
