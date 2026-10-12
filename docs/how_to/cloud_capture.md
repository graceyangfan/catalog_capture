# Long-running market data capture (cloud / bare metal)

Run from the **repository root**. Data under `./data/` (gitignored). No system install.

## 0) Prerequisites

```bash
# Ubuntu/Debian example
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libssl-dev curl git clang

curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"
rustup toolchain install 1.99.0
rustup default 1.99.0
```

Outbound mainnet access to the configured venues (HTTPS/WSS). The default
multi-venue profile uses public data and needs no API keys; Predict and Binance
Spot SBE profiles additionally need mounted credentials described in
[credentials](credentials.md).

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

For another profile, pass the matching feature set as well as its config; the
packager validates the config against the resulting binary before creating an
archive:

```bash
./scripts/package-cloud.sh \
  --config examples/capture.predict-orderbook.toml \
  --features venue-predict
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

### Independent HIP4 and Predict recorders

Run two separate processes with separate catalogs under the project's ignored
`data/` directory. HIP4 discovers BTC daily YES/NO instruments and records
quotes/trades in hourly UTC files under `data/hyperliquid-hip4-btc-daily/`.
Predict discovers BTC, ETH and BNB, each at 5m and 15m, and independently seals
one YES snapshot file per market under `data/predict-crypto-updown/`.

Configure only the local Predict API key as described in
[credentials](credentials.md); HIP4 public recording needs no key. Never add
credentials to the capture profile or deployment archive.

```bash
CAPTURE_FEATURES=venue-hyperliquid,venue-predict make build-release-capture
./bin/catalog-capture-cli validate \
  --config examples/capture.hyperliquid-hip4-btc-daily.toml
./bin/catalog-capture-cli validate \
  --config examples/capture.predict-crypto-updown.toml
# Terminal/service 1:
./bin/catalog-capture-cli run \
  --config examples/capture.hyperliquid-hip4-btc-daily.toml
# Terminal/service 2:
./bin/catalog-capture-cli run \
  --config examples/capture.predict-crypto-updown.toml
```

Each process runs until stopped. Use Ctrl+C or `kill -TERM <pid>` and wait for
`Capture completed` so every active file is sealed. Run one writer process per
catalog directory. For packaging, package each profile separately with its
matching feature (`venue-hyperliquid` or `venue-predict`).

## 4) Short smoke

```bash
CAPTURE_SECONDS=120 ./scripts/run-mainnet-capture.sh \
  examples/capture.multi-venue-mainnet.toml

find data -type f \( -name '*.parquet' -o -name '*.jsonl' -o -name '*.json' \) | head
du -sh data/*
```

## 5) Unattended with nohup

```bash
mkdir -p logs
CONFIG="examples/capture.multi-venue-mainnet.toml"
NAME="multi-venue"
LOG_FILE="logs/multi-venue-$(date -u +%Y%m%dT%H%M%SZ).log"
PID_FILE="logs/multi-venue.pid"

nohup env NAUTILUS_LOG='stdout=Info;is_colored=false' \
  ./bin/catalog-capture-cli run \
  --config "$CONFIG" \
  >"$LOG_FILE" 2>&1 </dev/null &
echo $! > "$PID_FILE"

# Check the actual recorder PID
ps -p "$(cat "$PID_FILE")" -o pid=,ppid=,sid=,pgid=,etime=,stat=,command=

# Stop (flush / seal)
kill -TERM "$(cat "$PID_FILE")"
```

Start the binary directly; do not background `scripts/run-capture-service.sh`.
The wrapper is a foreground build/logging helper and its PID is not the
recorder PID. The CLI subcommand is `run`; Cargo options such as `--features`
belong to the build command, not the runtime command. Wait for `Capture
completed` after sending `SIGTERM`. Never use `kill -9` for a live recorder.

`nohup` protects against SSH disconnects. It does not restart after OOM,
reboot, or external `SIGKILL`; restart the binary manually if its PID exits.

## 6) Monitor

```bash
# The config must contain [runtime.metrics] enabled = true.
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
| Segment seal | Configured rotation boundary (`capture.multi-venue-mainnet.toml` uses each UTC hour) |

## 8) Cleanup

```bash
./scripts/cleanup-tmp-captures.sh
./scripts/cleanup-tmp-captures.sh /tmp
```
