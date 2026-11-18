# Record and replay Bullet public data

The [BTC profile](../../examples/capture.bullet-btc-perp.toml) records metadata,
explicit top-20 L2, aggregate trades, mark/index prices, settled hourly gross
funding and five-second funding estimates. It needs no credentials. The
[protocol contract](../bullet-public-capture-design.md) defines sequence,
precision, timestamps, rate basis and recovery behavior.

## Run the recorder

```bash
./scripts/cargo.sh build -p catalog-capture-cli --no-default-features --features venue-bullet
./target/debug/catalog-capture-cli validate --config examples/capture.bullet-btc-perp.toml
./target/debug/catalog-capture-cli run --config examples/capture.bullet-btc-perp.toml
```

The profile runs until stopped. For a finite run, copy the TOML, set
`runtime.capture_seconds`, and use a fresh absolute `output.catalog_uri`.
The [funding-only profile](../../examples/capture.bullet-btc-funding.toml) records
metadata, settled rates and estimates without market subscriptions.

## Read through one Native BacktestEngine

After clean shutdown, query Native `OrderBookDelta` and `TradeTick` directly from
`ParquetDataCatalog`. The recorded stream already contains recovery `CLEAR` controls and
hourly initialization snapshots. No window-aware loader or replay-time reset
injection is required. Native event ordering uses `ts_init`; preserve `F_LAST`
framing when passing grouped batches rather than flat deltas.

The opt-in acceptance test uses ordinary catalog queries and flat Native Delta
input, one engine and one strategy. It checks callback counts, sequence metadata
and reconstructed book state; it places no orders.

```bash
BULLET_BACKTEST_CATALOG=/absolute/path/data/CATALOG \
BULLET_PLAIN_CATALOG_REPORT=/absolute/path/data/CATALOG/metadata/plain_native_backtest.json \
  ./scripts/cargo.sh test -p catalog-capture-runtime-adapter --test replay_windows \
    recorded_bullet_plain_catalog_runs_in_one_native_engine -- --ignored --nocapture
```

For all seven families, use the combined Native codec/readback check. The optional
settlement selector requires that event exactly once and estimate rollover:

```bash
BULLET_SOAK_CATALOG=/absolute/path/data/CATALOG \
BULLET_SOAK_SETTLEMENT_NS=EXPECTED_SETTLEMENT_UNIX_NANOSECONDS \
  ./scripts/cargo.sh test -p catalog-capture-cli --no-default-features \
    --features venue-bullet bullet_btc_combined_catalog_readback -- --ignored --nocapture
```

Omit `BULLET_SOAK_SETTLEMENT_NS` for a run that does not span settlement. Reports
are written under the catalog's ignored `metadata/` directory. The reader checks
precision/identity, book integrity, persisted invalidations, complete checkpoint
contents, unique trades and matching mark/index times.

For the funding-only profile:

```bash
BULLET_FUNDING_CATALOG=/absolute/path/data/CATALOG \
  ./scripts/cargo.sh test -p nautilus-bullet recorded_funding_catalog_readback \
    -- --ignored --nocapture
```

`BULLET_FUNDING_SETTLEMENT_NS` optionally requires a specific settlement and rollover.

## Explicit reconnect and settlement soak

Use a finite combined-profile copy with venue ID `bullet_main`, all seven
families, and a fresh absolute catalog path. Start before an hourly UTC settlement
and allow time afterward for confirmed history to appear. Choose reconnect times
inside the run, leaving at least 20 seconds before shutdown.

The ignored harness calls production `run_capture` and sends Native controller
commands. It does not introduce production fault-injection settings.

```bash
BULLET_SOAK_CONFIG=/absolute/path/capture.bullet-soak.toml \
BULLET_SOAK_RECONNECT_UNIX_NS=FIRST_UNIX_NS,SECOND_UNIX_NS \
  ./scripts/cargo.sh test -p catalog-capture-cli --no-default-features \
    --features venue-bullet bullet_btc_combined_soak -- --ignored --nocapture \
    > /absolute/path/bullet-soak.log 2>&1

python tests/bullet_soak_evidence.py --catalog /absolute/path/data/CATALOG \
  --native-log /absolute/path/bullet-soak.log --prepare-recovery --require-invalidation
```

Then run both readback commands above. Recovery evidence requires accepted
commands, new epochs, all three physical ACKs, persisted invalidations and fresh
snapshots. A healthy hourly boundary carries an initializer; a boundary during
recovery must not carry an old cached checkpoint.

The production quality verdict remains incomplete for explicit source gaps. The
soak harness accepts that expected outcome only when the complete failure journal
contains solely book source gaps, with no admission/writer failures and drained
queues. A successful restart does not prove uninterrupted delivery or fill behavior.

## Optional independent source comparison

`tests/bullet_soak_evidence.py` also compares Native readback against independently
collected public responses. Retain exact decimal strings and integer IDs in:

- `ws.jsonl`: envelopes with `received_ns` and decoded `body`, or explicit `error`;
  subscribe to `BTC-USD@depth20`, `BTC-USD@aggTrade` and `BTC-USD@markPrice`, with
  application PING every 20 seconds.
- `http.jsonl`: `request_ns`, `received_ns`, `path`, `status`, decoded `body`, or
  `error`, for premium index, funding info and settled funding history.

```bash
python tests/bullet_soak_evidence.py --catalog /absolute/path/data/CATALOG \
  --native-log /absolute/path/bullet-soak.log --source /absolute/path/data/SOURCE
```

The comparison checks common trade IDs, oracle publications and exact settled
rates. Observer failures and unmatched observations are reported separately;
independent connections do not establish complete Native event coverage.

## Offline regression checks

```bash
./scripts/cargo.sh test -p nautilus-bullet --lib
./scripts/cargo.sh test -p catalog-capture-cli --no-default-features --features venue-bullet
./scripts/cargo.sh test -p catalog-capture-runtime-adapter --test replay_windows
python -m unittest discover -s tests -p test_bullet_soak_evidence.py
```

Fixtures and source provenance are in the adapter's
[test-data guide](../../crates/nautilus-bullet/test_data/README.md). Operational
catalogs, logs and measurements belong in ignored `data/`, not versioned reports.
