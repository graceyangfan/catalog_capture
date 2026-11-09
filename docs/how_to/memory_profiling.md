# Profile recorder memory

Use this procedure when RSS keeps rising during capture or when a market rollover
appears to retain state. Start with a stock build and use a temporary profiling
build to attribute live allocations to call stacks. The heap workflow follows
[Paradigm Reth's profiling guide](https://reth.rs/run/faq/profiling/) and uses
jemalloc's `jeprof`.

## Establish a baseline

Record the recorder and Nautilus revisions, host, enabled venues, feed mode,
duration, and seal schedule. Use a bounded run with a separate catalog directory.
Keep the pressure limits enabled and sample before and after several market
rollovers and at the same phase of successive seal intervals.

- For local mixed capture, copy
  [`capture.predict-binance-bitfinex-btc.toml`](../../examples/capture.predict-binance-bitfinex-btc.toml)
  to a scratch directory. Keep Predict BTC 5m/15m and Bitfinex BTC/USDT L3.
  Change the Binance venue to `kind = "binance_futures"` with
  `product_type = "usd_m"`, change the instrument IDs to `BTCUSDT-PERP.BINANCE`,
  and set the Binance L2 `depth = 50`. Supply the Predict credentials using
  the [credentials guide](credentials.md).
- For a Binance Spot SBE incident, use the original SBE subscription and the
  target Linux host. Compare Binance alone with mixed capture under the same
  settings. The local futures feed exercises rollover and writing at a different
  load from Spot SBE.
- For Bitfinex alone, use the [L3 example](../../examples/capture.bitfinex-spot-l3.toml).
  A short seal interval can exercise the same file handoff as an hourly cut.

Compare OS RSS with `native_data_queue_depth`, the changes in
`native_data_dispatched` and `native_data_busy_ns`, and the writer queue,
in-flight, and buffered byte counters. Use `ps -o rss= -p "$pid"` for current RSS
in KiB on macOS because the recorder's macOS RSS metric reports the peak.
`vmmap -summary "$pid"` can also show allocator and mapping categories on macOS.

## Capture a heap profile

Use a disposable checkout with the same Nautilus revision. In the CLI crate's
`[dependencies]`, add the allocator used in the profiling experiment.

```toml
tikv-jemallocator = { version = "=0.7.0", features = ["profiling", "stats"] }
tikv-jemalloc-ctl = { version = "=0.7.0", features = ["stats"] }
```

Register it in `crates/catalog-capture-cli/src/main.rs`.

```rust
#[global_allocator]
static PROFILING_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

Build the optimized binary with symbols and run it with a scratch TOML that sets
a finite `runtime.capture_seconds` and an isolated `output.catalog_uri`.
Export `PREDICT_CREDENTIALS_FILE` with the absolute path of the existing
credential file when the scratch checkout has no local `env/` directory.

```bash
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/catalog-capture-profile.XXXXXX")
# Save the bounded config as "$run_dir/capture.toml" with catalog_uri = "file://<run_dir>/catalog".
mkdir "$run_dir/heap"
CARGO_PROFILE_RELEASE_DEBUG=1 CARGO_PROFILE_RELEASE_STRIP=none \
  ./scripts/cargo.sh build --release -p catalog-capture-cli \
  --no-default-features --features venue-predict,venue-binance,venue-bitfinex

binary="$PWD/target/release/catalog-capture-cli"
_RJEM_MALLOC_CONF="prof:true,prof_active:true,lg_prof_sample:16,lg_prof_interval:27,prof_prefix:$run_dir/heap/capture" \
  "$binary" run --config "$run_dir/capture.toml" > "$run_dir/capture.log" 2>&1 &
pid=$!
```

Interval dumps are driven by bytes allocated. For samples tied to an exact
transition, the diagnostic run used `prof.dump` in a temporary `SIGUSR1` handler
and after `node.run().await` returned. Dispatch the signal-triggered dump through
`tokio::task::spawn_blocking` in the profiling checkout. Advance
`tikv_jemalloc_ctl::epoch` before reading `stats::allocated`, `stats::active`,
and `stats::resident`. See the [jemalloc manual](https://jemalloc.net/jemalloc.3.html).

Keep the exact executable and its symbols beside the profiles. Compare two dumps
from the same build with `jeprof` from the matching jemalloc release. Choose one
after warm-up and another after a rollover or after a backlog has drained.

```bash
jeprof --text --show_bytes --inuse_space --base="$early_heap" "$binary" "$late_heap"
```

Keep the executable path stable during the run and analysis so the mapped paths
in the dumps resolve. If the report shows only addresses, resolve symbolization
before drawing conclusions from individual stacks.

## Interpret the result

| Observation | What to inspect |
| --- | --- |
| Native queue grows while writer queues stay low | Compare arrival and dispatch rates and inspect the consumer's hot stacks. A one-shot consumer pause in a disposable build can confirm what queued events look like in a heap diff. |
| Writer ingress empties but in-flight bytes or age stay high | Check encoding, sync, and seal timings. An empty ingress queue can coexist with a blocked worker. |
| Queues drain but live allocation rises after each rollover | Compare retired and active instrument, book, subscription, and writer counts. Use the heap diff to find the retained objects. |
| Live allocation falls after sealing while RSS stays high | Inspect allocator resident pages and other mappings. Compare more than one boundary to establish the trend. |

In the local non-SBE check, an injected eight-second consumer pause exposed
queued-event allocation stacks, and the queue drained after dispatch resumed.
In a separate stock run, current malloc allocations fell across an hourly seal
while RSS rose. These checks distinguish temporary backlog and retained pages
from a rising live-heap baseline. Linux Spot SBE requires its own soak.

Finish the bounded run and validate the sealed files. For the mixed example,
also check that Predict's retired windows have separate files.

```bash
wait "$pid"
"$binary" validate-order-book --catalog-uri "file://$run_dir/catalog" \
  --instrument-id BTCUSDT-PERP.BINANCE --instrument-id BTCUST.BITFINEX
```

Record the capture result and any gaps, drops or write errors alongside the heap
results. After shutdown and replay, remove the scratch capture and profiling
checkout. Keep catalogs, credentials, heap dumps, and raw logs out of version
control.
