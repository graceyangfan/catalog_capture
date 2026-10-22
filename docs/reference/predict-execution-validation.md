# Predict adapter validation

This reference records evidence for the [adapter contract](../predict-execution-adapter-design.md).
[Test commands](../../crates/nautilus-predict/tests/README.md) cover reproducible
regressions; [live evidence](predict-live-evidence.md) describes venue acceptance.
Timing results are observations for the stated workload, not production guarantees.

## Source and evidence boundaries

| Experiment | Source and method |
| --- | --- |
| Identity/routing | Catalog baseline 2727fdc; same strategy/mock harness and Native edd0b0f; three ABBA blocks, six captures per version |
| Pending/query and callback comparison | Catalog baseline d1c84bb; matched harness and Native edd0b0f; repeated ABBA blocks with explicit INFO/WARN controls |
| Purge/timer | Catalog 8de7d28 plus archived working-tree changes; Native 797d986 plus recorded repair files; this source pair differs from the other experiments |
| Public snapshot/depth | Native edd0b0f; exact numeric decoding on both CPU routes; three independent captures per workload |
| Signing reference | Predict Python SDK 6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1 |

Sanitized protocol fixtures are versioned and hash-indexed in the
[evidence manifest](../../crates/nautilus-predict/tests/fixtures/evidence_manifest.json).
Synthetic failure scenarios, derived replays and real exchange captures are labeled
separately. Operator source archives, executable hashes, raw logs and measurements
stay under Git-ignored data/predict-validation/ and are not distributed test inputs.
Published summaries do not imply access to those local archives.

## Regression coverage

| Boundary | Observable check |
| --- | --- |
| Protocol and signing | Exact numeric tokens/raw units, strict decimal IDs, SDK amount/hash/signature vectors and identity conflicts |
| Public data | Custom/depth/BBO ownership, both transports, reconnect/reset, stale-frame handling, rollover and Parquet readback |
| Commands | No funds/OPEN/metadata/book preflight; known-ID cancel, explicit unsupported intent and unknown-write handling |
| Strategy callbacks | Applied Native state, maker cancel/replacement, taker fills, early settlement, duplicate and late messages |
| Account/recovery | Asynchronous cash, order-local reservations, tight-cash replacement, scoped recovery and post-IO consistency |
| Retention | Native purge, nonempty position links, unresolved ownership, cold/verified timer retirement and actual strategy delivery |

The accepted full suites recorded 508 Native-feature and 492 legacy-feature tests
passing. After removing data timing probes, focused recording validation passed
153 tests and the legacy data-client suite passed four. Strict all-target Clippy,
minimal core/live builds, repository hooks and all 415 registered fixture hashes
passed. These counts identify the validation snapshot; the test guide defines the
commands rather than a permanently fixed expected test count.

## Message identity and numeric routing

The strict decimal uint256 codec avoids a temporary JSON wrapper. Wallet parsing
reuses message-derived venue/instrument identities while retaining Native ownership
checks. Six-capture median Criterion mean estimates:

| Numeric ID | DOM/string wrapper, ns | Direct codec, ns |
| --- | ---: | ---: |
| 10 digits | 45.97 | 16.61 |
| uint256 maximum | 180.79 | 159.35 |

Matched actual Rust strategy observations, in microseconds, are medians of each
capture's nearest-rank p50/p95/p99:

| Segment | Baseline | Direct codec/identity reuse |
| --- | --- | --- |
| Confirmed WS fill → Filled callback | 41.81 / 63.19 / 76.27 | 41.31 / 63.85 / 73.04 |
| Maker cancel confirmation → replacement POST | 254.10 / 299.21 / 329.04 | 252.19 / 296.04 / 323.69 |
| Taker Filled callback → next POST | 166.52 / 197.54 / 244.00 | 165.90 / 195.19 / 264.29 |

API counts and economic assertions match. Tail observations overlap and move in
both directions; the small codec gain does not establish a stable end-to-end
callback improvement or regression.

## Public snapshot and native depth delivery

The recording schema preserves full YES levels in their source order, optional
settlement fields, fixed-precision normalization, event/init timestamps and rolling
selector metadata. Numeric JSON tokens are decoded without f64 through
rust_decimal's standard arbitrary-precision support. A real token
3900.9999999999995 exposed the former conversion to 3900.9999999999990; a regression
covers this quantity and long decimal settlement fields. Historical files are not
rewritten.

The read-only mainnet probe uses Rust LiveNode strategies on BTC 5m/15m markets
with Sockudo and Tungstenite. Combined Custom + YES/NO depth, depth-only, YES-only
and resubscribed Custom phases produced 160 Custom and 400 depth callbacks.
An independent Decimal comparison matched every level and the recorded optional
fields. All 16 subscription/removal requests received successful venue ACKs;
partial removal preserved remaining owners. Managed book/BBO prices were available
inside depth callbacks. No execution client, signer, account RPC or order writes
participated. This short probe did not force reconnect or cross natural rollover;
those transitions have separate offline coverage.

Historical CPU measurements include complete Wire-to-snapshot conversion. Both
routes use exact numeric decoding and synthetic levels matching venue JSON numbers.
Values are medians of three captures' Criterion median estimates:

| Levels per side | DOM, us | Borrowed payload, us | Reduction |
| --- | --- | --- | --- |
| 10 | 17.43 | 7.16 | 58.9% |
| 100 | 120.41 | 48.15 | 60.0% |
| 1,000 | 1121.96 | 440.47 | 60.7% |

Separate loopback strategies sent 210 causally acknowledged frames per mode, with
ten warmups excluded. Medians of three captures' per-capture quantiles:

| Callback mode | p50 / p95 / p99, us |
| --- | --- |
| Custom only | 57.12 / 81.50 / 89.79 |
| Unmanaged depth | 68.29 / 81.75 / 83.71 |
| Managed depth with Native quotes | 68.42 / 101.75 / 112.29 |
| Combined: depth callback | 72.88 / 95.29 / 100.62 |
| Combined: Custom callback | 74.88 / 98.42 / 103.79 |

The interval begins before loopback server send and ends at the first callback
line, including local transport, parsing and Native dispatch/book maintenance.
It excludes signal-body work and does not measure exchange network latency.
Managed-depth p99 ranged 81.96–127.67 us across captures. Mode comparisons are not
a before/after whole-strategy test; independent percentiles must not be added.

Dedicated market-data timing probes, the CPU benchmark and latency JSON exporter
were retired after validation. Functional regressions and the opt-in public
mainnet lifecycle probe remain without callback timing collection. Event/init
timestamps remain data fields. The measured sources/results are retained in the
operator's market-data-mainnet-20261022 archive; the earlier quoted-level experiment
is a different workload and is not used for direct numeric-input comparisons.
Execution timing instrumentation remains available independently.

## Pending and target-query validation

Target query tests assert one order GET for zero fills and two order GETs plus
market-scoped matches for nonzero fills. Identity, signed terms and complete raw
history checks remain. Bulk recovery also covers positions without current orders.

With 9,999 applied fills and 256 pending facts, six-capture median p50 wallet
preparation decreased from 60.04 to 13.73 us; lifecycle raw summation decreased from
44.83 to 8.77 us. Zero-pending preparation remained about 1 us. These CPU windows
exclude parsing, initial quantity aggregation, snapshot locking, transport and
Native callbacks; short-order replays do not replace long-history measurements.

## Matched taker callback comparison

Release executables use the same Native dependency, strategy/mock source and logger
level. One warmup is excluded per version/control. Six ABBA blocks per INFO/WARN
level yield twelve captures per version/level. Each capture has 201 taker fills,
so nearest-rank p99 is the third-largest observation. Values below are medians of
capture quantiles; paired intervals bootstrap mean block differences with 10,000
resamples. They are exploratory intervals, not an equivalence test.

| Logger / segment | Baseline p50 / p95 / p99, us | Optimized p50 / p95 / p99, us | Paired p99 difference 95% interval, us |
| --- | --- | --- | --- |
| INFO / WS fill → strategy | 52.19 / 80.58 / 108.29 | 49.56 / 76.79 / 89.69 | -23.77 to 11.67 |
| WARN / WS fill → strategy | 43.98 / 71.21 / 91.04 | 43.54 / 71.13 / 88.00 | -14.15 to 11.96 |
| INFO / Filled callback → next POST | 190.92 / 243.90 / 355.85 | 186.17 / 235.81 / 311.56 | -60.07 to 8.22 |
| WARN / Filled callback → next POST | 170.56 / 217.50 / 310.35 | 171.77 / 216.38 / 294.21 | -63.03 to -1.98 |

WS callback p99 intervals span zero at both logger levels. The comparison does not
establish a stable adapter regression. Native formats order events before invoking
strategy hooks: WARN reduced typical callback cost, but its p99 effect also spans
zero. Native enqueue/scheduling and the loopback driver are included; no OS,
executor or lock-wait contribution is individually attributed.

API parity and economics hold across captures: maker has 101 creates/101 removals;
taker has 201 creates/one removal. Each has 201 Filled, 202 early Submitted,
201 Confirmed and one Failed notice, with no lost/invalid correlations. Metadata
misses and book GETs are zero; startup/background reads are counted separately.
Final orders and positions are empty. Results are local; no venue orders were sent.

## Timer CPU comparison

VirtualClock drives the installed production timer. Values are medians of four
complete sweeps including final destruction; setup and one warmup are excluded.
The historical source pair is listed above and must not be mixed with other tests.

| Owners × fills | Prefix | Complete sweep before → after, ms | First callback before → after, ms |
| --- | --- | --- | --- |
| 1 × 100,000 | Cold | 22.401 → 23.061 | 22.401 → 23.061 |
| 1 × 100,000 | Verified | 16.120 → 14.603 | 16.120 → 14.603 |
| 32 × 9,999 | Cold | 316.014 → 78.979 | 23.197 → 20.126 |
| 32 × 9,999 | Verified | 42.497 → 54.319 | 1.136 → 1.354 |

Repeated cold membership work is reduced; single-owner validation/disposal remains
linear, and the verified multi-owner observation regressed. These are not universal
cleanup improvements or pause bounds. A separate actual LiveClock replay imported
100,000 REST facts, received independent books, retired Native ownership and
verified delivery. Its waiting window also includes account IO and probe scheduling,
so wall time is not timer CPU and cannot establish a hard latency guarantee.

## Two-repository handoff

CI, bootstrap and installation pin
[edd0b0f1588f241690f578ef8b141a947997486a](https://github.com/graceyangfan/nautilus_trader/commit/edd0b0f1588f241690f578ef8b141a947997486a).
Use make bootstrap-deps for a clean checkout. This fork supplies borrowed history
access, Portfolio-first account publication, snapshot-covered fill handling and
physical purge repairs; an arbitrary upstream develop checkout is not equivalent.

Native dependency validation included 435 Cache tests, 52 order-model tests,
account publication coverage and twelve snapshot-covered/multi-owner fill cases.
Publish and validate a dependency update first, then update CI/bootstrap/install
pins together. Recheck supported features and callbacks against that exact source.
