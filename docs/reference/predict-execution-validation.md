# Predict execution validation and handoff

Measurement date: 2026-10-22. Results use local simulated services and the repaired
Native dependency. See the [architecture](../predict-execution-adapter-design.md),
[current review](../predict-execution-structure-review.md) and
[test commands](../../crates/nautilus-predict/tests/README.md).

## Source and evidence boundaries

| Item | Recorded source |
| --- | --- |
| Purge comparison baseline | 8de7d28b210353aee8548fe770993b69792b6d73 plus the archived cumulative working-tree changes |
| Native measurement baseline | 797d98618ddbb26dd2bf843df52dd494daad8c7b plus nine local Native repair files |
| Published Native dependency | [graceyangfan/nautilus_trader edd0b0f158](https://github.com/graceyangfan/nautilus_trader/commit/edd0b0f1588f241690f578ef8b141a947997486a) |
| Earlier purge upstream comparison | e88439b64dfe818e395401b8baef7a79b874ca68 |
| Pending/query upstream comparison | b5a36d3301f358c19a487dfcb35f12f188c030ca; inspected, not merged |
| Pending/query baseline and evidence | d1c84bb plus an explicit benchmark harness; ignored data/predict-validation/execution-pending-query-20261022/ |
| Predict amount/signing reference | Python SDK 6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1 |
| Purge and timer evidence | Local ignored data/predict-validation/execution-review-followup-20261022/ |
| Matched callback evidence | d1c84bb versus the pending/query implementation; ignored data/predict-validation/taker-tail-20261022/ |

Each scenario's local evidence bundle retains source manifests and archives,
reproduction patches, complete captures, checks and machine-readable summaries.
The purge comparison uses its archived Native repair sources; the pending/query
and matched callback comparisons use the published Native revision. Measurements
from different source pairs are kept separate. Raw archives and diagnostic logs
are excluded from Git. This versioned summary and retained exporters let reviewers
reproduce the scenarios without requiring the local bundles.

Immutable sanitized venue recordings remain under tests/fixtures and test_data,
indexed by the [evidence manifest](../../crates/nautilus-predict/tests/fixtures/evidence_manifest.json).
They are distinct from the new mock replays. Historical live observations are in
[Predict live evidence](predict-live-evidence.md). The measurements below include
no live venue orders or new venue recording.

## Accepted implementation checks

| Check | Result |
| --- | --- |
| Full native-quantity-commission suite | 498 passed, 0 failed, 120 ignored; 17 targets; default parallel scheduling also passes |
| Legacy execution suite | 482 passed, 0 failed, 109 ignored; default parallel scheduling |
| Default workspace tests, preceding delivery | 331 passed, 0 failed, 3 ignored |
| Unchanged Python surface / immutable fixtures | Preceding 161 tests passed; 415 fixture hashes reverified |
| Strict Native-feature all-target Clippy | Passed with -D warnings |
| No-default-feature Native library | Passed |
| Product predict-execution + native-quantity-commission | Passed |
| Repository hooks | Full repository hooks passed |
| Purge negative behavioral controls | Four failures as expected; original source restored exactly |
| Pending/query negative behavioral controls | Two failures as expected; original source restored exactly |
| Simulated strategy callbacks, per continuous capture | 201 Filled, 202 early Submitted, 201 Confirmed, 1 Failed |
| Normal maker/taker API counters | Exactly matched prior accepted capture |
| LiveClock evidence scenarios | Cold and verified, each with 100,000 REST-populated facts; all emitted books delivered |
| Native source/HEAD in final review-follow-up | Unchanged; earlier Native repairs still required |

The four purge controls remove automatic readiness eligibility, unresolved-cancel
reset protection, membership proof and the unlocked proof phase separately. Each fails a
behavioral regression. Native position purge now establishes both nonempty forward
links before asserting removal.

## Pending and target-query validation

Native purge and cleanup scheduling are unchanged. Target order queries and single
reports share collection/conversion; ordinary instrument reports send marketId.
Zero-fill queries assert one order GET and no matches; nonzero target paths assert
two order GETs, one market-filtered matches request, two retained trade identities
and unchanged Native quantities. Complete recovery keeps its wider inventory scope.
Removing market filtering fails on a nonempty foreign-market response. Removing
the observer publication boundary fails the torn-snapshot regression.

The release CPU exporter uses 10 warmups and 100 samples for each applied/pending
combination. The table fixes 9,999 applied fills; values are microseconds,
before -> final. Wallet preparation excludes parsing, initial normalized-quantity
aggregation, snapshot locking, transport and Native callbacks. It is not the full
wallet-consumer span. Lifecycle values include pending lookup and raw summation.

| Pending | Wallet p50 | Wallet p95 | Wallet p99 | Lifecycle p50 |
| --- | --- | --- | --- | --- |
| 0 | 1.000 -> 1.000 | 1.250 -> 1.042 | 1.334 -> 1.083 | 0.083 -> 0.042 |
| 1 | 1.208 -> 1.042 | 1.250 -> 1.084 | 1.250 -> 1.125 | 0.250 -> 0.084 |
| 32 | 7.416 -> 2.250 | 7.625 -> 2.292 | 7.708 -> 2.375 | 4.667 -> 0.958 |
| 256 | 59.167 -> 13.625 | 59.959 -> 13.833 | 60.583 -> 16.875 | 43.750 -> 8.708 |

The matched strategy comparison below supersedes the preliminary single-baseline
callback comparison. Raw preliminary captures remain in the pending/query bundle;
interim captures are excluded from final-source results. Short continuously traded
orders do not replace long-history or timer measurements.

## Matched taker callback comparison

The earlier one-baseline/four-current captures motivated a controlled comparison;
they did not establish a stable adapter regression. Both versions use Native
edd0b0f, identical strategy/mock sources, release mode and separate processes.
Baseline is d1c84bb; current is the pending/query implementation recorded above.
The callback investigation itself adds no production source changes.

Build and copy both executables before measurement. Run one excluded warmup per
version/control, then ABBA blocks without compilation or concurrent suites:

- Original harness: four ABBA blocks, eight measured captures per version.
- Explicit Native logger control: six ABBA blocks per INFO/WARN level, twelve
  measured captures per version/level; level order alternates between blocks.
- The same pending CPU workload: three ABBA blocks, six captures per version.

Every strategy capture includes the same maker replay and 201 actual taker Filled
callbacks. Those 201 observations make p99 the third-largest sample. Quantiles are
nearest rank within each capture; table entries are medians of capture quantiles,
not percentiles pooled across captures. Paired intervals bootstrap the mean of six
ABBA-block differences (10,000 resamples, seed 20261022). These are exploratory
intervals, not an equivalence test or a production latency guarantee.

| Logger | Actual segment | Baseline p50 / p95 / p99, us | Current p50 / p95 / p99, us | Mean paired p99 difference 95% interval, us |
| --- | --- | --- | --- | --- |
| INFO | WS fill -> strategy | 52.19 / 80.58 / 108.29 | 49.56 / 76.79 / 89.69 | -23.77 to 11.67 |
| INFO | WS fill -> Native send | 21.21 / 28.96 / 37.12 | 20.62 / 26.83 / 33.71 | -6.62 to 3.25 |
| INFO | Native send -> strategy | 30.54 / 54.21 / 73.35 | 29.06 / 51.98 / 62.65 | -17.11 to 12.67 |
| INFO | Filled callback -> next POST | 190.92 / 243.90 / 355.85 | 186.17 / 235.81 / 311.56 | -60.07 to 8.22 |
| WARN | WS fill -> strategy | 43.98 / 71.21 / 91.04 | 43.54 / 71.13 / 88.00 | -14.15 to 11.96 |
| WARN | WS fill -> Native send | 20.71 / 27.29 / 36.25 | 20.08 / 26.92 / 36.10 | -5.15 to 3.08 |
| WARN | Native send -> strategy | 23.31 / 45.44 / 60.42 | 23.35 / 47.58 / 63.08 | -1.38 to 17.78 |
| WARN | Filled callback -> next POST | 170.56 / 217.50 / 310.35 | 171.77 / 216.38 / 294.21 | -63.03 to -1.98 |

The original harness also failed to reproduce a consistent regression: WS-fill
callback p99 medians were 89.81 -> 82.88 us, with overlapping capture ranges.
The WS-fill-to-strategy p99 intervals span zero at both log levels. These results
do not establish the earlier single-run difference as a process defect, nor do
they prove every workload is unaffected. Independent segment percentiles cannot be
added or used to identify the same slow event. Native send timestamps are captured
before emitter enqueue, so send-to-strategy includes enqueue, scheduling and Native
dispatch; it is not a pure Engine CPU measurement. All spans use local monotonic
time and do not measure venue network latency.

Native Strategy::handle_order_event formats/logs the event before invoking
on_order_filled. Portfolio and other Native events also participate in INFO
logging. Changing only the official LoggerConfig reduces typical callback cost:
current WS-fill callback p50 is 49.56 us at INFO and 43.54 us at WARN. The paired
WARN-minus-INFO p50 interval is -6.86 to -3.53 us. The current p99 logging-effect
interval still spans zero; logging is a measured contributor, not an explanation
for every tail sample. Native scheduling and the loopback driver/server are also
inside these measured spans; this experiment does not individually attribute OS,
executor, lock-wait or fixture scheduling costs.

The CPU result remains reproducible. With 9,999 applied fills and 256 pending,
median wallet-preparation p50 across six captures is 60.04 -> 13.73 us; lifecycle
raw summation is 44.83 -> 8.77 us. With zero pending, wallet preparation remains
about 1 us. CPU windows retain the exclusions documented above.

Across the 64 measured strategy captures, each preserves 201 Filled, 202 Submitted,
201 Confirmed and one Failed notification, without lost/dropped/invalid
correlations. API counters match
between versions and log levels: maker has 101 creates/101 removes, and taker has
201 creates/1 remove. Metadata misses and orderbook GETs are zero; startup and
background account reads are reported separately. Final open orders and positions
are zero. The callback/command paths gain no REST/RPC query, account wait or
additional queue. Polymarket, Bybit and OKX likewise send
tracked fills through Native events; the shared Native callback boundary remains
appropriate. Their dispatch sources and Native's strategy handler were inspected
at the pinned comparison revisions.

The callback investigation adds a test-only PREDICT_CONTINUOUS_LOG_LEVEL control
passed to the existing Native LoggerConfig, with INFO retained by default. Product
NodeProfile already passes node.logging through Native unchanged; an existing
profile test now covers WARN. No adapter lock boundary, collector API, funds state,
worker/scheduler or production logging default changes.

Complete execution order, executable/source hashes, all captures, the baseline
private-API benchmark adaptation, bootstrap script and checks are retained under
ignored data/predict-validation/taker-tail-20261022/. The short strategy runs do
not measure large-history destruction or physical purge. No live venue orders
were sent.

## Timer CPU comparison

Same installed production timer via VirtualClock. Values below are medians of four
complete sweeps, including final destruction; setup and one warmup are excluded.
They are neither live network latency nor guaranteed production pause bounds.

| Owners × fills | Prefix | Complete sweep before → final, ms | First callback before → final, ms |
| --- | --- | --- | --- |
| 1 × 100,000 | Cold | 22.401 → 23.061 | 22.401 → 23.061 |
| 1 × 100,000 | Verified | 16.120 → 14.603 | 16.120 → 14.603 |
| 32 × 9,999 | Cold | 316.014 → 78.979 | 23.197 → 20.126 |
| 32 × 9,999 | Verified | 42.497 → 54.319 | 1.136 → 1.354 |

Repeated cold membership work is reduced. Initial validation and single-owner
physical disposal remain linear; the verified multi-owner observation regressed.
Do not describe this as universal cleanup acceleration or a hard latency budget.

A separate actual LiveClock replay populates facts through 1,000 paginated REST
requests, receives independent 1 ms WS books, purges Native ownership and waits
for real timer retirement. A late ACK's unknown-owner gate proves removal. Periodic
account IO and 10 ms ACK probes participate. Its waiting window is elapsed wall time,
not the CPU duration of a callback. Cold window book-delivery p99 observed
50.809→5.608 ms and verified 0.856→1.674 ms, each from one run. Different waiting
windows/sample counts and background scheduling prevent attributing all differences
to cleanup. Whole-session setup is separate from the timer window.

## Two-repository handoff

CI and scripts/bootstrap-deps.sh pin the published Native revision
[edd0b0f1588f241690f578ef8b141a947997486a](https://github.com/graceyangfan/nautilus_trader/commit/edd0b0f1588f241690f578ef8b141a947997486a).
Use make bootstrap-deps for a clean checkout. Both repositories must use this
matching source pair; an arbitrary upstream develop checkout is not equivalent.
The Native commit includes borrowed event/history access, Portfolio-first account
publication, snapshot-covered direct-fill handling and physical purge repairs.

Native validation includes 435 Cache tests, 52 order-model tests, the account
publication regression and 12 snapshot-covered/multi-owner fill cases. Native
make format and the full make pre-commit gate pass using python/.venv. No
production-source change was needed during the final commit checks.

For future dependency updates, publish and validate the Native commit first, then
update CI/bootstrap/installation pins together. Recheck the adapter's supported
features and callbacks against that exact source. Keep local credentials, raw
operational logs and temporary archives outside versioned input.
