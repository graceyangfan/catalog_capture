# Predict execution validation and handoff

Measurement date: 2026-10-22. Results use local simulated services and the repaired
Native dependency. See the [architecture](../predict-execution-adapter-design.md),
[current review](../predict-execution-structure-review.md) and
[test commands](../../crates/nautilus-predict/tests/README.md).

## Source and evidence boundaries

| Item | Recorded source |
| --- | --- |
| Catalog starting HEAD | 8de7d28b210353aee8548fe770993b69792b6d73 plus cumulative working-tree changes |
| Native measurement baseline | 797d98618ddbb26dd2bf843df52dd494daad8c7b plus nine local Native repair files |
| Published Native dependency | [graceyangfan/nautilus_trader edd0b0f158](https://github.com/graceyangfan/nautilus_trader/commit/edd0b0f1588f241690f578ef8b141a947997486a) |
| Fetched upstream comparison | e88439b64dfe818e395401b8baef7a79b874ca68; not a claim about future upstream HEAD |
| Predict amount/signing reference | Python SDK 6da1708e1f3a9341f5cfd8d0b91bb66c8f43bef1 |
| Final review-follow-up evidence | Local ignored data/predict-validation/execution-review-followup-20261022/ |

The final local evidence bundle contains before/delivery source manifests and
archives, round-only.patch, reconstructed-source audit, negative controls, complete
suite output, final gates, CPU/callback JSON, summary.json and verification.json.
Only final captures support the results below. Source compatibility records
distinguish non-executable annotations from measured runtime code. Raw archives
and diagnostic logs are not Git inputs.
This versioned summary and retained exporters let reviewers reproduce the scenarios;
local bundles are not required to run regression tests.

Immutable sanitized venue recordings remain under tests/fixtures and test_data,
indexed by the [evidence manifest](../../crates/nautilus-predict/tests/fixtures/evidence_manifest.json).
They are distinct from the new mock replays. Historical live observations are in
[Predict live evidence](predict-live-evidence.md). The measurements below include
no live venue orders or new venue recording.

## Accepted implementation checks

| Check | Result |
| --- | --- |
| Full native-quantity-commission suite | 497 passed, 0 failed, 119 ignored; 17 targets; default parallel scheduling also passes |
| Legacy execution suite | 481 passed, 0 failed, 108 ignored; default parallel scheduling |
| Default workspace tests | 331 passed, 0 failed, 3 ignored |
| Offline Python tests / immutable fixtures | 161 tests passed; 415 fixture hashes verified |
| Strict Native-feature all-target Clippy | Passed with -D warnings |
| No-default-feature Native library | Passed |
| Product predict-execution + native-quantity-commission | Passed |
| Repository hooks | Passed on the implementation delivery files |
| Negative behavioral controls | Four failures as expected; original source restored exactly |
| Actual simulated strategy callbacks | 201 Filled, 202 early Submitted, 201 Confirmed, 1 Failed |
| Normal maker/taker API counters | Exactly matched prior accepted capture |
| LiveClock evidence scenarios | Cold and verified, each with 100,000 REST-populated facts; all emitted books delivered |
| Native source/HEAD in final review-follow-up | Unchanged; earlier Native repairs still required |

The controls remove, separately, automatic readiness eligibility, unresolved-cancel
reset protection, membership proof and the unlocked proof phase. Each fails a
behavioral regression. Native position purge now establishes both nonempty forward
links before asserting removal.

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

## Actual strategy execution spans

Final normal loopback replay; these are short continuously traded orders, not the
100,000-fill retirement workload. Counts are independent paired observations.

| Interval | Samples | p50 / p95 / p99, µs |
| --- | --- | --- |
| WS confirmed fill → actual Rust Filled callback | 201 | 49.458 / 64.583 / 83.875 |
| Maker cancellation confirmation → replacement POST | 99 | 249.750 / 310.083 / 435.042 |
| Taker Filled callback → next POST | 199 | 183.958 / 211.208 / 232.916 |

Maker has 101 creates/101 removes; taker has 201 creates/1 remove. Metadata misses
and orderbook GETs are zero. Background/startup account reads are reported separately
in the JSON. Cross-node correlations do not share resetting client-ID keys;
overlapping spans are not summed. The sample counts do not establish extreme-tail
production guarantees.

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
