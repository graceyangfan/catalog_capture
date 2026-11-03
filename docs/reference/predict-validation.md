# Predict validation

Behavior is defined by the [execution contract](../predict-execution-adapter-design.md)
and [market-data guide](../how_to/predict-market-data.md). Runnable commands and
focused regression groups are in the [test guide](../../crates/nautilus-predict/tests/README.md).

## Dependency contract

Build against the fork revision pinned in [installation](../getting_started/installation.md).
It combines official develop with inventory-unit commissions, exact gross/order
scale alignment, opt-in unresolved cancel/modify retention and terminal quantity
normalization. Native owns Cache, reports, order/position application and events;
the adapter owns protocol and raw venue evidence. See
[quantity commissions](quantity-commission.md).

## Test selection

| Changed boundary | Required evidence |
| --- | --- |
| Public trades/depth | Atomic conversion, direction/match types, reconnect/ownership, Strategy callbacks and catalog rollover readback |
| Signing and command parameters | SDK vectors, exact signed amounts and local request assertions |
| Fills, fees and inventory | Recorded settlement replay, gross/net accounting, duplicate suppression and subscriber-visible state |
| Recovery | Pending commands, late fills, terminal normalization and replay controls |
| Session/transport | Loopback renewal, ACK, reconnect and cancellation tests |
| Documentation/fixtures | Offline manifest verification; no funded venue run |

Ordinary tests use local services and saved inputs. Named ignored probes require
their documented scope. An offline replay does not establish live venue latency,
partial-tail behavior, long outages or general FOK enforcement. SDK acceptance
alone does not establish venue matching semantics.

## November 2026 DST rollover regression

On November 1, 2026, New York's 1AM hour occurs twice: once at 05:00 UTC
(EDT) and once at 06:00 UTC (EST). The previous title parser required a single
local-to-UTC result. Either endpoint being ambiguous was enough to fail: the
`12:55AM-1AM` five-minute window reproduced the defect before 1AM began.
Discovery then discarded the failed 2026 interpretation and accepted a 2027
interpretation. The future-window scheduler used that start directly as its
next refresh deadline.

Operator diagnosis placed the 15-minute selectors' stall at 04:45 UTC and the
five-minute selectors' stall at 04:55 UTC. All six BTC/ETH/BNB selectors could
therefore retain subscriptions to expired markets rather than discover current
markets. With no new executions on those markets, TradeTick output stops even
though its subscription and persistence path remains valid. Files previously
downloaded and removed from the capture directory cannot establish whether
trades were recorded before the stall.

The operator separately reported a 25-second live check of market `4148609`:
496 orderbook frames, 362 trade frames, and `success: true` acknowledgements for
both `predictOrderbook/4148609` and `predictTrades/4148609`. Reported trade fields
were `marketId`, `tsMs`, `taker`, and `makers`, matching `TradeWire`. This is
operator-reported evidence, not a versioned raw capture or a new-binary venue
acceptance test. An earlier diagnosis based on the public documentation omitting
`predictTrades` was incorrect; this fix retains the trade topic and projection.

The fix separates calendar-year inference from DST clock conversion. Rolling
category epochs identify the correct fold, elapsed duration validates endpoints,
and instrument definitions reuse the selected UTC window. Ambiguous search
results without a UTC category identity remain errors subject to retry. Future
discoveries wait until the earlier of the market start and the retry deadline;
active windows retain their normal end deadline.

Local validation:

- `cargo test -p nautilus-predict`: 98 passed, 8 ignored, no failures.
- Four new regressions failed after restoring the old discovery/scheduling
  implementation: both folds, nonexistent spring times, early-successor category
  discovery, and a distant-future refresh deadline.
- The six-selector loopback test walks 05:00 through 07:00 UTC, checking instrument
  activation/expiration, pending-to-current transition after a snapshot, output
  identity, old subscriptions, and registry cleanup. Additional tests cover
  year rollover and ambiguous search fallback.
- Product-crate Clippy with `-D warnings`, workspace formatting, `git diff --check`,
  and `cargo check -p nautilus-predict --no-default-features` passed.

Deployment acceptance still requires restarting with the fixed binary, checking
all six selectors against current UTC windows, and confirming new TradeTicks in
sealed catalog files. The local regression results do not establish that a
running recorder has been upgraded.

## Evidence integrity

Sanitized regression inputs are indexed by
[evidence_manifest.json](../../crates/nautilus-predict/tests/fixtures/evidence_manifest.json).
Keep original primary fixtures immutable. Synthetic schedules and locally rebound
identity/time are derived replay, even when economics come from real fills.
Historical captures apply to their recorded revision and do not validate a newer
binary. Local operational captures belong in ignored `data/` and can be removed
without deleting versioned regression fixtures.

## Measurement requirements

Record source revision, features, lockfile, logging and harness settings. Compare
venue timestamps, ingestion times and strategy callback times separately. Source
cadence is not transport latency; relays add forwarding overhead. Exclude
compilation, fixture setup and concurrent benchmarks from timed samples. Native
event logging is part of callback latency and must be held constant between runs.
