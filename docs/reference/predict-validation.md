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
