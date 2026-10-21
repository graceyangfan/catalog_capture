# Predict execution development status

The [architecture](predict-execution-adapter-design.md) defines the supported
contract. The [structure review](predict-execution-structure-review.md) describes
module ownership and remaining costs. Reproduction commands and accepted results
are maintained in the [test guide](../crates/nautilus-predict/tests/README.md) and
[validation record](reference/predict-execution-validation.md).

## Design constraints

- Strategies own trading intent, sizing, slippage, exposure and hedge policy.
- Native risk, Engine, Cache and Portfolio own admission, economic state and callbacks.
- The adapter owns protocol conversion, signing, identity and venue facts.
- Normal commands and callbacks do not wait for account audits, full recovery,
  historical cleanup or metadata/orderbook requests.
- Preserve precision, idempotency, unknown write outcomes, late fills, multi-strategy
  ownership and post-IO consistency checks.
- Reuse Native APIs and existing containers rather than another OMS, funds ledger,
  recovery scheduler or cleanup manager.

## Implemented behavior

| Area | Current implementation |
| --- | --- |
| Commands | No adapter lifetime trading budget or per-order funds/OPEN audit; known-ID cancel uses one POST |
| Market preparation | Background metadata/nonce preparation; both MARKET sides require a strategy price; no command GET fallback |
| Wallet feedback | Own lifecycle facts use Native events; confirmed fills retain exact precision and reports |
| Early settlement | Strategy-scoped CustomData for Submitted/Confirmed/Failed, separate from economic fill accounting |
| Native views | Borrowed fill indexes and observed order views replace repeated full-history copies |
| Raw normalization | Verified history identity/cutoff permits appended-prefix processing; cold evidence retains complete validation |
| Accounts | Order-local projection with proven prior state; remote cash refresh and OPEN verification remain separate |
| Publication | Native account application/publication flushes deferred reservations without a new ACK protocol |
| Recovery | Scoped snapshots reuse observed history and collected facts; unresolved commands prevent futile automatic capture |
| Retention | Native-owned lifetimes, bounded adapter retirement and no 10,000-entry fact/Accepted FIFO caps |
| Purge | Existing Native APIs retain full index cleanup with fewer temporary allocations/history copies |
| Reset | Unresolved submissions/cancellations preserve ownership until reconciled |
| Dependency | Published Native repairs are pinned consistently in CI, bootstrap and installation documentation |

## Remaining priorities

| Priority | Work | Acceptance criterion |
| --- | --- | --- |
| Measurement-driven | First cold validation and single large-history destruction during trading | Same-harness complete CPU sweeps and actual strategy tail latency, with nonempty evidence and a real timer |
| Measurement-driven | In-service full recovery, instrument scans and OPEN copy-on-write | Demonstrated repeated work removed without weakening late-fill, ownership or post-IO checks |
| Lower | Report/context orchestration | Smaller interfaces and shared preparation while preserving batch conflict atomicity and position-only instrument protection |

Startup speed has lower priority than trading-time feedback. Bounded work does not
establish a hard pause limit for one oversized owner. Additional caches, background
disposal queues and pre-signed order grids require evidence of a concrete benefit.

## Validation requirements

1. Assert observable Native state and subscriber callbacks, with nonempty controls
   and regressions that fail when the relevant behavior is removed.
2. Use native-quantity-commission for the maker/taker accounting contract; label
   legacy reconciliation separately.
3. Measure confirmed WS fill to actual strategy callback, cancellation confirmation
   to replacement POST, API counts and retention, alongside CPU microbenchmarks.
4. Keep timer CPU, wall-clock waiting windows and live network measurements distinct.
5. Run focused checks after changes, then repository gates. Do not repeat expensive
   suites without a new change, failure or unresolved concern.
6. Use loopback services by default. Live venue writes require a separate bounded
   calibration scope; offline validation never implies venue acceptance.
