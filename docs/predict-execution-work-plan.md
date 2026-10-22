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
| Wallet feedback | Own live lifecycle and confirmed fills use Native events; recovery retains precision/report handling |
| Early settlement | Strategy-scoped CustomData for Submitted/Confirmed/Failed, separate from economic fill accounting |
| Native views | Borrowed fill indexes and observed order views replace repeated full-history copies |
| Raw normalization | Verified history permits appended-prefix processing; pending facts borrow existing indexes; cold evidence retains full validation |
| Accounts | Order-local projection with proven prior state; remote cash refresh and OPEN verification remain separate |
| Publication | Native account application/publication flushes deferred reservations without a new ACK protocol |
| Recovery | Scoped snapshots reuse observed history and facts; unresolved commands prevent futile automatic capture; target queries share collection/conversion |
| Retention | Native-owned lifetimes, bounded adapter retirement and no 10,000-entry fact/Accepted FIFO caps |
| Purge | Existing Native APIs retain full index cleanup with fewer temporary allocations/history copies |
| Reset | Unresolved submissions/cancellations preserve ownership until reconciled |
| Dependency | Published Native repairs are pinned consistently in CI, bootstrap and installation documentation |

## Remaining priorities

| Priority | Work | Acceptance criterion |
| --- | --- | --- |
| Lower | First cold validation and single large-history destruction during occasional purge | Keep current Native cleanup; revisit only with callback evidence of operational impact |
| Measurement-driven | In-service full recovery, instrument scans and OPEN copy-on-write | Demonstrated repeated work removed without weakening late-fill, ownership or post-IO checks |
| Measurement-driven | Remaining bulk/context preparation and query-history IO | Remove proven repetition without new caches, preserving batch atomicity, raw units and position-only coverage |

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
