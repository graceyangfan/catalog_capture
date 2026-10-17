# Predict execution work plan

Current behavior: [adapter contract](predict-execution-adapter-design.md).
Tests and evidence: [test guide](../crates/nautilus-predict/tests/README.md).

## Priorities

1. **Deterministic offline execution tests.** Investigate the recorded-startup barrier timeout observed in a default-parallel run. The isolated test and four-thread suite passed; the cause remains unconfirmed. Preserve the asserted lifecycle and do not mask failures by increasing timeouts.
2. **Account recovery boundary.** QueryAccount still verifies OPEN orders, settlement coverage, positions and chain balances during recovery. Identify an existing Native report-application completion contract before removing further checks. Keep generation, unapplied-fill and balance-freshness protection.
3. **Long-running evidence.** Exercise real partial SELL, cancel/fill races and prolonged outages when operationally appropriate. Offline coverage is already available; it does not establish venue behavior under these conditions. Real orders require renewed operator authorization.
4. **Raw settlement capacity.** Evaluate the existing 10,000-entry evidence bound using observed growth. Any release policy must preserve deduplication, cumulative precision and the current inventory's required prefix; do not introduce FIFO eviction.

## Maintenance constraints

- Native owns OMS, positions, Portfolio, persistence, risk and reconciliation scheduling.
- The adapter owns venue protocol interpretation, authentication and resource evidence; strategies own trading intent and take-profit/stop-loss decisions.
- Keep actual captures immutable, credential-redacted and hash-verified. Mark derived and synthetic fixtures explicitly.
- Keep fixtures, test strategies and capture supervisors in tests. Prefer existing helpers and focused scenarios to duplicate full-system replay exporters.
- Preserve public capture, market discovery and subscription rollover behavior. Changes to the Nautilus dependency require separate agreement.
