# Predict Rust strategy and LiveNode calibration

Calibration strategies are test support. They use Predict client factories,
Native LiveNode, OrderFactory, RiskEngine, ExecutionEngine and Portfolio. Production
public recording and Up/Down subscription rotation are independent of these hosts.

## Configuration

Follow the [private API guide](predict-private-api-tests.md) for credentials and
exact monetary limits. Strategy controls live separately in
`env/predict-strategy.toml`. Create it only if missing:

```bash
mkdir -p env
[ -e env/predict-strategy.toml ] || cp examples/predict-strategy.toml.example env/predict-strategy.toml
chmod 600 env/predict-strategy.toml
```

```toml
[strategy]
phase = "observe"
run_secs = 30
stale_after_ms = 2000
minimum_rebound_ticks = 1
```

Market/token/instrument identities, outcome, price and quantity are discovered or
calculated at runtime. `stale_after_ms` is a strategy signal guard. It does not
implement authentication expiry or replace adapter execution checks.

## Offline scenarios

```bash
CARGO_INCREMENTAL=0 cargo test -p nautilus-predict --features execution --locked --test execution rust_strategy_livenode -- --test-threads=4
CARGO_INCREMENTAL=0 cargo test -p nautilus-predict --features native-quantity-commission --locked --test execution recorded_startup:: -- --test-threads=4
```

The [test guide](../../crates/nautilus-predict/tests/README.md) describes the layers
and complete regression commands. Normal tests connect to local venues and use
saved protocol facts without operator credentials.

| Scenario | Strategy action | Native observation |
| --- | --- | --- |
| Observe | Subscribe to canonical custom snapshots; do not submit | Nonempty data, applied reported account, zero POSTs |
| Resting | Submit a bounded LIMIT, request unsupported amend, cancel and replace only after terminal confirmation | Submitted/Accepted, ModifyRejected, unchanged terms, Canceled and no fill |
| Multi-order | Replace one order while another remains active | Native risk/reservations permit replacement without requiring whole-account locked zero |
| Partial fill and exit | Apply confirmed fills, cancel remaining entry/TP, submit only remaining net inventory | Gross order progress, net positions, fees, deduplication and asynchronous account update |
| Startup/adoption | Restore current holding and an existing TP before strategy start | Cache ownership established without duplicate entry or TP |

Snapshot signals are deterministic test heuristics, not evidence of profitable
reversal. Invalid, empty, crossed or stale signal books do not justify trading.
Attempted BUY notional consumes the configured lifetime cap and is not refunded
when an order is canceled.

## Strategy execution contract

The strategy computes signals from `PredictOrderbookSnapshot`. QuoteTicks are
routed through Native DataEngine where Native risk/valuation needs them. No
parallel order/position/account ledger is maintained by the strategy.

Take-profit is an ordinary maker SELL above entry price. Stop-loss is a strategy
P&L decision: cancel the active TP, wait for a confirmed terminal outcome, re-read
remaining net inventory and submit a taker MARKET SELL. A fill during cancellation
changes that quantity and may eliminate the stop order. Neither path bypasses
minimum notional, inventory/approval checks or configured budgets. Entry and exit
fees must be included when evaluating the cash break-even price.

Predict amend is not implemented. `modify_order` produces OrderModifyRejected;
replacement uses a new ClientOrderId and signed hash. Unknown submission or
cancellation results must be resolved through Native query/reconciliation rather
than automatically retried writes.

Order/position callbacks observe applied Native state. Reported cash can arrive
later; available account notifications and Cache reads are used together. The
pinned fork does not guarantee an account-topic callback for every balance refresh.
Native quote realized PnL and all-fee cash return are distinct measures.

## Read-only mainnet observation

Keep `test.allow_order_writes = false` and `strategy.phase = "observe"`:

```bash
cargo test -p nautilus-predict --features execution --locked --test private_api_live mainnet_rust_strategy_livenode_observe -- --ignored --nocapture
```

This named probe discovers a market, authenticates, runs the snapshot strategy
for its configured interval and disconnects. Native order submission stays disabled.
For renewal and explicit QueryAccount/QueryOrder checks, use the private API guide.
Do not run every ignored test: some are separately gated funded runners.

## Supervised funded runs

The root `tests/run_predict_resting_live.py`, `run_predict_fill_live.py` and
`run_predict_exit_only_live.py` supervisors are explicit funded tools. Review their
arguments, selected mode and monetary bounds before use. A successful offline
run or a TOML write flag alone is not authorization for another funded test.

The continuous exit mode uses the opt-in quantity-commission fork and can retain
an active maker TP while waiting for the strategy exit. Entry has a deadline;
holding need not. Stopping/failing cleans this run's tracked orders but can leave
inventory, and no strategy stop-loss runs after the process stops. Exit-only mode
must not add a new BUY. The narrow adoption helper is not a generic multi-strategy
intent-recovery service.

Supervise collectors and preserve raw redacted HTTP/RPC, wallet JSONL, independent
matches/chain checks and Native callbacks together. Journals and checkpoints are
for bounded calibration, not unlimited daemon storage. Use the existing catalog
for long public recording. Follow the test guide's immutable-fixture procedure;
never regenerate historical expected data in place.

[Live evidence and limits](../reference/predict-live-evidence.md) documents actual
maker, batch, startup, entry/exit and read-only runs separately from offline cases.
