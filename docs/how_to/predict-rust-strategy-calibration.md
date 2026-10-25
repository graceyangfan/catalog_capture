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

## Logging and callback measurements

The product profile already accepts Native logging configuration directly:

```toml
[node.logging]
stdout_level = "WARN"
```

Choose this explicitly when measuring a quiet trading configuration. INFO remains
the default. Native formats/logs order events before calling strategy hooks, so
logging belongs to WS-to-strategy latency. Rust strategies can also use Native
StrategyConfig.log_events and log_commands to control their event/command logs.
Keep logging identical between compared source versions and record the setting.
The [matched callback validation](../reference/predict-execution-validation.md#historical-measurements)
distinguishes logger cost from run-to-run tail variation; lowering log output does
not establish a universal p99 improvement.

## Market-data subscriptions

Custom snapshots and native depth share Predict's `predictOrderbook/{marketId}`
stream. Use CustomData for complete venue fields and existing recording. Use
`subscribe_book_depth` / `on_book_depth` for a standard L2 snapshot strategy:

```rust,ignore
self.subscribe_book_depth(
    instrument_id,
    BookType::L2_MBP,
    None, // Full depth; Some(NonZeroUsize) limits each derived side.
    Some(ClientId::from("PREDICT")),
    true, // Native maintains the cached OrderBook before the strategy callback.
    None,
);
```

Native `OrderBookDepth` supports more than ten levels. YES uses the venue book;
NO complements prices and swaps sides/sizes. Derived depth is sorted best-first,
with zero-size levels omitted. An empty side replaces the previous side. Sequence
and per-level order counts remain zero because Predict does not supply them.
The complete CustomSnapshot retains its original levels and settlement fields.

For Native MARKET risk pricing and Portfolio valuation, enable the existing
managed-book quote projection in the node profile:

```toml
[node.data_engine]
emit_quotes_from_book = true
```

Keep `emit_quotes_from_book_depths` disabled for this setup; the high-priority
managed book updater establishes the book and quote before ordinary depth callbacks.
`managed=false` avoids maintaining another book when the strategy consumes depth
itself, but does not populate Native risk/valuation prices with this configuration.
CustomData alone likewise does not populate quote prices. The standard
`subscribe_quotes` API remains available for BBO consumers; new managed-depth
strategies need not subscribe to it as well. Use depth/custom timestamps for
book freshness: Native may suppress a derived quote when its prices and sizes are
unchanged even though another complete snapshot has arrived. Native depth ignores
strictly older venue timestamps to avoid rewinding the managed book; equal-timestamp
snapshots are still published. CustomSnapshot recording keeps its original behavior.

Only requested views are published. Custom, depth and explicit BBO interests
retain one shared physical market subscription. Last-owner unsubscribe and reconnect
retain their existing lifecycle. Depth limits for one instrument must agree across
consumers. Rolling discovery retains its existing complete custom-snapshot output;
new depth interests can join discovered instruments without adding another venue
connection. Recording configuration, fields, timestamps, selector provenance and
rollover sealing remain unchanged.

## Offline scenarios

```bash
CARGO_INCREMENTAL=0 cargo test -p nautilus-predict --features execution --locked --test execution rust_strategy_livenode -- --test-threads=4
CARGO_INCREMENTAL=0 cargo test -p nautilus-predict --features execution --locked --test execution recorded_startup:: -- --test-threads=4
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
The test strategy owns its run budget and bounded order count. Production adapter
submission has no lifetime attempted-BUY budget; use Native risk configuration
and strategy exposure limits.

## Strategy execution contract

The strategy computes signals from complete CustomSnapshot or native depth.
Native DataEngine provides managed books and quote projections for risk/valuation. Native
owns confirmed orders, positions and accounts. A strategy may track provisional
hedge decisions by settlement, as described below, without mutating Native state.

Take-profit is an ordinary maker SELL above entry price. Stop-loss is a strategy
P&L decision: cancel the active TP, wait for a confirmed terminal outcome, re-read
remaining net inventory and submit a taker MARKET SELL. A fill during cancellation
changes that quantity and may eliminate the stop order. Both paths retain Native
risk, minimum notional and configured budgets. Venue inventory/approval rejection
is authoritative; submission does not add balance or allowance RPCs. Entry and
exit fees must be included when evaluating the cash break-even price.

The exit strategy reuses its already-validated snapshot depth walk to select the
worst consumed bid as a SELL MARKET reference. It passes the decimal string
through Native `SubmitOrder.params["market_price"]`, including `close_position`.
For example, `params.insert("market_price".into(), serde_json::json!("0.020"))`.
This is a reference before per-order `params["market_order_slippage_bps"]`, not a guaranteed
fill price. It eliminates the adapter's extra execution-book GET. The strategy
still checks snapshot freshness and depth before deciding. Both MARKET BUY and
SELL require this parameter; omitting it produces Native Denied before Submitted.
Taker entry selects the worst consumed ask (NO uses its own outcome prices),
while exit selects the worst consumed bid. Signed BUY collateral includes the
per-order slippage and the SDK's one-USDT-per-share protocol ceiling.

Predict amend is not implemented. `modify_order` produces OrderModifyRejected;
replacement uses a new ClientOrderId and signed hash. Unknown submission or
cancellation results must be resolved through Native query/reconciliation rather
than automatically retried writes.

Order/position callbacks observe applied Native state. Reported cash can arrive
later; available account notifications and Cache reads are used together. The
pinned fork does not guarantee an account-topic callback for every balance refresh.
Native quote realized PnL and all-fee cash return are distinct measures.

## Quantity fees and settlement callbacks

This strategy contract uses the repaired `quantity_commission` Native dependency
and the `execution` build feature, which always enables Native `defi` support and
exact inventory-fee mapping. There is no alternative legacy execution fee policy.

For a BUY-entry / SELL-exit strategy, charged BUY fills deduct shares from received
inventory; charged SELL fills deduct USDT from sale proceeds. These labels describe
trade direction, not a special entry/exit flag. Preserve a zero fee when it is reported; final fee asset and amount come from the
venue rather than a hardcoded maker/taker rate.
The [official fee schedule](https://docs.predict.fun/the-basics/predict-fees-and-limits)
describes maker/taker rates, and the
[wallet schema](https://dev.predict.fun/subscription-topics-1915507m0)
reports `fee.type` as `SHARES` or `COLLATERAL`.

| Confirmed fill | Native order progress | Native inventory / cash treatment |
| --- | --- | --- |
| BUY with a SHARES fee | Gross `last_qty` / `filled_qty` | `quantity_commission` reduces acquired inventory; do not also charge its USDT valuation |
| SELL with a COLLATERAL fee | Sold `last_qty` / `filled_qty` | Inventory decreases by sold shares; USDT proceeds decrease by `commission` |
| Zero-fee maker | Executed gross quantity | No fee deduction; do not invent one from the market's taker rate |

Example with assumed reported fees: BUY 100 shares at 0.40 USDT, paying 2 shares
in fees, completes a 100-share order and acquires 98 shares for 40 USDT. SELL those
98 at 0.50 with a 0.98 USDT fee closes the position and receives 48.02 USDT.
The flat-cycle cash profit is 8.02 USDT; subtracting the BUY fee's 0.80 USDT
valuation again would double-charge it. Gross filled quantity is for order
completion; net Native position quantity is for inventory and exit sizing.
Reported account cash can update asynchronously after the fill.

### Matching, settlement and actions

Predict matches orders off-chain and settles the match on-chain. A transaction
Submitted means a counterpart was matched and settlement was submitted; asset
exchange can still fail. It is not Native `OrderSubmitted`, which describes the
earlier local order submission. The official wallet event definitions are linked
above; the Rust mapping is:

| Wallet WS event | Rust callback | Meaning and action boundary |
| --- | --- | --- |
| `orderTransactionSubmitted` | `on_data(CustomData)` with `PredictSettlementStatus::Submitted` | Pending matched exposure. A strategy may hedge early or cancel other risky quotes. Native fill quantity and position do not change yet. |
| `orderTransactionSuccess` | `Confirmed` custom notice plus normal `on_order_filled` | Settlement succeeded. Native applies the final fill, fees and net position. Do not book the custom notice as another fill. |
| `orderTransactionFailed` | `Failed` custom notice | This settlement failed. Adjust its provisional hedge if one was sent; do not reject, reopen or cancel the entire order from this notice alone. |

Subscribe locally through the normal Rust data interface:

```rust
self.subscribe_data(
    predict_settlement_data_type(account_id, strategy_id),
    None,
    None,
);
```

- **Early hedge:** optionally act on Submitted using cached market/fee estimates.
  Submitted has gross execution facts but no final fee/isMaker evidence. Track
  the decision by `(account_id, venue_order_id, settlement_id)` with strategy
  ownership; a settlement may involve multiple orders. On success, reconcile the
  hedge to confirmed net exposure rather than sending the same hedge twice. On
  failure, correct only the exposure actually hedged for that settlement. The
  adapter does not submit these strategy hedge/compensation orders automatically.
- **Confirmed inventory:** use `on_order_filled`, position events and Native Cache.
  The data and execution channels have no guaranteed cross-channel callback order;
  receiving custom Confirmed alone does not prove the Native fill is applied yet.
  Native net inventory is not automatically free-to-sell inventory: existing SELLs
  and pending matched sales must also be accounted for when sizing another exit.
  Count overlapping live-order and pending-settlement commitments once, not twice.
- **Cancel / reprice:** act on current signals and Native order state without
  waiting for a settlement or querying chain balance before each command. Predict
  has no atomic amend in this adapter: cancel, obtain Native `OrderCanceled`, then
  re-evaluate size and submit a replacement with a new ClientOrderId. Cancellation
  removes remaining book liquidity, not a match already submitted for settlement.
  A late Filled can therefore follow Canceled; use it to update the remaining
  inventory and any replacement/hedge intent, not to discard the fill.

The wallet topic has no snapshot: Submitted need not precede every recovered
Filled. A strategy must handle success without an early notice, deduplicate late
or repeated notifications, and never let a late Submitted reopen a terminal
settlement decision. Native owns final fills, positions, recovery and order
deduplication; provisional hedge intent belongs to the strategy. These callbacks
do not require an extra per-event REST/RPC confirmation or a new chain polling
loop. Account reporting remains asynchronous and Native risk still admits commands.

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

The resting supervisor accepts `--features execution` for the
pinned fork. Its `--execute-minimum-resting` flag explicitly selects a separate
one-run profile with two orders capped at 1 USDT each and 2 USDT in total; these
are not the ordinary read-only profile's 0.10/0.20 USDT limits. Prefer the bounded
GTD cases when manually validating a resting order. Post-only prevents immediate
taking; a resting order can still fill later.

For one bounded latency pilot, prebuild before starting the wallet collector, then
select one GTD case (the same two-order 1/2 USDT limits apply):

```bash
cargo test --release -p nautilus-predict --features execution --locked --test private_api_live --no-run
uv run --python 3.12 --with predict-sdk==0.0.22 --with websocket-client --with python-dotenv --with requests python tests/run_predict_resting_live.py --execute-minimum-resting --case yes-gtd --features execution --measure-latency
```

This attaches the optional probe to execution and the real typed strategy
callbacks. Native feedback is buffered until shutdown, and metric aggregation/JSON
writes occur after LiveNode stops. `latency.json` contains monotonic segment
percentiles, counters and Native runner metrics; `latency_analysis.json` includes
milliseconds, metadata hit rate and private request counts. Historical captures
can contain admission OPEN/resource spans; the current submit path produces none.
Background OPEN/RPC has separate spans; do not sum concurrent requests.
Cancel request time and cancel-decision-to-callback also cover a REST-confirmed
terminal event with no matching WS report.

The safety relay keeps the durable pre-forward create identity journal. In
measurement mode it pools upstream connections per local connection and defers
completion journal writes until close; upstream HTTP durations are reported
separately. HTTP spans still include the relay. The profile submits two orders and requires explicit operator authorization;
p95/p99 are sample maxima, not stable live tail estimates. Public discovery/book
reads and independent collector/supervisor requests are excluded from execution
request counters. The ordinary independent wallet/chain checks and final complete
REST OPEN queries still run. Normal commands do not wait for those checks.

The amendment check expects Native `OrderModifyRejected` and unchanged terms.
It does not call a venue amend endpoint. A subsequent cancel and replacement are
two distinct remote operations, with terminal confirmation required between them.

Successful resting runs print `Live calibration completed` followed by their
archive path under Git-ignored `data/predict-validation/live_resting_<case>_<UTC>/`.
Rust and Python share a transient `fixtures/private_api/live_resting/` directory;
an interrupted run remains there until the next run archives it as `interrupted`.
No prior recording is deleted. Do not start another funded run merely to archive
or inspect a failure. `run_metadata.json` records the source/features used by new
runs. Review with `python3 tests/predict_evidence.py --run <directory>`; this
reuses the supervisor's offline checks without rewriting evidence or trading.
Existing frozen captures remain regression inputs and are not moved or refreshed.

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
