// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------
//! Full native strategy commands and specialized callback replay, without recovered entry orders.
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_live::node::config::LiveExecutionEngineConfig;
use nautilus_model::identifiers::ClientOrderId;
use nautilus_predict::{PredictDataClientConfig, PredictSubmissionConfig};

use super::{
    exit_orders::{
        PublicState, barrier, exit_socket, settlement,
        strategy::{ExitSettings, ExitStrategy},
        wire_event,
    },
    *,
};

#[path = "passive_strategy.rs"]
mod passive_strategy;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    Profit,
    NativeClose,
    TakerEntryClose,
    NativeQuotes,
    DefaultReduceOnly,
    ShareResidualBelow,
    ShareResidualAbove,
    ConcurrentStop,
    ConcurrentReconcile,
    ConcurrentPartialBuy,
    ReconnectStop,
    ReconnectCancelStop,
    PartialStop,
    TerminalStop,
    TerminalRetry,
    DelayedStopAck,
    EntryRejected,
    EntryDenied,
}
impl Case {
    fn is_concurrent(self) -> bool {
        matches!(
            self,
            Self::ConcurrentStop | Self::ConcurrentReconcile | Self::ConcurrentPartialBuy
        )
    }
    fn cancel_gap(self) -> bool {
        matches!(
            self,
            Self::ReconnectCancelStop | Self::ConcurrentReconcile | Self::ConcurrentPartialBuy
        )
    }
    fn native_reconcile(self) -> bool {
        matches!(self, Self::ConcurrentReconcile | Self::ConcurrentPartialBuy)
    }
    fn passive_holding(self) -> u32 {
        if self == Self::ConcurrentPartialBuy {
            40
        } else {
            0
        }
    }
    fn share_entry(self) -> bool {
        matches!(
            self,
            Self::TakerEntryClose
                | Self::NativeQuotes
                | Self::DefaultReduceOnly
                | Self::ShareResidualBelow
                | Self::ShareResidualAbove
        )
    }
    fn rejection_boundary(self) -> bool {
        matches!(
            self,
            Self::DefaultReduceOnly | Self::ShareResidualBelow | Self::ShareResidualAbove
        )
    }
    fn residual_retry(self) -> bool {
        matches!(self, Self::TerminalRetry | Self::ShareResidualAbove)
    }
    fn terminal_residual(self) -> bool {
        matches!(self, Self::TerminalStop | Self::ShareResidualBelow)
    }
    fn remaining_position(self) -> u32 {
        match self {
            Self::DefaultReduceOnly => 150,
            Self::ShareResidualBelow => 126,
            Self::TerminalStop => 100,
            _ => self.passive_holding(),
        }
    }
    fn entry_net(self) -> u32 {
        if self.share_entry() { 196 } else { 200 }
    }
    fn stop_quantity(self) -> u32 {
        self.entry_net() - 46
    }
    fn name(self) -> &'static str {
        match self {
            Self::Profit => "maker_profit",
            Self::NativeClose => "native_close_maker_entry",
            Self::TakerEntryClose => "native_close_taker_share_fee_entry",
            Self::NativeQuotes => "adapter_native_quotes_share_fee_exit",
            Self::DefaultReduceOnly => "native_default_reduce_only_denied",
            Self::ShareResidualBelow => "native_share_residual_below_minimum",
            Self::ShareResidualAbove => "native_share_residual_above_minimum",
            Self::ConcurrentStop => "concurrent_strategy_stop",
            Self::ConcurrentReconcile => "concurrent_native_reconciliation",
            Self::ConcurrentPartialBuy => "concurrent_partial_buy",
            Self::ReconnectStop => "reconnect_active_take_profit",
            Self::ReconnectCancelStop => "reconnect_cancel_fill_race",
            Self::PartialStop => "partial_market_stop",
            Self::TerminalStop => "terminal_market_stop",
            Self::TerminalRetry => "terminal_partial_retry",
            Self::DelayedStopAck => "unknown_then_late_wallet",
            Self::EntryRejected => "entry_rejected",
            Self::EntryDenied => "entry_denied",
        }
    }
}
fn path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/strategy_commands_synthetic_20261015.json")
}
fn send(private: &Server, events: &mut Vec<Value>, generated: Value, saved: Option<&Value>) {
    let event = if let Some(saved) = saved {
        let mut value = saved["walletEvents"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| {
                row["type"] == generated["type"]
                    && row["details"]["strategyType"] == generated["details"]["strategyType"]
                    && row["details"]["quoteType"] == generated["details"]["quoteType"]
                    && row["settlementId"] == generated["settlementId"]
                    && row["details"]["quantity"] == generated["details"]["quantity"]
            })
            .unwrap()
            .clone();
        for key in ["orderHash", "orderId", "timestamp"] {
            value[key] = generated[key].clone();
        }
        for key in ["details", "fill", "fee"] {
            assert_eq!(value[key], generated[key]);
        }
        value
    } else {
        generated
    };
    events.push(event.clone());
    private.wallet(event.clone());
    private.wallet(event);
}
pub(super) fn cash(private: &Server, delta: Decimal) {
    let mut block = private.state.rpc_frame.lock().unwrap();
    let scale = Decimal::from(1_000_000_000_000_000_000u64);
    let value = (delta * scale).normalize().mantissa().unsigned_abs();
    if delta < Decimal::ZERO {
        block.0 -= value;
    } else {
        block.0 += value;
    }
    block.1 += 1;
    block.2 = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1;
}
pub(super) fn stock(private: &Server, quantity: u32) {
    *private.state.sell_inventory.lock().unwrap() =
        U256::from(quantity) * U256::from(1_000_000_000_000_000_000u64);
}
pub(super) fn book(
    private: &Server,
    public: &PublicState,
    bid: &str,
    saved: Option<&Value>,
) -> Value {
    let mut data = if let Some(saved) = saved {
        saved["orderbooks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["bids"][0][0] == bid)
            .unwrap()
            .clone()
    } else {
        json!({"marketId":42,"orderCount":2,"bids":[[bid,"1000"]],"asks":[["0.014","1000"]]})
    };
    data["updateTimestampMs"] =
        json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
    *private.state.execution_book.lock().unwrap() = data.clone();
    for tx in public.0.lock().unwrap().iter() {
        tx.send(Message::Text(
            json!({"type":"M","topic":"predictOrderbook/42","data":data})
                .to_string()
                .into(),
        ))
        .unwrap();
    }
    data
}
pub(super) fn rest(private: &Server, body: &Value) {
    let mut row = super::live_node::rest_row(body, "OPEN");
    row["amountFilled"] = json!("0");
    // Concurrent Native queries are keyed by hash, just like the venue. A shared
    // FIFO can otherwise return another strategy's order depending on scheduling.
    private.state.orders_by_hash.lock().unwrap().insert(
        body["data"]["order"]["hash"].as_str().unwrap().to_owned(),
        json!({"success":true,"data":row.clone()}),
    );
    private.state.open_orders.lock().unwrap().push(row);
}
pub(super) fn callback(journal: &Rc<RefCell<Vec<Value>>>, name: &str) -> bool {
    journal.borrow().iter().any(|r| r["callback"] == name)
}
pub(super) fn native_id(cache: &Rc<RefCell<Cache>>, body: &Value) -> ClientOrderId {
    let id = VenueOrderId::new(body["data"]["order"]["hash"].as_str().unwrap());
    *cache.borrow().client_order_id(&id).unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn strategy_submit_modify_cancel_maker_exit_and_specialized_callbacks() {
    run(Case::Profit, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn market_partial_fills_wait_for_original_order_and_close_position_once() {
    run(Case::PartialStop, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn terminal_partial_market_rechecks_residual_minimum_without_duplicate_sell() {
    run(Case::TerminalStop, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn terminated_partial_market_retries_only_confirmed_residual() {
    run(Case::TerminalRetry, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn unknown_market_reply_never_reposts_then_late_wallet_resolves() {
    run(Case::DelayedStopAck, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn strategy_entry_rejection_unlocks_account_and_never_opens_position() {
    run(Case::EntryRejected, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn strategy_entry_denied_calls_specialized_handler_without_post() {
    run(Case::EntryDenied, true).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit synthetic loopback capture; no operator credentials or mainnet calls"]
async fn record_strategy_command_json() {
    let mut sessions = vec![];
    for case in [
        Case::Profit,
        Case::PartialStop,
        Case::TerminalStop,
        Case::TerminalRetry,
        Case::DelayedStopAck,
        Case::EntryRejected,
        Case::EntryDenied,
    ] {
        sessions.push(run(case, false).await);
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path())
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic offline Rust Strategy + LiveNode command/callback sessions, not mainnet transactions","sessions":sessions})).unwrap();
}

fn concurrent_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/concurrent_strategy_exit_synthetic_20261016.json")
}
fn exit_requests(private: &Server, case: Case) -> Vec<Value> {
    private
        .state
        .creations
        .lock()
        .unwrap()
        .iter()
        .filter(|r| {
            !case.is_concurrent()
                || !(r["data"]["order"]["side"] == 0
                    && r["data"]["order"]["takerAmount"] == "100000000000000000000")
        })
        .cloned()
        .collect()
}
#[tokio::test(flavor = "current_thread")]
async fn concurrent_strategy_buy_remains_owned_while_tp_cancel_race_and_taker_stop_close_position()
{
    run(Case::ConcurrentStop, true).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit synthetic loopback capture; no operator credentials or mainnet calls"]
async fn record_concurrent_strategy_exit_json() {
    let session = run(Case::ConcurrentStop, false).await;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(concurrent_path())
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"source":"synthetic offline Rust two-strategy LiveNode; not mainnet evidence", "sessions":[session]})).unwrap();
}

fn concurrent_reconciliation_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/concurrent_native_reconciliation_synthetic_20261016.json")
}
#[tokio::test(flavor = "current_thread")]
async fn concurrent_native_reports_restore_acceptance_and_cancel_gap_without_crossing_strategies() {
    run(Case::ConcurrentReconcile, true).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit synthetic loopback capture; never overwrite prior JSON"]
async fn record_concurrent_native_reconciliation_json() {
    let session = run(Case::ConcurrentReconcile, false).await;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(concurrent_reconciliation_path())
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic offline two-strategy native reconciliation; not mainnet feedback","sessions":[session]})).unwrap();
}

fn partial_buy_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/concurrent_partial_buy_synthetic_20261016.json")
}
#[tokio::test(flavor = "current_thread")]
async fn native_bulk_partial_buy_updates_only_own_position_and_remaining_cash_lock() {
    run(Case::ConcurrentPartialBuy, true).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit synthetic loopback capture; never overwrite saved JSON"]
async fn record_concurrent_partial_buy_json() {
    let session = run(Case::ConcurrentPartialBuy, false).await;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(partial_buy_path())
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic offline two-strategy partial BUY recovery; not mainnet evidence","sessions":[session]})).unwrap();
}

fn stock_for_case(private: &Server, case: Case, quantity: u32) {
    stock(private, quantity);
    if case.share_entry() {
        let mut row = holding(1);
        row["amount"] = json!(private.state.sell_inventory.lock().unwrap().to_string());
        *private.state.position_snapshot.lock().unwrap() =
            Some(if quantity == 0 { vec![] } else { vec![row] });
    }
}

fn native_close_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/native_close_fee_cycle_synthetic_20261016.json")
}
#[tokio::test(flavor = "current_thread")]
async fn native_position_close_follows_maker_tp_cancel() {
    run(Case::NativeClose, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn taker_share_fee_entry_waits_for_net_position_then_native_close() {
    run(Case::TakerEntryClose, true).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit synthetic offline fee/native-close capture; never overwrite evidence"]
async fn record_native_close_fee_cycle_json() {
    let sessions = vec![
        run(Case::NativeClose, false).await,
        run(Case::TakerEntryClose, false).await,
    ];
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(native_close_path())
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic Rust Strategy + LiveNode native close and taker share-fee entry; not mainnet evidence","sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit private-evidence Rust strategy + LiveNode capture; create_new only"]
async fn record_private_fill_native_livenode_json() {
    let session = run(Case::NativeQuotes, false).await;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/private_fill_native_livenode_synthetic_20261016.json");
    serde_json::to_writer_pretty(std::fs::OpenOptions::new().write(true).create_new(true).open(path).unwrap(),
        &json!({"source":"synthetic Rust custom snapshot Strategy + LiveNode after private fill evidence migration; not mainnet",
            "sessions":[session]})).unwrap();
}

fn quotes_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/native_quotes_shared_exit_synthetic_20261016.json")
}
#[tokio::test(flavor = "current_thread")]
async fn adapter_native_quotes_value_market_close_without_strategy_quote_bridge() {
    run(Case::NativeQuotes, true).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit synthetic offline native quote capture; never overwrite evidence"]
async fn record_native_quotes_exit_json() {
    let session = run(Case::NativeQuotes, false).await;
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(quotes_path())
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"source":"synthetic Rust Strategy + LiveNode native quote subscription; not mainnet evidence","sessions":[session]})).unwrap();
}

fn boundary_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/native_close_boundaries_synthetic_20261016.json")
}
#[tokio::test(flavor = "current_thread")]
async fn native_default_reduce_only_is_denied_without_post_or_position_loss() {
    run(Case::DefaultReduceOnly, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn native_close_preserves_share_fee_residual_below_slippage_minimum() {
    run(Case::ShareResidualBelow, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn native_close_completes_share_fee_residual_above_slippage_minimum() {
    run(Case::ShareResidualAbove, true).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit synthetic offline boundary capture; never overwrite evidence"]
async fn record_native_close_boundaries_json() {
    let sessions = vec![
        run(Case::DefaultReduceOnly, false).await,
        run(Case::ShareResidualBelow, false).await,
        run(Case::ShareResidualAbove, false).await,
    ];
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(boundary_path())
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"source":"synthetic offline Rust Strategy + LiveNode native close rejections and residuals; not mainnet feedback","sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "offline synthetic history-scope and actual MARKET-plan replay capture; create_new only"]
async fn record_live_fill_scope_regression_json() {
    let session = run(Case::NativeQuotes, false).await;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/live_fill_scope_synthetic_20261016.json");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic loopback Rust Strategy + LiveNode; actual saved historical fill facts and MARKET-plan shape rebound to test identities; no real orders", "session":session,"nativePositionLookbackMins":1,"priorRoundTripAdopted":false})).unwrap();
}

async fn run(case: Case, from_saved: bool) -> Value {
    let reconnect = case == Case::ReconnectStop || case.cancel_gap();
    let saved = if from_saved {
        let v: Value = serde_json::from_slice(
            &std::fs::read(if case == Case::NativeQuotes {
                quotes_path()
            } else if case.rejection_boundary() {
                boundary_path()
            } else if matches!(case, Case::NativeClose | Case::TakerEntryClose) {
                native_close_path()
            } else if case == Case::ConcurrentPartialBuy {
                partial_buy_path()
            } else if case.native_reconcile() {
                concurrent_reconciliation_path()
            } else if case.is_concurrent() {
                concurrent_path()
            } else if reconnect {
                reconnect_path()
            } else {
                path()
            })
            .unwrap(),
        )
        .unwrap();
        Some(
            v["sessions"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["case"] == case.name())
                .unwrap()
                .clone(),
        )
    } else {
        None
    };
    let private = server(vec![]).await;
    // Startup mass status must have complete empty sources before the strategy runs.
    *private.state.position_snapshot.lock().unwrap() = Some(vec![]);
    *private.state.match_snapshot.lock().unwrap() = Some(vec![]);
    *private.state.rpc_frame.lock().unwrap() = (
        1_000_000_000_000_000_000_000,
        1,
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000,
    );
    *private.state.sell_approved.lock().unwrap() = true;
    *private.state.market_fee.lock().unwrap() = 200;
    if case == Case::EntryRejected {
        private
            .state
            .create_replies
            .lock()
            .unwrap()
            .push_back((400, 0, vec![]));
    }
    let mut cfg = config(&private);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    cfg.account_refresh_interval_secs = 1;
    cfg.reconnect_reconciliation_interval_secs = reconnect.then_some(1);
    cfg.orderbook_removal_enabled = true;
    cfg.submission = Some(PredictSubmissionConfig {
        signer: Arc::new(
            nautilus_predict::signing::PredictAuthSigner::new(
                submissions::PUBLIC_KEY.into(),
                PredictEnvironment::BnbMainnet,
                Some(WALLET),
            )
            .unwrap(),
        ),

        market_slippage_bps: 100,
    });
    let public = PublicState(Arc::new(Mutex::new(vec![])));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(
        axum::serve(
            listener,
            Router::new()
                .route("/v1/markets/42", get(super::live_node::public_market))
                .route("/ws", get(exit_socket))
                .with_state(public.clone()),
        )
        .into_future(),
    );
    let mut live = node::build_node_with_engine(
        trader_id(),
        PredictDataClientConfig {
            api_key: KEY.into(),
            market_ids: vec![42],
            base_url_http: Some(format!("http://{addr}")),
            base_url_ws: Some(format!("ws://{addr}/ws")),
            http_timeout_secs: 2,
            ws_timeout_secs: 2,
            dynamic_refresh_secs: 30,
            transport_backend: TransportBackend::Tungstenite,
        },
        cfg,
        LiveExecutionEngineConfig {
            reconciliation: reconnect || case.share_entry(),
            position_check_lookback_mins: if case == Case::NativeQuotes { 1 } else { 60 },
            reconciliation_startup_delay_secs: 0.0,
            // Native inflight checks resolve a TP left PendingCancel across the gap.
            inflight_check_interval_ms: if reconnect { 20 } else { 0 },
            inflight_check_threshold_ms: 500,
            open_check_interval_secs: (case.native_reconcile()).then_some(1.0),
            open_check_lookback_mins: None,
            open_check_open_only: false,
            open_check_threshold_ms: 10,
            position_check_interval_secs: case.share_entry().then_some(0.05),
            position_check_threshold_ms: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let cache = live.kernel().cache.clone();
    let engine = live.kernel().exec_engine.clone();
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    // No cached order or position is inserted: all order state originates in this strategy.
    assert!(
        cache
            .borrow()
            .orders(None, None, None, None, None)
            .is_empty()
    );
    let journal = Rc::new(RefCell::new(vec![]));
    let strategy = ExitStrategy::new(
        instrument_id(1),
        1,
        journal.clone(),
        ExitSettings::parse(include_str!(
            "../../../../examples/predict-exit-strategy.toml.example"
        ))
        .unwrap(),
        200,
    )
    .with_native_entry(
        Price::from("0.010"),
        Quantity::from(if case == Case::EntryDenied { 50 } else { 250 }),
    );
    let strategy = if case == Case::DefaultReduceOnly {
        strategy.with_default_reduce_only()
    } else if case == Case::NativeClose || case.share_entry() {
        strategy.with_native_close()
    } else {
        strategy
    };
    let strategy = if case == Case::NativeQuotes {
        strategy
            .with_native_quotes()
            .with_live_identity(42, account_id(), u64::MAX)
    } else {
        strategy
    };
    let strategy = if case.share_entry() {
        strategy.with_taker_entry()
    } else {
        strategy
    };
    let strategy = if reconnect {
        let engine = engine.clone();
        let observations = journal.clone();
        let observed = std::cell::Cell::new(None);
        strategy.with_execution_probe(Rc::new(move || {
            let connected = engine.try_borrow_mut().ok().and_then(|mut engine| {
                engine
                    .get_client_adapter_mut(&"PREDICT".into())
                    .map(|adapter| adapter.is_connected())
            });
            if let Some(connected) = connected
                && observed.replace(Some(connected)) != Some(connected)
            {
                observations
                    .borrow_mut()
                    .push(json!({"type":"ExecutionConnectionObserved","connected":connected}));
            }
            connected.unwrap_or(false)
        }))
    } else {
        strategy
    };
    live.add_strategy(strategy).unwrap();
    let passive_phase = Rc::new(std::cell::Cell::new(passive_strategy::Phase::Idle));
    let passive_journal = Rc::new(RefCell::new(vec![]));
    if case.is_concurrent() {
        live.add_strategy(passive_strategy::PassiveStrategy::new(
            passive_phase.clone(),
            passive_journal.clone(),
        ))
        .unwrap();
    }
    if case == Case::NativeQuotes {
        live.add_strategy(
            passive_strategy::PassiveStrategy::new(passive_phase.clone(), passive_journal.clone())
                .with_native_quotes(),
        )
        .unwrap();
    }
    let mut concurrent_observations = vec![];
    let handle = live.handle();
    let mut events = vec![];
    let mut books = vec![];
    let final_cash = match case {
        Case::Profit => Decimal::new(10006, 1),
        Case::TakerEntryClose | Case::NativeQuotes | Case::ShareResidualAbove => {
            Decimal::new(999774, 3)
        }
        Case::DefaultReduceOnly => Decimal::new(998598, 3),
        Case::ShareResidualBelow => Decimal::new(99878616, 5),
        Case::ConcurrentPartialBuy => Decimal::new(99940536, 5),
        Case::TerminalStop => Decimal::new(99902136, 5),
        Case::EntryRejected | Case::EntryDenied => Decimal::from(1000),
        _ => Decimal::new(99980536, 5),
    };
    let driver = async {
        barrier("public and wallet subscriptions", || {
            !public.0.lock().unwrap().is_empty()
                && !private.state.sockets.lock().unwrap().is_empty()
        })
        .await;
        books.push(book(&private, &public, "0.009", saved.as_ref()));
        if case == Case::EntryDenied {
            barrier("native deny callback", || {
                callback(&journal, "on_order_denied")
            })
            .await;
            assert!(exit_requests(&private, case).is_empty());
        } else {
            barrier("native BUY POST", || {
                exit_requests(&private, case).len() == 1
            })
            .await;
            let entry = exit_requests(&private, case)[0].clone();
            assert_eq!(entry["data"]["order"]["side"], 0);
            assert_eq!(entry["data"]["isPostOnly"], !case.share_entry());
            assert_eq!(
                entry["data"]["order"]["takerAmount"],
                "250000000000000000000"
            );
            if case == Case::EntryRejected {
                barrier("native entry reject", || {
                    callback(&journal, "on_order_rejected")
                })
                .await;
            } else {
                rest(&private, &entry);
                send(
                    &private,
                    &mut events,
                    wire_event(&entry, "orderAccepted", 1, Decimal::ZERO),
                    saved.as_ref(),
                );
                barrier("modify rejection callback", || {
                    callback(&journal, "on_order_modify_rejected")
                })
                .await;
                if case == Case::NativeQuotes {
                    // Same-instrument prior round trip must not be claimed by a fresh strategy.
                    let actual: Value=serde_json::from_str(include_str!("../fixtures/private_api/live_fill_20261016T122233Z_independent/calibration.json")).unwrap();
                    let rows = actual["after"]["matches"].as_array().unwrap();
                    let mut historical = vec![];
                    for (index, source) in rows
                        .iter()
                        .filter(|r| r["market"]["id"] == 10835)
                        .enumerate()
                    {
                        let mut row = super::matches::match_row(1);
                        row["settlementId"] = source["settlementId"].clone();
                        row["executedAt"] = source["executedAt"].clone();
                        row["transactionHash"] = source["transactionHash"].clone();
                        row["amountFilled"] = source["amountFilled"].clone();
                        row["priceExecuted"] = source["priceExecuted"].clone();
                        for key in ["quoteType", "amount", "price", "fee"] {
                            row["taker"][key] = source["taker"][key].clone();
                        }
                        row["taker"]["hash"] = json!(format!(
                            "0x{}",
                            if index == 0 {
                                "cc".repeat(32)
                            } else {
                                "dd".repeat(32)
                            }
                        ));
                        historical.push(row);
                    }
                    assert_eq!(historical.len(), 2);
                    *private.state.match_snapshot.lock().unwrap() = Some(historical);
                }
                let gate = Arc::new(tokio::sync::Notify::new());
                *private.state.remove_gate.lock().unwrap() = Some(gate.clone());
                private.state.remove_replies.lock().unwrap().push_back((
                    200,
                    json!({"success":true,"removed":["12345"],"noop":[]}),
                    0,
                ));
                for (size, cumulative, id) in
                    [(67, 67, "native-entry-1"), (133, 200, "native-entry-2")]
                {
                    cash(&private, -Decimal::from(size) * Decimal::new(1, 2));
                    let mut receipt = settlement(
                        &entry,
                        1,
                        size,
                        cumulative,
                        "0.010",
                        id,
                        !case.share_entry(),
                    );
                    if case.share_entry() {
                        let fee = Decimal::from(size) * Decimal::new(2, 2);
                        receipt["fee"] = json!({"type":"SHARES"});
                        receipt["fee"]["amountWei"] = json!(
                            (fee * Decimal::from(1_000_000_000_000_000_000u64))
                                .normalize()
                                .to_string()
                        );
                        let net = Decimal::from(cumulative) * Decimal::new(98, 2);
                        *private.state.sell_inventory.lock().unwrap() = (net
                            * Decimal::from(1_000_000_000_000_000_000u64))
                        .normalize()
                        .to_string()
                        .parse()
                        .unwrap();
                        let mut position = holding(1);
                        position["amount"] =
                            json!(private.state.sell_inventory.lock().unwrap().to_string());
                        *private.state.position_snapshot.lock().unwrap() = Some(vec![position]);
                    } else {
                        stock_for_case(&private, case, cumulative);
                    }
                    send(&private, &mut events, receipt, saved.as_ref());
                    barrier("entry fill applied", || {
                        cache
                            .borrow()
                            .order(&ClientOrderId::new("EXIT-ENTRY-001"))
                            .unwrap()
                            .filled_qty()
                            .as_decimal()
                            == Decimal::from(cumulative)
                    })
                    .await;
                    if cumulative == 67 {
                        barrier("entry remainder cancel command", || {
                            private.state.removals.lock().unwrap().len() == 1
                        })
                        .await;
                    }
                }
                // 200 filled, 50 still open. Release cancellation only after both real native fills apply.
                books.push(book(&private, &public, "0.012", saved.as_ref()));
                barrier("fresh exit quote before entry cancel resolves", || {
                    cache
                        .borrow()
                        .quote(&instrument_id(1))
                        .is_some_and(|q| q.bid_price.as_decimal() == Decimal::new(12, 3))
                })
                .await;
                gate.notify_one();
                barrier("entry remainder canceled", || {
                    cache
                        .borrow()
                        .order(&ClientOrderId::new("EXIT-ENTRY-001"))
                        .unwrap()
                        .status()
                        == OrderStatus::Canceled
                })
                .await;
                *private.state.remove_gate.lock().unwrap() = None;
                if reconnect {
                    reconnect_snapshot(
                        &private,
                        &events,
                        &[(entry.clone(), "CANCELLED", 200)],
                        200,
                    );
                }
                books.push(book(&private, &public, "0.012", saved.as_ref()));
                barrier("take profit POST", || {
                    super::exit_orders::assert_no_submission_failure(&journal);
                    exit_requests(&private, case).len() == 2
                })
                .await;
                let maker = exit_requests(&private, case)[1].clone();
                assert_eq!(maker["data"]["pricePerShare"], "13000000000000000");
                assert_eq!(
                    maker["data"]["order"]["makerAmount"],
                    (u128::from(case.entry_net()) * 1_000_000_000_000_000_000).to_string()
                );
                rest(&private, &maker);
                send(
                    &private,
                    &mut events,
                    wire_event(&maker, "orderAccepted", 1, Decimal::ZERO),
                    saved.as_ref(),
                );
                barrier("take profit accepted", || {
                    cache
                        .borrow()
                        .orders(None, None, None, None, None)
                        .iter()
                        .any(|o| {
                            o.venue_order_id().is_some_and(|id| {
                                id.as_str() == maker["data"]["order"]["hash"].as_str().unwrap()
                            }) && o.status() == OrderStatus::Accepted
                        })
                })
                .await;
                if case.is_concurrent() {
                    private
                        .state
                        .create_order_ids
                        .lock()
                        .unwrap()
                        .push_back("77777".into());
                    passive_phase.set(passive_strategy::Phase::Submit);
                    books.push(book(&private, &public, "0.012", saved.as_ref()));
                    barrier("second strategy BUY POST", || {
                        private.state.creations.lock().unwrap().len() == 3
                    })
                    .await;
                    let buy = private.state.creations.lock().unwrap()[2].clone();
                    let mut row = super::live_node::rest_row(&buy, "OPEN");
                    row["id"] = json!("77777");
                    private.state.orders_by_hash.lock().unwrap().insert(
                        buy["data"]["order"]["hash"].as_str().unwrap().to_owned(),
                        json!({"success":true,"data":row.clone()}),
                    );
                    private.state.open_orders.lock().unwrap().push(row);
                    let mut ack = wire_event(&buy, "orderAccepted", 1, Decimal::ZERO);
                    ack["orderId"] = json!("77777");
                    if case.native_reconcile() {
                        // HTTP gives an ID, but no wallet Accepted is delivered for this order.
                        // Only native report scheduling is allowed to recover acceptance.
                        assert!(
                            !passive_journal
                                .borrow()
                                .iter()
                                .any(|r| r["type"] == "Accepted")
                        );
                        reconnect_snapshot(
                            &private,
                            &events,
                            &[
                                (entry.clone(), "CANCELLED", 200),
                                (maker.clone(), "OPEN", 0),
                                (buy.clone(), "OPEN", 0),
                            ],
                            200,
                        );
                    } else {
                        send(&private, &mut events, ack, saved.as_ref());
                    }
                    barrier("two strategies accepted with shared cash lock", || {
                        passive_journal
                            .borrow()
                            .iter()
                            .any(|r| r["type"] == "Accepted")
                            && cache.borrow().account(&account_id()).is_some_and(|a| {
                                a.balance_locked(Some(Currency::USDT()))
                                    .unwrap()
                                    .as_decimal()
                                    == Decimal::ONE
                            })
                    })
                    .await;
                    assert_eq!(
                        cache
                            .borrow()
                            .orders_open(None, Some(&instrument_id(1)), None, None, None)
                            .len(),
                        2
                    );
                    concurrent_observations.push(json!({"phase":"both_open","passiveStatus":"ACCEPTED","cashLocked":"1","openOrders":2}));
                    if case.native_reconcile() {
                        let buy_hash = buy["data"]["order"]["hash"].as_str().unwrap();
                        assert!(
                            private
                                .state
                                .order_requests
                                .lock()
                                .unwrap()
                                .iter()
                                .any(|p| p.ends_with(buy_hash)),
                            "native inflight query must recover missing Accepted"
                        );
                        barrier("native current orders plus known terminal hashes cover both strategies", || {
                            private.state.order_list_log.lock().unwrap().iter().any(|r| {
                                r["query"]["status"] == "OPEN"
                                    && r["response"]["data"].as_array().is_some_and(|rows|rows.iter().any(|r|r["order"]["hash"]==maker["data"]["order"]["hash"]))
                            }) && private.state.order_requests.lock().unwrap().iter().any(|p|p.ends_with(buy_hash))
                        }).await;
                        assert_eq!(
                            cache
                                .borrow()
                                .order(&native_id(&cache, &buy))
                                .unwrap()
                                .strategy_id()
                                .as_str(),
                            "PREDICT-PASSIVE-002"
                        );
                        assert_eq!(
                            cache
                                .borrow()
                                .order(&native_id(&cache, &maker))
                                .unwrap()
                                .strategy_id()
                                .as_str(),
                            "PREDICT-EXIT-001"
                        );
                        concurrent_observations.push(json!({"phase":"native_acceptance_and_bulk","passiveAcceptedWithoutWallet":true,"bothOwnersPreserved":true,"requests":3}));
                        if case == Case::ConcurrentPartialBuy {
                            for (size, cumulative, id) in
                                [(17, 17, "passive-partial-1"), (23, 40, "passive-partial-2")]
                            {
                                cash(&private, -Decimal::from(size) * Decimal::new(1, 2));
                                stock_for_case(&private, case, 200 + cumulative);
                                let mut receipt =
                                    settlement(&buy, 1, size, cumulative, "0.010", id, true);
                                receipt["orderId"] = json!("77777");
                                if cumulative == 17 {
                                    send(&private, &mut events, receipt, saved.as_ref());
                                } else {
                                    // The second receipt is absent from WS until native bulk reports apply it.
                                    events.push(receipt);
                                }
                                reconnect_snapshot(
                                    &private,
                                    &events,
                                    &[
                                        (entry.clone(), "CANCELLED", 200),
                                        (maker.clone(), "OPEN", 0),
                                        (buy.clone(), "OPEN", cumulative),
                                    ],
                                    200 + cumulative,
                                );
                                barrier(
                                    "partial BUY applied and native leaves lock refreshed",
                                    || {
                                        cache
                                            .borrow()
                                            .order(&native_id(&cache, &buy))
                                            .unwrap()
                                            .filled_qty()
                                            .as_decimal()
                                            == Decimal::from(cumulative)
                                            && cache
                                                .borrow()
                                                .account(&account_id())
                                                .unwrap()
                                                .balance_locked(Some(Currency::USDT()))
                                                .unwrap()
                                                .as_decimal()
                                                == Decimal::from(100 - cumulative)
                                                    * Decimal::new(1, 2)
                                    },
                                )
                                .await;
                                assert_eq!(
                                    passive_journal
                                        .borrow()
                                        .iter()
                                        .filter(|r| r["type"] == "Filled")
                                        .count(),
                                    if cumulative == 17 { 1 } else { 2 }
                                );
                                concurrent_observations.push(json!({"phase":"passive_partial_buy","filled":cumulative,"leaves":100-cumulative,"cashLocked":(Decimal::from(100-cumulative)*Decimal::new(1,2)).to_string(),"restOnlyIncrement":cumulative==40}));
                            }
                            let late = events
                                .iter()
                                .find(|r| r["settlementId"] == "passive-partial-2")
                                .unwrap()
                                .clone();
                            send(&private, &mut events, late, saved.as_ref());
                            assert_eq!(
                                cache
                                    .borrow()
                                    .positions_open(None, Some(&instrument_id(1)), None, None, None)
                                    .len(),
                                2
                            );
                        }
                    }
                }
                if case == Case::ReconnectStop {
                    reconnect_snapshot(
                        &private,
                        &events,
                        &[
                            (entry.clone(), "CANCELLED", 200),
                            (maker.clone(), "OPEN", 0),
                        ],
                        200,
                    );
                    automatic_reconnect(&private, &engine, &cache, &maker, OrderStatus::Accepted)
                        .await;
                    assert_eq!(
                        exit_requests(&private, case).len(),
                        2,
                        "reconnect must retain the original TP without replacing it"
                    );
                }
                if case == Case::Profit {
                    for (size, cumulative, id) in
                        [(100, 100, "native-profit-1"), (100, 200, "native-profit-2")]
                    {
                        cash(&private, Decimal::from(size) * Decimal::new(13, 3));
                        stock_for_case(&private, case, 200 - cumulative);
                        send(
                            &private,
                            &mut events,
                            settlement(&maker, 1, size, cumulative, "0.013", id, true),
                            saved.as_ref(),
                        );
                        barrier("partial maker applied", || {
                            cache
                                .borrow()
                                .order(&native_id(&cache, &maker))
                                .unwrap()
                                .filled_qty()
                                .as_decimal()
                                == Decimal::from(cumulative)
                        })
                        .await;
                    }
                    private.state.open_orders.lock().unwrap().clear();
                } else {
                    let gate = Arc::new(tokio::sync::Notify::new());
                    *private.state.remove_gate.lock().unwrap() = Some(gate.clone());
                    private.state.remove_replies.lock().unwrap().push_back((
                        200,
                        json!({"success":true,"removed":["12345"],"noop":[]}),
                        0,
                    ));
                    books.push(book(&private, &public, "0.008", saved.as_ref()));
                    barrier("take profit cancel POST", || {
                        private.state.removals.lock().unwrap().len() == 2
                    })
                    .await;
                    cash(&private, Decimal::new(598, 3));
                    stock_for_case(
                        &private,
                        case,
                        case.stop_quantity() + case.passive_holding(),
                    );
                    let race = settlement(&maker, 1, 46, 46, "0.013", "native-cancel-race", true);
                    if case.cancel_gap() {
                        // Preserve the raw wallet JSON but lose this receipt during the gap.
                        events.push(race.clone());
                        assert_eq!(
                            cache
                                .borrow()
                                .order(&native_id(&cache, &maker))
                                .unwrap()
                                .filled_qty(),
                            Quantity::zero(16)
                        );
                    } else {
                        send(&private, &mut events, race.clone(), saved.as_ref());
                        barrier("late maker fill applied", || {
                            cache
                                .borrow()
                                .order(&native_id(&cache, &maker))
                                .unwrap()
                                .filled_qty()
                                .as_decimal()
                                == Decimal::from(46)
                        })
                        .await;
                    }
                    if case == Case::DelayedStopAck {
                        private
                            .state
                            .create_replies
                            .lock()
                            .unwrap()
                            .push_back((500, 0, vec![]));
                    }
                    if case.cancel_gap() {
                        // The remove reached the venue but its reply is still held. REST proves
                        // the 46-share race fill and cancellation; no REMOVE is retransmitted.
                        reconnect_snapshot(
                            &private,
                            &events,
                            &[
                                (entry.clone(), "CANCELLED", 200),
                                (maker.clone(), "CANCELLED", 46),
                            ],
                            154,
                        );
                        if case.native_reconcile() {
                            let buy = private.state.creations.lock().unwrap()[2].clone();
                            reconnect_snapshot(
                                &private,
                                &events,
                                &[
                                    (entry.clone(), "CANCELLED", 200),
                                    (maker.clone(), "CANCELLED", 46),
                                    (buy, "OPEN", case.passive_holding()),
                                ],
                                154 + case.passive_holding(),
                            );
                        }
                        let count = private.state.sockets.lock().unwrap().len();
                        private
                            .state
                            .sockets
                            .lock()
                            .unwrap()
                            .last()
                            .unwrap()
                            .send(Message::Close(None))
                            .unwrap();
                        barrier("wallet transport reconnects", || {
                            private.state.sockets.lock().unwrap().len() > count
                        })
                        .await;
                        gate.notify_one();
                        books.push(book(&private, &public, "0.008", saved.as_ref()));
                        barrier("REST restores canceled TP before stop", || {
                            cache
                                .borrow()
                                .order(&native_id(&cache, &maker))
                                .unwrap()
                                .status()
                                == OrderStatus::Canceled
                        })
                        .await;
                        cash(&private, Decimal::ZERO); // A canonical block after REST native status application.
                        // Order cancellation can apply before the wallet verification gate releases.
                        // Publish the final fresh signal after that gate, preserving the two frames.
                        barrier("wallet verified before fresh stop signal", || {
                            engine
                                .try_borrow_mut()
                                .ok()
                                .and_then(|mut engine| {
                                    engine
                                        .get_client_adapter_mut(&"PREDICT".into())
                                        .map(|client| client.is_connected())
                                })
                                .unwrap_or(false)
                        })
                        .await;
                        books.push(book(&private, &public, "0.008", saved.as_ref()));
                    } else {
                        gate.notify_one();
                    }
                    if case == Case::DefaultReduceOnly {
                        barrier("native default reduce-only denial callback", || {
                            callback(&journal, "on_order_denied")
                        })
                        .await;
                        assert_eq!(exit_requests(&private, case).len(), 2);
                    } else {
                        barrier("single MARKET stop POST", || {
                            exit_requests(&private, case).len() == 3
                        })
                        .await;
                        if case.cancel_gap() {
                            // REST already applied the missing fill; late stream duplicates and the
                            // original zero-fill ACK cannot add inventory changes or reopen the TP.
                            send(&private, &mut events, race, saved.as_ref());
                            let original_ack = events
                                .iter()
                                .find(|event| {
                                    event["type"] == "orderAccepted"
                                        && event["orderHash"] == maker["data"]["order"]["hash"]
                                })
                                .unwrap()
                                .clone();
                            send(&private, &mut events, original_ack, saved.as_ref());
                            assert_eq!(private.state.removals.lock().unwrap().len(), 2);
                        }
                        let stop = exit_requests(&private, case)[2].clone();
                        // The venue already owns a POSTed stop when Native's
                        // concurrent recovery timer queries it before wallet ACK.
                        if case.native_reconcile() {
                            let mut row = super::live_node::rest_row(&stop, "OPEN");
                            row["strategy"] = stop["data"]["strategy"].clone();
                            private.state.orders_by_hash.lock().unwrap().insert(
                                stop["data"]["order"]["hash"].as_str().unwrap().to_owned(),
                                json!({"success":true,"data":row}),
                            );
                        }
                        assert_eq!(stop["data"]["strategy"], "MARKET");
                        assert_eq!(
                            stop["data"]["order"]["makerAmount"],
                            (u128::from(case.stop_quantity()) * 1_000_000_000_000_000_000)
                                .to_string()
                        );
                        if case == Case::DelayedStopAck {
                            books.push(book(&private, &public, "0.008", saved.as_ref()));
                            barrier("unknown write remains Submitted", || {
                                cache
                                    .borrow()
                                    .orders(None, None, None, None, None)
                                    .iter()
                                    .any(|o| {
                                        o.order_type() == OrderType::Market
                                            && o.status() == OrderStatus::Submitted
                                    })
                            })
                            .await;
                            assert_eq!(exit_requests(&private, case).len(), 3);
                        }
                        send(
                            &private,
                            &mut events,
                            wire_event(&stop, "orderAccepted", 1, Decimal::ZERO),
                            saved.as_ref(),
                        );
                        if case == Case::NativeQuotes {
                            // Real 2026-10-16 MARKET progress notice, rebound to this loopback order.
                            // A plan must keep the stream alive without applying any fill/cancel.
                            let actual: Value = serde_json::from_str(include_str!("../fixtures/private_api/live_fill_20261016T122233Z_independent/calibration.json")).unwrap();
                            let template = actual["wallet"]["events"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .find(|e| e["type"] == "orderMarketMatchPlan")
                                .unwrap();
                            let mut plan =
                                wire_event(&stop, "orderMarketMatchPlan", 1, Decimal::ZERO);
                            for key in [
                                "amount",
                                "amountFilled",
                                "plannedFill",
                                "remainderCancelled",
                                "settlementIds",
                            ] {
                                plan[key] = template[key].clone();
                            }
                            plan["amount"] = stop["data"]["order"]["makerAmount"].clone();
                            plan["plannedFill"] = plan["amount"].clone();
                            send(&private, &mut events, plan, None);
                        }
                        let first = match case {
                            Case::TerminalRetry => 10,
                            Case::ShareResidualBelow => 24,
                            Case::ShareResidualAbove => 23,
                            _ => 54,
                        };
                        cash(&private, Decimal::from(first) * Decimal::new(784, 5));
                        stock_for_case(
                            &private,
                            case,
                            case.stop_quantity() - first + case.passive_holding(),
                        );
                        send(
                            &private,
                            &mut events,
                            settlement(&stop, 1, first, first, "0.008", "native-stop-1", false),
                            saved.as_ref(),
                        );
                        if case.native_reconcile() {
                            barrier(
                                "partial stop fill applied before refreshing its signal",
                                || {
                                    let cache = cache.borrow();
                                    cache
                                        .client_order_id(&VenueOrderId::new(
                                            stop["data"]["order"]["hash"].as_str().unwrap(),
                                        ))
                                        .and_then(|id| cache.order(id))
                                        .is_some_and(|order| {
                                            order.filled_qty().as_decimal() == Decimal::from(first)
                                        })
                                },
                            )
                            .await;
                            // Reconciliation can exceed the strategy's book freshness
                            // window under parallel load. Keep the policy's freshness
                            // check and provide a current quote after the fill barrier.
                            books.push(book(&private, &public, "0.008", saved.as_ref()));
                        }
                        barrier("partial stop and wait callback", || {
                            journal
                                .borrow()
                                .iter()
                                .any(|r| r["type"] == "AwaitingRemainingMarketFill")
                        })
                        .await;
                        if case.is_concurrent() {
                            let buy = private.state.creations.lock().unwrap()[2].clone();
                            assert_eq!(
                                cache
                                    .borrow()
                                    .order(&native_id(&cache, &buy))
                                    .unwrap()
                                    .status(),
                                if case.passive_holding() > 0 {
                                    OrderStatus::PartiallyFilled
                                } else {
                                    OrderStatus::Accepted
                                }
                            );
                            assert_eq!(
                                private
                                    .state
                                    .open_orders
                                    .lock()
                                    .unwrap()
                                    .iter()
                                    .filter(|r| r["id"] == "77777")
                                    .count(),
                                1
                            );
                            assert_eq!(private.state.removals.lock().unwrap().len(), 2);
                            assert_eq!(
                                cache.borrow().positions_open(
                                    None,
                                    Some(&instrument_id(1)),
                                    Some(&nautilus_model::identifiers::StrategyId::new(
                                        "PREDICT-EXIT-001"
                                    )),
                                    None,
                                    None
                                )[0]
                                .quantity
                                .as_decimal(),
                                Decimal::from(100)
                            );
                            assert_eq!(
                                cache
                                    .borrow()
                                    .account(&account_id())
                                    .unwrap()
                                    .balance_locked(Some(Currency::USDT()))
                                    .unwrap()
                                    .as_decimal(),
                                Decimal::from(100 - case.passive_holding()) * Decimal::new(1, 2)
                            );
                            concurrent_observations.push(json!({"phase":"stop_partial","passiveStatus":if case.passive_holding()>0 {"PARTIALLY_FILLED"}else{"ACCEPTED"},"stopFilled":54,"remainingPosition":100,"removals":2}));
                        }
                        let stop_id = native_id(&cache, &stop);
                        assert_eq!(
                            cache.borrow().order(&stop_id).unwrap().status(),
                            OrderStatus::PartiallyFilled
                        );
                        assert_eq!(exit_requests(&private, case).len(), 3);
                        if case.terminal_residual() {
                            send(
                                &private,
                                &mut events,
                                wire_event(&stop, "orderCancelled", 1, Decimal::from(first)),
                                saved.as_ref(),
                            );
                            barrier("residual minimum reaches the owning rejection path", || {
                                // Native minimum and adapter signed-floor minimum
                                // both deny before submission; neither sends a POST.
                                callback(&journal, "on_order_denied")
                            })
                            .await;
                            assert_eq!(exit_requests(&private, case).len(), 3);
                        } else if case.residual_retry() {
                            send(
                                &private,
                                &mut events,
                                wire_event(&stop, "orderCancelled", 1, Decimal::from(first)),
                                saved.as_ref(),
                            );
                            barrier(
                                "confirmed terminal allows exactly one residual POST",
                                || exit_requests(&private, case).len() == 4,
                            )
                            .await;
                            let residual = exit_requests(&private, case)[3].clone();
                            assert_eq!(
                                residual["data"]["order"]["makerAmount"],
                                (u128::from(case.stop_quantity() - first)
                                    * 1_000_000_000_000_000_000)
                                    .to_string()
                            );
                            send(
                                &private,
                                &mut events,
                                wire_event(&residual, "orderAccepted", 1, Decimal::ZERO),
                                saved.as_ref(),
                            );
                            cash(
                                &private,
                                Decimal::from(case.stop_quantity() - first) * Decimal::new(784, 5),
                            );
                            stock_for_case(&private, case, 0);
                            send(
                                &private,
                                &mut events,
                                settlement(
                                    &residual,
                                    1,
                                    case.stop_quantity() - first,
                                    case.stop_quantity() - first,
                                    "0.008",
                                    "native-residual-stop",
                                    false,
                                ),
                                saved.as_ref(),
                            );
                        } else {
                            cash(
                                &private,
                                Decimal::from(case.stop_quantity() - first) * Decimal::new(784, 5),
                            );
                            stock_for_case(&private, case, case.passive_holding());
                            send(
                                &private,
                                &mut events,
                                settlement(
                                    &stop,
                                    1,
                                    case.stop_quantity() - first,
                                    case.stop_quantity(),
                                    "0.008",
                                    "native-stop-2",
                                    false,
                                ),
                                saved.as_ref(),
                            );
                        }
                    }
                }
            }
        }
        if case.is_concurrent() {
            *private.state.remove_gate.lock().unwrap() = None;
            barrier("first strategy closed", || {
                callback(&journal, "on_position_closed")
            })
            .await;
            private.state.remove_replies.lock().unwrap().push_back((
                200,
                json!({"echoRemoved":true}),
                0,
            ));
            passive_phase.set(passive_strategy::Phase::Cancel);
            books.push(book(&private, &public, "0.008", saved.as_ref()));
            barrier("second strategy cancellation callback", || {
                passive_journal
                    .borrow()
                    .iter()
                    .any(|r| r["type"] == "Canceled")
            })
            .await;
            assert_eq!(private.state.removals.lock().unwrap().len(), 3);
            assert_eq!(
                private.state.removals.lock().unwrap()[2]["data"]["ids"],
                json!(["77777"])
            );
            assert_eq!(private.state.creations.lock().unwrap().len(), 4);
            let passive = passive_journal.borrow();
            assert_eq!(
                passive.iter().filter(|r| r["type"] == "Submitted").count(),
                1
            );
            assert_eq!(
                passive.iter().filter(|r| r["type"] == "Accepted").count(),
                1
            );
            assert_eq!(
                passive.iter().filter(|r| r["type"] == "Filled").count(),
                if case.passive_holding() > 0 { 2 } else { 0 }
            );
            assert!(
                !passive
                    .iter()
                    .any(|r| r["type"] == "Rejected" || r["type"] == "Denied")
            );
            let passive_id = passive[0]["id"].as_str().unwrap();
            assert!(
                !journal
                    .borrow()
                    .iter()
                    .any(|r| r["clientOrderId"] == passive_id)
            );
            concurrent_observations.push(
                json!({"phase":"cleanup","passiveStatus":"CANCELED","requests":4,"removals":3}),
            );
        }
        barrier("reported final cash", || {
            cache.borrow().account(&account_id()).is_some_and(|a| {
                a.balance_total(Some(Currency::USDT()))
                    .unwrap()
                    .as_decimal()
                    == final_cash
                    && a.balance_locked(Some(Currency::USDT())).unwrap().is_zero()
            })
        })
        .await;
        if case.rejection_boundary() {
            barrier("boundary final balance reaches strategy subscriber", || {
                journal.borrow().iter().any(|r| {
                    matches!(
                        r["type"].as_str(),
                        Some("NativeAccountReported" | "NativeAccountSnapshot")
                    ) && r["balances"].as_array().is_some_and(|balances| {
                        balances.iter().any(|b| {
                            b["currency"] == "USDT"
                                && b["total"]
                                    .as_str()
                                    .and_then(|v| Decimal::from_str_exact(v).ok())
                                    == Some(final_cash)
                                && b["locked"]
                                    .as_str()
                                    .and_then(|v| Decimal::from_str_exact(v).ok())
                                    == Some(Decimal::ZERO)
                        })
                    })
                })
            })
            .await;
        }
        if !matches!(case, Case::EntryRejected | Case::EntryDenied)
            && case.remaining_position() == 0
        {
            barrier("position closed callback", || {
                callback(&journal, "on_position_closed")
            })
            .await;
        }
        barrier("strategy account subscription observed", || {
            journal.borrow().iter().any(|r| {
                r["type"] == "NativeAccountReported" || r["type"] == "NativeAccountSnapshot"
            })
        })
        .await;
        handle.stop();
    };
    let deadline = async {
        tokio::time::sleep(Duration::from_secs(12)).await;
        barrier("strategy account subscription observed", || {
            journal.borrow().iter().any(|r| {
                r["type"] == "NativeAccountReported" || r["type"] == "NativeAccountSnapshot"
            })
        })
        .await;
        handle.stop();
    };
    tokio::select! {result=async{let (result,())=tokio::join!(live.run(),driver);result.unwrap();}=>{let ()=result;},()=deadline=>panic!("lifecycle watchdog: {:?}",journal.borrow())}
    task.abort();
    assert_eq!(
        *private.state.book_requests.lock().unwrap(),
        0,
        "strategy snapshot price must survive Native risk/submit/close-position routing"
    );
    if case == Case::NativeQuotes {
        for byte in ["cc", "dd"] {
            assert!(
                cache
                    .borrow()
                    .client_order_id(&VenueOrderId::new(format!("0x{}", byte.repeat(32))))
                    .is_none(),
                "prior round-trip fills must remain unclaimed"
            );
        }
    }
    let callbacks = journal.borrow();
    assert!(
        !callbacks
            .iter()
            .any(|r| r["callback"] == "on_order_updated")
    );
    for row in callbacks.iter().filter(|r| {
        r["callback"]
            .as_str()
            .is_some_and(|s| s.starts_with("on_position_"))
    }) {
        assert_eq!(
            Decimal::from_str_exact(row["eventQuantity"].as_str().unwrap()).unwrap(),
            Decimal::from_str_exact(row["cachedNetQuantity"].as_str().unwrap()).unwrap()
        );
    }
    if !matches!(case, Case::EntryRejected | Case::EntryDenied) {
        for name in [
            "on_order_submitted",
            "on_order_accepted",
            "on_order_modify_rejected",
            "on_order_pending_update",
            "on_order_pending_cancel",
            "on_order_canceled",
            "on_order_filled",
            "on_position_opened",
            "on_position_changed",
        ] {
            assert!(callbacks.iter().any(|r| r["callback"] == name), "{name}");
        }
        let entry_fills = callbacks
            .iter()
            .filter(|r| {
                r["callback"] == "on_order_filled" && r["clientOrderId"] == "EXIT-ENTRY-001"
            })
            .collect::<Vec<_>>();
        assert_eq!(entry_fills.len(), 2);
        for (row, cumulative, leaves) in [(entry_fills[0], 67, 183), (entry_fills[1], 200, 50)] {
            assert_eq!(
                Decimal::from_str_exact(row["filledQuantity"].as_str().unwrap()).unwrap(),
                Decimal::from(cumulative)
            );
            assert_eq!(
                Decimal::from_str_exact(row["leavesQuantity"].as_str().unwrap()).unwrap(),
                Decimal::from(leaves)
            );
        }
        assert_eq!(entry_fills[0]["cachedStatus"], "PARTIALLY_FILLED");
        assert_eq!(entry_fills[1]["cachedStatus"], "PENDING_CANCEL");
        let modified = callbacks
            .iter()
            .find(|r| r["callback"] == "on_order_modify_rejected")
            .unwrap();
        assert_eq!(
            Decimal::from_str_exact(modified["price"].as_str().unwrap()).unwrap(),
            Decimal::new(1, 2)
        );
        let entry = cache
            .borrow()
            .order_owned(&ClientOrderId::new("EXIT-ENTRY-001"))
            .unwrap();
        assert_eq!(entry.quantity(), Quantity::from(250));
        assert_eq!(entry.filled_qty(), Quantity::from(200));
        assert_eq!(entry.status(), OrderStatus::Canceled);
        let fills = callbacks
            .iter()
            .filter(|r| {
                r["callback"] == "on_order_filled"
                    && (!case.share_entry()
                        || private.state.creations.lock().unwrap().iter().any(|body| {
                            native_id(&cache, body).as_str() == r["clientOrderId"].as_str().unwrap()
                        }))
            })
            .count();
        assert_eq!(
            fills,
            if case == Case::DefaultReduceOnly {
                3
            } else if case == Case::Profit || case.terminal_residual() {
                4
            } else {
                5
            }
        );
        assert_eq!(
            callbacks
                .iter()
                .filter(|r| r["callback"] == "on_position_opened")
                .count(),
            1
        );
        assert_eq!(
            callbacks
                .iter()
                .filter(|r| r["callback"] == "on_position_closed")
                .count(),
            usize::from(case.remaining_position() == case.passive_holding())
        );
        if case.remaining_position() > case.passive_holding() {
            assert_eq!(
                cache
                    .borrow()
                    .positions_open(None, Some(&instrument_id(1)), None, None, None)[0]
                    .quantity
                    .as_decimal(),
                Decimal::from(case.remaining_position())
            );
        } else {
            let cache = cache.borrow();
            let positions = cache.positions(
                None,
                Some(&instrument_id(1)),
                Some(&nautilus_model::identifiers::StrategyId::new(
                    "PREDICT-EXIT-001",
                )),
                None,
                None,
            );
            assert_eq!(positions.len(), 1);
            assert!(positions[0].is_closed());
            assert_eq!(
                positions[0].realized_pnl.unwrap().as_decimal(),
                final_cash - Decimal::from(1000)
                    + Decimal::from(case.passive_holding()) * Decimal::new(1, 2)
            );
        }
    } else {
        assert!(
            !callbacks
                .iter()
                .any(|r| r["callback"] == "on_position_opened")
        );
    }
    assert!(
        callbacks
            .iter()
            .any(|r| r["type"] == "NativeAccountReported" || r["type"] == "NativeAccountSnapshot")
    );
    if case == Case::EntryDenied {
        assert!(
            callbacks
                .iter()
                .any(|r| r["type"] == "NativeAccountSnapshot")
        );
    } else {
        assert!(
            callbacks
                .iter()
                .any(|r| r["type"] == "NativeAccountReported")
        );
    }
    if reconnect {
        // Portfolio may coalesce equal balances and the reporter can supersede info.
        // The adapter's native connectivity probe includes verified wallet epoch;
        // an AccountState metadata marker is not a durable strategy permission.
        let observed = callbacks
            .iter()
            .filter(|r| r["type"] == "ExecutionConnectionObserved")
            .collect::<Vec<_>>();
        assert!(
            observed
                .windows(2)
                .any(|pair| pair[0]["connected"] == false && pair[1]["connected"] == true),
            "strategy must observe lost continuity then verified execution connectivity"
        );
        assert_eq!(private.state.sockets.lock().unwrap().len(), 2);
        assert_eq!(exit_requests(&private, case).len(), 3);
        assert_eq!(
            private.state.removals.lock().unwrap().len(),
            if case.is_concurrent() { 3 } else { 2 }
        );
    }
    assert!(private.state.open_orders.lock().unwrap().is_empty());
    assert_eq!(
        cache
            .borrow()
            .orders_open(None, Some(&instrument_id(1)), None, None, None)
            .len(),
        0
    );
    assert!(
        cache
            .borrow()
            .orders_inflight(None, Some(&instrument_id(1)), None, None, None)
            .is_empty()
    );
    if case.is_concurrent()
        && let Some(saved) = saved.as_ref()
    {
        assert_eq!(
            saved["concurrentObservations"],
            json!(concurrent_observations)
        );
        assert_eq!(
            saved["requests"].as_array().unwrap().len(),
            private.state.creations.lock().unwrap().len()
        );
        let actual = passive_journal.borrow();
        let expected = saved["passiveFeedback"].as_array().unwrap();
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            for key in ["type", "status", "strategyId"] {
                assert_eq!(actual[key], expected[key]);
            }
        }
    }
    if case == Case::NativeQuotes {
        let primary = callbacks
            .iter()
            .filter(|r| r["type"] == "NativeQuoteObserved")
            .collect::<Vec<_>>();
        let passive = passive_journal.borrow();
        assert_eq!(
            primary,
            passive.iter().collect::<Vec<_>>(),
            "one native quote must reach both strategies once"
        );
        let quote_keys = callbacks
            .iter()
            .filter(|r| r["type"] == "NativeQuoteObserved")
            .map(|r| {
                (
                    r["tsEvent"].as_u64().unwrap(),
                    r["bid"].as_str().unwrap(),
                    r["ask"].as_str().unwrap(),
                    r["bidSize"].as_str().unwrap(),
                    r["askSize"].as_str().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert!(!quote_keys.is_empty());
        assert_eq!(
            quote_keys
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            quote_keys.len(),
            "strategy must not re-publish the adapter quote"
        );
        assert!(
            callbacks
                .iter()
                .any(|r| r["type"] == "NativeQuoteObserved" && r["bid"] == "0.008")
        );
        assert_eq!(
            cache.borrow().quote(&instrument_id(1)).unwrap().bid_price,
            Price::from("0.008")
        );
    }
    if case.rejection_boundary() {
        let cache = cache.borrow();
        let orders = cache.orders(None, Some(&instrument_id(1)), None, None, None);
        let failed = orders
            .iter()
            .filter(|o| matches!(o.status(), OrderStatus::Denied | OrderStatus::Rejected))
            .collect::<Vec<_>>();
        if case == Case::ShareResidualAbove {
            assert!(failed.is_empty());
            assert_eq!(private.state.creations.lock().unwrap().len(), 4);
        } else {
            assert_eq!(failed.len(), 1);
            let order = failed[0];
            let default = case == Case::DefaultReduceOnly;
            assert_eq!(order.order_type(), OrderType::Market);
            assert_eq!(order.order_side(), OrderSide::Sell);
            assert_eq!(order.is_reduce_only(), default);
            assert_eq!(
                order.quantity().as_decimal(),
                Decimal::from(case.remaining_position())
            );
            assert!(order.filled_qty().is_zero());
            assert!(order.venue_order_id().is_none());
            assert_eq!(order.status(), OrderStatus::Denied);
            let id = order.client_order_id().to_string();
            let callback = "on_order_denied";
            assert_eq!(
                callbacks
                    .iter()
                    .filter(|r| r["clientOrderId"] == id && r["callback"] == callback)
                    .count(),
                1
            );
            assert_eq!(
                callbacks
                    .iter()
                    .filter(|r| r["clientOrderId"] == id && r["callback"] == "on_order_submitted")
                    .count(),
                0
            );
            assert!(
                !callbacks
                    .iter()
                    .any(|r| r["clientOrderId"] == id && r["callback"] == "on_order_accepted")
            );
            let reason = if default {
                "reduce-only is unsupported"
            } else {
                "minimum of 1 USDT"
            };
            assert!(callbacks.iter().any(|r| r["clientOrderId"] == id
                && r["reason"].as_str().is_some_and(|r| r.contains(reason))));
            assert_eq!(
                private.state.creations.lock().unwrap().len(),
                if default { 2 } else { 3 }
            );
        }
        assert_eq!(
            *private.state.sell_inventory.lock().unwrap(),
            U256::from(case.remaining_position()) * U256::from(1_000_000_000_000_000_000u64)
        );
        assert_eq!(private.state.removals.lock().unwrap().len(), 2);
        let close_requests = callbacks
            .iter()
            .filter(|r| r["type"] == "NativeClosePositionRequested")
            .collect::<Vec<_>>();
        assert_eq!(
            close_requests.len(),
            if case == Case::DefaultReduceOnly {
                1
            } else {
                2
            }
        );
        assert_eq!(
            Decimal::from_str_exact(close_requests[0]["quantity"].as_str().unwrap()).unwrap(),
            Decimal::from(150)
        );
        if case != Case::DefaultReduceOnly {
            assert_eq!(
                Decimal::from_str_exact(close_requests[1]["quantity"].as_str().unwrap()).unwrap(),
                Decimal::from(if case == Case::ShareResidualBelow {
                    126
                } else {
                    127
                })
            );
            assert!(close_requests.iter().all(|r| r["reduceOnly"] == false));
        } else {
            assert!(close_requests[0]["reduceOnly"].is_null());
        }
    }
    if case.native_reconcile() {
        let cache = cache.borrow();
        let orders = cache.orders(None, Some(&instrument_id(1)), None, None, None);
        let fills = orders
            .iter()
            .flat_map(|o| o.events().into_iter())
            .filter_map(|e| {
                if let nautilus_model::events::OrderEventAny::Filled(fill) = e {
                    Some(fill.trade_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let expected = if case.passive_holding() > 0 { 7 } else { 5 };
        assert_eq!(fills.len(), expected);
        assert_eq!(
            fills.iter().collect::<std::collections::HashSet<_>>().len(),
            expected
        );
    }
    if case == Case::ConcurrentPartialBuy {
        let cache = cache.borrow();
        let positions = cache.positions_open(None, Some(&instrument_id(1)), None, None, None);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].strategy_id.as_str(), "PREDICT-PASSIVE-002");
        assert_eq!(positions[0].quantity.as_decimal(), Decimal::from(40));
        assert_eq!(
            *private.state.sell_inventory.lock().unwrap(),
            U256::from(40) * U256::from(1_000_000_000_000_000_000u64)
        );
        let passive = passive_journal.borrow();
        assert_eq!(
            passive
                .iter()
                .filter(|r| r["type"] == "PositionOpened")
                .count(),
            1
        );
        assert_eq!(
            passive
                .iter()
                .filter(|r| r["type"] == "PositionChanged")
                .count(),
            1
        );
        assert!(!passive.iter().any(|r| r["type"] == "PositionClosed"));
    }
    json!({"concurrentObservations":concurrent_observations,"passiveFeedback":passive_journal.borrow().clone(),"nativeInflightChecks":reconnect,"customTimerQueriesOrders":false,"matchResponses":private.state.match_response_log.lock().unwrap().clone(),"positionResponses":private.state.position_response_log.lock().unwrap().clone(),"orderLists":private.state.order_list_log.lock().unwrap().clone(),"orderResponses":private.state.order_response_log.lock().unwrap().clone(),"matches":private.state.match_snapshot.lock().unwrap().clone(),"source":"synthetic loopback; native strategy initiates all orders; not a market recommendation or mainnet evidence","case":case.name(),"walletEvents":events,"orderbooks":books,"requests":private.state.creations.lock().unwrap().clone(),"removals":private.state.removals.lock().unwrap().clone(),"nativeFeedback":callbacks.clone(),"finalCash":final_cash.to_string(),"remainingPosition":case.remaining_position(),"mainnetWrites":0})
}

fn reconnect_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/automatic_reconnect_exit_synthetic_20261016.json")
}
pub(super) fn reconnect_snapshot(
    private: &Server,
    events: &[Value],
    orders: &[(Value, &str, u32)],
    net: u32,
) {
    private.state.orders.lock().unwrap().clear();
    let mut open = vec![];
    for (body, status, filled) in orders {
        let mut row = super::live_node::rest_row(body, status);
        if let Some(previous) = private
            .state
            .orders_by_hash
            .lock()
            .unwrap()
            .get(body["data"]["order"]["hash"].as_str().unwrap())
        {
            row["id"] = previous["data"]["id"].clone();
        }
        row["amountFilled"] = json!((u128::from(*filled) * 1_000_000_000_000_000_000).to_string());
        private.state.orders_by_hash.lock().unwrap().insert(
            body["data"]["order"]["hash"].as_str().unwrap().to_owned(),
            json!({"success":true,"data":row.clone()}),
        );
        if *status == "OPEN" {
            open.push(row);
        }
    }
    *private.state.open_orders.lock().unwrap() = open;
    let mut holding = holding(1);
    holding["amount"] = json!((u128::from(net) * 1_000_000_000_000_000_000).to_string());
    *private.state.position_snapshot.lock().unwrap() =
        Some(if net == 0 { vec![] } else { vec![holding] });
    let mut matches = vec![];
    for event in events
        .iter()
        .filter(|e| e["type"] == "orderTransactionSuccess")
    {
        let mut row = super::matches::match_row(1);
        row["settlementId"] = event["settlementId"].clone();
        row["executedAt"] = json!(
            chrono::DateTime::from_timestamp_millis(event["timestamp"].as_i64().unwrap())
                .unwrap()
                .to_rfc3339()
        );
        row["amountFilled"] = event["fill"]["executedSizeWei"].clone();
        row["priceExecuted"] = event["fill"]["executedPriceWei"].clone();
        for outcome in row["market"]["outcomes"].as_array_mut().unwrap() {
            outcome["status"] = Value::Null;
        }
        let mut own = row["taker"].clone();
        own["quoteType"] = json!(if event["details"]["quoteType"] == "ASK" {
            "Ask"
        } else {
            "Bid"
        });
        own["hash"] = event["orderHash"].clone();
        own["amount"] = event["fill"]["executedSizeWei"].clone();
        own["price"] = event["fill"]["executedPriceWei"].clone();
        own["fee"] = json!({"type":"COLLATERAL","amount":event["fee"]["amountWei"]});
        own["outcome"]["status"] = Value::Null;
        row["taker"] = own.clone();
        row["taker"]["signer"] = json!("0x2222222222222222222222222222222222222222");
        row["taker"]["hash"] = json!(format!("0x{}", "22".repeat(32)));
        row["taker"]["quoteType"] = json!(if own["quoteType"] == "Ask" {
            "Bid"
        } else {
            "Ask"
        });
        row["makers"] = json!([own]);
        matches.push(row.clone());
        matches.push(row); // Overlapping REST pages must not repeat native fills.
    }
    *private.state.match_snapshot.lock().unwrap() = Some(matches);
}
async fn automatic_reconnect(
    private: &Server,
    engine: &Rc<RefCell<ExecutionEngine>>,
    cache: &Rc<RefCell<Cache>>,
    maker: &Value,
    status: OrderStatus,
) {
    let count = private.state.sockets.lock().unwrap().len();
    private
        .state
        .sockets
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(Message::Close(None))
        .unwrap();
    barrier("adapter gates during wallet disconnect", || {
        !engine
            .borrow_mut()
            .get_client_adapter_mut(&"PREDICT".into())
            .unwrap()
            .is_connected()
    })
    .await;
    barrier("wallet transport reconnects", || {
        private.state.sockets.lock().unwrap().len() > count
    })
    .await;
    barrier("automatic reconciliation restores known maker", || {
        engine
            .borrow_mut()
            .get_client_adapter_mut(&"PREDICT".into())
            .unwrap()
            .is_connected()
            && cache
                .borrow()
                .order(&native_id(cache, maker))
                .unwrap()
                .status()
                == status
    })
    .await;
}
#[tokio::test(flavor = "current_thread")]
async fn automatic_reconnect_restores_active_take_profit_without_replacement_then_stops() {
    run(Case::ReconnectStop, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn automatic_reconnect_recovers_cancel_fill_race_before_taker_stop() {
    run(Case::ReconnectCancelStop, true).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline synthetic capture; never overwrite saved JSON"]
async fn record_automatic_reconnect_exit_json() {
    let sessions = vec![
        run(Case::ReconnectStop, false).await,
        run(Case::ReconnectCancelStop, false).await,
    ];
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(reconnect_path())
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"source":"synthetic Rust Strategy + LiveNode loopback, not mainnet feedback","sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline native inflight/account recovery exit recording"]
async fn record_native_account_exit_json() {
    let sessions = vec![
        run(Case::ReconnectStop, true).await,
        run(Case::ReconnectCancelStop, true).await,
    ];
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/private_api/native_account_exits_synthetic_20261016.json"),
        )
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({"source":"synthetic Rust Strategy + LiveNode; native inflight checks and account-only timer; no mainnet writes","sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit post-multi-admission exit replay; never overwrite historical JSON"]
async fn record_multi_admission_exit_replay_json() {
    let sessions = vec![
        run(Case::ReconnectCancelStop, true).await,
        run(Case::PartialStop, true).await,
        run(Case::DelayedStopAck, true).await,
    ];
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/multi_admission_exit_replay_synthetic_20261016.json");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic post-admission-change Rust Strategy + LiveNode exit replay; not mainnet; exit scenarios run separately from multi-order scenarios","sessions":sessions})).unwrap();
}
