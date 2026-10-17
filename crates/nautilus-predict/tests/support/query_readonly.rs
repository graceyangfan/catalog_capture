// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Explicit read-only live probe. A stale local context is test input, never a real submitted order.
use std::{cell::Cell, time::Duration};

use nautilus_common::{
    actor::DataActor, factories::OrderEventFactory, messages::ExecutionReport,
    msgbus::TypedIntoHandler,
};
use nautilus_model::{
    enums::{OrderSide, OrderStatus, OrderType},
    events::{OrderCanceled, OrderEventAny},
    identifiers::{ClientOrderId, StrategyId, VenueOrderId},
    instruments::Instrument,
    orders::{Order, builder::OrderTestBuilder},
    types::{Price, Quantity},
};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;

#[derive(Deserialize)]
pub struct RelayConfig {
    pub http_origin: String,
    pub rpc_origin: String,
    pub phase_file: PathBuf,
    pub feedback_path: PathBuf,
    pub wire_state_file: PathBuf,
}
#[derive(Debug)]
pub struct CommandProbe {
    pub core: StrategyCore,
    pub market_id: u64,
    pub account_id: AccountId,
    pub target: ClientOrderId,
    pub phase_file: PathBuf,
    pub journal: Rc<RefCell<Vec<Value>>>,
    pub order_requests: Rc<Cell<u8>>,
    pub sent: u8,
    pub initial_account: Option<UUID4>,
    pub account_observed: bool,
    pub account_request: Rc<Cell<bool>>,
    pub wire_state_file: PathBuf,
}
impl DataActor for CommandProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.journal
            .borrow_mut()
            .push(json!({"type":"StrategyOnStart"}));
        self.subscribe_data(
            nautilus_predict::predict_orderbook_data_type(self.market_id),
            Some(ClientId::new("PREDICT")),
            None,
        );
        Ok(())
    }
    fn on_data(&mut self, _: &nautilus_model::data::CustomData) -> anyhow::Result<()> {
        if self.account_request.get() && self.initial_account.is_none() {
            let cache = self.cache();
            let account = cache
                .account(&self.account_id)
                .ok_or_else(|| anyhow::anyhow!("Native account missing"))?;
            let state = account
                .last_event()
                .ok_or_else(|| anyhow::anyhow!("Native account event missing"))?;
            self.journal.borrow_mut().push(json!({"type":"BeforeQueryAccount","eventId":state.event_id,"balances":state.balances,"info":state.info.as_ref().map(|p|json!({"blockNumber":p.get("predict_block_number"),"blockHash":p.get("predict_block_hash")}))}));
            drop(cache);
            self.initial_account = Some(state.event_id);
            std::fs::write(&self.phase_file, "query-account")?;
            self.query_account(self.account_id, Some(ClientId::new("PREDICT")), None)?;
            self.journal
                .borrow_mut()
                .push(json!({"type":"StrategyQueryAccount"}));
        }
        if self.initial_account.is_some() && !self.account_observed {
            let cache = self.cache();
            let account = cache
                .account(&self.account_id)
                .ok_or_else(|| anyhow::anyhow!("Native account missing"))?;
            let state = account
                .last_event()
                .ok_or_else(|| anyhow::anyhow!("Native account event missing"))?;
            let wire: Value =
                serde_json::from_str(&std::fs::read_to_string(&self.wire_state_file)?)?;
            let block = state
                .info
                .as_ref()
                .and_then(|p| p.get("predict_block_hash"))
                .and_then(Value::as_str);
            if Some(state.event_id) != self.initial_account
                && block.is_some()
                && block == wire["queryAccountBlockHash"].as_str()
            {
                self.journal.borrow_mut().push(json!({"type":"AfterQueryAccount","eventId":state.event_id,"balances":state.balances,"info":state.info.as_ref().map(|p|json!({"blockNumber":p.get("predict_block_number"),"blockHash":p.get("predict_block_hash")})),"observation":"new applied AccountState; UUID is not a query correlation ID"}));
                drop(cache);
                self.account_observed = true;
            }
        }
        if self.order_requests.get() > self.sent {
            let order = self
                .cache()
                .order(&self.target)
                .ok_or_else(|| anyhow::anyhow!("query context missing"))?
                .clone();
            self.sent += 1;
            std::fs::write(&self.phase_file, format!("query-order-{}", self.sent))?;
            self.journal.borrow_mut().push(json!({"type":"StrategyQueryOrder","attempt":self.sent,"cachedStatus":order.status().to_string()}));
            self.query_order(&order, Some(ClientId::new("PREDICT")), None)?;
        }
        Ok(())
    }
}
nautilus_strategy!(CommandProbe, {
    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        let order = self.cache().order(&event.client_order_id).unwrap().clone();
        self.journal.borrow_mut().push(json!({"type":"on_order_canceled","cachedStatus":order.status().to_string(),"filledQty":order.filled_qty().to_string()}));
    }
    fn on_order_event(&mut self, event: OrderEventAny) {
        self.journal
            .borrow_mut()
            .push(json!({"type":"OrderEvent","kind":format!("{:?}",event.event_type())}));
    }
});

async fn until(label: &str, mut predicate: impl FnMut() -> bool) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(40), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("read-only causal barrier timed out: {label}"))
}

pub async fn run(config: RelayConfig) -> anyhow::Result<()> {
    let settings = operator_settings()?;
    let mut http_config = settings.private_config()?;
    http_config.base_url_http = Some(config.http_origin.clone());
    let signer = settings.signer()?;
    let private = nautilus_predict::http::PredictPrivateHttpClient::new(http_config.clone())?;
    let session = private.authenticate(&signer).await?;
    // Discover a prior zero-fill canceled LIMIT from recorded real history, not TOML market IDs.
    let prior: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/cold_start_readonly_20261016T133220Z.json"
    ))?;
    let hash = prior["ordersByHash"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| {
            r["status"] == "CANCELLED"
                && r["strategy"] == "LIMIT"
                && r["amountFilled"] == "0"
                && r["order"]["side"] == 1
        })
        .and_then(|r| r["order"]["hash"].as_str())
        .ok_or_else(|| anyhow::anyhow!("no recorded canceled SELL LIMIT"))?;
    let row = session.request_order(hash.parse()?).await?;
    anyhow::ensure!(
        row.status == nautilus_predict::http::account_models::PredictOrderStatus::Cancelled
            && row.amount_filled == "0",
        "historical order must still be zero-fill canceled"
    );
    let key = http_config
        .api_key
        .take()
        .ok_or_else(|| anyhow::anyhow!("API key required"))?;
    let mut profile = product_host::NodeProfile::parse(include_str!(
        "../../../../examples/predict-node.toml.example"
    ))?;
    profile.predict.http_url = Some(config.http_origin.clone());
    profile.predict.rpc_url = config.rpc_origin.clone();
    profile.predict.timeout_secs = settings.network.timeout_secs;
    profile.node.exec_engine.reconciliation_lookback_mins = Some(1);
    profile.node.exec_engine.reconciliation_startup_delay_secs = 0.0;
    profile.node.exec_engine.inflight_check_interval_ms = 0;
    profile.node.exec_engine.open_check_interval_secs = None;
    profile.node.exec_engine.position_check_interval_secs = None;
    profile.node.delay_post_stop = Duration::from_secs(1);
    anyhow::ensure!(
        profile.predict.submission.is_none() && !profile.predict.orderbook_removal_enabled,
        "read-only host required"
    );
    std::fs::write(&config.phase_file, "startup")?;
    let prepared = product_host::prepare(profile, key.clone(), signer).await?;
    let mut live = prepared.node;
    // Historical metadata is diagnostic-only: no expired universe or settlement feature is added.
    let mut provider = PredictInstrumentProvider::new(PredictHttpClient::new(
        key.expose_secret(),
        Some(&config.http_origin),
        settings.network.timeout_secs,
    )?);
    let (_, definitions) = provider
        .load_market_with_definitions(row.market_id, get_atomic_clock_realtime().get_time_ns())
        .await?;
    let instrument = definitions
        .iter()
        .find(|i| i.raw_symbol().as_str() == row.order.fields.token_id.to_string())
        .ok_or_else(|| anyhow::anyhow!("historical token absent"))?
        .clone();
    let cache = live.kernel().cache.clone();
    for definition in definitions {
        cache.borrow_mut().add_instrument(definition)?;
    }
    let trader_id = TraderId::new("PREDICT-NODE-001");
    let account_id = AccountId::new("PREDICT-001");
    let strategy_id = StrategyId::new("QUERY-READONLY-001");
    let target = ClientOrderId::new("READONLY-HISTORICAL-CONTEXT");
    let qty = rust_decimal::Decimal::from_str_exact(&row.order.fields.maker_amount.to_string())?
        / rust_decimal::Decimal::from(1_000_000_000_000_000_000u64);
    let collateral =
        rust_decimal::Decimal::from_str_exact(&row.order.fields.taker_amount.to_string())?
            / rust_decimal::Decimal::from(1_000_000_000_000_000_000u64);
    let mut stale = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id)
        .strategy_id(strategy_id)
        .client_order_id(target)
        .instrument_id(instrument.id())
        .side(OrderSide::Sell)
        .quantity(Quantity::from_decimal_dp(qty, instrument.size_precision())?)
        .price(Price::from_decimal_dp(
            collateral / qty,
            instrument.price_precision(),
        )?)
        .build();
    let factory = OrderEventFactory::new(trader_id, account_id, AccountType::Cash, None);
    stale.apply(
        factory.generate_order_submitted(&stale, get_atomic_clock_realtime().get_time_ns()),
    )?;
    let accepted = factory.generate_order_accepted(
        &stale,
        VenueOrderId::new(hash),
        get_atomic_clock_realtime().get_time_ns(),
        get_atomic_clock_realtime().get_time_ns(),
    );
    let engine = live.kernel().exec_engine.clone();
    let journal = Rc::new(RefCell::new(vec![]));
    // Test-only observation of the runner's Native report endpoint; delegate unchanged to Native.
    let engine_weak = Rc::downgrade(&engine);
    let applied = journal.clone();
    let observed_cache = cache.clone();
    msgbus::register_execution_report_endpoint(
        MessagingSwitchboard::exec_engine_reconcile_execution_report(),
        TypedIntoHandler::from(move |report: ExecutionReport| {
            if let Some(engine) = engine_weak.upgrade() {
                engine.borrow_mut().reconcile_execution_report(&report);
                if let ExecutionReport::Order(ref order) = report {
                    applied.borrow_mut().push(json!({"type":"NativeOrderReportApplied","report":order,"cachedStatus":observed_cache.borrow().order(&target).map(|o|o.status().to_string())}));
                }
            }
        }),
    );
    let order_requests = Rc::new(Cell::new(0));
    let account_request = Rc::new(Cell::new(false));
    live.add_strategy(CommandProbe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(strategy_id),
            ..Default::default()
        }),
        market_id: prepared.market_id,
        account_id,
        target,
        phase_file: config.phase_file.clone(),
        journal: journal.clone(),
        order_requests: order_requests.clone(),
        sent: 0,
        initial_account: None,
        account_observed: false,
        account_request: account_request.clone(),
        wire_state_file: config.wire_state_file.clone(),
    })?;
    let handle = live.handle();
    let driver = async {
        let result = async {
            until("strategy started", || {
                journal
                    .borrow()
                    .iter()
                    .any(|r| r["type"] == "StrategyOnStart")
            })
            .await?;
            let mut last_requests = None;
            let mut quiet_since = std::time::Instant::now();
            until("startup account IO applied and quiescent", || {
                let wire: Value = serde_json::from_str(
                    &std::fs::read_to_string(&config.wire_state_file).unwrap(),
                )
                .unwrap();
                let count = wire["requests"].as_u64();
                if wire["active"] != 0 || count != last_requests {
                    last_requests = count;
                    quiet_since = std::time::Instant::now();
                    return false;
                }
                let applied = cache
                    .borrow()
                    .account(&account_id)
                    .and_then(|a| a.last_event())
                    .and_then(|s| s.info)
                    .and_then(|p| p.get("predict_block_hash").cloned())
                    .and_then(|v| v.as_str().map(String::from));
                applied.is_some()
                    && applied.as_deref() == wire["lastBalanceBlockHash"].as_str()
                    && quiet_since.elapsed() >= Duration::from_millis(500)
            })
            .await?;
            journal.borrow_mut().push(json!({
                "type": "StartupAccountQuiescent",
                "wire": serde_json::from_str::<Value>(
                    &std::fs::read_to_string(&config.wire_state_file)?,
                )?,
            }));
            account_request.set(true);
            until("QueryAccount applied", || {
                journal
                    .borrow()
                    .iter()
                    .any(|r| r["type"] == "AfterQueryAccount")
            })
            .await?;
            anyhow::ensure!(
                cache
                    .borrow()
                    .orders_open(None, None, None, None, None)
                    .is_empty()
                    && cache
                        .borrow()
                        .positions_open(None, None, None, None, None)
                        .is_empty(),
                "diagnostic requires no real open orders/positions"
            );
            std::fs::write(&config.phase_file, "inject-stale-local-context")?;
            cache
                .borrow_mut()
                .add_order(stale, None, Some(ClientId::new("PREDICT")), false)?;
            engine.borrow_mut().process(&accepted);
            order_requests.set(1);
            until("QueryOrder canceled callback", || {
                journal
                    .borrow()
                    .iter()
                    .any(|r| r["type"] == "on_order_canceled")
            })
            .await?;
            until("QueryOrder Native report applied", || {
                journal
                    .borrow()
                    .iter()
                    .any(|r| r["type"] == "NativeOrderReportApplied")
            })
            .await?;
            order_requests.set(2);
            until("repeated QueryOrder Native report applied", || {
                journal
                    .borrow()
                    .iter()
                    .filter(|r| r["type"] == "NativeOrderReportApplied")
                    .count()
                    >= 2
            })
            .await?;
            anyhow::ensure!(
                journal
                    .borrow()
                    .iter()
                    .filter(|r| r["type"] == "on_order_canceled")
                    .count()
                    == 1,
                "duplicate cancel callback"
            );
            let cache = cache.borrow();
            let order = cache.order(&target).unwrap();
            anyhow::ensure!(
                order.status() == OrderStatus::Canceled && order.filled_qty().is_zero(),
                "terminal query state mismatch"
            );
            anyhow::ensure!(
                cache
                    .positions_open(None, None, None, None, None)
                    .is_empty(),
                "query invented a position"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        handle.stop();
        result
    };
    let (node_result, checks) = tokio::join!(live.run(), driver);
    // Always preserve evidence, including an unsuccessful read-only attempt.
    let out = json!({"source":"actual mainnet queries via Rust Strategy + product LiveNode; deliberately stale historical local context","historicalContextIsDerived":true,"historicalHash":hash,"historicalMarketId":row.market_id,"discoveredMarketId":prepared.market_id,"strategyJournal":journal.borrow().clone(),"nodeSucceeded":node_result.is_ok(),"checksSucceeded":checks.is_ok(),"submissionSignerInstalled":false,"orderbookRemovalEnabled":false,"realOrderWrites":0});
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&config.feedback_path)?,
        &out,
    )?;
    node_result?;
    checks
}
