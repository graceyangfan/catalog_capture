// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Test-only Rust strategy. Native Strategy/Cache/Portfolio own all trading state.

use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    actor::DataActor,
    msgbus::{self, TypedHandler},
};
use nautilus_live::node::LiveNodeHandle;
use nautilus_model::{
    data::CustomData,
    enums::{OrderSide, TimeInForce},
    events::{
        AccountState, OrderAccepted, OrderCancelRejected, OrderCanceled, OrderEventAny,
        OrderFilled, OrderModifyRejected, PositionEvent,
    },
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId},
    instruments::Instrument,
    orders::Order,
    types::{Price, Quantity},
};
use nautilus_predict::{PredictOrderbookSnapshot, predict_orderbook_data_type};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Deserialize;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Observe,
    Resting,
    Fill,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategySettings {
    pub phase: Phase,
    pub run_secs: u64,
    pub stale_after_ms: u64,
    pub minimum_rebound_ticks: u32,
}

impl StrategySettings {
    pub fn parse(source: &str) -> anyhow::Result<Self> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct File {
            strategy: StrategySettings,
        }
        let value: File = toml::from_str(source)
            .map_err(|_| anyhow::anyhow!("invalid Predict strategy TOML schema"))?;
        anyhow::ensure!(
            (1..=300).contains(&value.strategy.run_secs)
                && (1..=30_000).contains(&value.strategy.stale_after_ms)
                && (1..=100).contains(&value.strategy.minimum_rebound_ticks),
            "invalid Predict strategy bounds"
        );
        Ok(value.strategy)
    }
}

#[derive(Debug, Default)]
pub struct Observations {
    pub snapshots: usize,
    pub submitted: usize,
    pub accepted: usize,
    pub rejected: usize,
    pub denied: usize,
    pub cancel_rejected: usize,
    pub modify_rejected: usize,
    pub modify_terms_preserved: usize,
    pub canceled: usize,
    pub fills: usize,
    pub buy_fill_quantity_sum: Decimal,
    pub buy_commission_sum: Decimal,
    pub last_buy_fill_qty: Option<Quantity>,
    pub last_buy_commission: Option<nautilus_model::types::Money>,
    pub position_events: usize,
    pub account_events: usize,
    pub account_reports_observed: usize,
    pub events_with_cached_order: usize,
    pub last_position_qty: Option<Quantity>,
    pub last_cash_total: Option<Decimal>,
    pub last_cash_locked: Option<Decimal>,
    pub complete: bool,
    pub recorded_feedback: Vec<serde_json::Value>,
    pub feedback_write_failed: bool,
    /// Bounded normalized public snapshots for an explicit live test capture only.
    pub recorded_snapshots: Vec<serde_json::Value>,
}

pub struct CalibrationInputs {
    pub account_id: AccountId,
    pub settings: StrategySettings,
    /// Runtime-discovered identity; never read from operator TOML.
    pub market_id: u64,
    pub instrument_id: InstrumentId,
    pub outcome: u8,
    pub quantity: Quantity,
    pub maximum_price: Decimal,
    pub maximum_order_cost: Decimal,
    pub maximum_total_cost: Decimal,
    pub execution_enabled: bool,
    pub fee_rate_bps: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct RestingBookConfirmation {
    pub market_id: u64,
    pub outcome: u8,
    pub bid: Decimal,
    pub ask: Decimal,
    pub observed_ns: u64,
}

impl RestingBookConfirmation {
    pub fn matches(
        &self,
        now: u64,
        market: u64,
        outcome: u8,
        bid: Decimal,
        ask: Decimal,
        stale_ms: u64,
    ) -> bool {
        self.market_id == market
            && self.outcome == outcome
            && self.bid == bid
            && self.ask == ask
            && now
                .checked_sub(self.observed_ns)
                .is_some_and(|age| age <= stale_ms * 1_000_000)
    }
}

#[derive(Debug)]
pub struct CalibrationStrategy {
    core: StrategyCore,
    inputs: CalibrationInputs,
    observations: Rc<RefCell<Observations>>,
    handle: LiveNodeHandle,
    active: Option<ClientOrderId>,
    last_bid: Option<Decimal>,
    cancel_requested: bool,
    attempted_cost: Decimal,
    account_handler: Option<TypedHandler<AccountState>>,
    last_account_event: Option<nautilus_core::UUID4>,
    expected_reconciled_quantity: Option<Decimal>,
    expected_cash_total: Option<Decimal>,
    replay_fill_target: Option<Quantity>,
    feedback_path: Option<std::path::PathBuf>,
    fixed_resting_price: Option<Decimal>,
    cancel_due_ns: Option<u64>,
    resting_confirmation: Option<Rc<RefCell<Option<RestingBookConfirmation>>>>,
    pending_snapshot: Option<CustomData>,
    resting_time_in_force: TimeInForce,
}

impl std::fmt::Debug for CalibrationInputs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CalibrationInputs")
            .field("phase", &self.settings.phase)
            .field("market_id", &self.market_id)
            .finish_non_exhaustive()
    }
}

impl CalibrationStrategy {
    pub fn new(
        inputs: CalibrationInputs,
        observations: Rc<RefCell<Observations>>,
        handle: LiveNodeHandle,
    ) -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::new("PREDICT-CALIBRATION-001")),
                oms_type: Some(nautilus_model::enums::OmsType::Netting),
                ..Default::default()
            }),
            inputs,
            observations,
            handle,
            active: None,
            last_bid: None,
            cancel_requested: false,
            attempted_cost: Decimal::ZERO,
            account_handler: None,
            last_account_event: None,
            expected_reconciled_quantity: None,
            expected_cash_total: None,
            replay_fill_target: None,
            feedback_path: None,
            fixed_resting_price: None,
            cancel_due_ns: None,
            resting_confirmation: None,
            pending_snapshot: None,
            resting_time_in_force: TimeInForce::Gtc,
        }
    }

    /// Loopback-only barrier: keep the node running until its own position poll is applied.
    #[allow(dead_code)] // Shared support also compiles in the read-only live test binary.
    pub fn with_expected_reconciled_quantity(mut self, quantity: Decimal) -> Self {
        self.expected_reconciled_quantity = Some(quantity);
        self
    }

    /// Loopback-only barrier: order completion and unlocked funds can precede a new balance.
    #[allow(dead_code)] // Shared support also compiles in the read-only live test binary.
    pub fn with_expected_cash_total(mut self, total: Decimal) -> Self {
        self.expected_cash_total = Some(total);
        self
    }

    #[allow(dead_code)] // Loopback burst replay only; never changes mainnet strategy policy.
    pub fn with_fill_replay_target(mut self, quantity: Quantity) -> Self {
        self.replay_fill_target = Some(quantity);
        self
    }

    /// Explicit mainnet test journal. Only allowlisted native fields are retained.
    #[allow(dead_code)]
    pub fn with_live_feedback(mut self, path: std::path::PathBuf, price: Decimal) -> Self {
        self.feedback_path = Some(path);
        self.fixed_resting_price = Some(price);
        self
    }

    /// Offline maker replay uses the same two minimum-size terms as the live runner.
    #[allow(dead_code)]
    pub fn with_resting_price(mut self, price: Decimal) -> Self {
        self.fixed_resting_price = Some(price);
        self
    }

    /// Test matrix only: GTD is signed with a whole-second future expiration.
    #[allow(dead_code)]
    pub fn with_resting_time_in_force(mut self, time_in_force: TimeInForce) -> Self {
        assert!(matches!(time_in_force, TimeInForce::Gtc | TimeInForce::Gtd));
        self.resting_time_in_force = time_in_force;
        self
    }

    #[allow(dead_code)]
    pub fn with_resting_confirmation(
        mut self,
        value: Rc<RefCell<Option<RestingBookConfirmation>>>,
    ) -> Self {
        self.resting_confirmation = Some(value);
        self
    }

    fn record_feedback(&mut self, value: serde_json::Value) {
        let mut observed = self.observations.borrow_mut();
        if observed.recorded_feedback.len() >= 1000 {
            observed.feedback_write_failed = true;
            self.handle.stop();
            return;
        }
        observed.recorded_feedback.push(value);
        if let Some(path) = &self.feedback_path {
            let result = serde_json::to_vec_pretty(&observed.recorded_feedback)
                .map_err(std::io::Error::other)
                .and_then(|bytes| std::fs::write(path, bytes));
            if result.is_err() {
                observed.feedback_write_failed = true;
                self.handle.stop();
            }
        }
    }

    fn observe_account(&mut self) {
        if let Some(expected) = self.expected_reconciled_quantity {
            let observed = self.observations.borrow();
            if observed.complete
                && observed.last_cash_locked == Some(Decimal::ZERO)
                && self
                    .expected_cash_total
                    .is_none_or(|total| observed.last_cash_total == Some(total))
                && observed
                    .last_position_qty
                    .is_some_and(|qty| (qty.as_decimal() - expected).abs() <= Decimal::new(1, 12))
            {
                self.handle.stop();
            }
        }
        // The shared Rust runner sends venue AccountState to Portfolio's endpoint, not
        // events.account.*. Observe its applied native state through the strategy Cache API.
        if let Some(account) = self.cache().account(&self.inputs.account_id)
            && let Some(state) = account.last_event()
            && self.last_account_event != Some(state.event_id)
        {
            self.last_account_event = Some(state.event_id);
            let feedback = serde_json::json!({"type":"AccountStateApplied", "eventId":state.event_id.to_string(),
                "tsEvent":state.ts_event.as_u64(), "balances":state.balances.iter().map(|b|serde_json::json!({
                    "currency":b.currency.code, "total":b.total.as_decimal().to_string(),
                    "free":b.free.as_decimal().to_string(), "locked":b.locked.as_decimal().to_string()
                })).collect::<Vec<_>>()});
            let mut observed = self.observations.borrow_mut();
            observed.account_reports_observed += 1;
            if let Some(balance) = state
                .balances
                .iter()
                .find(|b| b.currency == nautilus_model::types::Currency::USDT())
            {
                observed.last_cash_total = Some(balance.total.as_decimal());
                observed.last_cash_locked = Some(balance.locked.as_decimal());
                if observed.complete
                    && balance.locked.is_zero()
                    && self
                        .expected_cash_total
                        .is_none_or(|total| balance.total.as_decimal() == total)
                    && self.expected_reconciled_quantity.is_none_or(|expected| {
                        observed.last_position_qty.is_some_and(|qty| {
                            (qty.as_decimal() - expected).abs() <= Decimal::new(1, 12)
                        })
                    })
                {
                    self.handle.stop();
                }
            }
            drop(observed);
            self.record_feedback(feedback);
        }
    }

    fn cancel_active(&mut self) -> anyhow::Result<()> {
        if let Some(id) = self.active
            && !self.cancel_requested
        {
            if self
                .cache()
                .order(&id)
                .is_none_or(|order| order.venue_order_id().is_none())
            {
                // An unknown submission is not an accepted venue order. The supervisor
                // independently checks REST; do not force a native PendingCancel here.
                self.record_feedback(serde_json::json!({"type":"CancelDeferredUnresolvedSubmission", "clientOrderId":id.to_string()}));
                return Ok(());
            }
            self.cancel_requested = true;
            self.cancel_order(id, Some(ClientId::new("PREDICT")), None)?;
        }
        Ok(())
    }
}

impl DataActor for CalibrationStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        if self.expected_reconciled_quantity.is_some() {
            self.set_external_order_instrument_ids(vec![self.inputs.instrument_id])?;
        }
        self.subscribe_data(
            predict_orderbook_data_type(self.inputs.market_id),
            Some(ClientId::new("PREDICT")),
            None,
        );
        let observations = self.observations.clone();
        let handler = TypedHandler::from(move |_: &AccountState| {
            let mut observed = observations.borrow_mut();
            observed.account_events += 1;
        });
        msgbus::subscribe_account_state(
            format!("events.account.{}", self.inputs.account_id).into(),
            handler.clone(),
            None,
        );
        self.account_handler = Some(handler);
        self.clock().set_timer(
            "PREDICT-CALIBRATION-ACCOUNT",
            std::time::Duration::from_millis(100),
            None,
            None,
            None,
            None,
            None,
        )?;
        Ok(())
    }

    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        let Some(book) = data
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
        else {
            return Ok(());
        };
        if book.market_id != self.inputs.market_id
            || data.data_type != predict_orderbook_data_type(self.inputs.market_id)
        {
            anyhow::bail!("strategy received a mismatched custom snapshot identity");
        }
        if self.resting_confirmation.is_some() {
            self.pending_snapshot = Some(data.clone());
        }
        self.observations.borrow_mut().snapshots += 1;
        if self.observations.borrow().recorded_snapshots.len() < 16 {
            let levels = |levels: &[nautilus_predict::data_types::PredictOrderbookLevel]| {
                levels
                    .iter()
                    .map(|level| {
                        serde_json::json!([
                            level.price.as_decimal().to_string(),
                            level.quantity.as_decimal().to_string()
                        ])
                    })
                    .collect::<Vec<_>>()
            };
            self.observations.borrow_mut().recorded_snapshots.push(serde_json::json!({
                "marketId":book.market_id, "updateTimestampMs":book.ts_event.as_u64()/1_000_000,
                "orderCount":book.order_count, "bids":levels(&book.bids), "asks":levels(&book.asks)
            }));
        }
        self.observe_account();
        let now = self.clock().timestamp_ns().as_u64();
        let age = now.checked_sub(book.ts_event.as_u64());
        let (bid, ask) = match self.inputs.outcome {
            1 => (
                book.bids.iter().map(|l| l.price.as_decimal()).max(),
                book.asks.iter().map(|l| l.price.as_decimal()).min(),
            ),
            2 => (
                book.asks
                    .iter()
                    .map(|l| Decimal::ONE - l.price.as_decimal())
                    .max(),
                book.bids
                    .iter()
                    .map(|l| Decimal::ONE - l.price.as_decimal())
                    .min(),
            ),
            _ => anyhow::bail!("invalid discovered outcome"),
        };
        let (Some(bid), Some(ask)) = (bid, ask) else {
            return Ok(());
        };
        if bid <= Decimal::ZERO || ask <= bid || ask > Decimal::ONE {
            return Ok(());
        }
        if let Some(confirmation) = &self.resting_confirmation {
            if !confirmation.borrow().is_some_and(|value| {
                value.matches(
                    now,
                    self.inputs.market_id,
                    self.inputs.outcome,
                    bid,
                    ask,
                    self.inputs.settings.stale_after_ms,
                )
            }) {
                return Ok(());
            }
        } else if age.is_none_or(|age| age > self.inputs.settings.stale_after_ms * 1_000_000) {
            return Ok(());
        }
        let previous_bid = self.last_bid.replace(bid);
        if self.observations.borrow().complete
            || self.inputs.settings.phase == Phase::Observe
            || !self.inputs.execution_enabled
            || (self.inputs.settings.phase == Phase::Fill && self.inputs.fee_rate_bps != 0)
            || self.active.is_some()
        {
            return Ok(());
        }
        let limit = if self.inputs.settings.phase == Phase::Resting {
            2
        } else {
            1
        };
        if self.observations.borrow().submitted >= limit {
            return Ok(());
        }
        let (tick, precision) = {
            let cache = self.cache();
            let instrument = cache
                .instrument(&self.inputs.instrument_id)
                .ok_or_else(|| anyhow::anyhow!("strategy instrument is not loaded"))?;
            (
                instrument.price_increment().as_decimal(),
                instrument.price_precision(),
            )
        };
        let price = if self.inputs.settings.phase == Phase::Resting {
            // Two ticks below bid, further capped at the cheap-price ceiling.
            self.fixed_resting_price
                .map(|price| {
                    if self.observations.borrow().submitted == 0 {
                        price
                    } else {
                        price * Decimal::from(2)
                    }
                })
                .unwrap_or((bid - tick * Decimal::from(2)).min(self.inputs.maximum_price))
        } else {
            // A small bid rebound is a test selection heuristic, not evidence of a profitable reversal.
            if previous_bid.is_none_or(|old| {
                bid - old < tick * Decimal::from(self.inputs.settings.minimum_rebound_ticks)
            }) || ask > self.inputs.maximum_price
            {
                return Ok(());
            }
            ask
        };
        let price =
            (price / tick).round_dp_with_strategy(0, RoundingStrategy::ToNegativeInfinity) * tick;
        let quantity =
            if self.fixed_resting_price.is_some() && self.observations.borrow().submitted > 0 {
                Quantity::from_decimal_dp(
                    self.inputs.quantity.as_decimal() / Decimal::from(2),
                    self.inputs.quantity.precision,
                )?
            } else {
                self.inputs.quantity
            };
        let cost = price * quantity.as_decimal();
        if self.fixed_resting_price.is_some()
            && (price > bid / Decimal::from(2) || bid < Decimal::new(2, 1))
        {
            return Ok(());
        }
        if price <= Decimal::ZERO
            || cost <= Decimal::ZERO
            || cost > self.inputs.maximum_order_cost
            || self.attempted_cost + cost > self.inputs.maximum_total_cost
        {
            return Ok(());
        }
        let order = self.order().limit(
            self.inputs.instrument_id,
            OrderSide::Buy,
            quantity,
            Price::from_decimal_dp(price, precision)?,
            Some(self.resting_time_in_force),
            (self.resting_time_in_force == TimeInForce::Gtd).then(|| {
                nautilus_core::UnixNanos::from((now / 1_000_000_000 + 300) * 1_000_000_000)
            }),
            Some(self.inputs.settings.phase == Phase::Resting),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.active = Some(order.client_order_id());
        self.cancel_requested = false;
        self.attempted_cost += cost;
        self.record_feedback(serde_json::json!({"type":"SubmissionDecision", "marketId":book.market_id,
            "bookTsEvent":book.ts_event.as_u64(),"observedNs":now,"bestBid":bid.to_string(),"bestAsk":ask.to_string(),
            "restConfirmed":self.resting_confirmation.is_some(),"timeInForce":self.resting_time_in_force.to_string(),
            "price":price.to_string(),"quantity":quantity.to_string(),"cost":cost.to_string()}));
        self.submit_order(order, None, Some(ClientId::new("PREDICT")), None)?;
        self.observations.borrow_mut().submitted += 1;
        Ok(())
    }

    fn on_time_event(&mut self, event: &nautilus_common::timer::TimeEvent) -> anyhow::Result<()> {
        if event.name == "PREDICT-CALIBRATION-ACCOUNT" {
            self.observe_account();
            if self.resting_confirmation.is_some()
                && let Some(data) = self.pending_snapshot.clone()
            {
                self.on_data(&data)?;
            }
            if self
                .cancel_due_ns
                .is_some_and(|due| self.clock().timestamp_ns().as_u64() >= due)
            {
                self.cancel_due_ns = None;
                self.cancel_active()?;
            }
        }
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.cancel_active()?;
        self.clock().cancel_timer("PREDICT-CALIBRATION-ACCOUNT");
        if let Some(handler) = self.account_handler.take() {
            msgbus::unsubscribe_account_state(
                format!("events.account.{}", self.inputs.account_id).into(),
                &handler,
            );
        }
        self.unsubscribe_data(
            predict_orderbook_data_type(self.inputs.market_id),
            Some(ClientId::new("PREDICT")),
            None,
        );
        Ok(())
    }
}

nautilus_strategy!(CalibrationStrategy, {
    fn on_order_event(&mut self, event: OrderEventAny) {
        let rejection_reason = match &event {
            OrderEventAny::Rejected(value) => Some(value.reason.as_str()),
            OrderEventAny::Denied(value) => Some(value.reason.as_str()),
            OrderEventAny::CancelRejected(value) => Some(value.reason.as_str()),
            OrderEventAny::ModifyRejected(value) => Some(value.reason.as_str()),
            _ => None,
        };
        let cached = self.cache().order(&event.client_order_id()).map(|order| serde_json::json!({
            "status":order.status().to_string(), "quantity":order.quantity().to_string(),
            "filledQuantity":order.filled_qty().to_string(), "price":order.price().map(|p|p.to_string()),
            "postOnly":order.is_post_only(), "timeInForce":order.time_in_force().to_string(),
            "expireTimeNs":order.expire_time().map(|v|v.as_u64())
        }));
        self.record_feedback(serde_json::json!({"type":format!("{:?}", event.event_type()),
            "clientOrderId":event.client_order_id().to_string(), "venueOrderId":event.venue_order_id().map(|id|id.to_string()),
            "tsEvent":event.ts_event().as_u64(), "cachedOrder":cached,
            "fill":if let OrderEventAny::Filled(value) = &event {Some(serde_json::json!({
                "tradeId":value.trade_id.to_string(),"quantity":value.last_qty.to_string(),
                "price":value.last_px.to_string(),"commission":value.commission.map(|v|v.to_string()),
                "liquiditySide":value.liquidity_side.to_string(),"orderSide":value.order_side.to_string()
            }))} else {None},
            "rejectionReason":rejection_reason,
            "duePostOnly":if let OrderEventAny::Rejected(value) = &event {Some(value.due_post_only)} else {None}}));
        if matches!(event, OrderEventAny::Denied(_) | OrderEventAny::Rejected(_)) {
            if matches!(event, OrderEventAny::Rejected(_)) {
                self.observations.borrow_mut().rejected += 1;
            } else {
                self.observations.borrow_mut().denied += 1;
            }
            self.active = None;
            self.observations.borrow_mut().complete = true;
            // A transmitted rejection can still have a reported collateral lock. Keep
            // the node alive until observe_account sees the native account unlock.
            if matches!(event, OrderEventAny::Denied(_)) {
                self.handle.stop();
            }
        }
        if self.cache().order(&event.client_order_id()).is_some() {
            self.observations.borrow_mut().events_with_cached_order += 1;
        }
    }
    fn on_order_accepted(&mut self, event: OrderAccepted) {
        self.observations.borrow_mut().accepted += 1;
        if self.inputs.settings.phase == Phase::Resting {
            // Atomic amend is expected to reject; original order must remain unchanged.
            let cached = self.cache().order(&event.client_order_id).unwrap().clone();
            let price = cached.price().unwrap().as_decimal();
            let tick = self
                .cache()
                .instrument(&self.inputs.instrument_id)
                .unwrap()
                .price_increment()
                .as_decimal();
            let new_price = if price > tick {
                price - tick
            } else {
                price + tick
            };
            let result = self.modify_order(
                event.client_order_id,
                None,
                Price::from_decimal_dp(new_price, cached.price().unwrap().precision).ok(),
                None,
                Some(ClientId::new("PREDICT")),
                None,
            );
            if result.is_err() {
                self.handle.stop();
            }
        }
    }
    fn on_order_modify_rejected(&mut self, event: OrderModifyRejected) {
        self.observations.borrow_mut().modify_rejected += 1;
        if let Some(order) = self.cache().order(&event.client_order_id)
            && order.price() == order.init_event().price
            && order.quantity() == order.init_event().quantity
        {
            self.observations.borrow_mut().modify_terms_preserved += 1;
        }
        if self.fixed_resting_price.is_some() {
            let due = self.clock().timestamp_ns().as_u64() + 2_000_000_000;
            self.cancel_due_ns = Some(due);
        } else if self.cancel_active().is_err() {
            self.handle.stop();
        }
    }
    fn on_order_cancel_rejected(&mut self, _event: OrderCancelRejected) {
        self.observations.borrow_mut().cancel_rejected += 1;
    }
    fn on_order_canceled(&mut self, _event: &OrderCanceled) {
        self.observations.borrow_mut().canceled += 1;
        self.active = None;
        if self.observations.borrow().canceled >= 2 || self.inputs.settings.phase == Phase::Fill {
            self.observations.borrow_mut().complete = true;
        }
    }
    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.observations.borrow_mut().fills += 1;
        if event.order_side == OrderSide::Buy {
            let mut observed = self.observations.borrow_mut();
            observed.buy_fill_quantity_sum += event.last_qty.as_decimal();
            observed.buy_commission_sum += event
                .commission
                .map_or(Decimal::ZERO, |fee| fee.as_decimal());
            observed.last_buy_fill_qty = Some(event.last_qty);
            observed.last_buy_commission = event.commission;
        }
        // Native reconciliation fills update the position but must not complete or
        // replace the strategy's independently managed entry order.
        if self.active != Some(event.client_order_id) {
            return;
        }
        if self.inputs.settings.phase == Phase::Resting {
            self.observations.borrow_mut().complete = true;
            self.handle.stop();
            return;
        }
        if self
            .cache()
            .order(&event.client_order_id)
            .is_some_and(|order| order.is_closed())
        {
            self.active = None;
            self.observations.borrow_mut().complete = true;
            return;
        }
        if self.replay_fill_target.is_some_and(|target| {
            self.cache()
                .order(&event.client_order_id)
                .is_some_and(|order| order.filled_qty() < target)
        }) {
            return;
        }
        if self.cancel_active().is_err() {
            self.handle.stop();
        }
    }
    fn on_position_event(&mut self, _event: PositionEvent) {
        self.observations.borrow_mut().position_events += 1;
        let positions = self.cache().positions(
            None,
            Some(&self.inputs.instrument_id),
            self.strategy_id().as_ref(),
            None,
            None,
        );
        self.observations.borrow_mut().last_position_qty = positions.first().map(|p| p.quantity);
        let state = positions.first().map(|position| serde_json::json!({
            "type":"PositionStateApplied","positionId":position.id.to_string(),
            "quantity":position.quantity.to_string(),"realizedPnl":position.realized_pnl.map(|v|v.to_string())
        }));
        if let Some(state) = state {
            self.record_feedback(state);
        }
    }
});
