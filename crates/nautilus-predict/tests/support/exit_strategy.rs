// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------
//! Test-only exit strategy: ordinary maker take-profit, cancel barrier, then taker stop.
use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    actor::DataActor,
    component::Component,
    messages::DataEvent,
    msgbus::{self, TypedHandler},
};
use nautilus_model::{
    data::{CustomData, Data, QuoteTick},
    enums::{OmsType, OrderSide, TimeInForce},
    events::{
        AccountState, OrderAccepted, OrderCancelRejected, OrderCanceled, OrderDenied,
        OrderEventAny, OrderFilled, OrderModifyRejected, OrderPendingCancel, OrderPendingUpdate,
        OrderRejected, OrderSubmitted, OrderUpdated, PositionChanged, PositionClosed,
        PositionEvent, PositionOpened,
    },
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId},
    instruments::Instrument,
    orders::Order,
    types::{Price, Quantity},
};
use nautilus_predict::{PredictOrderbookSnapshot, predict_orderbook_data_type};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use rust_decimal::{Decimal, RoundingStrategy};
use serde_json::{Value, json};

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExitSettings {
    take_profit_delta: Decimal,
    stop_loss_usdt: Decimal,
    stale_after_ms: u64,
    #[serde(default = "default_stop_attempts")]
    max_stop_attempts: u32,
}
fn default_stop_attempts() -> u32 {
    2
}
impl ExitSettings {
    pub fn parse(source: &str) -> anyhow::Result<Self> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct File {
            strategy: ExitSettings,
        }
        let value: File = toml::from_str(source)?;
        anyhow::ensure!(
            value.strategy.take_profit_delta > Decimal::ZERO
                && value.strategy.take_profit_delta < Decimal::ONE
                && value.strategy.stop_loss_usdt > Decimal::ZERO
                && (1..=30_000).contains(&value.strategy.stale_after_ms)
                && (1..=3).contains(&value.strategy.max_stop_attempts),
            "invalid exit strategy bounds"
        );
        Ok(value.strategy)
    }
}

struct ExecutionProbe(Rc<dyn Fn() -> bool>);
impl std::fmt::Debug for ExecutionProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ExecutionProbe")
    }
}

#[derive(Debug)]
pub struct ExitStrategy {
    core: StrategyCore,
    restart_recovery: bool,
    cold_exit_only: bool,
    restored: bool,
    execution_probe: Option<ExecutionProbe>,
    recorded_exit_signal: Option<Rc<std::cell::Cell<bool>>>,
    cancel_orders_on_stop: bool,
    instrument: InstrumentId,
    market_id: u64,
    account_id: AccountId,
    entry_deadline_ns: Option<u64>,
    live_entry_guard: bool,
    outcome: u8,
    entry: ClientOrderId,
    native_entry: Option<(Price, Quantity)>,
    taker_entry: bool,
    market_entry: bool,
    native_quotes: bool,
    native_close: bool,
    close_reduce_only: Option<bool>,
    native_close_pending: bool,
    entry_sent: bool,
    entry_cancel_sent: bool,
    entry_modify_sent: bool,
    stop_attempts: u32,
    max_stop_attempts: u32,
    market_wait_logged: Option<ClientOrderId>,
    delta: Decimal,
    max_loss: Decimal,
    fee_bps: u32,
    stale_after_ms: u64,
    entry_qty: Decimal,
    entry_cost: Decimal,
    entry_fee_shares: Decimal,
    realized_proceeds: Decimal,
    account_handler: Option<TypedHandler<AccountState>>,
    last_account_event: Option<nautilus_core::UUID4>,
    maker: Option<ClientOrderId>,
    taker: Option<ClientOrderId>,
    stop: bool,
    cancel_sent: bool,
    failed: bool,
    last_book: Option<PredictOrderbookSnapshot>,
    pub journal: Rc<RefCell<Vec<Value>>>,
}

impl ExitStrategy {
    pub fn new(
        instrument: InstrumentId,
        outcome: u8,
        journal: Rc<RefCell<Vec<Value>>>,
        settings: ExitSettings,
        fee_bps: u32,
    ) -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::new("PREDICT-EXIT-001")),
                oms_type: Some(OmsType::Netting),
                ..Default::default()
            }),
            restart_recovery: false,
            cold_exit_only: false,
            restored: false,
            execution_probe: None,
            recorded_exit_signal: None,
            cancel_orders_on_stop: true,
            instrument,
            market_id: 42,
            account_id: AccountId::new("PREDICT-fixture"),
            entry_deadline_ns: None,
            live_entry_guard: false,
            outcome,
            entry: ClientOrderId::new("EXIT-ENTRY-001"),
            native_entry: None,
            taker_entry: false,
            market_entry: false,
            native_quotes: false,
            native_close: false,
            close_reduce_only: Some(false),
            native_close_pending: false,
            entry_sent: false,
            entry_cancel_sent: false,
            entry_modify_sent: false,
            stop_attempts: 0,
            max_stop_attempts: settings.max_stop_attempts,
            market_wait_logged: None,
            delta: settings.take_profit_delta,
            max_loss: settings.stop_loss_usdt,
            fee_bps,
            stale_after_ms: settings.stale_after_ms,
            entry_qty: Decimal::ZERO,
            entry_cost: Decimal::ZERO,
            entry_fee_shares: Decimal::ZERO,
            realized_proceeds: Decimal::ZERO,
            account_handler: None,
            last_account_event: None,
            maker: None,
            taker: None,
            stop: false,
            cancel_sent: false,
            failed: false,
            last_book: None,
            journal,
        }
    }
    /// Explicit loopback entry plan; the operator's live configuration is never read.
    pub fn with_native_entry(mut self, price: Price, quantity: Quantity) -> Self {
        self.native_entry = Some((price, quantity));
        self
    }
    /// Test-only abrupt-disconnect simulation; normal/live shutdown keeps cancellation enabled.
    #[allow(dead_code)]
    pub fn without_shutdown_cancels_for_restart_replay(mut self) -> Self {
        self.cancel_orders_on_stop = false;
        self
    }
    /// Replays a recorded strategy exit decision without waiting its original wall-clock delay.
    #[allow(dead_code)] // Used by the actual-JSON replay integration target only.
    pub fn with_recorded_exit_signal(mut self, signal: Rc<std::cell::Cell<bool>>) -> Self {
        self.recorded_exit_signal = Some(signal);
        self
    }
    /// Explicit operator liquidation for the live exit test. Venue admission rechecks the book.
    #[allow(dead_code)] // Used only by the separately gated live calibration host.
    pub fn with_requested_exit(mut self) -> Self {
        self.recorded_exit_signal = Some(Rc::new(std::cell::Cell::new(true)));
        self
    }
    pub fn with_native_quotes(mut self) -> Self {
        self.native_quotes = true;
        self
    }
    pub fn with_taker_entry(mut self) -> Self {
        self.taker_entry = true;
        self
    }
    /// Opt-in mainnet quote diagnostic; existing LIMIT replay entry remains unchanged.
    pub fn with_market_entry(mut self) -> Self {
        self.market_entry = true;
        self
    }
    /// Runtime discovery supplies identity and an entry deadline; holdings have no time exit.
    pub fn with_live_identity(
        mut self,
        market_id: u64,
        account: AccountId,
        deadline_ns: u64,
    ) -> Self {
        self.market_id = market_id;
        self.account_id = account;
        self.entry_deadline_ns = Some(deadline_ns);
        self
    }
    #[allow(dead_code)] // Used only by the separately gated mainnet calibration binary.
    pub fn without_entry_amend(mut self) -> Self {
        self.entry_modify_sent = true;
        self.live_entry_guard = true;
        self
    }
    pub fn with_native_close(mut self) -> Self {
        self.native_close = true;
        self
    }
    pub fn with_default_reduce_only(mut self) -> Self {
        self.native_close = true;
        self.close_reduce_only = None;
        self
    }
    pub fn with_execution_probe(mut self, probe: Rc<dyn Fn() -> bool>) -> Self {
        self.execution_probe = Some(ExecutionProbe(probe));
        self
    }
    fn record_order_callback(&self, name: &str, id: ClientOrderId) {
        let cache = self.cache();
        let order = cache.order(&id).unwrap();
        self.journal.borrow_mut().push(json!({"type":"StrategyCallback","callback":name,"clientOrderId":id.to_string(),
            "venueOrderId":order.venue_order_id().map(|id|id.to_string()),"cachedTsLast":order.ts_last().as_u64(),"cachedStatus":order.status().to_string(),"cachedNetQuantity":self.quantity().to_string(),"quantity":order.quantity().to_string(),"filledQuantity":order.filled_qty().to_string(),"leavesQuantity":order.leaves_qty().to_string(),"price":order.price().map(|p|p.to_string())}));
    }
    fn record_position_callback(&self, name: &str, quantity: Quantity) {
        self.journal.borrow_mut().push(json!({"type":"StrategyCallback","callback":name,"eventQuantity":quantity.to_string(),"cachedNetQuantity":self.quantity().to_string()}));
    }
    fn quantity(&self) -> Decimal {
        self.cache()
            .positions_open(
                None,
                Some(&self.instrument),
                self.strategy_id().as_ref(),
                None,
                None,
            )
            .iter()
            .map(|p| p.quantity.as_decimal())
            .sum()
    }
    fn drive(&mut self) -> anyhow::Result<()> {
        if !self.is_running()
            || self.failed
            || self
                .execution_probe
                .as_ref()
                .is_some_and(|probe| !(probe.0)())
        {
            return Ok(());
        }
        let Some(book) = &self.last_book else {
            return Ok(());
        };
        let now = self.clock().timestamp_ns().as_u64();
        if !self.recorded_exit_signal.as_ref().is_some_and(|s| s.get())
            && now
                .checked_sub(if self.market_entry {
                    book.ts_init.as_u64()
                } else {
                    book.ts_event.as_u64()
                })
                .is_none_or(|age| age > self.stale_after_ms * 1_000_000)
        {
            return Ok(());
        }
        if self
            .cache()
            .quote(&self.instrument)
            .is_none_or(|q| q.ts_event != book.ts_event)
        {
            return Ok(());
        }
        if let Some((price, quantity)) = self.native_entry {
            if !self.entry_sent {
                if self.live_entry_guard
                    && self
                        .entry_deadline_ns
                        .is_some_and(|deadline| now >= deadline)
                {
                    self.failed = true;
                    self.journal
                        .borrow_mut()
                        .push(json!({"type":"EntryWindowExpired","submitted":false}));
                    return Ok(());
                }
                if self.live_entry_guard {
                    let (bids, asks) = if self.outcome == 1 {
                        (&book.bids, &book.asks)
                    } else {
                        (&book.asks, &book.bids)
                    };
                    let px = |p: Price| {
                        if self.outcome == 1 {
                            p.as_decimal()
                        } else {
                            Decimal::ONE - p.as_decimal()
                        }
                    };
                    let (Some(bid), Some(ask)) = (bids.first(), asks.first()) else {
                        return Ok(());
                    };
                    let admission_price = if self.market_entry {
                        let reference = px(ask.price);
                        if reference > Decimal::new(48, 2)
                            || reference * quantity.as_decimal() * Decimal::new(103, 2)
                                > Decimal::new(150, 2)
                        {
                            return Ok(());
                        }
                        reference
                    } else {
                        price.as_decimal()
                    };
                    if !live_entry_admissible_with_depth(
                        admission_price,
                        quantity.as_decimal(),
                        (px(bid.price), bid.quantity.as_decimal()),
                        (px(ask.price), ask.quantity.as_decimal()),
                        self.fee_bps,
                        self.max_loss,
                        Decimal::from(if self.market_entry { 3 } else { 5 }),
                    ) {
                        return Ok(());
                    }
                }
                self.entry_sent = true;
                if self.market_entry {
                    let order = self.order().market(
                        self.instrument,
                        OrderSide::Buy,
                        quantity,
                        Some(TimeInForce::Fok),
                        None,
                        Some(false),
                        None,
                        None,
                        None,
                        Some(self.entry),
                    );
                    let quote = self.cache().quote(&self.instrument).unwrap();
                    let reference = quote.ask_price.to_string();
                    self.journal.borrow_mut().push(json!({"type":"NativeMarketEntrySubmitted","clientOrderId":self.entry.to_string(),"reference":reference,"quantity":quantity.to_string(),"slippageBps":300,"observedBid":quote.bid_price.to_string(),"observedAsk":quote.ask_price.to_string(),"tsInit":quote.ts_init.as_u64()}));
                    let params = nautilus_core::Params::new(
                        [
                            ("market_price".into(), json!(reference)),
                            ("market_order_slippage_bps".into(), json!(300)),
                        ]
                        .into(),
                    );
                    self.submit_order(order, None, Some(ClientId::new("PREDICT")), Some(params))?;
                    return Ok(());
                }
                let order = self.order().limit(
                    self.instrument,
                    OrderSide::Buy,
                    quantity,
                    price,
                    Some(TimeInForce::Gtc),
                    None,
                    Some(!self.taker_entry),
                    Some(false),
                    Some(false),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(self.entry),
                );
                self.journal.borrow_mut().push(
                    json!({"type":"NativeEntrySubmitted","clientOrderId":self.entry.to_string()}),
                );
                self.submit_order(order, None, Some(ClientId::new("PREDICT")), None)?;
                return Ok(());
            }
            let cancel = self.cache().order(&self.entry).is_some_and(|o| {
                !o.is_closed()
                    && o.filled_qty().as_decimal() > Decimal::ZERO
                    && o.ts_accepted().is_some()
            });
            if cancel && !self.entry_cancel_sent {
                self.entry_cancel_sent = true;
                self.journal
                    .borrow_mut()
                    .push(json!({"type":"CancelEntryRemainderRequested"}));
                self.cancel_order(self.entry, Some(ClientId::new("PREDICT")), None)?;
                return Ok(());
            }
        }
        if self.entry_qty.is_zero()
            || self
                .cache()
                .order(&self.entry)
                .is_none_or(|o| !o.is_closed())
        {
            return Ok(());
        }
        if self.native_close_pending {
            return Ok(());
        }
        if let Some(id) = self.taker {
            let cached = self.cache().order(&id).unwrap().clone();
            if !cached.is_closed() {
                if cached.filled_qty().as_decimal() > Decimal::ZERO
                    && self.market_wait_logged != Some(id)
                {
                    self.market_wait_logged = Some(id);
                    self.journal.borrow_mut().push(json!({"type":"AwaitingRemainingMarketFill","clientOrderId":id.to_string(),"remainingQuantity":cached.leaves_qty().to_string()}));
                }
                return Ok(());
            }
            self.taker = None;
        }
        let qty = self.quantity();
        if qty.is_zero() {
            return Ok(());
        }
        let avg = self.entry_cost / self.entry_qty;
        // The entry uses SHARES fees in these replay cases. Wait until native holdings
        // reconciliation applies the net quantity, rather than selling gross fill quantity.
        let expected_net = self.entry_qty - self.entry_fee_shares;
        // Money commission has quote-currency precision. Its share equivalent can be
        // rounded by one quote unit; this barrier must not require impossible equality.
        let projection_tolerance = if self.entry_deadline_ns.is_some() {
            Decimal::new(1, 8) / avg + Decimal::new(1, 12)
        } else {
            Decimal::ZERO
        };
        if qty > expected_net + projection_tolerance {
            return Ok(());
        }
        let levels: Vec<_> = if self.outcome == 1 {
            book.bids
                .iter()
                .map(|l| (l.price.as_decimal(), l.quantity.as_decimal()))
                .collect()
        } else {
            book.asks
                .iter()
                .map(|l| (Decimal::ONE - l.price.as_decimal(), l.quantity.as_decimal()))
                .collect()
        };
        let Some((bid, _)) = levels.first() else {
            return Ok(());
        };
        let mut remaining = qty;
        let mut proceeds = Decimal::ZERO;
        let mut market_price = *bid;
        for (px, available) in &levels {
            let take = remaining.min(*available);
            let fee = Decimal::from(self.fee_bps) / Decimal::from(10_000)
                * (*px).min(Decimal::ONE - *px)
                * take;
            proceeds += px * take - fee;
            if take > Decimal::ZERO {
                market_price = *px;
            }
            remaining -= take;
            if remaining.is_zero() {
                break;
            }
        }
        if !remaining.is_zero() {
            return Ok(());
        }
        let round_pnl = self.realized_proceeds + proceeds - self.entry_cost;
        if !self.stop
            && (-round_pnl >= self.max_loss
                || self
                    .recorded_exit_signal
                    .as_ref()
                    .is_some_and(|signal| signal.get()))
        {
            self.stop = true;
            self.journal.borrow_mut().push(json!({"type":"StopTriggered","netQuantity":qty.to_string(),"estimatedProceeds":proceeds.to_string(),"entryCost":self.entry_cost.to_string(),"realizedProceeds":self.realized_proceeds.to_string(),"estimatedRoundPnl":round_pnl.to_string()}));
        }
        if self.stop {
            if let Some(id) = self.maker {
                if !self.cancel_sent
                    && self
                        .cache()
                        .order(&id)
                        .is_some_and(|o| o.ts_accepted().is_some() && !o.is_closed())
                {
                    self.cancel_sent = true;
                    self.journal
                        .borrow_mut()
                        .push(json!({"type":"CancelTakeProfitRequested"}));
                    self.cancel_order(id, Some(ClientId::new("PREDICT")), None)?;
                }
                return Ok(());
            }
            if self.stop_attempts >= self.max_stop_attempts {
                self.failed = true;
                self.journal.borrow_mut().push(
                    json!({"type":"StopAttemptLimitReached","remainingQuantity":qty.to_string()}),
                );
                return Ok(());
            }
            self.stop_attempts += 1;
            // The signal already walked this fresh snapshot's depth. Its worst tier
            // supplies the reference; the command supplies extra slippage once.
            let params = nautilus_core::Params::new(
                [
                    ("market_price".into(), json!(market_price.to_string())),
                    (
                        "market_order_slippage_bps".into(),
                        json!(if self.market_entry { 300 } else { 100 }),
                    ),
                ]
                .into(),
            );
            if self.native_close {
                let positions = self.cache().positions_open(
                    None,
                    Some(&self.instrument),
                    self.strategy_id().as_ref(),
                    None,
                    None,
                );
                anyhow::ensure!(
                    positions.len() == 1,
                    "native close requires one owned netting position"
                );
                let position = &positions[0];
                anyhow::ensure!(
                    position.quantity.as_decimal() == qty,
                    "native close quantity differs from owned position"
                );
                self.native_close_pending = true;
                self.journal.borrow_mut().push(json!({"type":"NativeClosePositionRequested","positionId":position.id.to_string(),"quantity":qty.to_string(),"reduceOnly":self.close_reduce_only}));
                self.close_position(
                    position,
                    Some(ClientId::new("PREDICT")),
                    None,
                    Some(TimeInForce::Gtc),
                    self.close_reduce_only,
                    Some(false),
                    Some(params),
                )?;
                return Ok(());
            }
            let quantity = Quantity::from_decimal_dp(
                qty,
                self.cache()
                    .instrument(&self.instrument)
                    .unwrap()
                    .size_precision(),
            )?;
            let order = self.order().market(
                self.instrument,
                OrderSide::Sell,
                quantity,
                Some(TimeInForce::Gtc),
                Some(false),
                Some(false),
                None,
                None,
                None,
                None,
            );
            self.taker = Some(order.client_order_id());
            self.journal
                .borrow_mut()
                .push(json!({"type":"TakerStopSubmitted","quantity":qty.to_string()}));
            self.submit_order(order, None, Some(ClientId::new("PREDICT")), Some(params))?;
        } else if self.maker.is_none() {
            let (tick, precision) = {
                let cache = self.cache();
                let i = cache.instrument(&self.instrument).unwrap();
                (i.price_increment().as_decimal(), i.price_precision())
            };
            let price = maker_take_profit_price(
                avg,
                self.entry_cost - self.realized_proceeds,
                qty,
                *bid,
                tick,
                self.delta,
            )?;
            let quantity = Quantity::from_decimal_dp(
                qty,
                self.cache()
                    .instrument(&self.instrument)
                    .unwrap()
                    .size_precision(),
            )?;
            let order = self.order().limit(
                self.instrument,
                OrderSide::Sell,
                quantity,
                Price::from_decimal_dp(price, precision)?,
                Some(TimeInForce::Gtc),
                None,
                Some(true),
                Some(false),
                Some(false),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            );
            self.maker = Some(order.client_order_id());
            self.journal.borrow_mut().push(json!({"type":"MakerTakeProfitSubmitted","price":price.to_string(),"quantity":qty.to_string(),"entryAverage":avg.to_string(),
                "entryCashCost":self.entry_cost.to_string(),"realizedNetProceeds":self.realized_proceeds.to_string(),
                "expectedMakerFeeUSDT":"0","expectedCashProfit":(self.realized_proceeds+qty*price-self.entry_cost).to_string()}));
            self.submit_order(order, None, Some(ClientId::new("PREDICT")), None)?;
        }
        Ok(())
    }
}
// Predict post-only maker exits have zero fee. Entry share fees are already reflected
// in net inventory: use actual cash cost / net quantity, not quote realized PnL.
fn maker_take_profit_price(
    average: Decimal,
    unpaid_cost: Decimal,
    net_quantity: Decimal,
    bid: Decimal,
    tick: Decimal,
    delta: Decimal,
) -> anyhow::Result<Decimal> {
    anyhow::ensure!(
        net_quantity > Decimal::ZERO && tick > Decimal::ZERO,
        "invalid take profit units"
    );
    let cash_floor = unpaid_cost.max(Decimal::ZERO) / net_quantity;
    let mut price = ((average + delta).max(cash_floor).max(bid + tick) / tick)
        .round_dp_with_strategy(0, RoundingStrategy::ToPositiveInfinity)
        * tick;
    if price * net_quantity <= unpaid_cost {
        price += tick;
    }
    anyhow::ensure!(
        price <= Decimal::ONE,
        "take profit exceeds binary price range"
    );
    Ok(price)
}

#[test]
fn maker_profit_covers_entry_shares_fee_and_rounds_above_cash_break_even() {
    let net = Decimal::new(3136, 2);
    let cost = Decimal::new(128, 2);
    let tick = Decimal::new(1, 3);
    let price = maker_take_profit_price(
        Decimal::new(4, 2),
        cost,
        net,
        Decimal::new(39, 3),
        tick,
        Decimal::new(1, 4),
    )
    .unwrap();
    assert_eq!(price, Decimal::new(41, 3));
    assert!(net * price > cost);
    assert!(net * (price - tick) <= cost);
    assert!(
        maker_take_profit_price(
            Decimal::new(99, 2),
            Decimal::ONE,
            Decimal::new(9, 1),
            Decimal::new(98, 2),
            tick,
            Decimal::new(1, 3)
        )
        .is_err()
    );
}

fn live_entry_admissible(
    price: Decimal,
    quantity: Decimal,
    bid: (Decimal, Decimal),
    ask: (Decimal, Decimal),
    fee_bps: u32,
    max_loss: Decimal,
) -> bool {
    live_entry_admissible_with_depth(
        price,
        quantity,
        bid,
        ask,
        fee_bps,
        max_loss,
        Decimal::from(5),
    )
}

fn live_entry_admissible_with_depth(
    price: Decimal,
    quantity: Decimal,
    bid: (Decimal, Decimal),
    ask: (Decimal, Decimal),
    fee_bps: u32,
    max_loss: Decimal,
    depth_multiple: Decimal,
) -> bool {
    if quantity <= Decimal::ZERO || price <= Decimal::ZERO || fee_bps > 10_000 {
        return false;
    }
    let rate = Decimal::from(fee_bps) / Decimal::from(10_000);
    let net = quantity * (Decimal::ONE - rate);
    let proceeds = bid.0 * net * (Decimal::ONE - rate);
    let floor = (bid.0 * net - max_loss) * Decimal::new(99, 2);
    ask.0 <= price
        && bid.0 > Decimal::ZERO
        && bid.0 < ask.0
        && ask.1 >= quantity * depth_multiple
        && bid.1 >= net * depth_multiple
        && price * quantity - proceeds <= max_loss * Decimal::from(2) / Decimal::from(3)
        && floor >= Decimal::new(105, 2)
}

#[test]
fn bounded_live_entry_rechecks_fee_depth_spread_and_exit_minimum() {
    let p = Decimal::new(4, 2);
    let q = Decimal::from(32);
    let loss = Decimal::new(15, 2);
    let bid = (Decimal::new(39, 3), Decimal::from(1000));
    let ask = (p, Decimal::from(1000));
    assert!(live_entry_admissible(p, q, bid, ask, 200, loss));
    assert!(!live_entry_admissible(
        p,
        q,
        (Decimal::new(3, 2), bid.1),
        ask,
        200,
        loss
    ));
    assert!(!live_entry_admissible(
        p,
        q,
        (bid.0, Decimal::ONE),
        ask,
        200,
        loss
    ));
    assert!(!live_entry_admissible(
        p,
        q,
        bid,
        (Decimal::new(41, 3), ask.1),
        200,
        loss
    ));
    assert!(!live_entry_admissible(
        p,
        Decimal::from(20),
        bid,
        ask,
        200,
        loss
    ));
}

impl DataActor for ExitStrategy {
    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.journal.borrow_mut().push(json!({"type":"NativeQuoteObserved","instrumentId":quote.instrument_id.to_string(),"bid":quote.bid_price.to_string(),"ask":quote.ask_price.to_string(),"bidSize":quote.bid_size.to_string(),"askSize":quote.ask_size.to_string(),"tsEvent":quote.ts_event.as_u64()}));
        Ok(())
    }
    fn on_save(&self) -> anyhow::Result<indexmap::IndexMap<String, Vec<u8>>> {
        self.save_recovery_state()
    }
    fn on_load(&mut self, state: indexmap::IndexMap<String, Vec<u8>>) -> anyhow::Result<()> {
        self.load_recovery_state(state)
    }
    fn on_start(&mut self) -> anyhow::Result<()> {
        if let Err(error) = self.adopt_reconciled_state() {
            self.failed = true;
            self.journal
                .borrow_mut()
                .push(json!({"type":"RestartRefused","reason":error.to_string()}));
            return Err(error);
        }
        self.set_external_order_instrument_ids(vec![self.instrument])?;
        if self.native_quotes {
            self.subscribe_quotes(self.instrument, Some(ClientId::new("PREDICT")), None);
        }
        self.subscribe_data(
            predict_orderbook_data_type(self.market_id),
            Some(ClientId::new("PREDICT")),
            None,
        );
        let journal = self.journal.clone();
        let handler = TypedHandler::from(move |state: &AccountState| {
            journal.borrow_mut().push(json!({"type":"NativeAccountReported","reconciliationReady":state.info.as_ref().and_then(|i|i.get("predict_reconciliation_ready")).cloned(),"connectionEpoch":state.info.as_ref().and_then(|i|i.get("predict_connection_epoch")).cloned(),"balances":state.balances.iter().map(|b|json!({"currency":b.currency.code,"total":b.total.as_decimal().to_string(),"free":b.free.as_decimal().to_string(),"locked":b.locked.as_decimal().to_string()})).collect::<Vec<_>>()}));
        });
        msgbus::subscribe_account_state(
            format!("events.account.{}", self.account_id).into(),
            handler.clone(),
            None,
        );
        self.account_handler = Some(handler);
        // Subscription alone cannot replay initialization published before on_start.
        // Read the native snapshot after subscribing, without fabricating a new event.
        if let Some(state) = self
            .cache()
            .account(&self.account_id)
            .and_then(|a| a.last_event())
        {
            self.journal.borrow_mut().push(json!({"type":"NativeAccountSnapshot","balances":state.balances.iter().map(|b|json!({"currency":b.currency.code,"total":b.total.as_decimal().to_string(),"free":b.free.as_decimal().to_string(),"locked":b.locked.as_decimal().to_string()})).collect::<Vec<_>>()}));
        }
        self.clock().set_timer(
            "PREDICT-EXIT-DRIVE",
            std::time::Duration::from_millis(20),
            None,
            None,
            None,
            None,
            None,
        )?;
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.clock().cancel_timer("PREDICT-EXIT-DRIVE");
        if self.cancel_orders_on_stop {
            for id in [Some(self.entry), self.maker].into_iter().flatten() {
                let active = self.cache().order(&id).is_some_and(|o| !o.is_closed());
                if active {
                    self.cancel_order(id, Some(ClientId::new("PREDICT")), None)?;
                }
            }
        }
        if self.native_quotes {
            self.unsubscribe_quotes(self.instrument, Some(ClientId::new("PREDICT")), None);
        }
        if let Some(handler) = self.account_handler.take() {
            msgbus::unsubscribe_account_state(
                format!("events.account.{}", self.account_id).into(),
                &handler,
            );
        }
        Ok(())
    }
    fn on_time_event(&mut self, _event: &nautilus_common::timer::TimeEvent) -> anyhow::Result<()> {
        // Portfolio can retain an unchanged-balance report in Cache without republishing
        // its metadata. Read native provenance as well as subscribing to account events.
        let state = self
            .cache()
            .account(&self.account_id)
            .and_then(|a| a.last_event());
        if let Some(state) = state
            && self.last_account_event != Some(state.event_id)
        {
            self.last_account_event = Some(state.event_id);
            self.journal.borrow_mut().push(json!({"type":"NativeAccountSnapshot","reconciliationReady":state.info.as_ref().and_then(|i|i.get("predict_reconciliation_ready")).cloned(),"connectionEpoch":state.info.as_ref().and_then(|i|i.get("predict_connection_epoch")).cloned(),"balances":state.balances.iter().map(|b|json!({"currency":b.currency.code,"total":b.total.as_decimal().to_string(),"free":b.free.as_decimal().to_string(),"locked":b.locked.as_decimal().to_string()})).collect::<Vec<_>>()}));
        }
        let result = self.drive();
        if result.is_err() {
            self.failed = true;
            self.journal
                .borrow_mut()
                .push(json!({"type":"StrategyDriveFailed"}));
        }
        result
    }
    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        if data.data_type != predict_orderbook_data_type(self.market_id) {
            return Ok(());
        }
        if let Some(book) = data
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
        {
            if self.native_quotes {
                self.last_book = Some(book.clone());
                return Ok(());
            }
            // Derive a native quote from exactly the custom snapshot received. Queue it
            // through DataEngine so normal RiskEngine MARKET valuation stays enabled.
            let cache = self.cache();
            let i = cache.instrument(&self.instrument).unwrap();
            let (bid, ask) = if self.outcome == 1 {
                (book.bids.first(), book.asks.first())
            } else {
                (book.asks.first(), book.bids.first())
            };
            let (Some(bid), Some(ask)) = (bid, ask) else {
                return Ok(());
            };
            let px = |p: Decimal| {
                Price::from_decimal_dp(
                    if self.outcome == 1 {
                        p
                    } else {
                        Decimal::ONE - p
                    },
                    i.price_precision(),
                )
            };
            let quote = QuoteTick::new(
                self.instrument,
                px(bid.price.as_decimal())?,
                px(ask.price.as_decimal())?,
                bid.quantity,
                ask.quantity,
                book.ts_event,
                book.ts_init,
            );
            nautilus_common::live::runner::try_get_data_event_sender()
                .ok_or_else(|| anyhow::anyhow!("native data sender unavailable"))?
                .send(DataEvent::Data(Data::Quote(quote)))?;
            drop(cache);
            self.last_book = Some(book.clone());
        }
        Ok(())
    }
}
nautilus_strategy!(ExitStrategy, {
    fn external_order_instrument_ids(&self) -> Option<Vec<InstrumentId>> {
        self.core.config.external_order_instrument_ids.clone()
    }
    fn on_order_updated(&mut self, event: OrderUpdated) {
        self.record_order_callback("on_order_updated", event.client_order_id);
    }
    fn on_order_pending_cancel(&mut self, event: OrderPendingCancel) {
        self.record_order_callback("on_order_pending_cancel", event.client_order_id);
    }
    fn on_order_pending_update(&mut self, event: OrderPendingUpdate) {
        self.record_order_callback("on_order_pending_update", event.client_order_id);
    }
    fn on_order_submitted(&mut self, event: OrderSubmitted) {
        if self.native_close_pending
            && self
                .cache()
                .order(&event.client_order_id)
                .is_some_and(|o| o.order_type() == nautilus_model::enums::OrderType::Market)
        {
            self.taker = Some(event.client_order_id);
            self.native_close_pending = false;
            self.journal.borrow_mut().push(json!({"type":"TakerStopSubmitted","quantity":self.cache().order(&event.client_order_id).unwrap().quantity().to_string()}));
        }
        self.record_order_callback("on_order_submitted", event.client_order_id);
    }
    fn on_order_accepted(&mut self, event: OrderAccepted) {
        self.record_order_callback("on_order_accepted", event.client_order_id);
        if self.native_entry.is_some()
            && event.client_order_id == self.entry
            && !self.entry_modify_sent
        {
            self.entry_modify_sent = true;
            let px = Price::from_decimal_dp(
                self.native_entry.unwrap().0.as_decimal()
                    + self
                        .cache()
                        .instrument(&self.instrument)
                        .unwrap()
                        .price_increment()
                        .as_decimal(),
                self.native_entry.unwrap().0.precision,
            )
            .unwrap();
            if let Err(error) = self.modify_order(
                self.entry,
                None,
                Some(px),
                None,
                Some(ClientId::new("PREDICT")),
                None,
            ) {
                self.failed = true;
                self.journal
                    .borrow_mut()
                    .push(json!({"type":"StrategyCommandError","reason":error.to_string()}));
            }
        }
    }
    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.record_order_callback("on_order_filled", event.client_order_id);
    }
    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        self.record_order_callback("on_order_canceled", event.client_order_id);
    }
    fn on_order_rejected(&mut self, event: OrderRejected) {
        self.record_order_callback("on_order_rejected", event.client_order_id);
    }
    fn on_order_denied(&mut self, event: OrderDenied) {
        self.record_order_callback("on_order_denied", event.client_order_id);
    }
    fn on_order_modify_rejected(&mut self, event: OrderModifyRejected) {
        self.record_order_callback("on_order_modify_rejected", event.client_order_id);
    }
    fn on_order_cancel_rejected(&mut self, event: OrderCancelRejected) {
        self.record_order_callback("on_order_cancel_rejected", event.client_order_id);
    }
    fn on_position_opened(&mut self, event: PositionOpened) {
        self.record_position_callback("on_position_opened", event.quantity);
    }
    fn on_position_changed(&mut self, event: PositionChanged) {
        self.record_position_callback("on_position_changed", event.quantity);
    }
    fn on_position_closed(&mut self, event: PositionClosed) {
        self.record_position_callback("on_position_closed", event.quantity);
    }
    fn on_order_event(&mut self, event: OrderEventAny) {
        let id = event.client_order_id();
        self.journal.borrow_mut().push(json!({"type":format!("{:?}",event.event_type()),"nativeEvent":&event,"clientOrderId":id.to_string(),"quantity":self.quantity().to_string(),"reason":match &event {OrderEventAny::Denied(e)=>Some(e.reason.as_str()),OrderEventAny::Rejected(e)=>Some(e.reason.as_str()),OrderEventAny::CancelRejected(e)=>Some(e.reason.as_str()),_=>None},
            "fill":if let OrderEventAny::Filled(e)=&event {Some(json!({"side":self.cache().order(&id).map(|o|o.order_side().to_string()),"quantity":e.last_qty.to_string(),"price":e.last_px.to_string(),"commission":e.commission.map(|v|v.to_string()),"quantityCommission":e.quantity_commission.map(|v|v.to_string()),"liquidity":e.liquidity_side.to_string()}))}else{None},
            "cachedStatus":self.cache().order(&id).map(|o|o.status().to_string())}));
        if let OrderEventAny::Filled(fill) = &event
            && id == self.entry
        {
            self.entry_qty += fill.last_qty.as_decimal();
            self.entry_cost += fill.last_qty.as_decimal() * fill.last_px.as_decimal();
            self.entry_fee_shares += fill
                .quantity_commission
                .map(|fee| fee.as_decimal())
                .unwrap_or_else(|| {
                    fill.commission.map_or(Decimal::ZERO, |v| {
                        v.as_decimal() / fill.last_px.as_decimal()
                    })
                });
        }
        if let OrderEventAny::Filled(fill) = &event
            && (self.maker == Some(id) || self.taker == Some(id))
        {
            self.realized_proceeds += fill.last_qty.as_decimal() * fill.last_px.as_decimal()
                - fill.commission.map_or(Decimal::ZERO, |v| v.as_decimal());
        }
        if matches!(event, OrderEventAny::CancelRejected(_))
            && id == self.entry
            && self.entry_cancel_sent
        {
            // Cancellation can lose to the remaining entry fill. Ask Native to query the
            // order and wait for its terminal state before deciding the exit quantity.
            let order = self.cache().order(&id);
            if let Some(order) = order {
                self.journal
                    .borrow_mut()
                    .push(json!({"type":"AwaitEntryResolutionAfterCancelRejected"}));
                if self
                    .query_order(&order, Some(ClientId::new("PREDICT")), None)
                    .is_err()
                {
                    self.failed = true;
                    self.journal
                        .borrow_mut()
                        .push(json!({"type":"EntryQueryFailed"}));
                }
            }
            return;
        }
        if matches!(
            event,
            OrderEventAny::Denied(_)
                | OrderEventAny::Rejected(_)
                | OrderEventAny::CancelRejected(_)
        ) {
            if self.market_entry
                && self.maker == Some(id)
                && matches!(&event, OrderEventAny::Rejected(rejected) if rejected.due_post_only)
                && self.cache().order(&id).is_some_and(|o| o.is_closed())
            {
                // A moving bid can cross a post-only SELL before the venue sees it.
                // This bounded test must still close its actual net inventory.
                self.maker = None;
                self.stop = true;
                self.cancel_sent = false;
                self.journal.borrow_mut().push(
                    json!({"type":"PostOnlyRejectedExitRequested","clientOrderId":id.to_string()}),
                );
            } else {
                self.failed = true;
            }
        }
        if self.maker == Some(id)
            && matches!(event, OrderEventAny::Canceled(_) | OrderEventAny::Filled(_))
            && self.cache().order(&id).is_some_and(|o| o.is_closed())
        {
            self.maker = None;
        }
        if self.drive().is_err() {
            self.failed = true;
            self.journal
                .borrow_mut()
                .push(json!({"type":"StrategyDriveFailed"}));
        }
    }
    fn on_position_event(&mut self, event: PositionEvent) {
        self.journal.borrow_mut().push(json!({"type":"NativePositionApplied","nativeEvent":event,"quantity":self.quantity().to_string(),"realizedPnl":self.cache().positions(None,Some(&self.instrument),self.strategy_id().as_ref(),None,None).first().and_then(|p|p.realized_pnl).map(|v|v.to_string())}));
    }
});

#[path = "exit_strategy_restart.rs"]
mod restart;
