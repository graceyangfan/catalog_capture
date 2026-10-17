// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Test-only two-order strategy. State remains in Native Cache, never in an adapter ledger.
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    time::Duration,
};

use nautilus_common::actor::DataActor;
use nautilus_core::UUID4;
use nautilus_live::node::LiveNodeHandle;
use nautilus_model::{
    data::{CustomData, QuoteTick},
    enums::{OrderSide, OrderStatus, TimeInForce},
    events::{OrderEventAny, OrderModifyRejected},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId},
    instruments::Instrument,
    orders::Order,
    types::{Price, Quantity},
};
use nautilus_predict::{PredictOrderbookSnapshot, predict_orderbook_data_type};
use nautilus_trading::{Strategy, StrategyCore, nautilus_strategy};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use super::strategy::RestingBookConfirmation;

#[derive(Debug)]
pub struct BatchProbe {
    pub core: StrategyCore,
    pub instrument: InstrumentId,
    pub market_id: u64,
    pub outcome: u8,
    pub phase: &'static str,
    pub place: bool,
    pub handle: LiveNodeHandle,
    pub confirmation: Rc<RefCell<Option<RestingBookConfirmation>>>,
    pub journal: Rc<RefCell<Vec<Value>>>,
    pub complete: Rc<Cell<bool>>,
    pub ids: Vec<ClientOrderId>,
    pub book: Option<PredictOrderbookSnapshot>,
    pub modified: bool,
    pub batch_sent: bool,
    pub modify_rejections: usize,
    pub last_account: Option<UUID4>,
}
impl BatchProbe {
    fn record(&self, mut row: Value) {
        row["phase"] = json!(self.phase);
        self.journal.borrow_mut().push(row);
    }
    fn progress(&mut self) -> anyhow::Result<()> {
        let account_id = if self.place {
            AccountId::new("PREDICT-calibration")
        } else {
            AccountId::new("PREDICT-001")
        };
        let state = self
            .cache()
            .account(&account_id)
            .and_then(|a| a.last_event());
        let locked = state
            .as_ref()
            .and_then(|s| s.balances.first())
            .map(|b| b.locked.as_decimal());
        if let Some(state) = state
            && self.last_account != Some(state.event_id)
        {
            self.last_account = Some(state.event_id);
            self.record(json!({"type":"AccountStateApplied","eventId":state.event_id,"balances":state.balances.iter().map(|b|json!({"total":b.total.as_decimal().to_string(),"locked":b.locked.as_decimal().to_string(),"free":b.free.as_decimal().to_string()})).collect::<Vec<_>>() }));
        }
        if self.ids.is_empty() && self.place {
            let Some(book) = self.book.clone() else {
                return Ok(());
            };
            let (Some(bid), Some(ask)) = (book.bids.first(), book.asks.first()) else {
                return Ok(());
            };
            let (bid, ask) = if self.outcome == 1 {
                (bid.price.as_decimal(), ask.price.as_decimal())
            } else {
                (
                    Decimal::ONE - ask.price.as_decimal(),
                    Decimal::ONE - bid.price.as_decimal(),
                )
            };
            let now = self.clock().timestamp_ns().as_u64();
            if bid < Decimal::new(20, 2)
                || Decimal::new(2, 2) > bid / Decimal::from(2)
                || !self
                    .confirmation
                    .borrow()
                    .is_some_and(|c| c.matches(now, self.market_id, self.outcome, bid, ask, 2000))
            {
                return Ok(());
            }
            for (price, quantity) in [("0.01", 100), ("0.02", 50)] {
                let order = self.order().limit(
                    self.instrument,
                    OrderSide::Buy,
                    Quantity::from(quantity),
                    Price::from(price),
                    Some(TimeInForce::Gtc),
                    None,
                    Some(true),
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
                self.ids.push(order.client_order_id());
                self.record(json!({"type":"SubmissionDecision","price":price,"quantity":quantity,"bookTsEvent":book.ts_event.as_u64(),"bestBid":bid.to_string(),"restConfirmed":true}));
                self.submit_order(order, None, Some(ClientId::new("PREDICT")), None)?;
            }
        }
        let orders = self
            .ids
            .iter()
            .filter_map(|id| self.cache().order(id))
            .collect::<Vec<_>>();
        if orders.iter().any(|o| {
            !o.filled_qty().is_zero()
                || matches!(o.status(), OrderStatus::Denied | OrderStatus::Rejected)
        }) {
            self.handle.stop();
            anyhow::bail!(
                "unexpected fill or rejected calibration order; supervisor cleanup required"
            );
        }
        if orders.len() != 2 {
            return Ok(());
        }
        if self.place {
            if orders.iter().all(|o| o.status() == OrderStatus::Accepted)
                && locked == Some(Decimal::from(2))
            {
                self.record(json!({"type":"LeaveTwoOrdersForColdStartup","venueOrderIds":orders.iter().map(|o|o.venue_order_id().unwrap().to_string()).collect::<Vec<_>>() }));
                self.complete.set(true);
                self.handle.stop();
            }
        } else {
            if !self.modified
                && locked == Some(Decimal::from(2))
                && orders.iter().all(|o| o.status() == OrderStatus::Accepted)
            {
                self.modified = true;
                for order in &orders {
                    self.modify_order(
                        order.client_order_id(),
                        None,
                        Some({
                            let instrument = self.cache().instrument(&self.instrument).unwrap();
                            Price::from_decimal_dp(
                                order.price().unwrap().as_decimal()
                                    + instrument.price_increment().as_decimal(),
                                instrument.price_precision(),
                            )?
                        }),
                        None,
                        Some(ClientId::new("PREDICT")),
                        None,
                    )?;
                }
            }
            if self.modify_rejections == 2
                && !self.batch_sent
                && orders.iter().all(|o| o.status() == OrderStatus::Accepted)
            {
                self.batch_sent = true;
                self.record(json!({"type":"StrategyBatchCancelOrders","clientOrderIds":self.ids.iter().map(ToString::to_string).collect::<Vec<_>>() }));
                self.cancel_orders(self.ids.clone(), Some(ClientId::new("PREDICT")), None)?;
            }
            if self.batch_sent
                && orders.iter().all(|o| o.status() == OrderStatus::Canceled)
                && locked == Some(Decimal::ZERO)
            {
                self.complete.set(true);
            }
        }
        Ok(())
    }
}
impl DataActor for BatchProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let existing = self
            .cache()
            .orders_open(
                None,
                Some(&self.instrument),
                self.strategy_id().as_ref(),
                None,
                None,
            )
            .into_iter()
            .map(|o| o.client_order_id())
            .collect::<Vec<_>>();
        self.record(json!({"type":"StrategyOnStart","existingOrders":existing.len(),"cachedOrders":self.cache().orders(None,None,None,None,None).len()}));
        if !self.place {
            anyhow::ensure!(
                existing.len() == 2,
                "Native cold startup did not assign two orders to strategy"
            );
            self.ids = existing;
        }
        self.subscribe_quotes(self.instrument, Some(ClientId::new("PREDICT")), None);
        self.subscribe_data(
            predict_orderbook_data_type(self.market_id),
            Some(ClientId::new("PREDICT")),
            None,
        );
        self.clock().set_timer(
            "BATCH-PROBE",
            Duration::from_millis(100),
            None,
            None,
            None,
            None,
            None,
        )?;
        Ok(())
    }
    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        if let Some(book) = data
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
        {
            anyhow::ensure!(book.market_id == self.market_id, "unexpected snapshot");
            self.record(json!({"type":"CustomSnapshot","snapshot":book}));
            self.book = Some(book.clone());
        }
        self.progress()
    }
    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.record(json!({"type":"QuoteTick","quote":quote}));
        Ok(())
    }
    fn on_time_event(&mut self, _: &nautilus_common::timer::TimeEvent) -> anyhow::Result<()> {
        self.progress()
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.clock().cancel_timer("BATCH-PROBE");
        // Place phase deliberately leaves only this test's orders for the next Native startup.
        // The outer Rust and Python supervisors always perform independently verified cleanup.
        if !self.place {
            for id in self.ids.clone() {
                if self.cache().order(&id).is_some_and(|o| o.is_open()) {
                    self.cancel_order(id, Some(ClientId::new("PREDICT")), None)?;
                }
            }
        }
        Ok(())
    }
}
nautilus_strategy!(BatchProbe, {
    fn external_order_instrument_ids(&self) -> Option<Vec<InstrumentId>> {
        self.core.config.external_order_instrument_ids.clone()
    }
    fn on_order_modify_rejected(&mut self, event: OrderModifyRejected) {
        assert!(
            event
                .reason
                .as_str()
                .contains("atomic order modification is unsupported")
        );
        let order = self.cache().order(&event.client_order_id).unwrap().clone();
        let expected = if order.quantity().as_decimal() == Decimal::from(100) {
            Decimal::new(1, 2)
        } else {
            Decimal::new(2, 2)
        };
        assert_eq!(order.price().unwrap().as_decimal(), expected);
        self.modify_rejections += 1;
        self.record(json!({"type":"ModifyTermsPreserved","clientOrderId":event.client_order_id.to_string(),"price":expected.to_string()}));
    }
    fn on_order_event(&mut self, event: OrderEventAny) {
        self.record(json!({"type":"StrategyOrderCallback","kind":format!("{:?}",event.event_type()),"clientOrderId":event.client_order_id().to_string(),"venueOrderId":event.venue_order_id().map(|v|v.to_string()),"cachedStatus":self.cache().order(&event.client_order_id()).map(|o|o.status().to_string())}));
    }
});
