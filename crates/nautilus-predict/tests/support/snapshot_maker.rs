// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Test-only snapshot policy: LIMIT acceptance, optional modify rejection and explicit cancel/replace.
//! No quote callback, order ledger, timer, manual cache writes or synthetic native events.
use std::{cell::RefCell, rc::Rc};

use nautilus_common::actor::DataActor;
use nautilus_model::{
    accounts::Account,
    data::CustomData,
    enums::{OrderSide, OrderStatus, TimeInForce},
    events::{
        OrderAccepted, OrderCancelRejected, OrderCanceled, OrderDenied, OrderModifyRejected,
        OrderRejected, OrderSubmitted,
    },
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId},
    instruments::Instrument,
    orders::Order,
    types::{Currency, Price, Quantity},
};
use nautilus_predict::{PredictOrderbookSnapshot, predict_orderbook_data_type};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use rust_decimal::Decimal;
use serde_json::{Value, json};

#[derive(Debug, Default)]
pub struct FlowTiming {
    pub canceled: Option<std::time::Instant>,
    pub replacement: Option<std::time::Instant>,
    pub admitted: Option<std::time::Instant>,
}

#[derive(Debug)]
pub struct SnapshotMaker {
    core: StrategyCore,
    timing: Option<Rc<RefCell<FlowTiming>>>,
    market_id: u64,
    instrument: InstrumentId,
    submitted: Option<ClientOrderId>,
    replace: bool,
    shared_account: bool,
    companion: bool,
    round: u8,
    account_before_query: Option<nautilus_core::UUID4>,
    fresh_block_replace: bool,
    block_before_query: Option<u64>,
    journal: Rc<RefCell<Vec<Value>>>,
}
impl SnapshotMaker {
    pub fn new(market_id: u64, instrument: InstrumentId, journal: Rc<RefCell<Vec<Value>>>) -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::new("HOST-EXEC-001")),
                use_uuid_client_order_ids: true,
                ..Default::default()
            }),
            timing: None,
            market_id,
            instrument,
            submitted: None,
            replace: false,
            shared_account: false,
            companion: false,
            round: 0,
            account_before_query: None,
            fresh_block_replace: false,
            block_before_query: None,
            journal,
        }
    }
    pub fn with_flow_timing(mut self, timing: Rc<RefCell<FlowTiming>>) -> Self {
        self.timing = Some(timing);
        self
    }
    pub fn with_cancel_replace(mut self) -> Self {
        self.replace = true;
        self
    }
    /// Historical explicit-query replays deliberately wait for a new chain block.
    pub fn with_fresh_block_replace(mut self) -> Self {
        self.fresh_block_replace = true;
        self
    }
    pub fn with_shared_account(mut self) -> Self {
        self.shared_account = true;
        self
    }
    pub fn companion(
        market_id: u64,
        instrument: InstrumentId,
        journal: Rc<RefCell<Vec<Value>>>,
    ) -> Self {
        let mut strategy = Self::new(market_id, instrument, journal);
        strategy.core = StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::new("HOST-OTHER-002")),
            use_uuid_client_order_ids: true,
            ..Default::default()
        });
        strategy.shared_account = true;
        strategy.companion = true;
        strategy
    }
    fn account_snapshot(&self) -> Value {
        let cache = self.cache();
        let account = cache.account(&AccountId::new("PREDICT-fixture")).unwrap();
        let event = account.last_event().unwrap();
        let mut snapshot = json!({"total":account.balance_total(Some(Currency::USDT())).unwrap().as_decimal().to_string(),
            "free":account.balance_free(Some(Currency::USDT())).unwrap().as_decimal().to_string(),
            "locked":account.balance_locked(Some(Currency::USDT())).unwrap().as_decimal().to_string(),
            "block":event.info.as_ref().and_then(|i|i.get("predict_block_number")).cloned()});
        if self.shared_account {
            snapshot["open"] = json!(cache.orders_open(None, None, None, None, None).len());
        }
        snapshot
    }
}
impl DataActor for SnapshotMaker {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_data(
            predict_orderbook_data_type(self.market_id),
            Some(ClientId::new("PREDICT")),
            None,
        );
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.unsubscribe_data(
            predict_orderbook_data_type(self.market_id),
            Some(ClientId::new("PREDICT")),
            None,
        );
        Ok(())
    }
    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        if data.data_type != predict_orderbook_data_type(self.market_id) {
            return Ok(());
        }
        let Some(book) = data
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
        else {
            return Ok(());
        };
        self.journal
            .borrow_mut()
            .push(json!({"callback":"on_data","source":"PredictOrderbookSnapshot"}));
        if let Some(previous) = self.submitted {
            if self.companion {
                if book.bids.iter().any(|l| l.price >= Price::from("0.632"))
                    && self
                        .cache()
                        .order(&previous)
                        .is_some_and(|o| o.status() == OrderStatus::Accepted)
                {
                    self.cancel_order(previous, Some(ClientId::new("PREDICT")), None)?;
                }
                return Ok(());
            }
            let Some(before_query) = self.account_before_query else {
                return Ok(());
            };
            if !self.replace || self.round != 0 {
                return Ok(());
            }
            let ready = {
                let cache = self.cache();
                let account = cache.account(&AccountId::new("PREDICT-fixture")).unwrap();
                cache
                    .order(&previous)
                    .is_some_and(|o| o.status() == OrderStatus::Canceled)
                    && account
                        .last_event()
                        .is_some_and(|e| e.event_id != before_query)
                    && (!self.fresh_block_replace
                        || account
                            .last_event()
                            .and_then(|e| e.info.as_ref()?.get("predict_block_number")?.as_u64())
                            > self.block_before_query)
            };
            let snapshot = self.account_snapshot();
            self.journal.borrow_mut().push(
                json!({"callback":if ready {"ReplacementAccountReady"} else {"ReplacementDeferred"},
                "account":snapshot,"source":"native_cache"}),
            );
            if !ready {
                return Ok(());
            }
            self.round = 1;
        }
        let Some(bid) = book.bids.iter().max_by_key(|level| level.price) else {
            return Ok(());
        };
        let cache = self.cache();
        let instrument = cache
            .instrument(&self.instrument)
            .expect("native bootstrap instrument");
        // Deliberately distant fixture order; this calibration policy is not an alpha signal.
        let signal_price = if self.companion {
            Decimal::ONE
                - book
                    .asks
                    .iter()
                    .min_by_key(|l| l.price)
                    .unwrap()
                    .price
                    .as_decimal()
                - Decimal::new(350, 3)
        } else {
            bid.price.as_decimal() - Decimal::new(620, 3)
        };
        let price = Price::from_decimal_dp(signal_price, instrument.price_precision())?;
        drop(cache);
        let order = self.order().limit(
            self.instrument,
            OrderSide::Buy,
            Quantity::from(100),
            price,
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
        self.submitted = Some(order.client_order_id());
        self.journal.borrow_mut().push(json!({"callback":"submit_order","price":price.to_string(),"quantity":"100","signal":"custom_snapshot"}));
        if self.replace {
            self.journal
                .borrow_mut()
                .push(json!({"callback":"OrderIntent","round":self.round,
                "account":self.account_snapshot(),"price":price.to_string()}));
        }
        if self.round == 1
            && let Some(timing) = &self.timing
        {
            timing.borrow_mut().replacement = Some(std::time::Instant::now());
        }
        self.submit_order(order, None, Some(ClientId::new("PREDICT")), None)
    }
}
nautilus_strategy!(SnapshotMaker, {
    fn on_order_submitted(&mut self, _event: OrderSubmitted) {
        if self.round == 1
            && let Some(timing) = &self.timing
        {
            timing.borrow_mut().admitted = Some(std::time::Instant::now());
        }
    }
    fn on_order_accepted(&mut self, event: OrderAccepted) {
        assert_eq!(Some(event.client_order_id), self.submitted);
        // Reported cash refresh is asynchronous; strategy feedback must not wait
        // for it. Native order state, rather than a coincidental RPC completion,
        // must already be visible in this callback.
        assert_eq!(
            self.cache().order(&event.client_order_id).unwrap().status(),
            OrderStatus::Accepted
        );
        self.journal
            .borrow_mut()
            .push(json!({"callback":"on_order_accepted"}));
        if self.shared_account {
            self.journal
                .borrow_mut()
                .push(json!({"callback":"AccountAtAccepted","account":self.account_snapshot()}));
        }
        if self.companion {
            return;
        }
        if self.replace && self.round == 0 {
            self.journal
                .borrow_mut()
                .push(json!({"callback":"modify_order","requestedPrice":"0.011"}));
            self.modify_order(
                event.client_order_id,
                None,
                Some(Price::from("0.011")),
                None,
                Some(ClientId::new("PREDICT")),
                None,
            )
            .unwrap();
        } else {
            self.cancel_order(event.client_order_id, Some(ClientId::new("PREDICT")), None)
                .unwrap();
        }
    }
    fn on_order_modify_rejected(&mut self, event: OrderModifyRejected) {
        assert!(self.replace && self.round == 0);
        assert_eq!(Some(event.client_order_id), self.submitted);
        let cache = self.cache();
        let order = cache.order(&event.client_order_id).unwrap();
        assert_eq!(order.status(), OrderStatus::Accepted);
        assert_eq!(order.price(), Some(Price::from("0.010")));
        assert_eq!(order.quantity(), Quantity::from(100));
        assert!(
            event
                .reason
                .as_str()
                .contains("atomic order modification is unsupported")
                || (self.shared_account
                    && event
                        .reason
                        .as_str()
                        .starts_with("NOTIONAL_EXCEEDS_FREE_BALANCE:")),
            "unexpected modification rejection: {}",
            event.reason
        );
        self.journal.borrow_mut().push(json!({"callback":"on_order_modify_rejected",
            "reason":event.reason.to_string(),"cachedPrice":order.price().unwrap().to_string(),
            "cachedQuantity":order.quantity().to_string(),"cachedStatus":format!("{:?}",order.status())}));
        drop(cache);
        self.cancel_order(event.client_order_id, Some(ClientId::new("PREDICT")), None)
            .unwrap();
    }
    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        if self.round == 0
            && let Some(timing) = &self.timing
        {
            timing.borrow_mut().canceled = Some(std::time::Instant::now());
        }
        assert_eq!(Some(event.client_order_id), self.submitted);
        self.journal
            .borrow_mut()
            .push(json!({"callback":"on_order_canceled"}));
        if self.replace && self.round == 0 {
            self.block_before_query = self.account_snapshot()["block"].as_u64();
            self.account_before_query = Some(
                self.cache()
                    .account(&AccountId::new("PREDICT-fixture"))
                    .unwrap()
                    .last_event()
                    .unwrap()
                    .event_id,
            );
            self.journal.borrow_mut().push(
                json!({"callback":"query_account","source":"native_strategy_command",
                "accountBeforeQuery":self.account_snapshot()}),
            );
            self.query_account(
                AccountId::new("PREDICT-fixture"),
                Some(ClientId::new("PREDICT")),
                None,
            )
            .unwrap();
        }
    }
    fn on_order_rejected(&mut self, event: OrderRejected) {
        self.journal
            .borrow_mut()
            .push(json!({"callback":"on_order_rejected","reason":event.reason.to_string()}));
    }
    fn on_order_denied(&mut self, event: OrderDenied) {
        self.journal
            .borrow_mut()
            .push(json!({"callback":"on_order_denied","reason":event.reason.to_string()}));
    }
    fn on_order_cancel_rejected(&mut self, event: OrderCancelRejected) {
        self.journal
            .borrow_mut()
            .push(json!({"callback":"on_order_cancel_rejected","reason":event.reason.to_string()}));
    }
});
