// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! A second native strategy keeps its own BUY active throughout the first strategy's exit.
use nautilus_common::actor::DataActor;
use nautilus_model::{
    data::CustomData,
    enums::TimeInForce,
    events::{OrderEventAny, PositionChanged, PositionClosed, PositionOpened},
    identifiers::{ClientOrderId, StrategyId},
};
use nautilus_predict::predict_orderbook_data_type;
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Idle,
    Submit,
    Cancel,
}

#[derive(Debug)]
pub(super) struct PassiveStrategy {
    core: StrategyCore,
    pub phase: Rc<std::cell::Cell<Phase>>,
    pub journal: Rc<RefCell<Vec<Value>>>,
    native_quotes: bool,
    submitted: bool,
    canceled: bool,
}
impl PassiveStrategy {
    pub fn with_native_quotes(mut self) -> Self {
        self.native_quotes = true;
        self
    }
    pub fn new(phase: Rc<std::cell::Cell<Phase>>, journal: Rc<RefCell<Vec<Value>>>) -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::new("PREDICT-PASSIVE-002")),
                use_uuid_client_order_ids: true,
                ..Default::default()
            }),
            phase,
            journal,
            native_quotes: false,
            submitted: false,
            canceled: false,
        }
    }
}
impl DataActor for PassiveStrategy {
    fn on_quote(&mut self, quote: &nautilus_model::data::QuoteTick) -> anyhow::Result<()> {
        self.journal.borrow_mut().push(json!({"type":"NativeQuoteObserved","instrumentId":quote.instrument_id.to_string(),"bid":quote.bid_price.to_string(),"ask":quote.ask_price.to_string(),"bidSize":quote.bid_size.to_string(),"askSize":quote.ask_size.to_string(),"tsEvent":quote.ts_event.as_u64()}));
        Ok(())
    }
    fn on_start(&mut self) -> anyhow::Result<()> {
        if self.native_quotes {
            self.subscribe_quotes(instrument_id(1), Some(ClientId::new("PREDICT")), None);
        }
        self.subscribe_data(
            predict_orderbook_data_type(42),
            Some(ClientId::new("PREDICT")),
            None,
        );
        Ok(())
    }
    fn on_data(&mut self, _: &CustomData) -> anyhow::Result<()> {
        if self.phase.get() == Phase::Submit && !self.submitted {
            let order = self.order().limit(
                instrument_id(1),
                OrderSide::Buy,
                Quantity::from(100),
                Price::from("0.010"),
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
            self.submitted = true;
            self.submit_order(order, None, Some(ClientId::new("PREDICT")), None)?;
        }
        if self.phase.get() == Phase::Cancel && !self.canceled {
            self.canceled = true;
            // Cleanup uses native per-strategy cache filtering.
            self.cancel_all_orders(
                instrument_id(1),
                None,
                Some(ClientId::new("PREDICT")),
                true,
                None,
            )?;
        }
        Ok(())
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        if self.native_quotes {
            self.unsubscribe_quotes(instrument_id(1), Some(ClientId::new("PREDICT")), None);
        }
        self.unsubscribe_data(
            predict_orderbook_data_type(42),
            Some(ClientId::new("PREDICT")),
            None,
        );
        Ok(())
    }
}
nautilus_strategy!(PassiveStrategy, {
    fn on_position_opened(&mut self, event: PositionOpened) {
        assert_eq!(event.strategy_id, StrategyId::new("PREDICT-PASSIVE-002"));
        self.journal.borrow_mut().push(json!({"type":"PositionOpened","strategyId":event.strategy_id.to_string(),"quantity":event.quantity.to_string()}));
    }
    fn on_position_changed(&mut self, event: PositionChanged) {
        assert_eq!(event.strategy_id, StrategyId::new("PREDICT-PASSIVE-002"));
        self.journal.borrow_mut().push(json!({"type":"PositionChanged","strategyId":event.strategy_id.to_string(),"quantity":event.quantity.to_string()}));
    }
    fn on_position_closed(&mut self, event: PositionClosed) {
        self.journal.borrow_mut().push(json!({"type":"PositionClosed","strategyId":event.strategy_id.to_string(),"quantity":event.quantity.to_string()}));
    }
    fn on_order_event(&mut self, event: OrderEventAny) {
        let id: ClientOrderId = event.client_order_id();
        let cache = self.cache();
        let order = cache.order(&id).unwrap();
        assert_eq!(order.strategy_id(), StrategyId::new("PREDICT-PASSIVE-002"));
        self.journal.borrow_mut().push(json!({"type":format!("{:?}",event.event_type()),
            "id":id.to_string(),"status":order.status().to_string(),"strategyId":order.strategy_id().to_string(),"filled":order.filled_qty().to_string(),"leaves":order.leaves_qty().to_string()}));
    }
});
