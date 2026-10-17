// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Read-only strategy for exercising the product host's startup boundary.
use std::{cell::RefCell, rc::Rc};

use nautilus_common::actor::DataActor;
use nautilus_model::{data::CustomData, identifiers::ClientId};
use nautilus_trading::{StrategyCore, nautilus_strategy};
use serde_json::{Value, json};

#[derive(Debug)]
pub struct HostObserver {
    pub core: StrategyCore,
    pub market_id: u64,
    pub journal: Rc<RefCell<Vec<Value>>>,
}
nautilus_strategy!(HostObserver);
impl DataActor for HostObserver {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let cache = self.cache();
        let instruments = cache.instruments(&nautilus_predict::common::PREDICT_VENUE, None);
        let orders = cache.orders_open(None, None, None, None, None);
        let positions = cache.positions_open(None, None, None, None, None);
        self.journal.borrow_mut().push(json!({
            "type":"StrategyOnStart",
            "accountBalances":cache.accounts_all().iter().filter_map(|a|a.last_event()).map(|e|e.balances.clone()).collect::<Vec<_>>(),
            "instruments":instruments.iter().map(|i|nautilus_model::instruments::Instrument::id(i).to_string()).collect::<Vec<_>>(),
            "openOrders":orders.len(),
            "positions":positions.iter().map(|p|json!({"instrumentId":p.instrument_id.to_string(),"quantity":p.quantity.to_string()})).collect::<Vec<_>>()
        }));
        drop(cache);
        self.subscribe_data(
            nautilus_predict::predict_orderbook_data_type(self.market_id),
            Some(ClientId::new("PREDICT")),
            None,
        );
        Ok(())
    }
    fn on_data(&mut self, _: &CustomData) -> anyhow::Result<()> {
        self.journal
            .borrow_mut()
            .push(json!({"type":"CustomSnapshot"}));
        Ok(())
    }
}
