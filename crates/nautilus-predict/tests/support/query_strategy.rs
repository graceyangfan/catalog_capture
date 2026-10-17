// Copyright (C) 2026 yfclark and contributors. LGPL-3.0.
//! Test-only native command/callback probe. Readiness is controlled by loopback evidence.
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use nautilus_common::{
    actor::DataActor,
    messages::DataEvent,
    msgbus::{self, TypedHandler},
};
use nautilus_model::{
    data::{CustomData, Data, QuoteTick},
    enums::{OmsType, OrderSide, TimeInForce},
    events::{
        AccountState, OrderAccepted, OrderCanceled, OrderEventAny, OrderFilled,
        OrderModifyRejected, PositionEvent,
    },
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId},
    orders::Order,
    types::{Price, Quantity},
};
use nautilus_predict::{PredictOrderbookSnapshot, predict_orderbook_data_type};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use serde_json::{Value, json};

#[derive(Debug)]
struct RecoveryCommands {
    query: Rc<Cell<bool>>,
    resume: Rc<Cell<bool>>,
}

#[derive(Debug)]
pub struct QueryStrategy {
    core: StrategyCore,
    instrument: InstrumentId,
    entry: Option<ClientOrderId>,
    fills_only: bool,
    account_handler: Option<TypedHandler<AccountState>>,
    last_account: Option<nautilus_core::UUID4>,
    resume_control: Option<RecoveryCommands>,
    resumed: Option<ClientOrderId>,
    resume_cancel_sent: bool,
    pub ready: Rc<Cell<bool>>,
    pub journal: Rc<RefCell<Vec<Value>>>,
}
impl QueryStrategy {
    pub fn new(
        instrument: InstrumentId,
        ready: Rc<Cell<bool>>,
        journal: Rc<RefCell<Vec<Value>>>,
    ) -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::new("PREDICT-QUERY-001")),
                oms_type: Some(OmsType::Netting),
                ..Default::default()
            }),
            instrument,
            entry: None,
            fills_only: false,
            resume_control: None,
            resumed: None,
            resume_cancel_sent: false,
            account_handler: None,
            last_account: None,
            ready,
            journal,
        }
    }
}
impl QueryStrategy {
    pub fn recovery_commands(mut self, query: Rc<Cell<bool>>, resume: Rc<Cell<bool>>) -> Self {
        self.resume_control = Some(RecoveryCommands { query, resume });
        self
    }
    pub fn recover_fills(mut self, existing: Option<ClientOrderId>) -> Self {
        self.entry = existing;
        self.fills_only = true;
        self
    }
}
impl DataActor for QueryStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.set_external_order_instrument_ids(vec![self.instrument])?;
        let journal = self.journal.clone();
        let handler = TypedHandler::from(move |state: &AccountState| {
            journal.borrow_mut().push(json!({"type":"AccountReported","balances":state.balances.iter().map(|b|json!({"total":b.total.as_decimal().to_string(),"locked":b.locked.as_decimal().to_string()})).collect::<Vec<_>>()}));
        });
        msgbus::subscribe_account_state(
            "events.account.PREDICT-fixture".into(),
            handler.clone(),
            None,
        );
        self.account_handler = Some(handler);
        self.subscribe_data(
            predict_orderbook_data_type(42),
            Some(ClientId::new("PREDICT")),
            None,
        );
        self.clock().set_timer(
            "QUERY-PROBE",
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
        self.clock().cancel_timer("QUERY-PROBE");
        if let Some(handler) = self.account_handler.take() {
            msgbus::unsubscribe_account_state("events.account.PREDICT-fixture".into(), &handler);
        }
        Ok(())
    }
    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        if let Some(book) = data
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
        {
            let quote = QuoteTick::new(
                self.instrument,
                book.bids[0].price,
                book.asks[0].price,
                book.bids[0].quantity,
                book.asks[0].quantity,
                book.ts_event,
                book.ts_init,
            );
            nautilus_common::live::runner::try_get_data_event_sender()
                .unwrap()
                .send(DataEvent::Data(Data::Quote(quote)))?;
        }
        Ok(())
    }
    fn on_time_event(&mut self, _: &nautilus_common::timer::TimeEvent) -> anyhow::Result<()> {
        let state = self
            .cache()
            .account(&AccountId::new("PREDICT-fixture"))
            .and_then(|a| a.last_event());
        if let Some(state) = state
            && self.last_account != Some(state.event_id)
        {
            self.last_account = Some(state.event_id);
            self.journal.borrow_mut().push(json!({"type":"AccountSnapshot","reconciliationReady":state.info.as_ref().and_then(|v|v.get("predict_reconciliation_ready")).cloned(),"balances":state.balances.iter().map(|b|json!({"total":b.total.as_decimal().to_string(),"locked":b.locked.as_decimal().to_string()})).collect::<Vec<_>>()}));
        }

        if let Some(RecoveryCommands { query, resume }) = &self.resume_control {
            let (query, resume) = (query.replace(false), resume.replace(false));
            if query {
                self.query_account(
                    AccountId::new("PREDICT-fixture"),
                    Some(ClientId::new("PREDICT")),
                    None,
                )?;
            }
            if resume {
                let order = self.order().limit(
                    self.instrument,
                    OrderSide::Buy,
                    Quantity::from(100),
                    Price::from("0.010"),
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
                    Some(ClientOrderId::new("RESUMED-001")),
                );
                self.resumed = Some(order.client_order_id());
                self.submit_order(order, None, Some(ClientId::new("PREDICT")), None)?;
            }
        }
        if let Some(id) = self.resumed
            && !self.resume_cancel_sent
            && self
                .cache()
                .order(&id)
                .is_some_and(|o| o.status() == nautilus_model::enums::OrderStatus::Accepted)
        {
            self.resume_cancel_sent = true;
            self.cancel_order(id, Some(ClientId::new("PREDICT")), None)?;
        }
        if self.entry.is_none() && self.cache().quote(&self.instrument).is_some() {
            let order = self.order().limit(
                self.instrument,
                OrderSide::Buy,
                Quantity::from(100),
                Price::from("0.620"),
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
            self.entry = Some(order.client_order_id());
            self.submit_order(order, None, Some(ClientId::new("PREDICT")), None)?;
        } else if self.ready.replace(false) {
            let order = self.cache().order(&self.entry.unwrap()).unwrap().clone();
            self.journal.borrow_mut().push(json!({"type":"StrategyQueryOrder","cachedStatus":order.status().to_string(),"venueOrderId":order.venue_order_id().map(|v|v.to_string())}));
            self.query_order(&order, Some(ClientId::new("PREDICT")), None)?;
        }
        Ok(())
    }
}
nautilus_strategy!(QueryStrategy, {
    fn on_order_accepted(&mut self, event: OrderAccepted) {
        assert_eq!(
            self.cache()
                .order(&event.client_order_id)
                .unwrap()
                .venue_order_id(),
            Some(event.venue_order_id)
        );
        self.journal.borrow_mut().push(json!({"callback":"on_order_accepted","cachedStatus":self.cache().order(&event.client_order_id).unwrap().status().to_string()}));
        if self.fills_only {
            return;
        }
        self.modify_order(
            event.client_order_id,
            None,
            Some(Price::from("0.621")),
            None,
            Some(ClientId::new("PREDICT")),
            None,
        )
        .unwrap();
    }
    fn on_order_modify_rejected(&mut self, event: OrderModifyRejected) {
        self.journal.borrow_mut().push(json!({"callback":"on_order_modify_rejected","price":self.cache().order(&event.client_order_id).unwrap().price().unwrap().to_string()}));
        self.cancel_order(event.client_order_id, Some(ClientId::new("PREDICT")), None)
            .unwrap();
    }
    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        self.journal.borrow_mut().push(json!({"callback":"on_order_canceled","cachedStatus":self.cache().order(&event.client_order_id).unwrap().status().to_string()}));
    }
    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.journal.borrow_mut().push(json!({"callback":"on_order_filled","clientOrderId":event.client_order_id.to_string(),"tradeId":event.trade_id.to_string(),"lastQty":event.last_qty.to_string(),"commission":event.commission.map(|v|v.to_string()),"cachedFilledQty":self.cache().order(&event.client_order_id).unwrap().filled_qty().to_string()}));
        if self.fills_only {
            self.query_account(
                AccountId::new("PREDICT-fixture"),
                Some(ClientId::new("PREDICT")),
                None,
            )
            .unwrap();
        }
    }
    fn on_position_event(&mut self, _: PositionEvent) {
        self.journal.borrow_mut().push(json!({"callback":"on_position_event","quantity":self.cache().positions_open(None,Some(&self.instrument),None,None,None).iter().map(|p|p.quantity.as_decimal()).sum::<rust_decimal::Decimal>().to_string()}));
    }
    fn on_order_event(&mut self, event: OrderEventAny) {
        self.journal.borrow_mut().push(json!({"type":format!("{:?}",event.event_type()),"cachedStatus":self.cache().order(&event.client_order_id()).unwrap().status().to_string()}));
    }
});
