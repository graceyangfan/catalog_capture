// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Observe Native's public raw reconciliation topics without replacing execution handlers.
use std::{cell::RefCell, rc::Rc};

use nautilus_common::msgbus::{self, MessagingSwitchboard, ShareableMessageHandler, TypedHandler};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    events::OrderEventAny,
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
};
use serde_json::{Value, json};

pub struct NativeInputJournal {
    pub rows: Rc<RefCell<Vec<Value>>>,
    phase: &'static str,
    reports: Vec<(String, ShareableMessageHandler)>,
    orders: TypedHandler<OrderEventAny>,
}
impl NativeInputJournal {
    pub fn attach(phase: &'static str, rows: Rc<RefCell<Vec<Value>>>) -> Self {
        let mut reports = vec![];
        macro_rules! listen {
            ($ty:ty,$topic:expr,$kind:expr)=>{{
                let output=rows.clone();
                let handler=ShareableMessageHandler::from_typed(move |report:&$ty| {
                    output.borrow_mut().push(json!({"phase":phase,"type":$kind,"observedNs":get_atomic_clock_realtime().get_time_ns().as_u64(),"report":report}));
                });
                let topic=$topic.to_string();
                msgbus::subscribe_any(topic.clone().into(),handler.clone(),None);
                reports.push((topic,handler));
            }};
        }
        listen!(
            OrderStatusReport,
            MessagingSwitchboard::reconciliation_raw_order_status_report_topic(),
            "RawOrderStatusReport"
        );
        listen!(
            FillReport,
            MessagingSwitchboard::reconciliation_raw_fill_report_topic(),
            "RawFillReport"
        );
        listen!(
            PositionStatusReport,
            MessagingSwitchboard::reconciliation_raw_position_status_report_topic(),
            "RawPositionStatusReport"
        );
        let output = rows.clone();
        let commands = ShareableMessageHandler::from_typed(
            move |diagnostic: &nautilus_live::execution::command::CommandRecoveryExhausted| {
                output.borrow_mut().push(
                    json!({"phase":phase,"type":"NativeCommandRecoveryExhausted",
                "clientOrderId":diagnostic.client_order_id.to_string(),
                "venueOrderId":diagnostic.venue_order_id.map(|id|id.to_string()),
                "status":diagnostic.order_status.to_string(),"retryCount":diagnostic.retry_count}),
                );
            },
        );
        let topic = MessagingSwitchboard::command_recovery_exhausted_topic().to_string();
        msgbus::subscribe_any(topic.clone().into(), commands.clone(), None);
        reports.push((topic, commands));
        let output = rows.clone();
        let orders = TypedHandler::from(move |e: &OrderEventAny| {
            output.borrow_mut().push(json!({"phase":phase,"type":"NativeOrderEvent","observedNs":get_atomic_clock_realtime().get_time_ns().as_u64(),"kind":format!("{:?}",e.event_type()),"clientOrderId":e.client_order_id().to_string(),"venueOrderId":e.venue_order_id().map(|id|id.to_string()),"tsEvent":e.ts_event().as_u64()}));
        });
        msgbus::subscribe_order_events("events.order.*".into(), orders.clone(), None);
        Self {
            rows,
            phase,
            reports,
            orders,
        }
    }
    pub fn detach(self) {
        for (topic, handler) in &self.reports {
            msgbus::unsubscribe_any(topic.clone().into(), handler);
        }
        msgbus::unsubscribe_order_events("events.order.*".into(), &self.orders);
        self.rows
            .borrow_mut()
            .push(json!({"phase":self.phase,"type":"RecorderDetached"}));
    }
}
