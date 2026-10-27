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

//! Notification identity, replay and terminal boundaries independent of economic accounting.
use super::*;
use crate::predict_settlement_data_type;
use nautilus_common::factories::OrderEventFactory;
use nautilus_model::{
    enums::{AccountType, OrderSide, OrderType},
    identifiers::StrategyId,
    orders::{Order, OrderTestBuilder},
};
use serde_json::json;

fn fixture() -> (WorkerOrder, PredictWalletEvent) {
    let mut native = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id("42-1.PREDICT".into())
        .side(OrderSide::Buy)
        .quantity(nautilus_model::types::Quantity::from(100))
        .price(nautilus_model::types::Price::from("0.040"))
        .build();
    let factory = OrderEventFactory::new(
        native.trader_id(),
        AccountId::from("PREDICT-notice"),
        AccountType::Cash,
        None,
    );
    native
        .apply(factory.generate_order_submitted(&native, 1.into()))
        .unwrap();
    native
        .apply(factory.generate_order_accepted(
            &native,
            VenueOrderId::new(format!("0x{}", "11".repeat(32))),
            2.into(),
            2.into(),
        ))
        .unwrap();
    let event=serde_json::from_value(json!({"type":"orderTransactionSubmitted","kind":"PURCHASE","settlementId":"notice-1","orderHash":format!("0x{}","11".repeat(32)),"orderId":"12345","walletAddress":"0x0000000000000000000000000000000000000022","timestamp":1,"details":{"marketId":42,"outcomeIndex":1,"marketQuestion":"test","outcome":"YES","quoteType":"BID","quantity":"100","quantityFilled":"0","price":"0.040","value":"4","valueFilled":"0","strategyType":"LIMIT","categorySlug":"test"},"fill":{"executedPriceWei":"40000000000000000","executedSizeWei":"1000000000000000000","executedValueWei":"40000000000000000"}})).unwrap();
    (WorkerOrder::capture(&native), event)
}

#[test]
fn matching_notifications_preserve_raw_units_identity_and_suppress_replay_and_downgrades() {
    let (order, submitted) = fixture();
    let mut notices = SettlementNotifications::default();
    let account = order.account_id().unwrap();
    let update = notices
        .update(&submitted, &order, account, 0, 2_000_000.into())
        .unwrap()
        .unwrap();
    assert_eq!(update.status, PredictSettlementStatus::Submitted);
    assert_eq!(update.executed_size_wei, "1000000000000000000");
    let encoded = serde_json::to_value(&update).unwrap();
    let decoded: PredictSettlementUpdate = serde_json::from_value(encoded).unwrap();
    let data = decoded.into_custom_data();
    assert_eq!(
        data.data_type,
        predict_settlement_data_type(account, order.strategy_id())
    );
    crate::register_predict_custom_data();
    let encoded = serde_json::to_value(nautilus_model::data::Data::Custom(data.clone())).unwrap();
    let decoded: nautilus_model::data::Data = serde_json::from_value(encoded).unwrap();
    let nautilus_model::data::Data::Custom(decoded) = decoded else {
        panic!("custom data envelope")
    };
    assert_eq!(decoded.data_type, data.data_type);
    assert_eq!(
        decoded
            .data
            .as_any()
            .downcast_ref::<PredictSettlementUpdate>()
            .unwrap()
            .executed_size_wei,
        update.executed_size_wei
    );
    assert_ne!(
        data.data_type,
        predict_settlement_data_type(AccountId::from("PREDICT-other"), order.strategy_id())
    );
    assert_ne!(
        data.data_type,
        predict_settlement_data_type(account, StrategyId::from("OTHER-002"))
    );
    assert!(
        notices
            .update(&submitted, &order, account, 0, 3_000_000.into())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        notices.pending_orders().collect::<Vec<_>>(),
        vec![update.venue_order_id]
    );
    let mut success = submitted.clone();
    let PredictWalletEventKind::OrderTransactionSubmitted {
        settlement_id,
        fill,
        ..
    } = submitted.kind.clone()
    else {
        unreachable!()
    };
    success.kind = PredictWalletEventKind::OrderTransactionSuccess {
        settlement_id,
        fill,
        is_maker: Some(true),
        fee: None,
    };
    assert_eq!(
        notices
            .update(&success, &order, account, 0, 4_000_000.into())
            .unwrap()
            .unwrap()
            .status,
        PredictSettlementStatus::Confirmed
    );
    assert!(
        notices
            .update(&submitted, &order, account, 1, 5_000_000.into())
            .unwrap()
            .is_none()
    );
    assert!(
        notices
            .update(&success, &order, account, 1, 6_000_000.into())
            .unwrap()
            .is_none()
    );
    assert_eq!(notices.pending_orders().count(), 0);
    notices.retire_orders(&HashSet::from([update.venue_order_id]));
    assert!(notices.entries.is_empty() && notices.by_order.is_empty());
}

#[test]
fn failed_settlement_resolves_only_its_pending_fact_and_conflicting_success_fails_before_emit() {
    let (order, submitted) = fixture();
    let account = order.account_id().unwrap();
    let mut notices = SettlementNotifications::default();
    let update = notices
        .update(&submitted, &order, account, 0, 2.into())
        .unwrap()
        .unwrap();
    let PredictWalletEventKind::OrderTransactionSubmitted {
        settlement_id,
        fill,
        ..
    } = submitted.kind.clone()
    else {
        unreachable!()
    };
    let mut failed = submitted.clone();
    failed.kind = PredictWalletEventKind::OrderTransactionFailed {
        settlement_id: settlement_id.clone(),
        fill: fill.clone(),
    };
    assert_eq!(
        notices
            .update(&failed, &order, account, 0, 3.into())
            .unwrap()
            .unwrap()
            .status,
        PredictSettlementStatus::Failed
    );
    assert!(
        notices
            .update(&failed, &order, account, 0, 4.into())
            .unwrap()
            .is_none()
    );
    assert_eq!(notices.pending_orders().count(), 0);
    assert!(order.filled_qty().is_zero());
    let mut success = submitted.clone();
    success.kind = PredictWalletEventKind::OrderTransactionSuccess {
        settlement_id,
        fill,
        is_maker: Some(true),
        fee: None,
    };
    assert!(
        notices
            .validate_success(&success, update.trade_id, update.venue_order_id)
            .is_err(),
        "terminal conflict must be detected before Native send"
    );
    assert!(
        notices
            .update(&success, &order, account, 0, 5.into())
            .is_err()
    );
    notices.retire_orders(&HashSet::from([update.venue_order_id]));
    assert!(notices.entries.is_empty() && notices.by_order.is_empty());
}

#[test]
fn native_fill_resolution_retires_pending_retention_but_does_not_consume_the_success_notification()
{
    let (order, submitted) = fixture();
    let account = order.account_id().unwrap();
    let mut notices = SettlementNotifications::default();
    let update = notices
        .update(&submitted, &order, account, 0, 2.into())
        .unwrap()
        .unwrap();
    notices.applied(update.trade_id);
    assert_eq!(notices.pending_orders().count(), 0);
    assert!(
        notices
            .update(&submitted, &order, account, 1, 3.into())
            .unwrap()
            .is_none()
    );
    let PredictWalletEventKind::OrderTransactionSubmitted {
        settlement_id,
        fill,
        ..
    } = submitted.kind.clone()
    else {
        unreachable!()
    };
    let mut success = submitted;
    success.kind = PredictWalletEventKind::OrderTransactionSuccess {
        settlement_id,
        fill,
        is_maker: Some(true),
        fee: None,
    };
    assert!(
        notices
            .update(&success, &order, account, 1, 4.into())
            .unwrap()
            .is_some()
    );
}

#[test]
fn terminal_notice_capacity_follows_native_owner_retirement() {
    let (order, submitted) = fixture();
    let account = order.account_id().unwrap();
    let mut notices = SettlementNotifications::default();
    let PredictWalletEventKind::OrderTransactionSubmitted { fill, .. } = submitted.kind.clone()
    else {
        unreachable!()
    };
    for index in 0..10000 {
        let mut event = submitted.clone();
        event.kind = PredictWalletEventKind::OrderTransactionFailed {
            settlement_id: format!("failed-{index}"),
            fill: fill.clone(),
        };
        assert!(
            notices
                .update(&event, &order, account, 0, 3.into())
                .unwrap()
                .is_some()
        );
    }
    assert_eq!(notices.pending_orders().count(), 0);
    assert!(
        notices
            .update(&submitted, &order, account, 0, 4.into())
            .unwrap()
            .is_some()
    );
    notices.retire_orders(&HashSet::from([order.venue_order_id().unwrap()]));
    assert!(
        notices
            .update(&submitted, &order, account, 0, 5.into())
            .unwrap()
            .is_some()
    );
}

#[test]
fn retiring_one_owner_preserves_other_pending_notifications_and_conflicts() {
    let (order, submitted) = fixture();
    let account = order.account_id().unwrap();
    let mut notices = SettlementNotifications::default();
    let first = notices
        .update(&submitted, &order, account, 0, 2.into())
        .unwrap()
        .unwrap();
    let mut other = submitted.clone();
    other.order.order_hash = alloy::primitives::B256::repeat_byte(0x22);
    let mut other_native = OrderTestBuilder::new(OrderType::Limit)
        .client_order_id("NOTICE-OTHER".into())
        .instrument_id("42-1.PREDICT".into())
        .side(OrderSide::Buy)
        .quantity(nautilus_model::types::Quantity::from(100))
        .price(nautilus_model::types::Price::from("0.040"))
        .build();
    let factory =
        OrderEventFactory::new(other_native.trader_id(), account, AccountType::Cash, None);
    other_native
        .apply(factory.generate_order_submitted(&other_native, 1.into()))
        .unwrap();
    other_native
        .apply(factory.generate_order_accepted(
            &other_native,
            VenueOrderId::new(format!("{:#x}", other.order.order_hash)),
            2.into(),
            2.into(),
        ))
        .unwrap();
    let other_order = WorkerOrder::capture(&other_native);
    let second = notices
        .update(&other, &other_order, account, 0, 3.into())
        .unwrap()
        .unwrap();
    assert_ne!(first.trade_id, second.trade_id);
    assert!(notices.has_pending(first.venue_order_id));
    assert!(notices.has_pending(second.venue_order_id));
    notices.retire_orders(&HashSet::from([first.venue_order_id]));
    assert!(!notices.has_pending(first.venue_order_id));
    assert!(notices.has_pending(second.venue_order_id));
    assert_eq!(notices.entries.len(), 1);
    assert_eq!(notices.by_order.len(), 1);
    assert!(
        notices
            .update(&other, &other_order, account, 1, 4.into())
            .unwrap()
            .is_none()
    );
    notices.retire_orders(&HashSet::from([second.venue_order_id]));
    assert!(notices.entries.is_empty() && notices.by_order.is_empty());
}
