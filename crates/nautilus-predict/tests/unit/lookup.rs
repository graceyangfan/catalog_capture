// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.
//! Worker snapshots follow Native retention; no independent expiry or FIFO.
use std::{cell::RefCell, rc::Rc};

use nautilus_common::{cache::Cache, factories::OrderEventFactory};
use nautilus_core::UnixNanos;
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{AccountType, OmsType, OrderType},
    identifiers::{AccountId, ClientId, TraderId, VenueOrderId},
    orders::{OrderAny, builder::OrderTestBuilder},
};

use super::*;
use crate::{
    common::{PREDICT_VENUE, credential::PredictAuthToken},
    config::{
        PredictExecutionClientConfig, PredictPrivateClientConfig, PredictWalletWebSocketConfig,
    },
    http::PredictPrivateHttpClient,
};

fn client() -> (PredictExecutionClient, Rc<RefCell<Cache>>) {
    let session = PredictPrivateHttpClient::new(PredictPrivateClientConfig {
        api_key: Some("offline".into()),
        ..Default::default()
    })
    .unwrap()
    .authenticated(
        PredictAuthToken::new("offline".into()).unwrap(),
        "0x0000000000000000000000000000000000000022"
            .parse()
            .unwrap(),
    )
    .unwrap();
    let account_id = AccountId::from("PREDICT-lookup");
    let cache = Rc::new(RefCell::new(Cache::default()));
    let client = PredictExecutionClient::new(
        ExecutionClientCore::new(
            TraderId::from("TESTER-001"),
            ClientId::from("PREDICT"),
            *PREDICT_VENUE,
            OmsType::Netting,
            account_id,
            AccountType::Cash,
            None,
            cache.clone(),
        ),
        PredictExecutionClientConfig::new(
            account_id,
            session,
            PredictWalletWebSocketConfig {
                api_key: Some("offline".into()),
                ..Default::default()
            },
        ),
    )
    .unwrap();
    (client, cache)
}

fn order(closed: bool) -> OrderAny {
    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id("42-1.PREDICT".into())
        .side(nautilus_model::enums::OrderSide::Buy)
        .quantity(nautilus_model::types::Quantity::from(32))
        .price(nautilus_model::types::Price::from("0.040"))
        .build();
    let factory = OrderEventFactory::new(
        order.trader_id(),
        AccountId::from("PREDICT-lookup"),
        AccountType::Cash,
        None,
    );
    order
        .apply(factory.generate_order_submitted(&order, UnixNanos::from(1)))
        .unwrap();
    order
        .apply(factory.generate_order_accepted(
            &order,
            VenueOrderId::from("0x1111"),
            UnixNanos::from(2),
            UnixNanos::from(2),
        ))
        .unwrap();
    if closed {
        order
            .apply(factory.generate_order_canceled(
                &order,
                order.venue_order_id(),
                UnixNanos::from(3),
                UnixNanos::from(3),
            ))
            .unwrap();
    }
    order
}

#[test]
fn native_buffer_and_purge_own_full_snapshot_retention() {
    let (client, cache) = client();
    let order = Arc::new(order(true));
    let id = order.client_order_id();
    let hash = order.venue_order_id().unwrap();
    let weak = Arc::downgrade(&order);
    cache
        .borrow_mut()
        .add_order((*order).clone(), None, None, false)
        .unwrap();
    cache.borrow_mut().build_index();
    client.orders.insert(hash, order.clone());
    client.account_orders.insert(id, order);
    client.prune_order_lookups();
    assert!(
        weak.upgrade().is_some(),
        "terminal alone must not retire context"
    );
    cache
        .borrow_mut()
        .purge_closed_orders(UnixNanos::from(1_000_000_000), 2);
    client.prune_order_lookups();
    assert!(weak.upgrade().is_some(), "Native buffer has not elapsed");
    cache
        .borrow_mut()
        .purge_closed_orders(UnixNanos::from(3_000_000_000), 2);
    assert!(client.core.cache().order(&id).is_none());
    client.prune_order_lookups();
    assert!(client.orders.is_empty() && client.account_orders.is_empty());
    assert!(
        weak.upgrade().is_none(),
        "no full OrderAny clone may outlive Native purge"
    );
    let revision = client.account_sync.state.lock().unwrap().revision;
    client.prune_order_lookups();
    assert_eq!(client.account_sync.state.lock().unwrap().revision, revision);
}

#[test]
fn active_snapshot_is_not_reclaimed_even_without_a_cache_owner() {
    let (client, _) = client();
    let order = Arc::new(order(false));
    client
        .orders
        .insert(order.venue_order_id().unwrap(), order.clone());
    client.account_orders.insert(order.client_order_id(), order);
    client.prune_order_lookups();
    assert_eq!(client.orders.len(), 1);
    assert_eq!(client.account_orders.len(), 1);
}

#[test]
fn queued_raw_settlement_pins_its_context_and_requires_reconciliation() {
    use crate::{
        execution::precision::{BufferedFill, RawFillFacts},
        http::account_models::PredictFeeType,
    };
    use alloy::primitives::U256;
    use nautilus_model::{
        enums::LiquiditySide,
        reports::FillReport,
        types::{Currency, Money, Price, Quantity},
    };

    let (client, _) = client();
    let order = Arc::new(order(true));
    let hash = order.venue_order_id().unwrap();
    let mut report = FillReport::new(
        client.core.account_id,
        order.instrument_id(),
        hash,
        "queued-retirement-fill".into(),
        order.order_side(),
        Quantity::from(1),
        Price::from("0.040"),
        Money::zero(Currency::USDT()),
        LiquiditySide::Taker,
        Some(order.client_order_id()),
        None,
        UnixNanos::from(10),
        UnixNanos::from(10),
        None,
    );
    if cfg!(feature = "native-quantity-commission") {
        report.quantity_commission = Some(rust_decimal::Decimal::new(2, 2));
    } else {
        report.commission = Money::from("0.0008 USDT");
    }
    let fill = BufferedFill {
        report,
        raw: RawFillFacts {
            settlement_id: "queued-retirement-fill".into(),
            gross_shares: U256::from(1_000_000_000_000_000_000u64),
            price: U256::from(40_000_000_000_000_000u64),
            fee: U256::from(20_000_000_000_000_000u64),
            fee_type: PredictFeeType::Shares,
            transaction_hash: None,
        },
    };
    client
        .fill_evidence
        .lock()
        .unwrap()
        .commit(&[fill])
        .unwrap();
    client.orders.insert(hash, order.clone());
    client.account_orders.insert(order.client_order_id(), order);
    client.prune_order_lookups();
    assert_eq!(client.orders.len(), 1);
    assert_eq!(client.account_orders.len(), 1);
    assert!(client.reconciliation_required.load(Ordering::Acquire));
    assert!(
        crate::execution::readiness::ReadinessCheck::new(&client)
            .err()
            .unwrap()
            .to_string()
            .contains("unapplied fills for a purged native order")
    );
    assert!(
        client
            .fill_evidence
            .lock()
            .unwrap()
            .unapplied_orders(&HashSet::new())
            .contains(&hash)
    );
}

#[test]
fn retiring_filled_snapshot_preserves_chain_freshness_watermark() {
    use nautilus_model::{
        enums::LiquiditySide,
        types::{Currency, Money},
    };
    let (client, _) = client();
    let mut order = order(false);
    let factory = OrderEventFactory::new(
        order.trader_id(),
        client.core.account_id,
        AccountType::Cash,
        None,
    );
    order
        .apply(factory.generate_order_filled(
            &order,
            order.venue_order_id().unwrap(),
            None,
            "applied-retirement-fill".into(),
            order.quantity(),
            order.price().unwrap(),
            Currency::USDT(),
            Some(Money::zero(Currency::USDT())),
            LiquiditySide::Taker,
            UnixNanos::from(100),
            UnixNanos::from(100),
        ))
        .unwrap();
    let order = Arc::new(order);
    let weak = Arc::downgrade(&order);
    client
        .orders
        .insert(order.venue_order_id().unwrap(), order.clone());
    client.account_orders.insert(order.client_order_id(), order);
    client.prune_order_lookups();
    assert!(weak.upgrade().is_none());
    assert_eq!(
        client.account_sync.state.lock().unwrap().latest_fill,
        UnixNanos::from(100)
    );
    client.prune_order_lookups();
    assert_eq!(
        client.account_sync.state.lock().unwrap().latest_fill,
        UnixNanos::from(100)
    );
}
