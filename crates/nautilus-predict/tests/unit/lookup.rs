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
    order_with_tags(closed, None)
}

fn order_with_tags(closed: bool, tags: Option<Vec<ustr::Ustr>>) -> OrderAny {
    let mut builder = OrderTestBuilder::new(OrderType::Limit);
    if let Some(tags) = tags {
        builder.tags(tags);
    }
    let mut order = builder
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
    client
        .rest_order_ids
        .insert(hash, alloy::primitives::U256::from(12345));
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
    assert!(client.rest_order_ids.is_empty());
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
    for synthetic in [false, true] {
        let (client, _) = client();
        let mut order = order_with_tags(
            false,
            synthetic
                .then(|| vec![nautilus_execution::reconciliation::RECONCILIATION_ORDER_TAG.into()]),
        );
        let factory = OrderEventFactory::new(
            order.trader_id(),
            client.core.account_id,
            AccountType::Cash,
            None,
        );
        let mut event = factory.generate_order_filled(
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
        );
        let nautilus_model::events::OrderEventAny::Filled(fill) = &mut event else {
            unreachable!()
        };
        // REST-recovered real fills are also reconciliation events. Only Native's
        // synthetic order tag distinguishes a local position adjustment.
        fill.reconciliation = true;
        order.apply(event).unwrap();
        let hash = order.venue_order_id().unwrap();
        let fill = crate::execution::precision::BufferedFill {
            report: nautilus_model::reports::FillReport::new(
                client.core.account_id,
                order.instrument_id(),
                hash,
                "applied-retirement-fill".into(),
                order.order_side(),
                order.quantity(),
                nautilus_model::types::Price::from("0.040"),
                Money::zero(Currency::USDT()),
                LiquiditySide::Taker,
                Some(order.client_order_id()),
                None,
                UnixNanos::from(100),
                UnixNanos::from(100),
                None,
            ),
            raw: crate::execution::precision::RawFillFacts {
                settlement_id: "applied-retirement-fill".into(),
                gross_shares: crate::common::parse::decimal_to_wei(order.quantity().as_decimal())
                    .unwrap(),
                price: alloy::primitives::U256::from(40_000_000_000_000_000u64),
                fee: alloy::primitives::U256::ZERO,
                fee_type: crate::http::account_models::PredictFeeType::Collateral,
                transaction_hash: None,
            },
        };
        client
            .fill_evidence
            .lock()
            .unwrap()
            .commit(&[fill])
            .unwrap();
        assert!(
            client
                .fill_evidence
                .lock()
                .unwrap()
                .unapplied_orders(&HashSet::new())
                .contains(&hash)
        );
        let order = Arc::new(order);
        let weak = Arc::downgrade(&order);
        client
            .orders
            .insert(order.venue_order_id().unwrap(), order.clone());
        client.account_orders.insert(order.client_order_id(), order);
        client.prune_order_lookups();
        assert!(weak.upgrade().is_none());
        assert!(
            client
                .fill_evidence
                .lock()
                .unwrap()
                .unapplied_orders(&HashSet::new())
                .is_empty()
        );
        assert_eq!(
            client.account_sync.state.lock().unwrap().latest_fill,
            if synthetic {
                UnixNanos::default()
            } else {
                UnixNanos::from(100)
            }
        );
        client.prune_order_lookups();
        assert_eq!(
            client.account_sync.state.lock().unwrap().latest_fill,
            if synthetic {
                UnixNanos::default()
            } else {
                UnixNanos::from(100)
            }
        );
    }
}

#[test]
fn instrument_snapshots_follow_native_purge_and_retained_order_ownership() {
    use crate::{
        http::models::{PredictMarket, PredictResponse},
        providers::instruments_from_market,
    };
    use nautilus_model::instruments::Instrument;
    let (client, cache) = client();
    let mut response: PredictResponse<PredictMarket> =
        serde_json::from_str(include_str!("../../test_data/market_2859346.json")).unwrap();
    response.data.id = 42;
    let (_, definitions) = instruments_from_market(&response.data, 1.into()).unwrap();
    let active = Arc::new(order(false));
    client
        .account_orders
        .insert(active.client_order_id(), active);
    for instrument in &definitions {
        cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        client
            .instruments
            .insert(instrument.id(), instrument.clone());
    }
    client.prune_order_lookups();
    assert_eq!(client.instruments.len(), 2);
    // The worker owner still needs YES, even if Native has released its definition.
    cache.borrow_mut().purge_instrument(definitions[0].id());
    cache.borrow_mut().purge_instrument(definitions[1].id());
    client.prune_order_lookups();
    assert!(
        client
            .instruments
            .get_cloned(&definitions[0].id())
            .is_some()
    );
    assert!(
        client
            .instruments
            .get_cloned(&definitions[1].id())
            .is_none()
    );
    client.account_orders.store(Default::default());
    client.prune_order_lookups();
    assert!(client.instruments.is_empty());
}

#[test]
fn wallet_snapshot_cannot_mix_old_order_with_new_native_pending_markers() {
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
    let open = Arc::new(order(false));
    let hash = open.venue_order_id().unwrap();
    client.orders.insert(hash, open.clone());
    let fill = BufferedFill {
        report: FillReport::new(
            client.core.account_id,
            open.instrument_id(),
            hash,
            "pending".into(),
            nautilus_model::enums::OrderSide::Buy,
            Quantity::from(1),
            Price::from("0.040"),
            Money::zero(Currency::USDT()),
            LiquiditySide::Maker,
            Some(open.client_order_id()),
            None,
            UnixNanos::from(2),
            UnixNanos::from(2),
            None,
        ),
        raw: RawFillFacts {
            settlement_id: "pending".into(),
            gross_shares: U256::from(1_000_000_000_000_000_000u64),
            price: U256::from(40_000_000_000_000_000u64),
            fee: U256::ZERO,
            fee_type: PredictFeeType::Collateral,
            transaction_hash: None,
        },
    };
    client
        .fill_evidence
        .lock()
        .unwrap()
        .queue_wallet(&fill)
        .unwrap();
    let context = runtime::WalletReportContext {
        instruments: client.instruments.clone(),
        orders: client.orders.clone(),
        pending: client.pending_submissions.clone(),
        rest_ids: client.rest_order_ids.clone(),
        sync: client.account_sync.clone(),
        fill_evidence: client.fill_evidence.clone(),
        reconciliation_required: client.reconciliation_required.clone(),
        allow_share_fees: true,
        emitter: client.emitter.clone(),
        account: client.core.account_id,
        wallet: client.config.session.account_address(),
        clock: client.clock,
    };
    let (before, queued) = context.order_snapshot(hash).unwrap();
    assert!(!before.is_closed());
    assert_eq!(queued.len(), 1);
    // Pause the Native observer between clearing a marker and publishing the new order.
    let state = client.account_sync.state.lock().unwrap();
    client
        .fill_evidence
        .lock()
        .unwrap()
        .applied(fill.report.trade_id);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        result_tx.send(context.order_snapshot(hash)).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(
        result_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err(),
        "a torn snapshot must not escape while Native is publishing"
    );
    let mut closed = (*open).clone();
    let factory = OrderEventFactory::new(
        open.trader_id(),
        client.core.account_id,
        client.core.account_type,
        None,
    );
    closed
        .apply(factory.generate_order_canceled(
            &closed,
            Some(hash),
            UnixNanos::from(3),
            UnixNanos::from(3),
        ))
        .unwrap();
    client.orders.insert(hash, Arc::new(closed));
    drop(state);
    let (after, queued) = result_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap()
        .unwrap();
    reader.join().unwrap();
    assert!(after.is_closed());
    assert!(queued.is_empty());
}
