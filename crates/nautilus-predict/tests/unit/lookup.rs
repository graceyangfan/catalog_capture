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
            VenueOrderId::new(format!("0x{}", "11".repeat(32))),
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
    cache
        .borrow_mut()
        .add_order((*order).clone(), None, None, false)
        .unwrap();
    cache.borrow_mut().build_index();
    let order = Arc::new(super::super::context::WorkerOrder::capture(&order));
    let weak = Arc::downgrade(&order);
    client.orders.lock().unwrap().insert(hash, order.clone());
    client
        .rest_order_ids
        .lock()
        .unwrap()
        .insert(hash, alloy::primitives::U256::from(12345));
    client.account_orders.lock().unwrap().insert(id, order);
    client.account_sync.confirm_terminal(hash, Some(id));
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
    assert!(
        client.orders.lock().unwrap().is_empty()
            && client.account_orders.lock().unwrap().is_empty()
    );
    assert!(client.rest_order_ids.lock().unwrap().is_empty());
    assert!(
        client
            .account_sync
            .state
            .lock()
            .unwrap()
            .confirmed_terminal
            .is_empty()
    );
    assert!(
        weak.upgrade().is_none(),
        "no full OrderAny clone may outlive Native purge"
    );
    let revision = client.account_sync.state.lock().unwrap().revision;
    client.prune_order_lookups();
    assert_eq!(client.account_sync.state.lock().unwrap().revision, revision);
}

#[test]
fn retirement_visits_a_native_owner_once_across_both_indexes_and_keeps_hash_only_coverage() {
    use crate::execution::context::{TRADE_ID_READS, WorkerOrder};
    for in_client_index in [true, false] {
        let (client, _) = client();
        let native = order(true);
        let id = native.client_order_id();
        let hash = native.venue_order_id().unwrap();
        let view = Arc::new(WorkerOrder::capture(&native));
        client
            .orders
            .lock()
            .unwrap()
            .insert(hash, Arc::clone(&view));
        if in_client_index {
            client.account_orders.lock().unwrap().insert(id, view);
        }
        TRADE_ID_READS.with(|reads| reads.set(0));
        client.prune_order_lookups();
        assert_eq!(
            TRADE_ID_READS.with(|reads| reads.get()),
            0,
            "retirement checks existing membership instead of collecting Native IDs"
        );
        assert!(client.account_orders.lock().unwrap().is_empty());
        assert!(client.orders.lock().unwrap().is_empty());
    }
}

#[test]
fn active_snapshot_is_not_reclaimed_even_without_a_cache_owner() {
    let (client, _) = client();
    let order = Arc::new(order(false));
    client
        .orders
        .lock()
        .unwrap()
        .insert(order.venue_order_id().unwrap(), view(&order));
    client
        .account_orders
        .lock()
        .unwrap()
        .insert(order.client_order_id(), view(&order));
    client.prune_order_lookups();
    assert_eq!(client.orders.lock().unwrap().len(), 1);
    assert_eq!(client.account_orders.lock().unwrap().len(), 1);
}

#[test]
fn queued_raw_settlement_pins_its_context_and_requires_reconciliation() {
    for queued in [false, true] {
        use alloy::primitives::U256;
        use nautilus_model::{
            enums::LiquiditySide,
            reports::FillReport,
            types::{Currency, Money, Price, Quantity},
        };

        use crate::{
            execution::precision::{BufferedFill, RawFillFacts},
            http::account_models::PredictFeeType,
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
        if queued {
            client.fill_evidence.lock().unwrap().queue(&[fill]).unwrap();
        } else {
            client
                .fill_evidence
                .lock()
                .unwrap()
                .commit(&[fill])
                .unwrap();
        }
        let command = nautilus_common::messages::execution::SubmitOrder::from_order(
            &order,
            order.trader_id(),
            None,
            None,
            nautilus_core::UUID4::new(),
            UnixNanos::from(20),
        );
        client.orders.lock().unwrap().insert(hash, view(&order));
        client
            .account_orders
            .lock()
            .unwrap()
            .insert(order.client_order_id(), view(&order));
        if queued {
            assert!(client.submit_order(command).is_err());
            assert!(
                client.reconciliation_required.load(Ordering::Acquire),
                "queued facts must gate a command before the next cleanup tick"
            );
        }
        client.prune_order_lookups();
        assert_eq!(client.orders.lock().unwrap().len(), 1);
        assert_eq!(client.account_orders.lock().unwrap().len(), 1);
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
                .has_unapplied(hash, &view(&order))
        );
    }
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
        let retired_view = view(&order);
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
                .has_unapplied(hash, &view(&self::order(true)))
        );
        let order = view(&order);
        let weak = Arc::downgrade(&order);
        client
            .orders
            .lock()
            .unwrap()
            .insert(order.venue_order_id().unwrap(), order.clone());
        client
            .account_orders
            .lock()
            .unwrap()
            .insert(order.client_order_id(), order);
        client.prune_order_lookups();
        assert!(weak.upgrade().is_none());
        assert!(
            !client
                .fill_evidence
                .lock()
                .unwrap()
                .has_unapplied(hash, &retired_view)
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
    use nautilus_model::instruments::Instrument;

    use crate::{
        http::models::{PredictMarket, PredictResponse},
        providers::instruments_from_market,
    };
    let (client, cache) = client();
    let mut response: PredictResponse<PredictMarket> =
        serde_json::from_str(include_str!("../../test_data/market_2859346.json")).unwrap();
    response.data.id = 42;
    let (_, definitions) = instruments_from_market(&response.data, 1.into()).unwrap();
    let active = Arc::new(order(false));
    client
        .account_orders
        .lock()
        .unwrap()
        .insert(active.client_order_id(), view(&active));
    for instrument in &definitions {
        cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        client
            .instruments
            .insert(instrument.id(), instrument.clone().into());
    }
    let metadata_owners = definitions
        .iter()
        .map(|instrument| {
            let entry = client.instruments.get_cloned(&instrument.id()).unwrap();
            Arc::downgrade(&entry.market)
        })
        .collect::<Vec<_>>();
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
    client.account_orders.lock().unwrap().clear();
    client.prune_order_lookups();
    assert!(client.instruments.is_empty());
    assert!(
        metadata_owners.iter().all(|slot| slot.upgrade().is_none()),
        "metadata cannot outlive its instrument worker view"
    );
}

#[test]
fn wallet_snapshot_cannot_mix_old_order_with_new_native_pending_markers() {
    use alloy::primitives::U256;
    use nautilus_model::{
        enums::LiquiditySide,
        reports::FillReport,
        types::{Currency, Money, Price, Quantity},
    };

    use crate::{
        execution::precision::{BufferedFill, RawFillFacts},
        http::account_models::PredictFeeType,
    };
    let (client, _) = client();
    let open = Arc::new(order(false));
    let hash = open.venue_order_id().unwrap();
    client.orders.lock().unwrap().insert(hash, view(&open));
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
        settlement_sender: None,
        latency: None,
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
    assert_eq!(queued.pending_for(&before, hash).count(), 1);
    assert!(
        client.account_sync.state.try_lock().is_ok(),
        "normalization releases the account publication lock"
    );
    assert!(
        matches!(
            client.fill_evidence.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ),
        "the pending borrow must keep Native application from tearing the sampled view"
    );
    drop(queued);
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
        let result = context
            .order_snapshot(hash)
            .map(|(order, facts)| (order.is_closed(), facts.pending_for(&order, hash).count()));
        result_tx.send(result).unwrap();
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
    client.orders.lock().unwrap().insert(hash, view(&closed));
    drop(state);
    let (after, queued) = result_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap()
        .unwrap();
    reader.join().unwrap();
    assert!(after);
    assert_eq!(queued, 0);
}

#[test]
fn native_command_feedback_updates_views_without_redundant_resource_refresh() {
    use futures_util::FutureExt;
    use nautilus_common::msgbus::{self, MessageBus};
    use nautilus_core::UUID4;
    use nautilus_model::{
        enums::OrderStatus,
        events::{OrderEventAny, OrderPendingUpdate},
    };

    let (mut client, cache) = client();
    msgbus::set_message_bus(Rc::new(RefCell::new(MessageBus::new(
        client.core.trader_id,
        UUID4::new(),
        None,
        None,
    ))));
    let order = order(false);
    let id = order.client_order_id();
    let hash = order.venue_order_id().unwrap();
    let factory = OrderEventFactory::new(
        order.trader_id(),
        client.core.account_id,
        AccountType::Cash,
        None,
    );
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    client.prepare_lookup().unwrap();
    let pending = OrderEventAny::PendingUpdate(OrderPendingUpdate::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        id,
        order.account_id(),
        UUID4::new(),
        UnixNanos::from(3),
        UnixNanos::from(3),
        false,
        Some(hash),
    ));
    cache.borrow_mut().update_order(&pending).unwrap();
    msgbus::publish_order_event("events.order.TESTER".into(), &pending);
    assert_eq!(
        client.orders.lock().unwrap()[&hash].status(),
        OrderStatus::PendingUpdate
    );
    assert_eq!(client.account_sync.state.lock().unwrap().revision, 1);
    assert_eq!(
        client.account_sync.state.lock().unwrap().resource_revision,
        0
    );
    assert!(
        client
            .account_sync
            .notify
            .notified()
            .now_or_never()
            .is_none()
    );

    let rejection = factory.generate_order_modify_rejected(
        &cache.borrow().order(&id).unwrap(),
        Some(hash),
        "unsupported modification",
        UnixNanos::from(4),
        UnixNanos::from(4),
    );
    cache.borrow_mut().update_order(&rejection).unwrap();
    msgbus::publish_order_event("events.order.TESTER".into(), &rejection);
    assert_eq!(
        client.account_orders.lock().unwrap()[&id].status(),
        OrderStatus::Accepted
    );
    assert_eq!(
        cache.borrow().order(&id).unwrap().status(),
        OrderStatus::Accepted
    );
    assert_eq!(client.account_sync.state.lock().unwrap().revision, 2);
    assert_eq!(
        client.account_sync.state.lock().unwrap().resource_revision,
        0
    );
    assert!(
        client
            .account_sync
            .notify
            .notified()
            .now_or_never()
            .is_none()
    );

    let canceled = factory.generate_order_canceled(
        &cache.borrow().order(&id).unwrap(),
        Some(hash),
        UnixNanos::from(5),
        UnixNanos::from(5),
    );
    cache.borrow_mut().update_order(&canceled).unwrap();
    msgbus::publish_order_event("events.order.TESTER".into(), &canceled);
    assert_eq!(
        client.orders.lock().unwrap()[&hash].status(),
        OrderStatus::Canceled
    );
    assert_eq!(client.account_sync.state.lock().unwrap().revision, 3);
    assert_eq!(
        client.account_sync.state.lock().unwrap().resource_revision,
        1
    );
    assert!(
        client
            .account_sync
            .notify
            .notified()
            .now_or_never()
            .is_some()
    );
    client.clear_observer();
}

#[test]
fn instrument_replacement_and_session_setup_detach_both_outcome_metadata_owners() {
    use nautilus_model::instruments::Instrument;

    use crate::{
        http::models::{PredictMarket, PredictResponse},
        providers::instruments_from_market,
    };
    let (mut client, cache) = client();
    let mut response: PredictResponse<PredictMarket> =
        serde_json::from_str(include_str!("../../test_data/market_2859346.json")).unwrap();
    response.data.id = 42;
    let (_, definitions) = instruments_from_market(&response.data, 1.into()).unwrap();
    for definition in &definitions {
        cache
            .borrow_mut()
            .add_instrument(definition.clone())
            .unwrap();
    }
    client.prepare_lookup().unwrap();
    let original = client
        .instruments
        .get_cloned(&definitions[0].id())
        .unwrap()
        .market;
    assert!(Arc::ptr_eq(
        &original,
        &client
            .instruments
            .get_cloned(&definitions[1].id())
            .unwrap()
            .market
    ));
    client.on_instrument(definitions[1].clone());
    let replacement = client
        .instruments
        .get_cloned(&definitions[0].id())
        .unwrap()
        .market;
    assert!(!Arc::ptr_eq(&original, &replacement));
    assert!(Arc::ptr_eq(
        &replacement,
        &client
            .instruments
            .get_cloned(&definitions[1].id())
            .unwrap()
            .market
    ));
    // Old workers can still own their slot, but cannot reattach it to this view.
    client.prepare_lookup().unwrap();
    let next_session = client
        .instruments
        .get_cloned(&definitions[0].id())
        .unwrap()
        .market;
    assert!(!Arc::ptr_eq(&replacement, &next_session));
    assert!(Arc::ptr_eq(
        &next_session,
        &client
            .instruments
            .get_cloned(&definitions[1].id())
            .unwrap()
            .market
    ));
    client.clear_observer();
}

#[test]
fn native_timer_retires_purged_snapshots_without_a_trading_command() {
    use nautilus_common::clock::{Clock, VirtualClock};
    use nautilus_network::websocket::AuthTracker;
    let (mut client, cache) = client();
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    client.recovery_clock = Some(clock.clone());
    client.wallet_auth = Some(AuthTracker::new());
    client.wallet_epoch = Some(Arc::new(std::sync::atomic::AtomicU64::new(0)));
    client.healthy.store(true, Ordering::Release);
    let native = order(true);
    cache
        .borrow_mut()
        .add_order(native.clone(), None, None, false)
        .unwrap();
    let retained = view(&native);
    let weak = Arc::downgrade(&retained);
    client
        .orders
        .lock()
        .unwrap()
        .insert(retained.venue_order_id().unwrap(), retained.clone());
    client
        .account_orders
        .lock()
        .unwrap()
        .insert(retained.client_order_id(), retained.clone());
    client.start_recovery_timer().unwrap();
    let fire = |ns| {
        let events = clock.borrow_mut().advance_time(UnixNanos::from(ns), true);
        let handlers = clock.borrow().match_handlers(events);
        for handler in handlers {
            handler.run();
        }
    };
    fire(1_000_000_000);
    assert_eq!(
        client.orders.lock().unwrap().len(),
        1,
        "Native still owns this snapshot"
    );
    cache.borrow_mut().purge_order(retained.client_order_id());
    drop(retained);
    fire(31_000_000_000);
    assert!(client.orders.lock().unwrap().is_empty());
    assert!(client.account_orders.lock().unwrap().is_empty());
    assert!(weak.upgrade().is_none());
    client.stop_recovery_timer();
    assert!(clock.borrow().timer_names().is_empty());
}

fn view(order: &OrderAny) -> Arc<super::super::context::WorkerOrder> {
    Arc::new(super::super::context::WorkerOrder::capture(order))
}

#[test]
#[ignore = "explicit Native lookup sweep timing; PREDICT_CLEANUP_BENCH_PATH create_new JSON"]
fn benchmark_native_lookup_retirement() {
    use std::{hint::black_box, time::Instant};
    let mut rows = Vec::new();
    for retained in [0, 1000, 10000] {
        for native_retains in [true, false] {
            let mut samples = Vec::new();
            for iteration in 0..23 {
                let (client, cache) = client();
                seed_cleanup(&client, &cache, retained, native_retains);
                let lookups = client.order_lookups();
                let started = Instant::now();
                let more = black_box(&lookups).prune();
                let elapsed = started.elapsed().as_nanos() as u64;
                let remaining_after_first = client.account_orders.lock().unwrap().len();
                let full_started = Instant::now();
                let mut more = more;
                let mut calls = 1;
                while more {
                    more = black_box(&lookups).prune();
                    calls += 1;
                    assert!(calls < 200);
                }
                let full_elapsed = elapsed + full_started.elapsed().as_nanos() as u64;
                assert_eq!(
                    client.orders.lock().unwrap().len(),
                    if native_retains { retained } else { 0 }
                );
                assert_eq!(
                    client.account_orders.lock().unwrap().len(),
                    if native_retains { retained } else { 0 }
                );
                if iteration == 22 {
                    rows.push(serde_json::json!({"detail":true,"retained_orders":retained,"native_retains":native_retains,"remaining_after_first":remaining_after_first,"calls_to_complete":calls,"full_sweep_ns":full_elapsed}));
                }
                if iteration >= 3 {
                    samples.push(elapsed);
                }
            }
            samples.sort_unstable();
            rows.push(serde_json::json!({"retained_orders":retained,"native_retains":native_retains,"p50_ns":samples[9],"p95_ns":samples[18],"p99_ns":samples[19],"samples_ns":samples}));
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_CLEANUP_BENCH_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file,&serde_json::json!({"source":"release production Native-thread first bounded lookup batch; full sweep duration/calls reported separately; 3 warmups + 20 samples per case; no fills/instruments/remote tasks in this isolated sweep; fixture setup excluded; 20-sample p99 is maximum","rows":rows})).unwrap();
}

#[test]
fn native_purge_keeps_pending_match_evidence_until_a_terminal_settlement_fact() {
    let (client, _) = recovery_client();
    let snapshot = view(&order(true));
    let hash = snapshot.venue_order_id().unwrap();
    client.orders.lock().unwrap().insert(hash, snapshot.clone());
    client
        .account_orders
        .lock()
        .unwrap()
        .insert(snapshot.client_order_id(), snapshot.clone());
    let mut event:crate::websocket::private_messages::PredictWalletEvent=serde_json::from_value(serde_json::json!({
        "type":"orderTransactionSubmitted","kind":"PURCHASE","settlementId":"pending-retirement","orderHash":hash.to_string(),"orderId":"12345","walletAddress":"0x0000000000000000000000000000000000000022","timestamp":1,
        "details":{"marketId":42,"outcomeIndex":1,"marketQuestion":"test","outcome":"YES","quoteType":"BID","quantity":"32","quantityFilled":"0","price":"0.040","value":"1.28","valueFilled":"0","strategyType":"LIMIT","categorySlug":"test"},
        "fill":{"executedSizeWei":"1000000000000000000","executedPriceWei":"40000000000000000","executedValueWei":"40000000000000000"}
    })).unwrap();
    client
        .fill_evidence
        .lock()
        .unwrap()
        .notifications
        .update(&event, &snapshot, client.core.account_id, 0, 2.into())
        .unwrap();
    client.prune_order_lookups();
    assert_eq!(
        client.orders.lock().unwrap().len(),
        1,
        "unsettled matching facts survive Native owner purge"
    );
    assert!(
        super::super::readiness::ReadinessCheck::new(&client)
            .err()
            .unwrap()
            .to_string()
            .contains("unapplied fills for a purged")
    );
    let crate::websocket::private_messages::PredictWalletEventKind::OrderTransactionSubmitted {
        settlement_id,
        fill,
        ..
    } = event.kind
    else {
        unreachable!()
    };
    event.kind =
        crate::websocket::private_messages::PredictWalletEventKind::OrderTransactionFailed {
            settlement_id,
            fill,
        };
    client
        .fill_evidence
        .lock()
        .unwrap()
        .notifications
        .update(&event, &snapshot, client.core.account_id, 0, 3.into())
        .unwrap();
    assert!(
        super::super::readiness::ReadinessCheck::new(&client).is_ok(),
        "a confirmed failed settlement no longer pins recovery before its cleanup batch"
    );
    client.prune_order_lookups();
    assert!(client.orders.lock().unwrap().is_empty());
    assert!(client.account_orders.lock().unwrap().is_empty());
    assert_eq!(
        client
            .fill_evidence
            .lock()
            .unwrap()
            .notifications
            .pending_orders()
            .count(),
        0
    );
}

fn seed_cleanup(
    client: &PredictExecutionClient,
    cache: &Rc<RefCell<Cache>>,
    count: usize,
    retained: bool,
) {
    for index in 0..count {
        let mut native = OrderTestBuilder::new(OrderType::Limit)
            .client_order_id(format!("CLEAN-{index}").as_str().into())
            .instrument_id("42-1.PREDICT".into())
            .side(nautilus_model::enums::OrderSide::Buy)
            .quantity(nautilus_model::types::Quantity::from(32))
            .price(nautilus_model::types::Price::from("0.040"))
            .build();
        let factory = OrderEventFactory::new(
            native.trader_id(),
            client.core.account_id,
            AccountType::Cash,
            None,
        );
        let hash = VenueOrderId::new(format!("0x{:064x}", index + 1));
        native
            .apply(factory.generate_order_submitted(&native, 1.into()))
            .unwrap();
        native
            .apply(factory.generate_order_accepted(&native, hash, 2.into(), 2.into()))
            .unwrap();
        native
            .apply(factory.generate_order_canceled(&native, Some(hash), 3.into(), 3.into()))
            .unwrap();
        if retained {
            cache
                .borrow_mut()
                .add_order(native.clone(), None, None, false)
                .unwrap();
        }
        let snapshot = view(&native);
        client.orders.lock().unwrap().insert(hash, snapshot.clone());
        client
            .account_orders
            .lock()
            .unwrap()
            .insert(native.client_order_id(), snapshot);
        client
            .rest_order_ids
            .lock()
            .unwrap()
            .insert(hash, alloy::primitives::U256::from(index + 1));
    }
}

#[test]
fn cleanup_resumes_bounded_batches_and_reclaims_over_ten_thousand_lifecycles() {
    let (client, cache) = client();
    seed_cleanup(&client, &cache, 10017, false);
    let active = view(&order(false));
    let active_id = active.client_order_id();
    let active_hash = active.venue_order_id().unwrap();
    client
        .orders
        .lock()
        .unwrap()
        .insert(active_hash, active.clone());
    client
        .account_orders
        .lock()
        .unwrap()
        .insert(active_id, active);
    let lookups = client.order_lookups();
    let mut calls = 0;
    loop {
        let before = client.account_orders.lock().unwrap().len();
        let more = lookups.prune();
        let after = client.account_orders.lock().unwrap().len();
        assert!(
            before - after <= CLEANUP_BATCH * 2,
            "one callback must not drain the full indexes"
        );
        calls += 1;
        if !more {
            break;
        }
        assert!(calls < 200, "swap removal must not starve moved owners");
    }
    assert!(calls > 10017 / (CLEANUP_BATCH * 2));
    assert_eq!(client.account_orders.lock().unwrap().len(), 1);
    assert_eq!(client.orders.lock().unwrap().len(), 1);
    assert!(
        client
            .account_orders
            .lock()
            .unwrap()
            .contains_key(&active_id)
    );
    assert!(client.rest_order_ids.lock().unwrap().is_empty());
    // A retained Native owner must become collectible on a subsequent pass.
    let (retained_client, retained_cache) = self::client();
    seed_cleanup(
        &retained_client,
        &retained_cache,
        CLEANUP_BATCH * 3 + 17,
        true,
    );
    let retained_lookups = retained_client.order_lookups();
    let mut checks = 0;
    while retained_lookups.prune() {
        checks += 1;
        assert!(checks < 10);
    }
    assert_eq!(
        retained_client.orders.lock().unwrap().len(),
        CLEANUP_BATCH * 3 + 17
    );
    let ids = retained_client
        .account_orders
        .lock()
        .unwrap()
        .keys()
        .copied()
        .collect::<Vec<_>>();
    for id in ids {
        retained_cache.borrow_mut().purge_order(id);
    }
    checks = 0;
    while retained_lookups.prune() {
        checks += 1;
        assert!(checks < 20);
    }
    assert!(retained_client.orders.lock().unwrap().is_empty());
}

#[test]
fn emitted_acceptance_follows_owner_snapshots_without_fifo_eviction() {
    let mut native = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id("42-1.PREDICT".into())
        .side(nautilus_model::enums::OrderSide::Buy)
        .quantity(nautilus_model::types::Quantity::from(32))
        .price(nautilus_model::types::Price::from("0.040"))
        .build();
    let factory = OrderEventFactory::new(
        native.trader_id(),
        AccountId::from("PREDICT-lookup"),
        AccountType::Cash,
        None,
    );
    native
        .apply(factory.generate_order_submitted(&native, 1.into()))
        .unwrap();
    let target = view(&native);
    assert_eq!(target.ts_accepted(), None);
    assert_eq!(target.queued_acceptance(), None);
    target.record_acceptance(UnixNanos::from(7));
    for index in 0..10017 {
        let unrelated_native = OrderTestBuilder::new(OrderType::Limit)
            .client_order_id(format!("ACCEPTED-{index}").as_str().into())
            .instrument_id("42-1.PREDICT".into())
            .side(nautilus_model::enums::OrderSide::Buy)
            .quantity(nautilus_model::types::Quantity::from(32))
            .price(nautilus_model::types::Price::from("0.040"))
            .build();
        let unrelated = view(&unrelated_native);
        unrelated.record_acceptance(UnixNanos::from(9));
        assert_eq!(unrelated.queued_acceptance(), Some(UnixNanos::from(9)));
    }
    let event = *native.events().last().unwrap();
    let observed = super::super::context::WorkerOrder::observe(&native, event, Some(&target));
    assert_eq!(observed.queued_acceptance(), Some(UnixNanos::from(7)));
    observed.record_acceptance(UnixNanos::from(11));
    assert_eq!(target.queued_acceptance(), Some(UnixNanos::from(7)));
    assert_eq!(
        view(&native).queued_acceptance(),
        None,
        "new owner has no retained transport marker"
    );
}

#[test]
fn native_observer_keeps_emitted_acceptance_from_the_pre_ack_hash_view() {
    use nautilus_common::msgbus::MessageBus;
    use nautilus_core::UUID4;
    let (mut client, cache) = client();
    msgbus::set_message_bus(Rc::new(RefCell::new(MessageBus::new(
        client.core.trader_id,
        UUID4::new(),
        None,
        None,
    ))));
    let mut native = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id("42-1.PREDICT".into())
        .side(nautilus_model::enums::OrderSide::Buy)
        .quantity(nautilus_model::types::Quantity::from(32))
        .price(nautilus_model::types::Price::from("0.040"))
        .build();
    let factory = OrderEventFactory::new(
        native.trader_id(),
        client.core.account_id,
        AccountType::Cash,
        None,
    );
    native
        .apply(factory.generate_order_submitted(&native, 1.into()))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(native.clone(), None, None, false)
        .unwrap();
    client.prepare_lookup().unwrap();
    let hash = VenueOrderId::new(format!("0x{}", "11".repeat(32)));
    let transmitted = view(&native);
    transmitted.record_acceptance(2.into());
    client.orders.lock().unwrap().insert(hash, transmitted);
    client.pending_submissions.lock().unwrap().entries.insert(
        native.client_order_id(),
        super::super::orders::Submission {
            venue_id: Some(hash),
            wallet_epoch: Some(0),
            market_buy_collateral: None,
        },
    );
    assert_eq!(
        client.account_orders.lock().unwrap()[&native.client_order_id()].queued_acceptance(),
        None
    );
    // Re-publishing Submitted models a Native update while the Accepted is queued.
    let event = *native.events().last().unwrap();
    msgbus::publish_order_event("events.order.TESTER".into(), event);
    let refreshed = client.orders.lock().unwrap()[&hash].clone();
    assert_eq!(refreshed.ts_accepted(), None);
    assert_eq!(refreshed.queued_acceptance(), Some(2.into()));
    assert!(Arc::ptr_eq(
        &refreshed,
        &client.account_orders.lock().unwrap()[&native.client_order_id()]
    ));
    client.clear_observer();
}

#[test]
fn native_timer_continues_bounded_cleanup_on_following_ticks() {
    use nautilus_common::clock::VirtualClock;
    use nautilus_network::websocket::AuthTracker;
    let (mut client, cache) = client();
    seed_cleanup(&client, &cache, CLEANUP_BATCH * 3 + 17, false);
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    client.recovery_clock = Some(clock.clone());
    client.wallet_auth = Some(AuthTracker::new());
    client.wallet_epoch = Some(Arc::new(std::sync::atomic::AtomicU64::new(0)));
    client.healthy.store(true, Ordering::Release);
    client.start_recovery_timer().unwrap();
    for second in 1..=8 {
        let before = client.account_orders.lock().unwrap().len();
        let events = clock
            .borrow_mut()
            .advance_time(UnixNanos::from(second * 1_000_000_000), true);
        let handlers = clock.borrow().match_handlers(events);
        for handler in handlers {
            handler.run();
        }
        let after = client.account_orders.lock().unwrap().len();
        assert!(before - after <= CLEANUP_BATCH * 2);
        if second == 1 {
            assert!(after > 0, "first callback cannot drain the full sweep");
        }
    }
    assert!(
        client.account_orders.lock().unwrap().is_empty(),
        "next ticks must continue before the 30-second cleanup interval"
    );
    assert!(client.rest_order_ids.lock().unwrap().is_empty());
    client.stop_recovery_timer();
}

#[test]
fn owner_retirement_keeps_unrelated_terminal_proof() {
    let mut state = account::AccountSyncState::default();
    let retired = VenueOrderId::from("retired-hash");
    let retained = VenueOrderId::from("retained-hash");
    state.confirmed_terminal.extend([retired, retained]);
    state.retire_orders(&HashSet::from([retired]));
    assert!(!state.confirmed_terminal.contains(&retired));
    assert!(state.confirmed_terminal.contains(&retained));
    state.retire_orders(&HashSet::from([retired]));
    assert_eq!(state.confirmed_terminal.len(), 1);
}

fn recovery_client() -> (PredictExecutionClient, Rc<RefCell<Cache>>) {
    use nautilus_model::{
        accounts::cash::CashAccount,
        types::{AccountBalance, Money},
    };
    let (mut client, cache) = client();
    client.chain = Some(
        crate::blockchain::PredictChainClient::new(crate::config::PredictChainClientConfig::new(
            crate::PredictEnvironment::BnbMainnet,
            "http://127.0.0.1:1".into(),
        ))
        .unwrap(),
    );
    client.wallet_auth = Some(nautilus_network::websocket::AuthTracker::new());
    client.wallet_epoch = Some(Arc::new(std::sync::atomic::AtomicU64::new(0)));
    let factory = OrderEventFactory::new(
        client.core.trader_id,
        client.core.account_id,
        AccountType::Cash,
        None,
    );
    let state = factory.generate_account_state(
        vec![AccountBalance::new(
            Money::from("1000 USDT"),
            Money::from("0 USDT"),
            Money::from("1000 USDT"),
        )],
        vec![],
        true,
        1.into(),
        1.into(),
        None,
    );
    cache
        .borrow_mut()
        .add_account(AccountAny::Cash(CashAccount::new(state, false, false)))
        .unwrap();
    (client, cache)
}

#[test]
fn recovery_eligibility_ignores_cleanup_backlog_and_pure_retirement_keeps_revision() {
    use futures_util::FutureExt;
    let (client, cache) = recovery_client();
    seed_cleanup(&client, &cache, CLEANUP_BATCH * 3 + 17, false);
    client
        .reconciliation_required
        .store(true, Ordering::Release);
    let (revision, resource_revision) = {
        let state = client.account_sync.state.lock().unwrap();
        (state.revision, state.resource_revision)
    };
    let lookups = client.order_lookups();
    assert!(lookups.prune());
    assert!(
        !client.orders.lock().unwrap().is_empty(),
        "real unvisited backlog is required"
    );
    let _check = super::super::readiness::ReadinessCheck::new(&client).unwrap();
    while lookups.prune() {}
    let state = client.account_sync.state.lock().unwrap();
    assert_eq!(
        (state.revision, state.resource_revision),
        (revision, resource_revision)
    );
    assert!(
        client
            .account_sync
            .notify
            .notified()
            .now_or_never()
            .is_none(),
        "pure retirement cannot request another account projection"
    );
}

#[test]
fn recovery_backlog_still_blocks_real_unapplied_and_pending_matching_facts() {
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
    let (client, cache) = recovery_client();
    seed_cleanup(&client, &cache, CLEANUP_BATCH * 3 + 17, false);
    let snapshot = view(&order(true));
    let hash = snapshot.venue_order_id().unwrap();
    client.orders.lock().unwrap().insert(hash, snapshot.clone());
    client
        .account_orders
        .lock()
        .unwrap()
        .insert(snapshot.client_order_id(), snapshot.clone());
    let fill = BufferedFill {
        report: FillReport::new(
            client.core.account_id,
            snapshot.instrument_id(),
            hash,
            "unapplied-backlog".into(),
            snapshot.order_side(),
            Quantity::from(1),
            Price::from("0.040"),
            Money::zero(Currency::USDT()),
            LiquiditySide::Taker,
            Some(snapshot.client_order_id()),
            None,
            10.into(),
            10.into(),
            None,
        ),
        raw: RawFillFacts {
            settlement_id: "unapplied-backlog".into(),
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
    assert!(
        super::super::readiness::ReadinessCheck::new(&client)
            .err()
            .unwrap()
            .to_string()
            .contains("unapplied fills for a purged")
    );
    // No Native application was invented merely to drain maintenance work.
    assert!(
        client
            .fill_evidence
            .lock()
            .unwrap()
            .has_unapplied(hash, &snapshot)
    );
    while client.order_lookups().prune() {}
    assert!(client.orders.lock().unwrap().contains_key(&hash));
}

fn cold_instrument_cleanup_fixture(
    owners: usize,
    expired: usize,
) -> (PredictExecutionClient, Rc<RefCell<Cache>>) {
    use crate::{
        http::models::{PredictMarket, PredictResponse},
        providers::instruments_from_market,
    };
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide},
        events::OrderEventAny,
        instruments::InstrumentAny,
        position::Position,
        types::{Currency, Money, Price, Quantity},
    };
    let (client, cache) = client();
    seed_cleanup(&client, &cache, owners, true);
    let mut response: PredictResponse<PredictMarket> =
        serde_json::from_str(include_str!("../../test_data/market_2859346.json")).unwrap();
    response.data.id = 42;
    let (_, definitions) = instruments_from_market(&response.data, 1.into()).unwrap();
    let InstrumentAny::BinaryOption(template) = &definitions[0] else {
        unreachable!()
    };
    let mut map = ahash::AHashMap::new();
    for market in [42, 43, 44, 45].into_iter().chain(10000..10000 + expired) {
        let mut instrument = template.clone();
        instrument.id = format!("{market}-1.PREDICT").as_str().into();
        if market == 44 {
            cache
                .borrow_mut()
                .add_instrument(InstrumentAny::BinaryOption(instrument.clone()))
                .unwrap();
        }
        map.insert(
            instrument.id,
            super::super::types::ExecutionInstrument::from(InstrumentAny::BinaryOption(instrument)),
        );
    }
    let factory = OrderEventFactory::new(
        "TESTER-001".into(),
        client.core.account_id,
        AccountType::Cash,
        None,
    );
    let mut hash_only = OrderTestBuilder::new(OrderType::Limit)
        .client_order_id("HASH-ONLY".into())
        .instrument_id("45-1.PREDICT".into())
        .side(OrderSide::Buy)
        .quantity(Quantity::from(1))
        .price(Price::from("0.010"))
        .build();
    hash_only
        .apply(factory.generate_order_submitted(&hash_only, 1.into()))
        .unwrap();
    let hash = VenueOrderId::new(format!("0x{:064x}", 1000001));
    hash_only
        .apply(factory.generate_order_accepted(&hash_only, hash, 2.into(), 2.into()))
        .unwrap();
    client.orders.lock().unwrap().insert(hash, view(&hash_only));
    let opening = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id("43-1.PREDICT".into())
        .side(OrderSide::Buy)
        .quantity(Quantity::from(1))
        .price(Price::from("0.010"))
        .build();
    let OrderEventAny::Filled(fill) = factory.generate_order_filled(
        &opening,
        hash,
        Some("POSITION-ONLY".into()),
        "POSITION-TRADE".into(),
        Quantity::from(1),
        Price::from("0.010"),
        Currency::USDT(),
        Some(Money::from("0 USDT")),
        LiquiditySide::Maker,
        1.into(),
        1.into(),
    ) else {
        unreachable!()
    };
    let position = Position::new(&map[&"43-1.PREDICT".into()].instrument, fill);
    cache
        .borrow_mut()
        .add_position_without_order(&position, OmsType::Netting)
        .unwrap();
    client.instruments.store(map);
    (client, cache)
}

#[test]
fn cold_instrument_cleanup_visits_owners_once_and_preserves_all_native_ownership_paths() {
    use nautilus_model::identifiers::InstrumentId;
    let (client, cache) = cold_instrument_cleanup_fixture(1000, 100);
    INSTRUMENT_OWNER_READS.with(|n| n.set(0));
    client.order_lookups().prune();
    assert_eq!(INSTRUMENT_OWNER_READS.with(|n| n.get()), 2001);
    let expected =
        [42, 43, 44, 45].map(|id| format!("{id}-1.PREDICT").parse::<InstrumentId>().unwrap());
    let actual = client.instruments.load();
    assert_eq!(actual.len(), 4);
    assert!(expected.iter().all(|id| actual.contains_key(id)));
    assert_eq!(client.account_orders.lock().unwrap().len(), 1000);
    assert_eq!(client.orders.lock().unwrap().len(), 1001);
    assert_eq!(
        cache
            .borrow()
            .positions_open(None, None, None, None, None)
            .len(),
        1
    );
    for id in expected {
        cache
            .borrow_mut()
            .add_instrument(actual[&id].instrument.clone())
            .unwrap();
    }
    drop(actual);
    INSTRUMENT_OWNER_READS.with(|n| n.set(0));
    client.order_lookups().prune();
    assert_eq!(
        INSTRUMENT_OWNER_READS.with(|n| n.get()),
        0,
        "normal retained definitions must not trigger an owner sweep"
    );
    assert_eq!(
        client.account_sync.state.lock().unwrap().resource_revision,
        0
    );
}

#[test]
#[ignore = "explicit cold instrument cleanup timing; PREDICT_INSTRUMENT_CLEANUP_PATH create_new JSON"]
fn benchmark_cold_instrument_cleanup() {
    use std::{hint::black_box, time::Instant};
    let mut rows = Vec::new();
    for owners in [1000, 10000] {
        for expired in [10, 100] {
            let mut samples = Vec::new();
            for iteration in 0..13 {
                let (client, cache) = cold_instrument_cleanup_fixture(owners, expired);
                let lookup = client.order_lookups();
                let start = Instant::now();
                black_box(&lookup).prune();
                let ns = start.elapsed().as_nanos() as u64;
                assert_eq!(client.instruments.load().len(), 4);
                assert_eq!(client.account_orders.lock().unwrap().len(), owners);
                assert_eq!(
                    cache
                        .borrow()
                        .positions_open(None, None, None, None, None)
                        .len(),
                    1
                );
                if iteration >= 3 {
                    samples.push(ns);
                }
            }
            samples.sort_unstable();
            rows.push(serde_json::json!({"owners":owners,"expired_unowned":expired,"samples_ns":samples,"p50_ns":samples[4],"p95_ns":samples[9],"p99_ns":samples[9]}));
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_INSTRUMENT_CLEANUP_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file,&serde_json::json!({"source":"production Native-thread prune including cold instrument ownership; retained Native orders, hash-only owner and position-only owner preserved; setup/drop excluded; 3 warmups and 10 samples; p95/p99 maximum","rows":rows})).unwrap();
}

fn history_cleanup_fixture(
    owners: usize,
    fills: usize,
) -> (PredictExecutionClient, Rc<RefCell<Cache>>) {
    history_retention_fixture(owners, fills, true, 0)
}

fn history_retention_fixture(
    owners: usize,
    fills: usize,
    purge: bool,
    close_step_ns: u64,
) -> (PredictExecutionClient, Rc<RefCell<Cache>>) {
    use crate::execution::{
        context::WorkerOrder,
        precision::{BufferedFill, RawFillFacts},
    };
    use alloy::primitives::U256;
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide},
        reports::FillReport,
        types::{Currency, Money, Price, Quantity},
    };
    let (client, cache) = client();
    let factory = OrderEventFactory::new(
        client.core.trader_id,
        client.core.account_id,
        AccountType::Cash,
        None,
    );
    for owner in 0..owners {
        let hash = VenueOrderId::new(format!("0x{:064x}", owner + 1));
        let mut order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(client.core.trader_id)
            .client_order_id(format!("HISTORY-{owner}").as_str().into())
            .instrument_id("42-1.PREDICT".into())
            .side(OrderSide::Buy)
            .quantity(Quantity::from((fills + 1).max(20000) as u64))
            .price(Price::from("0.040"))
            .build();
        order
            .apply(factory.generate_order_submitted(&order, 1.into()))
            .unwrap();
        order
            .apply(factory.generate_order_accepted(&order, hash, 2.into(), 2.into()))
            .unwrap();
        let mut reports = Vec::new();
        for index in 0..fills {
            let trade = format!("H-{owner}-{index}").as_str().into();
            order
                .apply(factory.generate_order_filled(
                    &order,
                    hash,
                    None,
                    trade,
                    Quantity::from(1),
                    Price::from("0.040"),
                    Currency::USDT(),
                    Some(Money::zero(Currency::USDT())),
                    LiquiditySide::Taker,
                    (index as u64 + 3).into(),
                    (index as u64 + 3).into(),
                ))
                .unwrap();
            reports.push(BufferedFill {
                report: FillReport::new(
                    client.core.account_id,
                    order.instrument_id(),
                    hash,
                    trade,
                    OrderSide::Buy,
                    Quantity::from(1),
                    Price::from("0.040"),
                    Money::zero(Currency::USDT()),
                    LiquiditySide::Taker,
                    Some(order.client_order_id()),
                    None,
                    (index as u64 + 3).into(),
                    (index as u64 + 3).into(),
                    None,
                ),
                raw: RawFillFacts {
                    settlement_id: format!("settlement-{owner}-{index}"),
                    gross_shares: U256::from(1_000_000_000_000_000_000u64),
                    price: U256::from(40_000_000_000_000_000u64),
                    fee: U256::ZERO,
                    fee_type: crate::http::account_models::PredictFeeType::Collateral,
                    transaction_hash: None,
                },
            });
        }
        order
            .apply(factory.generate_order_canceled(
                &order,
                Some(hash),
                ((owner as u64 + 1) * close_step_ns + fills as u64 + 4).into(),
                ((owner as u64 + 1) * close_step_ns + fills as u64 + 4).into(),
            ))
            .unwrap();
        let snapshot = Arc::new(WorkerOrder::capture(&order));
        client
            .account_orders
            .lock()
            .unwrap()
            .insert(order.client_order_id(), Arc::clone(&snapshot));
        client.orders.lock().unwrap().insert(hash, snapshot);
        client
            .fill_evidence
            .lock()
            .unwrap()
            .commit(&reports)
            .unwrap();
        client
            .account_sync
            .confirm_terminal(hash, Some(order.client_order_id()));
        cache
            .borrow_mut()
            .add_order(order, None, None, false)
            .unwrap();
    }
    cache.borrow_mut().build_index();
    if purge {
        cache
            .borrow_mut()
            .purge_closed_orders(1_000_000_000_000u64.into(), 0);
    }
    client.account_sync.state.lock().unwrap().latest_fill = (fills as u64 + 2).into();
    (client, cache)
}

#[test]
fn large_histories_retire_one_owner_per_callback_and_preserve_remaining_facts() {
    use crate::execution::context::APPLIED_MEMBERSHIP_READS;
    let (client, _cache) = history_cleanup_fixture(4, 1000);
    let revision = client.account_sync.state.lock().unwrap().resource_revision;
    for remaining in (0..4).rev() {
        APPLIED_MEMBERSHIP_READS.with(|n| n.set(0));
        client.prune_order_lookups();
        assert_eq!(
            client.orders.lock().unwrap().len(),
            remaining,
            "one oversized history per Native callback"
        );
        assert_eq!(client.account_orders.lock().unwrap().len(), remaining);
        assert_eq!(
            APPLIED_MEMBERSHIP_READS.with(|n| n.get()),
            if remaining == 3 { 4 } else { 0 },
            "cold owners are checked once; deferred owners reuse their verified cursor"
        );
        let orders = client.orders.lock().unwrap();
        let mut evidence = client.fill_evidence.lock().unwrap();
        for (owner, order) in orders.iter() {
            assert_eq!(evidence.retirement_work(*owner), 1000);
            assert!(!evidence.has_unapplied(*owner, order));
        }
        assert_eq!(
            client.account_sync.state.lock().unwrap().resource_revision,
            revision
        );
    }
    assert!(!client.reconciliation_required.load(Ordering::Acquire));
}

#[test]
#[ignore = "explicit history cleanup bench; PREDICT_HISTORY_CLEANUP_PATH is create_new JSON"]
fn benchmark_large_history_cleanup() {
    use crate::execution::context::APPLIED_MEMBERSHIP_READS;
    use std::time::Instant;
    let mut rows = Vec::new();
    for owners in [1, 4] {
        for fills in [1000, 9999] {
            let mut callbacks = Vec::new();
            for iteration in 0..13 {
                let (client, _cache) = history_cleanup_fixture(owners, fills);
                let mut per_run = Vec::new();
                while !client.orders.lock().unwrap().is_empty() {
                    let before = client.orders.lock().unwrap().len();
                    APPLIED_MEMBERSHIP_READS.with(|n| n.set(0));
                    let started = Instant::now();
                    client.prune_order_lookups();
                    let ns = started.elapsed().as_nanos() as u64;
                    let after = client.orders.lock().unwrap().len();
                    assert!(after < before);
                    per_run.push(serde_json::json!({"ns":ns,"retired_owners":before-after,"membership_reads":APPLIED_MEMBERSHIP_READS.with(|n|n.get())}));
                }
                if iteration >= 3 {
                    callbacks.push(per_run);
                }
            }
            let mut worst = callbacks
                .iter()
                .map(|run| run.iter().map(|r| r["ns"].as_u64().unwrap()).max().unwrap())
                .collect::<Vec<_>>();
            worst.sort_unstable();
            rows.push(serde_json::json!({"owners":owners,"fills_per_owner":fills,"callbacks":callbacks,"worst_callback_p50_ns":worst[4],"worst_callback_p95_ns":worst[9],"worst_callback_p99_ns":worst[9]}));
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_HISTORY_CLEANUP_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file,&serde_json::json!({"source":"complete production adapter prune after Native owners already purged; context and raw evidence released; fixture seeding/Native purge/drop excluded; 3 warmups + 10 sweeps; worst callback per sweep, p95/p99 maximum; no notifications seeded", "rows":rows})).unwrap();
}

#[test]
fn retirement_budget_does_not_defer_the_unapplied_evidence_gate() {
    use crate::execution::precision::{BufferedFill, RawFillFacts};
    use alloy::primitives::U256;
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide},
        reports::FillReport,
        types::{Currency, Money, Price, Quantity},
    };
    let (client, _cache) = history_cleanup_fixture(2, 1000);
    let (owner, order) = client
        .orders
        .lock()
        .unwrap()
        .get_index(1)
        .map(|(h, o)| (*h, Arc::clone(o)))
        .unwrap();
    let fill = BufferedFill {
        report: FillReport::new(
            client.core.account_id,
            order.instrument_id(),
            owner,
            "NOT-APPLIED".into(),
            OrderSide::Buy,
            Quantity::from(1),
            Price::from("0.040"),
            Money::zero(Currency::USDT()),
            LiquiditySide::Taker,
            Some(order.client_order_id()),
            None,
            2000.into(),
            2000.into(),
            None,
        ),
        raw: RawFillFacts {
            settlement_id: "pending-after-purge".into(),
            gross_shares: U256::from(1_000_000_000_000_000_000u64),
            price: U256::from(40_000_000_000_000_000u64),
            fee: U256::ZERO,
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
    client.prune_order_lookups();
    assert!(
        client.reconciliation_required.load(Ordering::Acquire),
        "retirement work budget must not delay gating unapplied venue facts"
    );
    assert_eq!(client.orders.lock().unwrap().len(), 1);
    assert!(client.orders.lock().unwrap().contains_key(&owner));
    assert_eq!(
        client.fill_evidence.lock().unwrap().retirement_work(owner),
        1001
    );
}

#[test]
fn retired_native_history_is_released_after_shared_index_guards() {
    use crate::execution::context::HISTORY_DROP_PROBE;
    let (client, _cache) = history_cleanup_fixture(1, 1000);
    let journal = Rc::new(RefCell::new(Vec::new()));
    let seen = Rc::clone(&journal);
    let clients = Arc::clone(&client.account_orders);
    let hashes = Arc::clone(&client.orders);
    let sync = Arc::clone(&client.account_sync);
    HISTORY_DROP_PROBE.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            seen.borrow_mut().push((
                clients.try_lock().is_ok(),
                hashes.try_lock().is_ok(),
                sync.state.try_lock().is_ok(),
            ));
        }))
    });
    client.prune_order_lookups();
    HISTORY_DROP_PROBE.with(|slot| slot.borrow_mut().take());
    assert_eq!(
        *journal.borrow(),
        vec![(true, true, true)],
        "actual last Native history destructor must run outside shared index locks"
    );
    assert!(client.orders.lock().unwrap().is_empty());
}

#[test]
fn failed_notification_only_owners_share_the_retirement_work_budget() {
    let (client, _cache) = history_cleanup_fixture(2, 0);
    let owners = client
        .orders
        .lock()
        .unwrap()
        .iter()
        .map(|(hash, order)| (*hash, Arc::clone(order)))
        .collect::<Vec<_>>();
    for (owner, order) in &owners {
        for index in 0..1000 {
            let event=serde_json::from_value(serde_json::json!({"type":"orderTransactionFailed","settlementId":format!("FAILED-{index}"),"orderHash":owner.as_str(),"orderId":"12345","walletAddress":"0x0000000000000000000000000000000000000022","timestamp":1,"details":{"marketId":42,"outcomeIndex":1,"marketQuestion":"test","outcome":"YES","quoteType":"BID","quantity":"20000","quantityFilled":"0","price":"0.040","value":"800","valueFilled":"0","strategyType":"LIMIT","categorySlug":"test"},"fill":{"executedPriceWei":"40000000000000000","executedSizeWei":"1000000000000000000","executedValueWei":"40000000000000000"}})).unwrap();
            client
                .fill_evidence
                .lock()
                .unwrap()
                .notifications
                .update(&event, order, client.core.account_id, 0, 1_000_000.into())
                .unwrap();
        }
        assert_eq!(
            client.fill_evidence.lock().unwrap().retirement_work(*owner),
            1000
        );
    }
    drop(owners);
    client.prune_order_lookups();
    assert_eq!(
        client.orders.lock().unwrap().len(),
        1,
        "notice-only histories must also consume the retirement budget"
    );
    let owner = *client.orders.lock().unwrap().keys().next().unwrap();
    assert_eq!(
        client.fill_evidence.lock().unwrap().retirement_work(owner),
        1000
    );
    assert!(!client.reconciliation_required.load(Ordering::Acquire));
    client.prune_order_lookups();
    assert!(client.orders.lock().unwrap().is_empty());
    assert_eq!(
        client.fill_evidence.lock().unwrap().retirement_work(owner),
        0
    );
}

#[test]
fn report_requests_leave_scheduled_retirement_to_the_native_timer() {
    use nautilus_common::{clock::VirtualClock, messages::execution::GenerateFillReports};
    use nautilus_network::websocket::AuthTracker;
    let (mut client, cache) = client();
    seed_cleanup(&client, &cache, CLEANUP_BATCH * 2, false);
    let retained = client.account_orders.lock().unwrap().len();
    assert!(retained > CLEANUP_BATCH);
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    client.recovery_clock = Some(clock.clone());
    client.wallet_auth = Some(AuthTracker::new());
    client.wallet_epoch = Some(Arc::new(std::sync::atomic::AtomicU64::new(0)));
    client.healthy.store(true, Ordering::Release);
    client.start_recovery_timer().unwrap();
    let cmd = GenerateFillReports::new(
        nautilus_core::UUID4::new(),
        1.into(),
        None,
        None,
        None,
        None,
        None,
        None,
    );
    drop(client.generate_fill_reports_task(&cmd));
    assert_eq!(
        client.account_orders.lock().unwrap().len(),
        retained,
        "a report must not release history already owned by periodic maintenance"
    );
    let events = clock
        .borrow_mut()
        .advance_time(UnixNanos::from(1_000_000_000), true);
    for handler in clock.borrow().match_handlers(events) {
        handler.run();
    }
    assert!(client.account_orders.lock().unwrap().len() < retained);
    let remaining = client.account_orders.lock().unwrap().len();
    assert!(remaining > 0);
    client.stop_recovery_timer();
    drop(client.generate_fill_reports_task(&cmd));
    assert!(
        client.account_orders.lock().unwrap().len() < remaining,
        "a client without an installed timer retains manual cleanup"
    );
}

#[test]
#[ignore = "explicit Native purge plus adapter retirement bench; PREDICT_NATIVE_PURGE_PATH create_new JSON"]
fn benchmark_native_purge_and_adapter_retirement() {
    use std::time::Instant;
    let mut rows = Vec::new();
    for (owners, fills) in [(1, 9999), (1000, 4), (10000, 1)] {
        let mut samples = Vec::new();
        for i in 0..13 {
            let (client, cache) = history_retention_fixture(owners, fills, false, 0);
            assert_eq!(
                cache
                    .borrow()
                    .orders_total_count(None, None, None, None, None),
                owners
            );
            let start = Instant::now();
            cache
                .borrow_mut()
                .purge_closed_orders(1_000_000_000_000u64.into(), 0);
            let native_purge_ns = start.elapsed().as_nanos() as u64;
            assert_eq!(
                cache
                    .borrow()
                    .orders_total_count(None, None, None, None, None),
                0
            );
            let mut callbacks = Vec::new();
            while !client.account_orders.lock().unwrap().is_empty() {
                let start = Instant::now();
                client.order_lookups().prune();
                callbacks.push(start.elapsed().as_nanos() as u64);
            }
            assert!(client.orders.lock().unwrap().is_empty());
            if i >= 3 {
                samples.push(serde_json::json!({"native_purge_ns":native_purge_ns,"adapter_callbacks_ns":callbacks}));
            }
        }
        rows.push(serde_json::json!({"owners":owners,"fills_per_owner":fills,"samples":samples}));
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_NATIVE_PURGE_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file,&serde_json::json!({"source":"real Native Cache purge followed by production adapter retirement; 3 warmups + 10 samples, no IO; setup outside timing; complete sweep CPU separately from worst callback; not a simultaneous strategy callback measurement","rows":rows})).unwrap();
}

#[test]
#[ignore = "explicit steady Native purge config comparison; PREDICT_STEADY_PURGE_PATH create_new JSON"]
fn benchmark_steady_native_purge_cadence() {
    use std::time::Instant;
    let mut rows = Vec::new();
    let orders_per_minute = 100usize;
    let retained = 60 * orders_per_minute;
    let close_step_ns = 600_000_000u64;
    for interval in [1usize, 15] {
        let eligible = interval * orders_per_minute;
        let owners = retained + eligible;
        let mut samples = Vec::new();
        for iteration in 0..13 {
            let (client, cache) = history_retention_fixture(owners, 1, false, close_step_ns);
            let now = owners as u64 * close_step_ns + 5;
            let start = Instant::now();
            cache.borrow_mut().purge_closed_orders(now.into(), 3600);
            let native_ns = start.elapsed().as_nanos() as u64;
            assert_eq!(
                cache
                    .borrow()
                    .orders_total_count(None, None, None, None, None),
                retained
            );
            let mut callbacks = Vec::new();
            while client.account_orders.lock().unwrap().len() > retained {
                let start = Instant::now();
                client.order_lookups().prune();
                callbacks.push(start.elapsed().as_nanos() as u64);
            }
            assert_eq!(client.orders.lock().unwrap().len(), retained);
            assert!(!client.reconciliation_required.load(Ordering::Acquire));
            if iteration >= 3 {
                samples.push(serde_json::json!({"native_purge_ns":native_ns,"adapter_callbacks_ns":callbacks}));
            }
        }
        rows.push(serde_json::json!({"interval_mins":interval,"buffer_mins":60,"orders_per_minute":orders_per_minute,"retained_orders":retained,"eligible_orders":eligible,"samples":samples}));
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_STEADY_PURGE_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file,&serde_json::json!({"source":"official Native purge API, modeled steady close times at an explicit input rate, fixed 60-minute retention; 3 warmups + 10 samples; fixture setup outside timing; not measured live traffic or simultaneous strategy callbacks","rows":rows})).unwrap();
}

#[test]
#[ignore = "explicit retirement CPU/lock-phase capture; PREDICT_RETIREMENT_PHASE_PATH create_new JSON"]
fn benchmark_verified_owner_retirement_phases() {
    use crate::execution::context::APPLIED_MEMBERSHIP_READS;
    use std::time::Instant;
    let mut rows = Vec::new();
    for fills in [1000, 9999] {
        for verified in [false, true] {
            let mut samples = Vec::new();
            for iteration in 0..23 {
                let (client, _cache) = history_cleanup_fixture(1, fills);
                let (hash, order) = client
                    .orders
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(id, order)| (*id, Arc::clone(order)))
                    .next()
                    .unwrap();
                if verified {
                    assert!(
                        client
                            .fill_evidence
                            .lock()
                            .unwrap()
                            .confirmed_raw(&order, order.venue_order_id().unwrap())
                            .unwrap()
                            .is_some()
                    );
                }
                APPLIED_MEMBERSHIP_READS.with(|n| n.set(0));
                let probe = Instant::now();
                for _ in 0..100 {
                    assert!(
                        !client
                            .fill_evidence
                            .lock()
                            .unwrap()
                            .has_unapplied(hash, &order)
                    );
                }
                let membership_ns = probe.elapsed().as_nanos() as u64 / 100;
                let membership_reads = APPLIED_MEMBERSHIP_READS.with(|n| n.get()) / 100;
                drop(order);
                let start = Instant::now();
                client.prune_order_lookups();
                let complete_prune_ns = start.elapsed().as_nanos() as u64;
                assert!(client.orders.lock().unwrap().is_empty());
                assert!(!client.reconciliation_required.load(Ordering::Acquire));

                // Isolate raw evidence retirement with the same retained facts.
                // Final code may return detached values: their drop is timed separately.
                let (client, _cache) = history_cleanup_fixture(1, fills);
                let hashes = client.orders.lock().unwrap().keys().copied().collect();
                let start = Instant::now();
                let mut evidence = client.fill_evidence.lock().unwrap();
                #[allow(clippy::let_unit_value)]
                let retired = evidence.retire_orders(&hashes);
                drop(evidence);
                let evidence_guard_ns = start.elapsed().as_nanos() as u64;
                let start = Instant::now();
                #[allow(clippy::unit_arg, dropping_copy_types)]
                drop(retired);
                let detached_drop_ns = start.elapsed().as_nanos() as u64;
                if iteration >= 3 {
                    samples.push(serde_json::json!({"membership_ns":membership_ns,"membership_reads":membership_reads,"complete_prune_ns":complete_prune_ns,"evidence_guard_ns":evidence_guard_ns,"detached_drop_ns":detached_drop_ns}));
                }
            }
            rows.push(
                serde_json::json!({"fills":fills,"verified_prefix":verified,"samples":samples}),
            );
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_RETIREMENT_PHASE_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file,&serde_json::json!({"source":"production retirement; verified prefix is prepared outside timing, cold cases retain full validation; 3 warmups + 20 samples; membership averages 100 checks; complete prune includes all drops; evidence phase excludes only returned-value drop when available; no venue IO","rows":rows})).unwrap();
}

#[test]
fn verified_prefix_retains_a_purged_owner_until_pending_matching_is_resolved() {
    let (client, _cache) = history_cleanup_fixture(1, 1000);
    let (owner, order) = client
        .orders
        .lock()
        .unwrap()
        .iter()
        .map(|(id, order)| (*id, Arc::clone(order)))
        .next()
        .unwrap();
    assert!(
        client
            .fill_evidence
            .lock()
            .unwrap()
            .confirmed_raw(&order, order.venue_order_id().unwrap())
            .unwrap()
            .is_some()
    );
    let mut event: crate::websocket::private_messages::PredictWalletEvent = serde_json::from_value(serde_json::json!({"type":"orderTransactionSubmitted","kind":"PURCHASE","settlementId":"PENDING-AFTER-CANCEL","orderHash":owner.as_str(),"orderId":"12345","walletAddress":"0x0000000000000000000000000000000000000022","timestamp":1,"details":{"marketId":42,"outcomeIndex":1,"marketQuestion":"test","outcome":"YES","quoteType":"BID","quantity":"20000","quantityFilled":"1000","price":"0.040","value":"800","valueFilled":"40","strategyType":"LIMIT","categorySlug":"test"},"fill":{"executedPriceWei":"40000000000000000","executedSizeWei":"1000000000000000000","executedValueWei":"40000000000000000"}})).unwrap();
    client
        .fill_evidence
        .lock()
        .unwrap()
        .notifications
        .update(&event, &order, client.core.account_id, 0, 1_000_000.into())
        .unwrap();
    client.prune_order_lookups();
    assert!(client.orders.lock().unwrap().contains_key(&owner));
    assert!(client.reconciliation_required.load(Ordering::Acquire));
    let crate::websocket::private_messages::PredictWalletEventKind::OrderTransactionSubmitted {
        settlement_id,
        fill,
        ..
    } = event.kind
    else {
        unreachable!()
    };
    event.kind =
        crate::websocket::private_messages::PredictWalletEventKind::OrderTransactionFailed {
            settlement_id,
            fill,
        };
    client
        .fill_evidence
        .lock()
        .unwrap()
        .notifications
        .update(&event, &order, client.core.account_id, 0, 1_000_001.into())
        .unwrap();
    drop(order);
    client.prune_order_lookups();
    assert!(client.orders.lock().unwrap().is_empty());
    assert_eq!(
        client.fill_evidence.lock().unwrap().retirement_work(owner),
        0
    );
}

#[test]
#[ignore = "explicit Native position purge history bench; PREDICT_POSITION_PURGE_PATH create_new JSON"]
fn benchmark_native_position_purge_history() {
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide},
        events::OrderEventAny,
        position::Position,
        types::{Currency, Money, Price, Quantity},
    };
    use std::time::Instant;
    let mut market: crate::http::models::PredictResponse<crate::http::models::PredictMarket> =
        serde_json::from_str(include_str!("../../test_data/market_2859346.json")).unwrap();
    market.data.id = 42;
    let (_, instruments) =
        crate::providers::instruments_from_market(&market.data, 1.into()).unwrap();
    let instrument = &instruments[0];
    let mut rows = Vec::new();
    for fills in [1000, 9999] {
        for closed in [false, true] {
            let mut samples = Vec::new();
            for iteration in 0..13 {
                let owner = OrderTestBuilder::new(OrderType::Limit)
                    .instrument_id("42-1.PREDICT".into())
                    .side(OrderSide::Buy)
                    .quantity(Quantity::from(20000))
                    .price(Price::from("0.040"))
                    .build();
                let factory = OrderEventFactory::new(
                    owner.trader_id(),
                    AccountId::from("PREDICT-position"),
                    AccountType::Cash,
                    None,
                );
                let position_id = "POSITION-HISTORY".into();
                let mut position: Option<Position> = None;
                for index in 0..fills {
                    let event = factory.generate_order_filled(
                        &owner,
                        "BUY-HISTORY".into(),
                        Some(position_id),
                        format!("POSITION-{index}").as_str().into(),
                        Quantity::from(1),
                        Price::from("0.040"),
                        Currency::USDT(),
                        Some(Money::zero(Currency::USDT())),
                        LiquiditySide::Maker,
                        (index as u64 + 1).into(),
                        (index as u64 + 1).into(),
                    );
                    let OrderEventAny::Filled(fill) = event else {
                        unreachable!()
                    };
                    if let Some(position) = &mut position {
                        position.apply(&fill);
                    } else {
                        position = Some(Position::new(instrument, fill));
                    }
                }
                let mut position = position.unwrap();
                if closed {
                    let mut fill = position.events[0].clone();
                    fill.client_order_id = "CLOSE-HISTORY".into();
                    fill.trade_id = "POSITION-CLOSE".into();
                    fill.order_side = OrderSide::Sell;
                    fill.last_qty = Quantity::from(fills as u64);
                    fill.ts_event = (fills as u64 + 1).into();
                    fill.ts_init = fill.ts_event;
                    position.apply(&fill);
                }
                assert_eq!(position.is_closed(), closed);
                let ids = position.client_order_ids();
                let mut cache = Cache::default();
                cache.add_instrument(instrument.clone()).unwrap();
                cache.add_position(&position, OmsType::Netting).unwrap();
                for id in &ids {
                    cache
                        .add_position_id(
                            &position_id,
                            &position.instrument_id.venue,
                            id,
                            &position.strategy_id,
                        )
                        .unwrap();
                    assert_eq!(cache.position_id(id), Some(&position_id));
                }
                drop(position);
                let start = Instant::now();
                cache.purge_position(position_id);
                let elapsed = start.elapsed().as_nanos() as u64;
                assert_eq!(cache.position_exists(&position_id), !closed);
                if closed {
                    assert!(ids.iter().all(|id| cache.position_id(id).is_none()));
                } else {
                    assert_eq!(cache.position(&position_id).unwrap().events.len(), fills);
                }
                if iteration >= 3 {
                    samples.push(elapsed);
                }
            }
            rows.push(serde_json::json!({"fills":fills,"closed":closed,"purge_ns":samples}));
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_POSITION_PURGE_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file,&serde_json::json!({"source":"official Native purge_position API; history construction/Cache insertion excluded; 3 warmups + 10 samples; closed cases include last cache-owned history destruction; open guard must retain all events; no venue IO","rows":rows})).unwrap();
}

#[test]
#[ignore = "explicit timer/cold evidence sweep; PREDICT_TIMER_EVIDENCE_PATH create_new JSON"]
fn benchmark_native_timer_evidence_sweeps() {
    use nautilus_common::clock::VirtualClock;
    use nautilus_network::websocket::AuthTracker;
    use std::time::Instant;
    let mut rows = Vec::new();
    for (owners, fills) in [(1, 100000), (32, 9999)] {
        for warm in [false, true] {
            let mut sweeps = Vec::new();
            for iteration in 0..5 {
                let (mut client, cache) = history_cleanup_fixture(owners, fills);
                if warm {
                    let orders = client.orders.lock().unwrap();
                    let mut facts = client.fill_evidence.lock().unwrap();
                    for order in orders.values() {
                        assert!(
                            facts
                                .confirmed_raw(order.as_ref(), order.venue_order_id().unwrap())
                                .unwrap()
                                .is_some()
                        );
                    }
                }
                let clock = Rc::new(RefCell::new(VirtualClock::new()));
                client.recovery_clock = Some(clock.clone());
                let auth = AuthTracker::new();
                auth.succeed();
                client.wallet_auth = Some(auth);
                client.wallet_epoch = Some(Arc::new(std::sync::atomic::AtomicU64::new(0)));
                client.healthy.store(true, Ordering::Release);
                client.start_recovery_timer().unwrap();
                let mut callbacks = Vec::new();
                for second in 1..=(owners as u64 * 4 + 4) {
                    let before = client.orders.lock().unwrap().len();
                    let events = clock
                        .borrow_mut()
                        .advance_time((second * 1_000_000_000).into(), true);
                    let handlers = clock.borrow().match_handlers(events);
                    let started = Instant::now();
                    for handler in handlers {
                        handler.run();
                    }
                    callbacks.push(started.elapsed().as_nanos() as u64);
                    let after = client.orders.lock().unwrap().len();
                    assert!(
                        before - after <= 1,
                        "one large owner per existing timer tick"
                    );
                    if after == 0 {
                        break;
                    }
                }
                assert!(client.orders.lock().unwrap().is_empty());
                assert!(client.account_orders.lock().unwrap().is_empty());
                assert_eq!(
                    client
                        .fill_evidence
                        .lock()
                        .unwrap()
                        .retirement_work(VenueOrderId::new(format!("0x{:064x}", 1))),
                    0
                );
                assert!(!client.reconciliation_required.load(Ordering::Acquire));
                client.stop_recovery_timer();
                if iteration > 0 {
                    sweeps.push(callbacks);
                }
                drop(cache);
            }
            rows.push(serde_json::json!({"owners":owners,"fills":fills,"verified_prefix":warm,"timer_callback_ns":sweeps}));
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_TIMER_EVIDENCE_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file, &serde_json::json!({"source":"actual installed Native recovery timer, VirtualClock dispatch; Native owners already purged; real raw facts and strings retired; one warmup + four complete sweeps; no WS/strategy callback claim", "rows":rows})).unwrap();
}

#[test]
fn cold_membership_does_not_hold_account_or_order_registry_locks() {
    let (client, _cache) = history_cleanup_fixture(1, 1000);
    let order = client
        .orders
        .lock()
        .unwrap()
        .values()
        .next()
        .unwrap()
        .clone();
    let trade = order.trade_ids()[0];
    let sync = client.account_sync.clone();
    let by_client = client.account_orders.clone();
    let by_hash = client.orders.clone();
    let facts = client.fill_evidence.clone();
    let (ready, waiting) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        order.with_applied_fill(trade, |fill| {
            assert!(fill.is_some());
            ready.send(()).unwrap();
            let started = std::time::Instant::now();
            while facts.try_lock().is_ok() {
                assert!(
                    started.elapsed() < std::time::Duration::from_secs(5),
                    "prune must reach cold evidence validation"
                );
                std::thread::yield_now();
            }
            assert!(
                sync.state.try_lock().is_ok(),
                "membership must not hold account sync"
            );
            assert!(
                by_client.try_lock().is_ok(),
                "membership must not hold client index"
            );
            assert!(
                by_hash.try_lock().is_ok(),
                "membership must not hold hash index"
            );
        });
    });
    waiting
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    client.prune_order_lookups();
    worker.join().unwrap();
    assert!(client.orders.lock().unwrap().is_empty());
    assert!(!client.reconciliation_required.load(Ordering::Acquire));
}

#[test]
fn changed_worker_view_between_proof_and_retirement_is_retained() {
    let (client, _cache) = history_cleanup_fixture(1, 1000);
    let (hash, order) = client
        .orders
        .lock()
        .unwrap()
        .iter()
        .next()
        .map(|(id, order)| (*id, order.clone()))
        .unwrap();
    let trade = order.trade_ids()[0];
    let replacement = Arc::new((*order).clone());
    let by_client = client.account_orders.clone();
    let by_hash = client.orders.clone();
    let facts = client.fill_evidence.clone();
    let (ready, waiting) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        order.with_applied_fill(trade, |_| {
            ready.send(()).unwrap();
            let started = std::time::Instant::now();
            while facts.try_lock().is_ok() {
                assert!(started.elapsed() < std::time::Duration::from_secs(5));
                std::thread::yield_now();
            }
            by_client
                .lock()
                .unwrap()
                .insert(order.client_order_id(), replacement.clone());
            by_hash.lock().unwrap().insert(hash, replacement);
        });
    });
    waiting
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    client.prune_order_lookups();
    worker.join().unwrap();
    assert!(client.orders.lock().unwrap().contains_key(&hash));
    assert_eq!(
        client.fill_evidence.lock().unwrap().retirement_work(hash),
        1000
    );
    client.prune_order_lookups();
    assert!(client.orders.lock().unwrap().is_empty());
}
