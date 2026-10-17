// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.

//! Existing native position reconciliation feasibility; this does not enable live share fees.
use nautilus_common::msgbus::TypedHandler;
use nautilus_live::execution::{config::ExecutionManagerConfig, manager::ExecutionManager};
use nautilus_model::{
    enums::{LiquiditySide, PositionSide},
    events::PositionEvent,
    reports::{FillReport, PositionStatusReport},
};

use super::*;

#[tokio::test]
async fn native_manager_reconciles_full_and_cancelled_partial_share_fee_positions_without_double_cash_debit()
 {
    for (index, gross) in [(1, 100), (2, 100), (1, 17), (2, 17)] {
        let gross = Decimal::from(gross);
        let fee_shares = gross * Decimal::new(2, 2);
        let net = gross - fee_shares;
        let cost = gross * Decimal::new(1, 2);
        let fee_value =
            Money::from_decimal(fee_shares * Decimal::new(1, 2), Currency::USDT()).unwrap();
        let final_cash = Money::from_decimal(Decimal::from(1000) - cost, Currency::USDT()).unwrap();
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = reported_client(&server);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(index)))
            .unwrap();
        let _portfolio = portfolio(&cache);
        connect_reported(&mut client, &mut rx).await;
        let mut original = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .instrument_id(instrument_id(index))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .price(Price::from("0.010"))
            .build();
        let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
        original
            .apply(factory.generate_order_submitted(&original, UnixNanos::from(10u64)))
            .unwrap();
        original
            .apply(factory.generate_order_accepted(
                &original,
                VenueOrderId::new(HASH),
                UnixNanos::from(100u64),
                UnixNanos::from(100u64),
            ))
            .unwrap();
        cache
            .borrow_mut()
            .register_external_order_claims(original.strategy_id(), &[original.instrument_id()])
            .unwrap();
        cache
            .borrow_mut()
            .add_order(original.clone(), None, None, false)
            .unwrap();
        let mut base_clock = VirtualClock::new();
        base_clock.advance_time(UnixNanos::from(OBSERVED), true);
        let clock = Rc::new(RefCell::new(base_clock));
        let mut engine = ExecutionEngine::new(clock.clone(), cache.clone(), None);
        let observed = Rc::new(RefCell::new(Vec::new()));
        let target = observed.clone();
        let callback_cache = cache.clone();
        let handler = TypedHandler::from(move |_: &PositionEvent| {
            let cache = callback_cache.borrow();
            let positions = cache.positions_open(
                None,
                Some(&instrument_id(index)),
                None,
                Some(&account_id()),
                None,
            );
            target.borrow_mut().push(
                positions
                    .iter()
                    .map(|p| p.quantity.as_decimal())
                    .sum::<Decimal>(),
            );
        });
        let pattern = format!("events.position.{}", original.strategy_id()).into();
        msgbus::subscribe_position_events(pattern, handler.clone(), None);
        // Quote commission is the economic equivalent, not a second cash transfer.
        let report = FillReport::new(
            account_id(),
            instrument_id(index),
            VenueOrderId::new(HASH),
            "NATIVE-GROSS-1".into(),
            OrderSide::Buy,
            Quantity::from_decimal_dp(gross, FIXED_PRECISION).unwrap(),
            Price::from("0.010"),
            fee_value,
            LiquiditySide::Taker,
            Some(original.client_order_id()),
            None,
            UnixNanos::from(1000u64),
            UnixNanos::from(OBSERVED),
            None,
        );
        engine.reconcile_execution_report(&ExecutionReport::Fill(Box::new(report)));
        assert_eq!(
            cache
                .borrow()
                .order(&original.client_order_id())
                .unwrap()
                .filled_qty()
                .as_decimal(),
            gross
        );
        if gross < Decimal::from(100) {
            let cached = cache
                .borrow()
                .order_owned(&original.client_order_id())
                .unwrap();
            engine.process(&factory.generate_order_canceled(
                &cached,
                Some(VenueOrderId::new(HASH)),
                UnixNanos::from(OBSERVED),
                UnixNanos::from(OBSERVED),
            ));
            assert_eq!(
                cache
                    .borrow()
                    .order(&original.client_order_id())
                    .unwrap()
                    .status(),
                OrderStatus::Canceled
            );
        }
        let mut account = cache.borrow().account_owned(&account_id()).unwrap();
        let AccountAny::Cash(cash) = &mut account else {
            panic!("cash")
        };
        assert!(!cash.base.calculate_account_state);
        assert_eq!(
            account.balance_total(Some(Currency::USDT())).unwrap(),
            Money::from("1000 USDT")
        );
        let state = AccountState::new(
            account_id(),
            AccountType::Cash,
            vec![AccountBalance::new(
                final_cash,
                Money::from("0 USDT"),
                final_cash,
            )],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::from(OBSERVED),
            UnixNanos::from(OBSERVED),
            None,
        );
        msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state);
        let mut venue = PositionStatusReport::new(
            account_id(),
            instrument_id(index),
            PositionSide::Long,
            Quantity::from_decimal_dp(net, FIXED_PRECISION).unwrap(),
            UnixNanos::from(OBSERVED),
            UnixNanos::from(OBSERVED),
            None,
            None,
            None,
        );
        // The core report path publishes/computes mismatch only; the live manager resolves it.
        engine.reconcile_execution_report(&ExecutionReport::Position(Box::new(venue.clone())));
        assert_eq!(
            cache.borrow().positions(None, None, None, None, None)[0]
                .quantity
                .as_decimal(),
            gross
        );
        let mut manager = ExecutionManager::new(
            clock.clone(),
            cache.clone(),
            ExecutionManagerConfig {
                // Native activity grace uses monotonic time, not VirtualClock.
                position_check_threshold_ns: nautilus_core::DurationNanos::default(),
                ..Default::default()
            },
        )
        .unwrap();
        let check = manager.prepare_position_report_check(UUID4::new(), &[&client]);
        let queried = std::iter::once(client.client_id()).collect();
        manager.record_position_activity(instrument_id(index), account_id());
        assert!(
            manager
                .reconcile_position_reports(
                    &check,
                    vec![venue.clone()],
                    &queried,
                    &Default::default()
                )
                .is_empty()
        );
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(1_000_000u64 + OBSERVED), true);
        venue.ts_last = clock.borrow().timestamp_ns();
        venue.ts_init = venue.ts_last;
        let check = manager.prepare_position_report_check(UUID4::new(), &[&client]);

        let events = manager.reconcile_position_reports(
            &check,
            vec![venue.clone()],
            &queried,
            &Default::default(),
        );
        assert!(!events.is_empty());
        for event in &events {
            engine.process(event);
        }
        let positions = cache
            .borrow()
            .positions(None, None, None, None, None)
            .iter()
            .map(|p| (**p).clone())
            .collect::<Vec<_>>();
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].strategy_id, original.strategy_id());
        assert_eq!(positions[0].quantity.as_decimal(), net);
        assert_eq!(positions[0].realized_pnl.unwrap(), -fee_value);
        assert_eq!(
            cache
                .borrow()
                .order(&original.client_order_id())
                .unwrap()
                .filled_qty()
                .as_decimal(),
            gross
        );
        assert_eq!(
            cache
                .borrow()
                .account(&account_id())
                .unwrap()
                .balance_total(Some(Currency::USDT()))
                .unwrap(),
            final_cash
        );
        assert_eq!(observed.borrow().last().copied(), Some(net));
        let repeat = manager.prepare_position_report_check(UUID4::new(), &[&client]);
        assert!(
            manager
                .reconcile_position_reports(&repeat, vec![venue], &queried, &Default::default())
                .is_empty()
        );
        msgbus::unsubscribe_position_events(pattern, &handler);
        assert!(server.state.creations.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn historical_share_fee_holding_normalizes_down_without_overstating_net_shares() {
    let settlement: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/share_fee_settlement.json"
    ))
    .unwrap();
    let raw = |key: &str| {
        settlement[key]
            .as_str()
            .unwrap()
            .parse::<alloy::primitives::U256>()
            .unwrap()
    };
    assert_eq!(
        raw("gross_shares_wei") - raw("fee_shares_wei"),
        raw("received_shares_wei")
    );
    for outcome in [1, 2] {
        let mut row = holding(outcome);
        row["amount"] = settlement["received_shares_wei"].clone();
        let server = server(vec![json!({"success":true,"data":[row]})]).await;
        let (client, mut rx, cache) = client(&server);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(outcome)))
            .unwrap();
        let command = GeneratePositionStatusReports::new(
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
            None,
            None,
            None,
        );
        let reports = client
            .generate_position_status_reports(&command)
            .await
            .unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(
            reports[0].quantity.as_decimal(),
            Decimal::from_str_exact("27.6770544615384615").unwrap()
        );
        assert!(
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );
        assert!(rx.try_recv().is_err());
        assert!(server.state.creations.lock().unwrap().is_empty());
    }
}
