// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Integer-wei partial fills crossing a native quantum, REST/WS replay and net reconciliation.
use nautilus_live::execution::{config::ExecutionManagerConfig, manager::ExecutionManager};

use super::*;

fn wallet_fill(outcome: u8, raw: U256, settlement: &str) -> Value {
    let mut event = fill(outcome);
    event["settlementId"] = json!(settlement);
    event["isMaker"] = json!(false);
    event["fee"] =
        json!({"type":"SHARES","amountWei":(raw*U256::from(2)/U256::from(100)).to_string()});
    event["fill"] = json!({"executedSizeWei":raw.to_string(),"executedPriceWei":"130000000000000000",
        "executedValueWei":(raw*U256::from(130_000_000_000_000_000u64)/U256::from(1_000_000_000_000_000_000u64)).to_string()});
    event
}

async fn next_fill(
    rx: &mut mpsc::UnboundedReceiver<ExecutionEvent>,
) -> nautilus_model::reports::FillReport {
    loop {
        match next(rx).await {
            ExecutionEvent::Report(ExecutionReport::Fill(report)) => return *report,
            ExecutionEvent::Account(state) => {
                msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state)
            }
            other => panic!("unexpected replay event {other:?}"),
        }
    }
}

#[tokio::test]
async fn wallet_atomic_partial_fills_carry_to_full_quantity_and_rest_replays_preserve_allocations()
{
    for (outcome, full) in [(1, true), (2, true), (1, false), (2, false)] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = reported_client(&server);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(outcome)))
            .unwrap();
        let original = order(outcome);
        cache
            .borrow_mut()
            .add_order(original.clone(), None, None, false)
            .unwrap();
        cache
            .borrow_mut()
            .add_venue_order_id(&original.client_order_id(), &HASH.into(), false)
            .unwrap();
        cache
            .borrow_mut()
            .register_external_order_claims(original.strategy_id(), &[instrument_id(outcome)])
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let mut base = VirtualClock::new();
        base.advance_time(UnixNanos::from(OBSERVED), true);
        let clock = Rc::new(RefCell::new(base));
        let mut engine = ExecutionEngine::new(clock.clone(), cache.clone(), None);
        // Native live fees require exact gross execution at the order arithmetic scale.
        // Keep the historical projection fixture immutable; use exact synthetic inputs here.
        let vector = json!({
            "gross_shares_wei": ["17000000000000000000", if full { "83000000000000000000" } else { "5000000000000000000" }],
            "fee_shares_wei": ["340000000000000000", if full { "1660000000000000000" } else { "100000000000000000" }],
            "executed_value_wei": ["2210000000000000000", if full { "10790000000000000000" } else { "650000000000000000" }],
            "native_gross_quantity": if full { "100" } else { "22" },
            "native_net_report_quantity": if full { "98" } else { "21.56" },
            "net_shares_wei": if full { "98000000000000000000" } else { "21560000000000000000" },
            "reported_balance_usdt": if full { "987" } else { "997.14" },
            "realized_pnl_usdt": if full { "-0.26" } else { "-0.0572" },
            "commission_usdt": ["0.0442", if full { "0.2158" } else { "0.013" }]
        });
        let first = vector["gross_shares_wei"][0]
            .as_str()
            .unwrap()
            .parse::<U256>()
            .unwrap();
        let second = vector["gross_shares_wei"][1]
            .as_str()
            .unwrap()
            .parse::<U256>()
            .unwrap();
        let raw_total = first + second;
        let expected_gross = Quantity::from(vector["native_gross_quantity"].as_str().unwrap());
        let expected_net = Quantity::from(vector["native_net_report_quantity"].as_str().unwrap());
        let money = |key: &str| {
            Money::from_decimal(
                Decimal::from_str_exact(vector[key].as_str().unwrap()).unwrap(),
                Currency::USDT(),
            )
            .unwrap()
        };
        // Both fee paths include incurred inventory cost in quote realized PnL.
        let expected_pnl = money("realized_pnl_usdt");
        let one = wallet_fill(outcome, first, "precision-1");
        let two = wallet_fill(outcome, second, "precision-2");
        for (index, event) in [&one, &two].into_iter().enumerate() {
            assert_eq!(event["fee"]["amountWei"], vector["fee_shares_wei"][index]);
            assert_eq!(
                event["fill"]["executedValueWei"],
                vector["executed_value_wei"][index]
            );
        }
        let px = U256::from(130_000_000_000_000_000u64);
        let scale = U256::from(1_000_000_000_000_000_000u64);
        let cash = U256::from(1_000_000_000_000_000_000_000u128)
            - first * px / scale
            - second * px / scale;
        let cash_decimal = Decimal::from_i128_with_scale(cash.to::<u128>() as i128, 18);
        // The retained fixture describes the former currency-scale floor. Native now
        // preserves every representable digit while raw wei remains in account info.
        assert_eq!(
            cash_decimal.round_dp_with_strategy(8, rust_decimal::RoundingStrategy::ToZero),
            money("reported_balance_usdt").as_decimal()
        );
        let expected_cash = Money::from_decimal_exact(
            cash_decimal.round_dp_with_strategy(
                u32::from(FIXED_PRECISION),
                rust_decimal::RoundingStrategy::ToZero,
            ),
            Currency::USDT(),
        )
        .unwrap();
        *server.state.rpc_frame.lock().unwrap() = (cash.to::<u128>(), 2, 2);
        // Queue both without applying the first to Cache, then replay both before application.
        server.wallet(one.clone());
        server.wallet(one.clone());
        server.wallet(two.clone());
        server.wallet(two.clone());
        let report1 = next_fill(&mut rx).await;
        let report2 = next_fill(&mut rx).await;
        assert_eq!(report1.last_qty, Quantity::from(17));
        assert_eq!(
            report2.last_qty,
            if full {
                Quantity::from(83)
            } else {
                Quantity::from(5)
            }
        );
        {
            assert!(report1.commission.is_zero() && report2.commission.is_zero());
            assert_eq!(
                report1.quantity_commission.map(|fee| fee.as_decimal()),
                Some(
                    Decimal::from_str_exact(&(first * U256::from(2) / U256::from(100)).to_string())
                        .unwrap()
                        / Decimal::from(1_000_000_000_000_000_000u64)
                )
            );
        }
        for report in [&report1, &report2] {
            engine.reconcile_execution_report(&ExecutionReport::Fill(Box::new(report.clone())));
        }
        assert_eq!(
            cache
                .borrow()
                .order(&original.client_order_id())
                .unwrap()
                .status(),
            if full {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            }
        );
        assert_eq!(
            cache
                .borrow()
                .order(&original.client_order_id())
                .unwrap()
                .filled_qty(),
            expected_gross
        );
        // Native allocations survive disconnect; the new generation reconstructs raw facts from REST.
        client.disconnect().await.unwrap();
        connect_reported(&mut client, &mut rx).await;
        let mut rows = vec![];
        for (raw, settlement, tx) in [(second, "precision-2", "44"), (first, "precision-1", "33")] {
            let mut row = matches::match_row(outcome);
            row["taker"]["amount"] = json!(raw.to_string());
            row["taker"]["price"] = json!("130000000000000000");
            row["taker"]["fee"] =
                json!({"type":"SHARES","amount":(raw*U256::from(2)/U256::from(100)).to_string()});
            row["settlementId"] = json!(settlement);
            row["transactionHash"] = json!(format!("0x{}", tx.repeat(32)));
            rows.push(row.clone());
            rows.push(row);
        }
        for _ in 0..2 {
            server.state.match_pages.lock().unwrap().push_back((
                200,
                json!({"success":true,"data":rows,"cursor":null}),
                0,
            ));
        }
        for _ in 0..2 {
            let reports = client
                .generate_fill_reports(GenerateFillReports::new(
                    UUID4::new(),
                    UnixNanos::default(),
                    Some(instrument_id(outcome)),
                    None,
                    None,
                    None,
                    None,
                    None,
                ))
                .await
                .unwrap();
            assert_eq!(reports.len(), 2);
            for report in reports {
                engine.reconcile_execution_report(&ExecutionReport::Fill(Box::new(report)));
            }
        }
        // Replaying the same wallet events after REST/cache application is also harmless.
        server.wallet(one);
        server.wallet(two);
        let mut barrier = event(
            if full {
                "orderAccepted"
            } else {
                "orderCancelled"
            },
            outcome,
        );
        barrier["details"]["quantityFilled"] =
            json!(Decimal::from_i128_with_scale(raw_total.to::<u128>() as i128, 18).to_string());
        let (control_hash, control) = super::wallet_lifecycle::control(&mut engine, &barrier);
        server.wallet(barrier);
        server.wallet(control);
        let mut own_canceled = false;
        loop {
            match next(&mut rx).await {
                ExecutionEvent::Order(
                    native @ nautilus_model::events::OrderEventAny::Canceled(_),
                ) => {
                    let is_control = native.venue_order_id() == Some(control_hash);
                    assert!(is_control || !full && native.venue_order_id() == Some(HASH.into()));
                    engine.process(&native);
                    if is_control {
                        break;
                    }
                    own_canceled = true;
                }
                ExecutionEvent::Account(state) => msgbus::send_account_state(
                    MessagingSwitchboard::portfolio_update_account(),
                    &state,
                ),
                other => panic!("duplicate was not suppressed: {other:?}"),
            }
        }
        assert_eq!(own_canceled, !full);
        assert_eq!(
            cache
                .borrow()
                .order(&original.client_order_id())
                .unwrap()
                .filled_qty(),
            expected_gross
        );
        assert!(client.is_connected());
        let fees =
            (first * U256::from(2) / U256::from(100)) + (second * U256::from(2) / U256::from(100));
        let mut row = holding(outcome);
        row["amount"] = json!((first + second - fees).to_string());
        server
            .state
            .pages
            .lock()
            .unwrap()
            .push_back(json!({"success":true,"data":[row]}));
        let reports = client
            .generate_position_status_reports(&GeneratePositionStatusReports::new(
                UUID4::new(),
                UnixNanos::default(),
                Some(instrument_id(outcome)),
                None,
                None,
                None,
                None,
            ))
            .await
            .unwrap();
        assert_eq!(reports[0].quantity, expected_net);
        let mut manager = ExecutionManager::new(
            clock,
            cache.clone(),
            ExecutionManagerConfig {
                position_check_threshold_ns: nautilus_core::DurationNanos::default(),
                ..Default::default()
            },
        )
        .unwrap();
        let check = manager.prepare_position_report_check(UUID4::new(), &[&client]);
        let queried = std::iter::once(client.client_id()).collect();
        let events =
            manager.reconcile_position_reports(&check, reports, &queried, &Default::default());
        assert!(events.is_empty());
        for event in events {
            engine.process(&event);
        }
        let cached_net = cache.borrow().positions(None, None, None, None, None)[0]
            .quantity
            .as_decimal();
        assert!(
            (cached_net - expected_net.as_decimal()).abs()
                <= client.position_reconciliation_tolerance()
        );
        // Register the public client tolerance through the existing native checker and prove
        // this f64 residual causes no second synthetic order/position update.
        let mut repeated = holding(outcome);
        repeated["amount"] = json!((first + second - fees).to_string());
        server
            .state
            .pages
            .lock()
            .unwrap()
            .push_back(json!({"success":true,"data":[repeated]}));
        assert!(
            manager
                .check_positions_consistency(&[&client])
                .await
                .is_empty()
        );
        assert_eq!(
            cache.borrow().positions(None, None, None, None, None)[0]
                .realized_pnl
                .unwrap(),
            expected_pnl
        );
        client
            .query_account(QueryAccount::new(
                trader_id(),
                None,
                account_id(),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ))
            .unwrap();
        loop {
            let state = apply_account(&mut rx).await;
            if state.balances[0].locked.is_zero() && state.balances[0].total == expected_cash {
                break;
            }
        }
        assert_eq!(
            cache
                .borrow()
                .account(&account_id())
                .unwrap()
                .balance_total(Some(Currency::USDT()))
                .unwrap(),
            expected_cash
        );
        assert!(server.state.creations.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn projected_legacy_fill_cannot_be_reinterpreted_as_atomic_share_fee_history() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let original = order(1);
    cache
        .borrow_mut()
        .add_order(original.clone(), None, None, false)
        .unwrap();
    cache
        .borrow_mut()
        .add_venue_order_id(&original.client_order_id(), &HASH.into(), false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let mut engine = ExecutionEngine::new(
        Rc::new(RefCell::new(VirtualClock::new())),
        cache.clone(),
        None,
    );
    let report = nautilus_model::reports::FillReport::new(
        account_id(),
        instrument_id(1),
        HASH.into(),
        settlement_trade_id("prefix-1", HASH.parse().unwrap()),
        OrderSide::Buy,
        Quantity::from(17),
        Price::from("0.130"),
        Money::from("0.0442 USDT"),
        nautilus_model::enums::LiquiditySide::Taker,
        Some(original.client_order_id()),
        None,
        UnixNanos::from(1_000_000u64),
        UnixNanos::from(OBSERVED),
        None,
    );
    // Native restored event has quantity/commission, but no raw wei prefix in this generation.
    engine.reconcile_execution_report(&ExecutionReport::Fill(Box::new(report)));
    let first = U256::from(17_000_000_000_000_000_099u128);
    let second = U256::from(100_000_000_000_000_000_000u128) - first;
    let mut rows = vec![];
    for (raw, id) in [(second, "prefix-2"), (first, "prefix-1")] {
        let mut row = matches::match_row(1);
        row["settlementId"] = json!(id);
        row["taker"]["amount"] = json!(raw.to_string());
        row["taker"]["price"] = json!("130000000000000000");
        row["taker"]["fee"] =
            json!({"type":"SHARES","amount":(raw*U256::from(2)/U256::from(100)).to_string()});
        rows.push(row);
    }
    server.state.match_pages.lock().unwrap().push_back((
        200,
        json!({"success":true,"data":rows,"cursor":null}),
        0,
    ));
    let reports = client
        .generate_fill_reports(GenerateFillReports::new(
            UUID4::new(),
            UnixNanos::default(),
            Some(instrument_id(1)),
            None,
            None,
            None,
            None,
            None,
        ))
        .await;
    {
        // An old persisted quote-valued fee cannot be silently reinterpreted as quantity fee.
        assert!(
            reports
                .unwrap_err()
                .to_string()
                .contains("contradicts an already applied native fill")
        );
        assert_eq!(
            cache
                .borrow()
                .order(&original.client_order_id())
                .unwrap()
                .filled_qty(),
            Quantity::from(17)
        );
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn wallet_replay_rejects_one_wei_fee_conflicts_even_when_money_rounding_is_equal() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let original = order(1);
    cache
        .borrow_mut()
        .add_order(original.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let raw = U256::from(17_000_000_000_000_000_000u128);
    let event = wallet_fill(1, raw, "raw-conflict");
    server.wallet(event.clone());
    let report = next_fill(&mut rx).await;
    assert_eq!(report.last_qty, Quantity::from(17));
    let mut bad = event;
    bad["fee"]["amountWei"] =
        json!((raw * U256::from(2) / U256::from(100) + U256::from(1)).to_string());
    server.wallet(bad);
    super::wallet_processed(&server).await;
    while let Ok(event) = rx.try_recv() {
        assert!(matches!(event, ExecutionEvent::Account(_)));
    }
    // No cache application was requested; both the valid queued fill and invalid replay
    // leave the native order untouched until the consumer applies the valid report.
    assert!(
        cache
            .borrow()
            .order(&original.client_order_id())
            .unwrap()
            .filled_qty()
            .is_zero()
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn wallet_atomic_fee_reaches_native_net_inventory_without_rounding_or_double_charge() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let original = order(1);
    cache
        .borrow_mut()
        .add_order(original.clone(), None, None, false)
        .unwrap();
    cache
        .borrow_mut()
        .add_venue_order_id(&original.client_order_id(), &HASH.into(), false)
        .unwrap();
    cache
        .borrow_mut()
        .register_external_order_claims(original.strategy_id(), &[instrument_id(1)])
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let mut engine = ExecutionEngine::new(
        Rc::new(RefCell::new(VirtualClock::new())),
        cache.clone(),
        None,
    );
    let mut event = wallet_fill(
        1,
        U256::from(17_000_000_000_000_000_000u128),
        "exact-atomic-fee",
    );
    event["fee"]["amountWei"] = json!("340000000000000001");
    server.wallet(event.clone());
    let report = next_fill(&mut rx).await;
    assert_eq!(report.last_qty.as_decimal(), Decimal::from(17));
    assert_eq!(
        report.quantity_commission.unwrap().as_decimal(),
        Decimal::from_str_exact("0.340000000000000001").unwrap()
    );
    assert!(report.commission.is_zero());
    let execution = ExecutionReport::Fill(Box::new(report));
    engine.reconcile_execution_report(&execution);
    engine.reconcile_execution_report(&execution);
    {
        let cache_ref = cache.borrow();
        let positions = cache_ref.positions(None, Some(&instrument_id(1)), None, None, None);
        assert_eq!(positions.len(), 1);
        assert_eq!(
            positions[0].quantity.as_decimal(),
            Decimal::from_str_exact("16.659999999999999999").unwrap()
        );
        assert_eq!(positions[0].events.len(), 1);
        assert_eq!(
            positions[0].cost_open,
            Decimal::from_str_exact("2.16579999999999999987").unwrap()
        );
        assert_eq!(
            cache_ref
                .order(&original.client_order_id())
                .unwrap()
                .filled_qty()
                .as_decimal(),
            Decimal::from(17)
        );
        assert_eq!(
            cache_ref
                .account(&account_id())
                .unwrap()
                .balance_total(Some(Currency::USDT()))
                .unwrap(),
            Money::from("1000 USDT")
        );
    }
    client.disconnect().await.unwrap();
}
