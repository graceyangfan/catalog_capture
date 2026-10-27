// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Native ingestion contracts shared by live wallet facts and REST recovery.

use nautilus_common::msgbus::TypedHandler;
use nautilus_model::{
    events::{OrderEventAny, PositionEvent},
    reports::FillReport,
};

use super::*;

#[test]
fn native_order_atomic_gross_preserves_original_fill_facts() {
    use alloy::primitives::U256;

    let mut own = order(1);
    let gross = Quantity::from_u256(U256::from(17_000_000_000_000_000_001u128), 18).unwrap();
    let mut event = native_fee_paths::fill(
        &instrument(1).into(),
        1,
        OrderSide::Buy,
        "17",
        "0.620",
        Money::zero(Currency::USDT()),
    );
    event.client_order_id = own.client_order_id();
    event.strategy_id = own.strategy_id();
    event.venue_order_id = own.venue_order_id().unwrap();
    event.last_qty = gross;
    event.quantity_commission =
        Some(Quantity::from_u256(U256::from(340_000_000_000_000_001u64), 18).unwrap());
    own.apply(OrderEventAny::Filled(event.clone())).unwrap();
    assert_eq!(own.filled_qty(), gross);
    assert_eq!(own.leaves_qty(), Quantity::from("82.999999999999999999"));
    assert_eq!(own.quantity(), Quantity::from(100));
    assert!(
        matches!(own.events().last().unwrap(), OrderEventAny::Filled(applied) if applied.last_qty == event.last_qty && applied.quantity_commission == event.quantity_commission)
    );
    let before = serde_json::to_value(&own).unwrap();
    assert!(own.apply(OrderEventAny::Filled(event)).is_err());
    assert_eq!(serde_json::to_value(&own).unwrap(), before);
}

fn wallet(settlement: &str, hash: &str, fee: &str) -> Value {
    let mut wire = fill(1);
    wire["orderHash"] = json!(hash);
    wire["settlementId"] = json!(settlement);
    wire["isMaker"] = json!(fee == "0");
    wire["fee"] = json!({"type":"SHARES","amountWei":fee});
    wire
}

async fn wallet_reports(
    server: &Server,
    rx: &mut mpsc::UnboundedReceiver<ExecutionEvent>,
    engine: &mut ExecutionEngine,
    reference: &Value,
) -> Vec<FillReport> {
    // A separate non-economic lifecycle event proves every earlier WS packet was consumed.
    let (hash, control) = wallet_lifecycle::control(engine, reference);
    server.wallet(control);
    let mut reports = Vec::new();
    loop {
        match next(rx).await {
            ExecutionEvent::Report(ExecutionReport::Fill(report)) => reports.push(*report),
            ExecutionEvent::Account(state) => {
                msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state)
            }
            ExecutionEvent::Order(event @ OrderEventAny::Canceled(_))
                if event.venue_order_id() == Some(hash) =>
            {
                engine.process(&event);
                return reports;
            }
            event => panic!("unexpected event before wallet barrier: {event:?}"),
        }
    }
}

async fn rest_report(
    server: &Server,
    client: &PredictExecutionClient,
    raw_gross: &str,
) -> FillReport {
    let mut row = super::matches::match_row(1);
    row["settlementId"] = json!("ingress-shared");
    row["taker"]["amount"] = json!(raw_gross);
    row["taker"]["fee"] = json!({"type":"SHARES","amount":"340000000000000001"});
    server.state.match_pages.lock().unwrap().push_back((
        200,
        json!({"success":true,"data":[row.clone(),row],"cursor":null}),
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
        .await
        .unwrap();
    assert_eq!(
        reports.len(),
        1,
        "overlapping REST rows describe one execution"
    );
    reports.into_iter().next().unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn wallet_and_rest_permutations_apply_one_fill_and_publish_one_net_position() {
    for raw_gross in ["17000000000000000000", "17000000000000000099"] {
        for rest_first in [false, true] {
            for apply_first_before_other_source in [false, true] {
                let server = server(vec![]).await;
                let (mut client, mut rx, cache) = reported_client(&server);
                let _portfolio = portfolio(&cache);
                cache
                    .borrow_mut()
                    .add_instrument(instrument(1).into())
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
                let mut engine = super::cancellations::engine(&cache);
                let observed = Rc::new(RefCell::new(Vec::new()));
                let handler = TypedHandler::from({
                    let observed = observed.clone();
                    let cache = cache.clone();
                    move |_: &PositionEvent| {
                        let cache = cache.borrow();
                        let positions =
                            cache.positions(None, Some(&instrument_id(1)), None, None, None);
                        assert_eq!(positions.len(), 1);
                        observed
                            .borrow_mut()
                            .push((positions[0].quantity, positions[0].realized_pnl));
                    }
                });
                msgbus::subscribe_position_events(
                    "events.position.*".into(),
                    handler.clone(),
                    None,
                );
                let mut wire = wallet("ingress-shared", HASH, "340000000000000001");
                let gross = alloy::primitives::U256::from_str_radix(raw_gross, 10).unwrap();
                let gross_decimal = Decimal::from_i128_with_scale(raw_gross.parse().unwrap(), 18);
                wire["fill"]["executedSizeWei"] = json!(raw_gross);
                wire["fill"]["executedValueWei"] = json!(
                    (gross * alloy::primitives::U256::from(620_000_000_000_000_000u64)
                        / alloy::primitives::U256::from(1_000_000_000_000_000_000u64))
                    .to_string()
                );
                wire["details"]["quantityFilled"] = json!(gross_decimal.to_string());
                let mut all = Vec::new();
                if rest_first {
                    let report = rest_report(&server, &client, raw_gross).await;
                    if apply_first_before_other_source {
                        engine.reconcile_execution_report(&ExecutionReport::Fill(Box::new(
                            report.clone(),
                        )));
                    }
                    all.push(report);
                }
                server.wallet(wire.clone());
                server.wallet(wire.clone());
                let received = wallet_reports(&server, &mut rx, &mut engine, &wire).await;
                assert_eq!(
                    received.len(),
                    usize::from(!rest_first || !apply_first_before_other_source)
                );
                if !rest_first && apply_first_before_other_source {
                    engine.reconcile_execution_report(&ExecutionReport::Fill(Box::new(
                        received[0].clone(),
                    )));
                }
                all.extend(received);
                if !rest_first {
                    all.push(rest_report(&server, &client, raw_gross).await);
                }
                assert!(all.iter().all(|report| report.trade_id == all[0].trade_id));
                for report in all.iter().rev().chain(&all) {
                    engine.reconcile_execution_report(&ExecutionReport::Fill(Box::new(
                        report.clone(),
                    )));
                }
                {
                    let state = cache.borrow();
                    let own = state.order(&original.client_order_id()).unwrap();
                    assert_eq!(own.filled_qty().as_decimal(), gross_decimal);
                    assert_eq!(own.status(), OrderStatus::PartiallyFilled);
                    assert_eq!(own.trade_ids().len(), 1);
                    let positions =
                        state.positions(None, Some(&instrument_id(1)), None, None, None);
                    assert_eq!(positions.len(), 1);
                    let net = Quantity::from_decimal(
                        gross_decimal - Decimal::from_str_exact("0.340000000000000001").unwrap(),
                    )
                    .unwrap();
                    assert_eq!(positions[0].quantity, net);
                    assert_eq!(positions[0].events.len(), 1);
                    assert_eq!(
                        positions[0].events[0].quantity_commission,
                        Some(Quantity::from("0.340000000000000001"))
                    );
                    assert_eq!(
                        positions[0].cost_open,
                        net.as_decimal() * Decimal::new(62, 2)
                    );
                    assert_eq!(
                        observed.borrow().as_slice(),
                        &[(net, Some(Money::from("-0.2108 USDT")))]
                    );
                    assert_eq!(
                        state
                            .account(&account_id())
                            .unwrap()
                            .balance_total(Some(Currency::USDT()))
                            .unwrap(),
                        Money::from("1000 USDT")
                    );
                    assert_eq!(server.state.creations.lock().unwrap().len(), 0);
                }
                msgbus::unsubscribe_position_events("events.position.*".into(), &handler);
                client.disconnect().await.unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn equal_timestamp_executions_keep_distinct_settlement_and_strategy_owners() {
    use nautilus_model::identifiers::StrategyId;
    for fee in ["0", "340000000000000000"] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = reported_client(&server);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(instrument(1).into())
            .unwrap();
        let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
        let mut orders = Vec::new();
        for (owner, hash) in [
            ("OWNER-A-001", HASH.to_string()),
            ("OWNER-B-001", format!("0x{}", "ab".repeat(32))),
        ] {
            let mut order = OrderTestBuilder::new(OrderType::Limit)
                .trader_id(trader_id())
                .strategy_id(owner.into())
                .client_order_id(owner.into())
                .instrument_id(instrument_id(1))
                .side(OrderSide::Buy)
                .quantity(Quantity::from(100))
                .price(Price::from("0.620"))
                .build();
            order
                .apply(factory.generate_order_submitted(&order, 10.into()))
                .unwrap();
            order
                .apply(factory.generate_order_accepted(
                    &order,
                    VenueOrderId::new(&hash),
                    100.into(),
                    100.into(),
                ))
                .unwrap();
            cache
                .borrow_mut()
                .add_order(order.clone(), None, None, false)
                .unwrap();
            cache
                .borrow_mut()
                .add_venue_order_id(&order.client_order_id(), &VenueOrderId::new(&hash), false)
                .unwrap();
            orders.push((order, hash));
        }
        connect_reported(&mut client, &mut rx).await;
        let mut engine = super::cancellations::engine(&cache);
        let first = wallet("same-settlement", &orders[0].1, fee);
        let other = wallet("same-settlement", &orders[1].1, fee);
        let mut later = wallet("different-settlement", &orders[0].1, fee);
        later["details"]["quantityFilled"] = json!("34");
        for wire in [&first, &first, &other, &other, &later, &later] {
            server.wallet(wire.clone());
        }
        let reports = wallet_reports(&server, &mut rx, &mut engine, &first).await;
        assert_eq!(reports.len(), 3);
        assert!(
            reports
                .iter()
                .all(|report| report.ts_event == reports[0].ts_event)
        );
        assert!(
            reports
                .iter()
                .all(|report| report.last_px == reports[0].last_px
                    && report.last_qty == reports[0].last_qty)
        );
        assert_eq!(
            reports
                .iter()
                .map(|report| report.trade_id)
                .collect::<std::collections::HashSet<_>>()
                .len(),
            3
        );
        for report in reports.iter().chain(reports.iter().rev()) {
            engine.reconcile_execution_report(&ExecutionReport::Fill(Box::new(report.clone())));
        }
        {
            let state = cache.borrow();
            let net = Decimal::from_str_exact(if fee == "0" { "17" } else { "16.66" }).unwrap();
            for (owner, expected_fills) in [("OWNER-A-001", 2u32), ("OWNER-B-001", 1)] {
                let own = state.order(&owner.into()).unwrap();
                assert_eq!(own.strategy_id(), StrategyId::new(owner));
                assert_eq!(
                    own.filled_qty().as_decimal(),
                    Decimal::from(17 * expected_fills)
                );
                let positions = state.positions(
                    None,
                    Some(&instrument_id(1)),
                    Some(&StrategyId::new(owner)),
                    None,
                    None,
                );
                assert_eq!(positions.len(), 1);
                assert_eq!(
                    positions[0].quantity.as_decimal(),
                    net * Decimal::from(expected_fills)
                );
                assert_eq!(positions[0].events.len(), expected_fills as usize);
            }
            assert_eq!(
                state.orders(None, None, None, None, None).len(),
                3,
                "known reports must not materialize extra execution orders"
            );
        }
        client.disconnect().await.unwrap();
    }
}
