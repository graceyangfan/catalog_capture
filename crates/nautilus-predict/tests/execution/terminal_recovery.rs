// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Recorded atomic settlements through loopback REST and the Native execution engine.

use nautilus_common::msgbus::TypedHandler;
use nautilus_model::events::{OrderEventAny, OrderPendingCancel, OrderPendingUpdate};

use super::*;

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../fixtures/private_api/native_market_filled_atomic_remainder_20261025.json"
    ))
    .unwrap()
}

fn recovery_order(row: &Value, quote: bool) -> OrderAny {
    let hash = VenueOrderId::new(row["order"]["hash"].as_str().unwrap());
    let mut order = OrderTestBuilder::new(OrderType::Market)
        .trader_id(trader_id())
        .instrument_id(instrument_id(1))
        .side(OrderSide::Buy)
        .quantity(Quantity::from(if quote { "1.989" } else { "7.65" }))
        .quote_quantity(quote)
        .build();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    if quote {
        order
            .apply(factory.generate_order_updated(
                &order,
                hash,
                Quantity::from("7.65"),
                None,
                None,
                None,
                1.into(),
                1.into(),
            ))
            .unwrap();
    }
    order
        .apply(factory.generate_order_submitted(&order, 10.into()))
        .unwrap();
    order
        .apply(factory.generate_order_accepted(&order, hash, 100.into(), 100.into()))
        .unwrap();
    order
}

fn pending(order: &OrderAny, status: OrderStatus) -> OrderEventAny {
    match status {
        OrderStatus::PendingCancel => OrderEventAny::PendingCancel(OrderPendingCancel::new(
            order.trader_id(),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            Some(account_id()),
            UUID4::new(),
            1_500_000.into(),
            1_500_000.into(),
            false,
            order.venue_order_id(),
        )),
        OrderStatus::PendingUpdate => OrderEventAny::PendingUpdate(OrderPendingUpdate::new(
            order.trader_id(),
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            Some(account_id()),
            UUID4::new(),
            1_500_000.into(),
            1_500_000.into(),
            false,
            order.venue_order_id(),
        )),
        _ => panic!("expected pending command"),
    }
}

fn recorded_rows() -> (Value, Vec<Value>) {
    let saved = fixture();
    // Rebind public identity and signing fields to the loopback fixture. Economic
    // execution quantities, fees, settlement IDs and the terminal remainder are retained.
    let mut row = rest_order_data("FILLED", 1, OrderSide::Buy);
    for key in ["amount", "amountFilled", "strategy"] {
        row[key] = saved["restFinalOrder"][key].clone();
    }
    row["order"]["makerAmount"] = json!("1989000000000000000");
    row["order"]["takerAmount"] = json!("7650000000000000000");
    row["order"]["feeRateBps"] = json!("200");
    let fields = serde_json::from_value(row["order"].clone()).unwrap();
    let hash = nautilus_predict::signing::PredictOrderHasher::new(PredictEnvironment::BnbMainnet)
        .order_hash(
            &fields,
            nautilus_predict::common::contracts::PredictExchangeType::Standard,
        );
    row["order"]["hash"] = json!(hash);
    let matches = saved["successfulSettlements"]
        .as_array()
        .unwrap()
        .iter()
        .map(|settlement| {
            let mut row = super::matches::match_row(1);
            row["taker"]["hash"] = json!(hash);
            row["taker"]["amount"] = settlement["fill"]["executedSizeWei"].clone();
            row["taker"]["price"] = settlement["fill"]["executedPriceWei"].clone();
            row["taker"]["fee"] = json!({"type":"SHARES", "amount":settlement["fee"]["amountWei"]});
            row["settlementId"] = settlement["settlementId"].clone();
            row
        })
        .collect();
    (row, matches)
}

#[test]
fn signed_quantity_validation_preserves_atomic_terms() {
    let (mut row, _) = recorded_rows();
    row["status"] = json!("OPEN");
    row["amountFilled"] = json!("0");
    let original = recovery_order(&row, false);
    let parse = |row: Value, order: &OrderAny| {
        parse_rest_order_report(
            &serde_json::from_value(row).unwrap(),
            order,
            &instrument(1),
            account_id(),
            WALLET.parse().unwrap(),
            OBSERVED.into(),
        )
    };
    assert!(parse(row.clone(), &original).is_ok());
    // At instrument precision 16 this differs only below its quantity increment.
    row["order"]["takerAmount"] = json!("7650000000000000001");
    assert!(parse(row.clone(), &original).is_err());
    // A restored order with these exact signed terms must also be accepted,
    // independently of the instrument's lower display precision.
    let mut init = original.init_event().clone();
    init.quantity = Quantity::from("7.650000000000000001");
    let mut atomic = OrderAny::try_from(init).unwrap();
    for event in original.events().iter().skip(1) {
        atomic.apply((*event).clone()).unwrap();
    }
    assert!(parse(row.clone(), &atomic).is_ok());
    row["order"]["takerAmount"] = json!("7650000000000000002");
    assert!(parse(row, &atomic).is_err());
}

async fn recover(
    client: &PredictExecutionClient,
    rx: &mut mpsc::UnboundedReceiver<ExecutionEvent>,
    engine: &mut ExecutionEngine,
    original: &OrderAny,
) -> nautilus_model::reports::OrderStatusReport {
    match client
        .generate_order_status_report(&order_report_cmd(original))
        .await
    {
        Ok(Some(report)) => report,
        Err(error) => {
            assert!(
                error.to_string().contains("defer status until applied"),
                "{error:#}"
            );
            loop {
                match next(rx).await {
                    ExecutionEvent::Report(report @ ExecutionReport::OrderWithFills(_, _)) => {
                        engine.reconcile_execution_report(&report);
                        break;
                    }
                    ExecutionEvent::Account(state) => msgbus::send_account_state(
                        MessagingSwitchboard::portfolio_update_account(),
                        &state,
                    ),
                    other => panic!("unexpected recovery feedback {other:?}"),
                }
            }
            client
                .generate_order_status_report(&order_report_cmd(original))
                .await
                .unwrap()
                .unwrap()
        }
        other => panic!("expected owned recovery report, got {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn filled_atomic_remainder_closes_pending_commands_and_replays_without_new_fills() {
    for quote in [false, true] {
        for pending_status in [
            OrderStatus::Accepted,
            OrderStatus::PendingCancel,
            OrderStatus::PendingUpdate,
        ] {
            for fills_first in [false, true] {
                let server = server(vec![]).await;
                let (mut client, mut rx, cache) = reported_client(&server);
                let _portfolio = portfolio(&cache);
                cache
                    .borrow_mut()
                    .add_instrument(instrument(1).into())
                    .unwrap();
                connect_reported(&mut client, &mut rx).await;
                let (row, matches) = recorded_rows();
                let original = recovery_order(&row, quote);
                cache
                    .borrow_mut()
                    .add_order(original.clone(), None, None, false)
                    .unwrap();
                cache
                    .borrow_mut()
                    .add_venue_order_id(
                        &original.client_order_id(),
                        &original.venue_order_id().unwrap(),
                        false,
                    )
                    .unwrap();
                *server.state.order_snapshot.lock().unwrap() =
                    Some(json!({"success":true,"data":row}));
                *server.state.match_snapshot.lock().unwrap() = Some(matches);
                let mut engine = super::cancellations::engine(&cache);
                let observed = Rc::new(RefCell::new(Vec::new()));
                let handler = TypedHandler::from({
                    let cache = cache.clone();
                    let observed = observed.clone();
                    move |event: &OrderEventAny| {
                        if let OrderEventAny::Updated(update) = event {
                            observed.borrow_mut().push((
                                update.reconciliation,
                                cache
                                    .borrow()
                                    .order(&update.client_order_id)
                                    .unwrap()
                                    .status(),
                            ));
                        }
                    }
                });
                let topic = format!("events.order.{}", original.strategy_id()).into();
                msgbus::subscribe_order_events(topic, handler.clone(), None);
                if fills_first {
                    let reports = client
                        .generate_fill_reports(GenerateFillReports::new(
                            UUID4::new(),
                            UnixNanos::default(),
                            Some(original.instrument_id()),
                            original.venue_order_id(),
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
                if pending_status != OrderStatus::Accepted {
                    let current = cache
                        .borrow()
                        .order_owned(&original.client_order_id())
                        .unwrap();
                    engine.process(&pending(&current, pending_status));
                    assert_eq!(
                        cache
                            .borrow()
                            .order(&original.client_order_id())
                            .unwrap()
                            .status(),
                        pending_status
                    );
                }
                let report = recover(&client, &mut rx, &mut engine, &original).await;
                assert_eq!(report.order_status, OrderStatus::Filled);
                assert_eq!(report.filled_qty, Quantity::from("7.649999999999999999"));
                assert_eq!(report.quantity, report.filled_qty);
                assert_eq!(report.filled_qty.precision, 18);
                if fills_first && pending_status != OrderStatus::Accepted {
                    let applied = cache
                        .borrow()
                        .order_owned(&original.client_order_id())
                        .unwrap();
                    // Companion fill replay must not be needed to clear a pending command.
                    let mut current = original.clone();
                    for event in applied.events() {
                        if matches!(event, OrderEventAny::Filled(_)) {
                            current.apply(event.clone()).unwrap();
                        }
                    }
                    current.apply(pending(&current, pending_status)).unwrap();
                    assert_eq!(current.status(), pending_status);
                    let updates =
                        nautilus_execution::reconciliation::generate_reconciliation_order_pre_fill_events(
                            &current,
                            &report,
                            OBSERVED.into(),
                        );
                    assert!(
                        matches!(updates.as_slice(), [OrderEventAny::Updated(update)] if update.reconciliation),
                        "confirmed terminal quantity must close a pending order without replaying trades: {updates:?}"
                    );
                }
                engine.reconcile_execution_report(&ExecutionReport::Order(Box::new(report)));
                let restored = cache
                    .borrow()
                    .order_owned(&original.client_order_id())
                    .unwrap();
                assert_eq!(restored.status(), OrderStatus::Filled);
                assert!(restored.leaves_qty().is_zero());
                assert_eq!(restored.trade_ids().len(), 2);
                assert_eq!(restored.quantity(), restored.filled_qty());
                assert_eq!(observed.borrow().len(), 1);
                assert!(observed.borrow()[0].0);
                if fills_first {
                    assert_eq!(observed.borrow()[0].1, OrderStatus::Filled);
                }
                let position =
                    cache
                        .borrow()
                        .positions(None, Some(&instrument_id(1)), None, None, None)[0]
                        .clone();
                assert_eq!(position.quantity, Quantity::from("7.497"));
                assert_eq!(position.events.len(), 2);
                assert_eq!(
                    position
                        .events
                        .iter()
                        .map(|fill| fill.quantity_commission.unwrap().as_decimal())
                        .sum::<Decimal>(),
                    Decimal::from_str_exact("0.152999999999999999").unwrap()
                );
                assert!(
                    position
                        .events
                        .iter()
                        .all(|fill| fill.commission.is_none_or(|fee| fee.is_zero()))
                );
                let mut replay = OrderAny::try_from(restored.init_event().clone()).unwrap();
                for event in restored.events().iter().skip(1) {
                    replay.apply((*event).clone()).unwrap();
                }
                assert_eq!(replay.status(), OrderStatus::Filled);
                assert_eq!(replay.quantity(), restored.quantity());
                assert_eq!(
                    replay.init_event().quantity,
                    Quantity::from(if quote { "1.989" } else { "7.65" })
                );
                let before = serde_json::to_value(&restored).unwrap();
                // A new report revalidates the original signed quantity even after normalization.
                let repeated = recover(&client, &mut rx, &mut engine, &original).await;
                engine.reconcile_execution_report(&ExecutionReport::Order(Box::new(repeated)));
                assert_eq!(
                    serde_json::to_value(
                        cache
                            .borrow()
                            .order_owned(&original.client_order_id())
                            .unwrap()
                    )
                    .unwrap(),
                    before
                );
                assert_eq!(observed.borrow().len(), 1);
                assert_eq!(
                    cache
                        .borrow()
                        .positions(None, Some(&instrument_id(1)), None, None, None)[0]
                        .events
                        .len(),
                    2
                );
                client.disconnect().await.unwrap();
                connect_reported(&mut client, &mut rx).await;
                let cold = recover(&client, &mut rx, &mut engine, &original).await;
                engine.reconcile_execution_report(&ExecutionReport::Order(Box::new(cold)));
                assert_eq!(
                    serde_json::to_value(
                        cache
                            .borrow()
                            .order_owned(&original.client_order_id())
                            .unwrap()
                    )
                    .unwrap(),
                    before
                );
                assert_eq!(observed.borrow().len(), 1);
                timeout(Duration::from_secs(3), async {
                    loop {
                        if cache
                            .borrow()
                            .account(&account_id())
                            .unwrap()
                            .balance_locked(Some(Currency::USDT()))
                            .is_some_and(|amount| amount.is_zero())
                        {
                            break;
                        }
                        let ExecutionEvent::Account(state) = next(&mut rx).await else {
                            panic!("terminal recovery must not publish another execution");
                        };
                        msgbus::send_account_state(
                            MessagingSwitchboard::portfolio_update_account(),
                            &state,
                        );
                    }
                })
                .await
                .unwrap();
                assert!(server.state.creations.lock().unwrap().is_empty());
                assert!(server.state.removals.lock().unwrap().is_empty());
                msgbus::unsubscribe_order_events(topic, &handler);
                client.disconnect().await.unwrap();
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_recovery_waits_for_missing_settlements_without_inventing_fills() {
    for case in [
        "missing_settlement",
        "unknown_units",
        "unknown_units_atomic_remainder",
    ] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = reported_client(&server);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(instrument(1).into())
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let (mut row, matches) = recorded_rows();
        if case != "missing_settlement" {
            row["amount"] = json!("unknown-unit");
        }
        let original = recovery_order(&row, false);
        cache
            .borrow_mut()
            .add_order(original.clone(), None, None, false)
            .unwrap();
        cache
            .borrow_mut()
            .add_venue_order_id(
                &original.client_order_id(),
                &original.venue_order_id().unwrap(),
                false,
            )
            .unwrap();
        *server.state.order_snapshot.lock().unwrap() =
            Some(json!({"success":true,"data":row.clone()}));
        let known = if case == "unknown_units_atomic_remainder" {
            matches.clone()
        } else {
            vec![matches[0].clone()]
        };
        let known_count = known.len();
        *server.state.match_snapshot.lock().unwrap() = Some(known);
        let mut engine = super::cancellations::engine(&cache);
        let report = recover(&client, &mut rx, &mut engine, &original).await;
        assert_eq!(report.order_status, OrderStatus::Filled);
        assert_eq!(report.quantity, Quantity::from("7.65"));
        assert_eq!(
            report.filled_qty,
            Quantity::from(if known_count == 1 {
                "2.293784615384615382"
            } else {
                "7.649999999999999999"
            })
        );
        engine.reconcile_execution_report(&ExecutionReport::Order(Box::new(report)));
        assert_eq!(
            cache
                .borrow()
                .order(&original.client_order_id())
                .unwrap()
                .trade_ids()
                .len(),
            known_count
        );
        // Once the endpoint cumulative and both settlements agree, Native can close.
        row["amount"] = json!("7650000000000000000");
        *server.state.order_snapshot.lock().unwrap() = Some(json!({"success":true,"data":row}));
        *server.state.match_snapshot.lock().unwrap() = Some(matches);
        let complete = recover(&client, &mut rx, &mut engine, &original).await;
        engine.reconcile_execution_report(&ExecutionReport::Order(Box::new(complete)));
        let restored = cache
            .borrow()
            .order_owned(&original.client_order_id())
            .unwrap();
        assert_eq!(restored.status(), OrderStatus::Filled);
        assert_eq!(restored.trade_ids().len(), 2);
        assert_eq!(
            restored.filled_qty(),
            Quantity::from("7.649999999999999999")
        );
        assert_eq!(
            cache
                .borrow()
                .positions(None, Some(&instrument_id(1)), None, None, None)[0]
                .quantity,
            Quantity::from("7.497")
        );
        assert!(server.state.creations.lock().unwrap().is_empty());
        assert!(server.state.removals.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}

// Derived scheduling controls reuse captured economic facts. They do not assert
// that the venue produced this particular partial-fill/no-match sequence.
fn recorded_wallet_partial(row: &Value) -> Value {
    let mut wire = fixture()["successfulSettlements"][0].clone();
    wire["walletAddress"] = json!(WALLET);
    wire["orderHash"] = row["order"]["hash"].clone();
    wire["details"]["marketId"] = json!(42);
    wire["details"]["outcome"] = json!("YES");
    wire["details"]["outcomeIndex"] = json!(1);
    wire
}

fn recorded_remainder_rejection(partial: &Value) -> Value {
    let saved: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/native_market_no_match_20261024.json"
    ))
    .unwrap();
    let mut wire = saved["walletEvents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["type"] == "orderNotAccepted")
        .unwrap()
        .clone();
    for key in ["walletAddress", "orderHash", "orderId", "details"] {
        wire[key] = partial[key].clone();
    }
    wire["timestamp"] = partial["timestamp"].clone();
    wire
}

#[tokio::test(flavor = "current_thread")]
async fn recorded_partial_rejection_requires_independent_terminal_confirmation() {
    for (fill_first, cumulative) in [
        (false, "rounded"),
        (false, "exact"),
        (true, "rounded"),
        (true, "exact"),
        (true, "stale_zero"),
    ] {
        for pending_status in [
            OrderStatus::Accepted,
            OrderStatus::PendingCancel,
            OrderStatus::PendingUpdate,
        ] {
            let private = server(vec![]).await;
            let (row, _) = recorded_rows();
            let own = recovery_order(&row, false);
            let (data_tx, mut data_rx) = mpsc::unbounded_channel();
            nautilus_common::live::runner::replace_data_event_sender(data_tx);
            let (mut client, mut rx, cache) = reported_client(&private);
            let _portfolio = portfolio(&cache);
            cache
                .borrow_mut()
                .add_instrument(instrument(1).into())
                .unwrap();
            cache
                .borrow_mut()
                .add_order(own.clone(), None, None, false)
                .unwrap();
            cache
                .borrow_mut()
                .add_venue_order_id(
                    &own.client_order_id(),
                    &own.venue_order_id().unwrap(),
                    false,
                )
                .unwrap();
            connect_reported(&mut client, &mut rx).await;
            let partial = recorded_wallet_partial(&row);
            let gross = Quantity::from_u256(
                partial["fill"]["executedSizeWei"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                18,
            )
            .unwrap();
            let fee = Quantity::from_u256(
                partial["fee"]["amountWei"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap(),
                18,
            )
            .unwrap();
            let events = Rc::new(RefCell::new(Vec::new()));
            let handler = TypedHandler::from({
                let events = events.clone();
                let cache = cache.clone();
                let id = own.client_order_id();
                move |event: &OrderEventAny| {
                    if event.client_order_id() != id {
                        return;
                    }
                    assert!(!matches!(
                        event,
                        OrderEventAny::Rejected(_) | OrderEventAny::FillVoided(_)
                    ));
                    if matches!(event, OrderEventAny::Canceled(_)) {
                        assert_eq!(
                            cache.borrow().order(&id).unwrap().status(),
                            OrderStatus::Canceled
                        );
                    }
                    events.borrow_mut().push(event.clone());
                }
            });
            let topic = format!("events.order.{}", own.strategy_id()).into();
            msgbus::subscribe_order_events(topic, handler.clone(), None);
            let mut engine = submissions::engine(&cache);
            if fill_first {
                private.wallet(partial.clone());
                timeout(Duration::from_secs(2), async {
                    while cache
                        .borrow()
                        .order(&own.client_order_id())
                        .unwrap()
                        .filled_qty()
                        != gross
                    {
                        submissions::apply(&mut engine, &next(&mut rx).await);
                    }
                })
                .await
                .expect("recorded partial fill");
            }
            if pending_status != OrderStatus::Accepted {
                let current = cache
                    .borrow()
                    .order(&own.client_order_id())
                    .unwrap()
                    .clone();
                engine.process(&pending(&current, pending_status));
            }
            // Captured display cumulative is rounded to 7.650, while this one
            // confirmed settlement is 2.293784615384615382. Neither is a new fill.
            let mut rejection = recorded_remainder_rejection(&partial);
            if cumulative != "rounded" {
                rejection["details"]["quantityFilled"] = json!(if cumulative == "exact" {
                    gross.as_decimal().to_string()
                } else {
                    "0".to_string()
                });
            }
            private.wallet(rejection.clone());
            let observation = timeout(Duration::from_secs(2), async {
                loop {
                    if let Some(nautilus_common::messages::DataEvent::Data(
                        nautilus_model::data::Data::Custom(data),
                    )) = data_rx.recv().await
                        && let Some(update) =
                            data.data
                                .as_any()
                                .downcast_ref::<nautilus_predict::PredictOrderNotAcceptedUpdate>()
                    {
                        assert_eq!(
                            data.data_type,
                            nautilus_predict::predict_order_not_accepted_data_type(
                                account_id(),
                                own.strategy_id()
                            )
                        );
                        break update.clone();
                    }
                }
            })
            .await
            .expect("unresolved rejection must be observable without a false terminal");
            assert_eq!(observation.reason, "noMarketMatch");
            assert!(!observation.native_status.is_closed());
            assert!(
                !cache
                    .borrow()
                    .order(&own.client_order_id())
                    .unwrap()
                    .is_closed()
            );
            // The separate REST terminal supplies the missing lifecycle proof.
            // It must not manufacture the display cumulative as settlement inventory.
            let mut terminal = row.clone();
            terminal["status"] = json!("CANCELLED");
            terminal["amountFilled"] = json!("0");
            private
                .state
                .orders
                .lock()
                .unwrap()
                .push_back(json!({"success":true,"data":terminal}));
            client
                .query_order(nautilus_common::messages::execution::QueryOrder::new(
                    trader_id(),
                    Some(ClientId::new("PREDICT")),
                    own.strategy_id(),
                    own.instrument_id(),
                    own.client_order_id(),
                    own.venue_order_id(),
                    UUID4::new(),
                    OBSERVED.into(),
                    None,
                    None,
                ))
                .unwrap();
            timeout(Duration::from_secs(2), async {
                while cache
                    .borrow()
                    .order(&own.client_order_id())
                    .unwrap()
                    .status()
                    != OrderStatus::Canceled
                {
                    submissions::apply(&mut engine, &next(&mut rx).await);
                }
            })
            .await
            .expect("explicit REST cancellation closes the remainder without awaiting settlements");
            assert_eq!(
                cache
                    .borrow()
                    .order(&own.client_order_id())
                    .unwrap()
                    .filled_qty(),
                if fill_first { gross } else { Quantity::from(0) }
            );
            if !fill_first {
                assert!(
                    cache
                        .borrow()
                        .positions(None, None, None, None, None)
                        .is_empty()
                );
                private.wallet(partial.clone());
                timeout(Duration::from_secs(2), async {
                    while cache
                        .borrow()
                        .order(&own.client_order_id())
                        .unwrap()
                        .filled_qty()
                        != gross
                    {
                        submissions::apply(&mut engine, &next(&mut rx).await);
                    }
                })
                .await
                .expect("late confirmed fill remains applicable after remainder closure");
            }
            assert_eq!(
                cache
                    .borrow()
                    .order(&own.client_order_id())
                    .unwrap()
                    .status(),
                OrderStatus::Canceled
            );
            assert_eq!(
                cache.borrow().positions(None, None, None, None, None)[0]
                    .quantity
                    .as_decimal(),
                gross.as_decimal() - fee.as_decimal()
            );
            // A separate owned control event establishes that both duplicate facts
            // were consumed before we assert exactly-once Native publications.
            private.wallet(rejection);
            private.wallet(partial.clone());
            let (control_hash, control) = wallet_lifecycle::control(&mut engine, &partial);
            private.wallet(control);
            timeout(Duration::from_secs(2), async {
                loop {
                    let event = next(&mut rx).await;
                    let barrier = matches!(&event, ExecutionEvent::Order(OrderEventAny::Canceled(e))
                        if e.venue_order_id == Some(control_hash));
                    submissions::apply(&mut engine, &event);
                    if barrier {
                        break;
                    }
                }
            })
            .await
            .expect("duplicate replay barrier");
            assert_eq!(
                events
                    .borrow()
                    .iter()
                    .filter(|e| matches!(e, OrderEventAny::Filled(_)))
                    .count(),
                1
            );
            assert_eq!(
                events
                    .borrow()
                    .iter()
                    .filter(|e| matches!(e, OrderEventAny::Canceled(_)))
                    .count(),
                1
            );
            assert_eq!(private.state.order_requests.lock().unwrap().len(), 1);
            assert!(private.state.match_requests.lock().unwrap().is_empty());
            msgbus::unsubscribe_order_events(topic, &handler);
            client.disconnect().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn recorded_rest_execution_survives_a_newer_wallet_terminal() {
    for (kind, expected) in [
        ("orderCancelled", OrderStatus::Canceled),
        ("orderExpired", OrderStatus::Expired),
    ] {
        let private = server(vec![]).await;
        let (mut row, mut trades) = recorded_rows();
        row["status"] = json!("OPEN");
        trades.truncate(1);
        row["amountFilled"] = trades[0]["taker"]["amount"].clone();
        let own = recovery_order(&row, false);
        let (mut client, mut rx, cache) = reported_client(&private);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(instrument(1).into())
            .unwrap();
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let callbacks = Rc::new(RefCell::new(Vec::new()));
        let handler = TypedHandler::from({
            let callbacks = callbacks.clone();
            let cache = cache.clone();
            let id = own.client_order_id();
            move |event: &OrderEventAny| {
                if event.client_order_id() == id {
                    assert!(!matches!(event, OrderEventAny::FillVoided(_)));
                    assert_eq!(cache.borrow().order(&id).unwrap().status(), expected);
                    callbacks.borrow_mut().push(event.clone());
                }
            }
        });
        let topic = format!("events.order.{}", own.strategy_id()).into();
        msgbus::subscribe_order_events(topic, handler.clone(), None);
        private
            .state
            .orders
            .lock()
            .unwrap()
            .push_back(json!({"success":true,"data":row}));
        let gate = Arc::new(tokio::sync::Notify::new());
        *private.state.match_gate.lock().unwrap() = Some(gate.clone());
        private.state.match_pages.lock().unwrap().push_back((
            200,
            json!({"success":true,"cursor":null,"data":trades}),
            0,
        ));
        client
            .query_order(nautilus_common::messages::execution::QueryOrder::new(
                trader_id(),
                Some(ClientId::new("PREDICT")),
                own.strategy_id(),
                own.instrument_id(),
                own.client_order_id(),
                own.venue_order_id(),
                UUID4::new(),
                OBSERVED.into(),
                None,
                None,
            ))
            .unwrap();
        exit_orders::barrier("OPEN sampled and trade history held", || {
            private.state.match_requests.lock().unwrap().len() == 1
        })
        .await;
        let partial = recorded_wallet_partial(&row);
        let mut terminal = recorded_remainder_rejection(&partial);
        terminal["type"] = json!(kind);
        terminal.as_object_mut().unwrap().remove("reason");
        private.wallet(terminal);
        let mut engine = submissions::engine(&cache);
        timeout(Duration::from_secs(2), async {
            while cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .status()
                != expected
            {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .expect("wallet terminal precedes REST match completion");
        gate.notify_one();
        timeout(Duration::from_secs(2), async {
            while cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .filled_qty()
                .is_zero()
            {
                submissions::apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .expect("older OPEN status cannot discard confirmed REST execution");
        let gross = Quantity::from_u256(
            partial["fill"]["executedSizeWei"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
            18,
        )
        .unwrap();
        let fee = Quantity::from_u256(
            partial["fee"]["amountWei"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap(),
            18,
        )
        .unwrap();
        assert_eq!(
            cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .filled_qty(),
            gross
        );
        assert_eq!(
            cache.borrow().positions(None, None, None, None, None)[0]
                .quantity
                .as_decimal(),
            gross.as_decimal() - fee.as_decimal()
        );
        assert_eq!(callbacks.borrow().len(), 2);
        assert_eq!(private.state.order_requests.lock().unwrap().len(), 1);
        assert_eq!(private.state.match_requests.lock().unwrap().len(), 1);
        msgbus::unsubscribe_order_events(topic, &handler);
        client.disconnect().await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_and_unrecognized_rejections_cannot_terminate_the_original_order() {
    for reason in ["rejectedDuplicate", "futureRejection"] {
        for partial_first in [false, true] {
            let private = server(vec![]).await;
            let (row, _) = recorded_rows();
            let own = recovery_order(&row, false);
            let (data_tx, mut data_rx) = mpsc::unbounded_channel();
            nautilus_common::live::runner::replace_data_event_sender(data_tx);
            let (mut client, mut rx, cache) = reported_client(&private);
            let _portfolio = portfolio(&cache);
            cache
                .borrow_mut()
                .add_instrument(instrument(1).into())
                .unwrap();
            cache
                .borrow_mut()
                .add_order(own.clone(), None, None, false)
                .unwrap();
            cache
                .borrow_mut()
                .add_venue_order_id(
                    &own.client_order_id(),
                    &own.venue_order_id().unwrap(),
                    false,
                )
                .unwrap();
            connect_reported(&mut client, &mut rx).await;
            let mut engine = submissions::engine(&cache);
            let partial = recorded_wallet_partial(&row);
            if partial_first {
                private.wallet(partial.clone());
                timeout(Duration::from_secs(2), async {
                    while cache
                        .borrow()
                        .order(&own.client_order_id())
                        .unwrap()
                        .filled_qty()
                        .is_zero()
                    {
                        submissions::apply(&mut engine, &next(&mut rx).await);
                    }
                })
                .await
                .unwrap();
            }
            let before = serde_json::to_value(
                cache
                    .borrow()
                    .order(&own.client_order_id())
                    .unwrap()
                    .clone(),
            )
            .unwrap();
            let original_events = Rc::new(RefCell::new(Vec::new()));
            let handler = TypedHandler::from({
                let events = original_events.clone();
                let id = own.client_order_id();
                move |event: &OrderEventAny| {
                    if event.client_order_id() == id {
                        events.borrow_mut().push(event.clone());
                    }
                }
            });
            let topic = format!("events.order.{}", own.strategy_id()).into();
            msgbus::subscribe_order_events(topic, handler.clone(), None);
            let mut rejected = recorded_remainder_rejection(&partial);
            rejected["reason"] = json!(reason);
            // Even a zero displayed cumulative cannot turn duplicate rejection
            // into evidence that an existing original order stopped working.
            rejected["details"]["quantityFilled"] = json!("0");
            private.wallet(rejected.clone());
            private.wallet(rejected);
            let observation = timeout(Duration::from_secs(2), async {
                loop {
                    if let Some(nautilus_common::messages::DataEvent::Data(
                        nautilus_model::data::Data::Custom(data),
                    )) = data_rx.recv().await
                        && let Some(update) =
                            data.data
                                .as_any()
                                .downcast_ref::<nautilus_predict::PredictOrderNotAcceptedUpdate>()
                    {
                        break update.clone();
                    }
                }
            })
            .await
            .expect("ambiguous feedback remains visible");
            assert_eq!(
                observation.reason,
                if reason == "rejectedDuplicate" {
                    reason
                } else {
                    "unknown"
                }
            );
            assert!(!observation.native_status.is_closed());
            let (control_hash, control) = wallet_lifecycle::control(&mut engine, &partial);
            private.wallet(control);
            timeout(Duration::from_secs(2), async {
                loop {
                    let event = next(&mut rx).await;
                    let barrier = matches!(&event, ExecutionEvent::Order(OrderEventAny::Canceled(e)) if e.venue_order_id == Some(control_hash));
                    submissions::apply(&mut engine, &event);
                    if barrier { break; }
                }
            }).await.unwrap();
            assert_eq!(
                serde_json::to_value(
                    cache
                        .borrow()
                        .order(&own.client_order_id())
                        .unwrap()
                        .clone()
                )
                .unwrap(),
                before
            );
            assert!(original_events.borrow().is_empty());
            assert!(private.state.order_requests.lock().unwrap().is_empty());
            assert!(private.state.match_requests.lock().unwrap().is_empty());
            msgbus::unsubscribe_order_events(topic, &handler);
            client.disconnect().await.unwrap();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn zero_display_no_match_cannot_reject_an_order_with_pending_settlement() {
    let private = server(vec![]).await;
    let (row, _) = recorded_rows();
    let own = recovery_order(&row, false);
    let (data_tx, mut data_rx) = mpsc::unbounded_channel();
    nautilus_common::live::runner::replace_data_event_sender(data_tx);
    let (mut client, mut rx, cache) = reported_client(&private);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(instrument(1).into())
        .unwrap();
    cache
        .borrow_mut()
        .add_order(own.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let success = recorded_wallet_partial(&row);
    let mut submitted = success.clone();
    submitted["type"] = json!("orderTransactionSubmitted");
    submitted["kind"] = json!("PURCHASE");
    submitted["details"]["quantityFilled"] = json!("0");
    submitted.as_object_mut().unwrap().remove("fee");
    submitted.as_object_mut().unwrap().remove("isMaker");
    private.wallet(submitted);
    let mut rejected = recorded_remainder_rejection(&success);
    rejected["details"]["quantityFilled"] = json!("0");
    private.wallet(rejected);
    timeout(Duration::from_secs(2), async {
        loop {
            if let Some(nautilus_common::messages::DataEvent::Data(
                nautilus_model::data::Data::Custom(data),
            )) = data_rx.recv().await
                && let Some(update) =
                    data.data
                        .as_any()
                        .downcast_ref::<nautilus_predict::PredictOrderNotAcceptedUpdate>()
            {
                assert_eq!(update.reason, "noMarketMatch");
                assert_eq!(
                    update.reported_quantity_filled.parse::<Decimal>().unwrap(),
                    Decimal::ZERO
                );
                assert!(!update.native_status.is_closed());
                break;
            }
        }
    })
    .await
    .expect("pending on-chain settlement prevents a false zero-fill rejection");
    let observed = Rc::new(RefCell::new(Vec::new()));
    let handler = TypedHandler::from({
        let observed = observed.clone();
        let id = own.client_order_id();
        move |event: &OrderEventAny| {
            if event.client_order_id() == id {
                assert!(
                    matches!(event, OrderEventAny::Filled(_)),
                    "only the real settlement can advance economic state"
                );
                observed.borrow_mut().push(event.clone());
            }
        }
    });
    let topic = format!("events.order.{}", own.strategy_id()).into();
    msgbus::subscribe_order_events(topic, handler.clone(), None);
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Accepted
    );
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    private.wallet(success.clone());
    let mut engine = submissions::engine(&cache);
    timeout(Duration::from_secs(2), async {
        while cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .filled_qty()
            .is_zero()
        {
            submissions::apply(&mut engine, &next(&mut rx).await);
        }
    })
    .await
    .expect("confirmed settlement remains applicable");
    let gross = Quantity::from_u256(
        success["fill"]["executedSizeWei"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap(),
        18,
    )
    .unwrap();
    let fee = Quantity::from_u256(
        success["fee"]["amountWei"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap(),
        18,
    )
    .unwrap();
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::PartiallyFilled
    );
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None)[0]
            .quantity
            .as_decimal(),
        gross.as_decimal() - fee.as_decimal()
    );
    assert_eq!(observed.borrow().len(), 1);
    msgbus::unsubscribe_order_events(topic, &handler);
    client.disconnect().await.unwrap();
}
