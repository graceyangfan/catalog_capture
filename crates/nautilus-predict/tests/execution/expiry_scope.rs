// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/lgpl-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Expired inventory leaves trading reports without changing Native positions or cash.
use super::*;

fn command() -> GeneratePositionStatusReports {
    GeneratePositionStatusReports::new(UUID4::new(), OBSERVED.into(), None, None, None, None, None)
}

fn expired_market(id: u64) -> Value {
    json!({"success":true,"data":{
        "id":id,"title":"Bitcoin Up or Down - October 24, 3PM-3:05PM ET",
        "createdAt":"2026-10-24T18:00:00Z","variantData":{"type":"CRYPTO_UP_DOWN"},
        "tradingStatus":"OPEN","isNegRisk":false,"isYieldBearing":false,"feeRateBps":0,
        "decimalPrecision":3,"outcomes":[
            {"name":"Yes","indexSet":1,"onChainId":"11"},
            {"name":"No","indexSet":2,"onChainId":"22"}]}})
}

#[tokio::test]
async fn expired_unknown_unresolved_holdings_skip_without_loading_and_share_metadata_read() {
    let venue = server(vec![]).await;
    *venue.state.market_snapshot.lock().unwrap() = Some(expired_market(42));
    *venue.state.position_snapshot.lock().unwrap() = Some(vec![holding(1), holding(2)]);
    let now = 1_792_886_400_000_000_000u64; // 2026-10-25 UTC, after the fixture window.
    let (client, mut rx, cache) = submissions::setup_client(&venue, now);
    let reports = client
        .generate_position_status_reports(&command())
        .await
        .unwrap();
    assert!(reports.is_empty());
    assert_eq!(*venue.state.market_requests.lock().unwrap(), 1);
    assert!(cache.borrow().instrument(&instrument_id(1)).is_none());
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    assert!(
        rx.try_recv().is_err(),
        "expiry filtering must not publish cash or fills"
    );
    let ids = nautilus_predict::execution::reconciliation_market_ids(&config(&venue))
        .await
        .unwrap();
    assert!(
        ids.is_empty(),
        "startup must not reload retired definitions"
    );
    assert_eq!(
        *venue.state.market_requests.lock().unwrap(),
        2,
        "one lookup per collection, not per outcome"
    );
}

#[tokio::test]
async fn cached_expired_holdings_need_no_metadata_query_and_keep_the_definition() {
    let venue = server(vec![]).await;
    *venue.state.position_snapshot.lock().unwrap() = Some(vec![holding(1)]);
    let (client, mut rx, cache) = client(&venue);
    let mut binary = instrument(1);
    binary.expiration_ns = OBSERVED.into();
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(binary))
        .unwrap();
    assert!(
        client
            .generate_position_status_reports(&command())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(*venue.state.market_requests.lock().unwrap(), 0);
    assert!(cache.borrow().instrument(&instrument_id(1)).is_some());
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn final_outcome_without_instrument_needs_no_metadata_query() {
    for status in ["WON", "LOST"] {
        let venue = server(vec![]).await;
        let mut row = holding(1);
        row["outcome"]["status"] = json!(status);
        *venue.state.position_snapshot.lock().unwrap() = Some(vec![row]);
        let (client, mut rx, cache) = client(&venue);
        assert!(
            client
                .generate_position_status_reports(&command())
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(*venue.state.market_requests.lock().unwrap(), 0);
        assert!(cache.borrow().instrument(&instrument_id(1)).is_none());
        assert!(rx.try_recv().is_err());
    }
}

#[tokio::test]
async fn active_unknown_holding_still_fails_and_later_pages_are_not_lost() {
    let mut retired = holding(2);
    retired["outcome"]["status"] = json!("LOST");
    let venue = server(vec![
        json!({"success":true,"cursor":"next","data":[retired]}),
        json!({"success":true,"data":[holding(1)]}),
    ])
    .await;
    let (client, mut rx, _cache) = client(&venue);
    let error = client
        .generate_position_status_reports(&command())
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("instrument is missing"),
        "{error:#}"
    );
    assert_eq!(venue.state.requests.lock().unwrap().len(), 2);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn metadata_identity_failure_is_not_treated_as_retirement() {
    let venue = server(vec![]).await;
    *venue.state.position_snapshot.lock().unwrap() = Some(vec![holding(1)]);
    *venue.state.market_snapshot.lock().unwrap() = Some(expired_market(999));
    let (client, _, _) = client(&venue);
    let error = client
        .generate_position_status_reports(&command())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("identity differs"), "{error:#}");
}

#[tokio::test]
async fn retired_inventory_does_not_block_reported_account_restart_or_mass_status() {
    let venue = server(vec![]).await;
    let mut retired = holding(1);
    retired["market"]["id"] = json!(3707656);
    retired["outcome"]["status"] = json!("LOST");
    *venue.state.position_snapshot.lock().unwrap() = Some(vec![retired]);
    let mut old = super::matches::match_row(1);
    old["market"]["id"] = json!(3707656);
    old["settlementId"] = Value::Null;
    *venue.state.match_snapshot.lock().unwrap() = Some(vec![old]);
    let (mut client, mut rx, cache, _portfolio, _) = submissions::connected(&venue, 1).await;
    for _ in 0..2 {
        assert!(client.is_connected());
        assert!(client.is_submission_ready());
        let mass = client.generate_mass_status(Some(1)).await.unwrap().unwrap();
        assert!(mass.position_reports().is_empty());
        assert!(venue.state.match_requests.lock().unwrap().is_empty());
        assert!(
            cache
                .borrow()
                .instrument(&"3707656-1.PREDICT".parse().unwrap())
                .is_none()
        );
        client.disconnect().await.unwrap();
        connect_reported(&mut client, &mut rx).await;
    }
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn account_refresh_does_not_scan_history_or_gate_a_different_initialized_order() {
    let venue = server(vec![]).await;
    let (mut client, mut rx, cache, _portfolio, order) = submissions::connected(&venue, 1).await;
    let mut historical = fill(1);
    historical["orderHash"] = json!(format!("0x{}", "77".repeat(32)));
    venue.wallet(historical);
    wallet_processed(&venue).await;
    assert!(client.is_submission_ready());
    let before = venue.state.rpc_methods.lock().unwrap().len();
    client
        .query_account(QueryAccount::new(
            trader_id(),
            Some("PREDICT".into()),
            account_id(),
            UUID4::new(),
            OBSERVED.into(),
            None,
            None,
        ))
        .unwrap();
    let mut engine = submissions::engine(&cache);
    timeout(Duration::from_secs(3), async {
        loop {
            let event = next(&mut rx).await;
            let account = matches!(&event, ExecutionEvent::Account(_));
            submissions::apply(&mut engine, &event);
            if account && venue.state.rpc_methods.lock().unwrap().len() > before {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(venue.state.match_requests.lock().unwrap().is_empty());
    assert!(
        venue.state.position_response_log.lock().unwrap().is_empty(),
        "QueryAccount does not fetch historical holdings"
    );
    venue
        .state
        .create_replies
        .lock()
        .unwrap()
        .push_back((201, 0, vec!["orderAccepted".into()]));
    client.submit_order(submissions::submit(&order)).unwrap();
    timeout(Duration::from_secs(3), async {
        while cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status()
            != OrderStatus::Accepted
        {
            submissions::apply(&mut engine, &next(&mut rx).await);
        }
    })
    .await
    .unwrap();
    assert_eq!(venue.state.creations.lock().unwrap().len(), 1);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn expired_native_residual_with_no_remote_row_survives_refresh_and_restart_unchanged() {
    use nautilus_model::position::Position;
    let venue = server(vec![]).await;
    *venue.state.position_snapshot.lock().unwrap() = Some(vec![]);
    *venue.state.match_snapshot.lock().unwrap() = Some(vec![]);
    let (mut client, mut rx, cache, _portfolio, _) = submissions::connected(&venue, 1).await;
    let mut binary = instrument(2);
    binary.expiration_ns = OBSERVED.into();
    let definition = InstrumentAny::BinaryOption(binary);
    cache
        .borrow_mut()
        .add_instrument(definition.clone())
        .unwrap();
    let opening = native_fee_paths::fill(
        &definition,
        1,
        OrderSide::Buy,
        "0.0000920634920634",
        "0.620",
        Money::zero(Currency::USDT()),
    );
    let position = Position::new(&definition, opening);
    cache
        .borrow_mut()
        .add_position_without_order(&position, OmsType::Netting)
        .unwrap();
    for _ in 0..2 {
        let mass = client.generate_mass_status(Some(1)).await.unwrap().unwrap();
        assert!(mass.position_reports().is_empty());
        {
            let cache = cache.borrow();
            let retained = cache.position(&position.id).unwrap();
            assert_eq!(retained.quantity, position.quantity);
            assert!(
                retained.is_open(),
                "report filtering must not manufacture closure"
            );
        }
        client.disconnect().await.unwrap();
        connect_reported(&mut client, &mut rx).await;
        assert!(client.is_submission_ready());
    }
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn terminal_history_with_purged_definition_does_not_trigger_startup_order_reads() {
    let venue = server(vec![]).await;
    *venue.state.position_snapshot.lock().unwrap() = Some(vec![]);
    *venue.state.match_snapshot.lock().unwrap() = Some(vec![]);
    let (mut client, _rx, cache, _portfolio, active) = submissions::connected(&venue, 1).await;
    let retired_id = "3707656-2.PREDICT".parse().unwrap();
    let mut retired = instrument(2);
    retired.id = retired_id;
    retired.expiration_ns = OBSERVED.into();
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(retired))
        .unwrap();
    let mut closed = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .client_order_id("RETIRED-ONLY".into())
        .instrument_id(retired_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from(100))
        .price(Price::from("0.620"))
        .build();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    cache
        .borrow_mut()
        .add_order(closed.clone(), None, None, false)
        .unwrap();
    closed = cache
        .borrow_mut()
        .update_order(&factory.generate_order_submitted(&closed, 1.into()))
        .unwrap();
    closed = cache
        .borrow_mut()
        .update_order(&factory.generate_order_accepted(&closed, HASH.into(), 2.into(), 2.into()))
        .unwrap();
    closed = cache
        .borrow_mut()
        .update_order(&factory.generate_order_canceled(
            &closed,
            closed.venue_order_id(),
            OBSERVED.into(),
            OBSERVED.into(),
        ))
        .unwrap();
    cache.borrow_mut().purge_instrument(retired_id);
    assert!(cache.borrow().instrument(&retired_id).is_none());
    let mass = client.generate_mass_status(Some(1)).await.unwrap().unwrap();
    assert!(mass.order_reports().is_empty());
    assert!(venue.state.order_requests.lock().unwrap().is_empty());
    assert!(
        cache.borrow().order(&closed.client_order_id()).is_some(),
        "scope filtering does not purge history"
    );
    let rpc_before = venue.state.rpc_methods.lock().unwrap().len();
    *venue.state.rpc_failure.lock().unwrap() = true;
    client.submit_order(submissions::submit(&active)).unwrap();
    exit_orders::barrier("active market POST after retired definition purge", || {
        venue.state.creations.lock().unwrap().len() == 1
    })
    .await;
    assert_eq!(
        venue.state.creations.lock().unwrap()[0]["data"]["order"]["nonce"],
        "7"
    );
    assert_eq!(venue.state.rpc_methods.lock().unwrap().len(), rpc_before);
    assert!(venue.state.order_requests.lock().unwrap().is_empty());
    assert!(venue.state.match_requests.lock().unwrap().is_empty());
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn retained_canceled_expired_order_recovers_previously_unseen_fill() {
    use nautilus_predict::{common::contracts::PredictExchangeType, signing::PredictOrderHasher};
    let venue = server(vec![]).await;
    *venue.state.position_snapshot.lock().unwrap() = Some(vec![]);
    let (mut client, _rx, cache, _portfolio, _) = submissions::connected(&venue, 1).await;
    let mut retired = instrument(2);
    retired.expiration_ns = OBSERVED.into();
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(retired))
        .unwrap();
    let mut row = rest_order_data("CANCELLED", 2, OrderSide::Buy);
    let hash = PredictOrderHasher::new(PredictEnvironment::BnbMainnet).order_hash(
        &serde_json::from_value(row["order"].clone()).unwrap(),
        PredictExchangeType::Standard,
    );
    let venue_id = VenueOrderId::new(format!("{hash:#x}"));
    row["order"]["hash"] = json!(venue_id.to_string());
    row["amountFilled"] = json!("17000000000000000000");
    venue
        .state
        .orders_by_hash
        .lock()
        .unwrap()
        .insert(venue_id.to_string(), json!({"success":true,"data":row}));
    let mut fill = super::matches::match_row(2);
    fill["taker"]["hash"] = json!(venue_id.to_string());
    *venue.state.match_snapshot.lock().unwrap() = Some(vec![fill]);
    let mut closed = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .client_order_id("RETIRED-LATE-FILL".into())
        .instrument_id(instrument_id(2))
        .side(OrderSide::Buy)
        .quantity(Quantity::from(100))
        .price(Price::from("0.620"))
        .build();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    cache
        .borrow_mut()
        .add_order(closed.clone(), None, None, false)
        .unwrap();
    closed = cache
        .borrow_mut()
        .update_order(&factory.generate_order_submitted(&closed, 1.into()))
        .unwrap();
    closed = cache
        .borrow_mut()
        .update_order(&factory.generate_order_accepted(&closed, venue_id, 2.into(), 2.into()))
        .unwrap();
    closed = cache
        .borrow_mut()
        .update_order(&factory.generate_order_canceled(
            &closed,
            Some(venue_id),
            OBSERVED.into(),
            OBSERVED.into(),
        ))
        .unwrap();
    let mut engine = submissions::engine(&cache);
    for _ in 0..2 {
        let mass = client.generate_mass_status(Some(1)).await.unwrap().unwrap();
        assert!(
            mass.position_reports().is_empty(),
            "expired tokens are outside trading reports"
        );
        let report = mass.order_reports().get(&venue_id).unwrap().clone();
        let fills = mass.fill_reports().get(&venue_id).unwrap().clone();
        assert_eq!(report.filled_qty, Quantity::from(17));
        assert_eq!(report.order_status, OrderStatus::Canceled);
        assert_eq!(fills.len(), 1);
        engine
            .reconcile_execution_report(&ExecutionReport::OrderWithFills(Box::new(report), fills));
        assert_eq!(
            cache
                .borrow()
                .order(&closed.client_order_id())
                .unwrap()
                .filled_qty(),
            Quantity::from(17)
        );
        assert_eq!(
            cache
                .borrow()
                .positions_open(None, Some(&instrument_id(2)), None, None, None)[0]
                .quantity,
            Quantity::from(17)
        );
    }
    let bounded = GenerateFillReports::new(
        UUID4::new(),
        OBSERVED.into(),
        Some(instrument_id(2)),
        None,
        Some(UnixNanos::default()),
        Some(OBSERVED.into()),
        None,
        None,
    );
    let retained = client.generate_fill_reports(bounded).await.unwrap();
    assert_eq!(
        retained.len(),
        1,
        "known expired orders remain in bounded recovery"
    );
    assert_eq!(retained[0].last_qty, Quantity::from(17));
    assert!(
        venue
            .state
            .match_requests
            .lock()
            .unwrap()
            .iter()
            .all(|query| query.get("marketId").map(String::as_str) == Some("42"))
    );
    client.disconnect().await.unwrap();
}

#[derive(Debug)]
struct RestartObserver {
    core: nautilus_trading::StrategyCore,
    started: Rc<std::cell::Cell<bool>>,
}
impl nautilus_common::actor::DataActor for RestartObserver {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.started.set(true);
        Ok(())
    }
}
nautilus_trading::nautilus_strategy!(RestartObserver, {});

#[tokio::test(flavor = "current_thread")]
async fn native_startup_with_retired_remote_and_cached_residuals_restarts_without_history() {
    use nautilus_model::position::Position;
    let venue = server(vec![]).await;
    let mut remote = holding(1);
    remote["market"]["id"] = json!(3707656);
    remote["outcome"]["status"] = json!("LOST");
    *venue.state.position_snapshot.lock().unwrap() = Some(vec![remote]);
    let mut old = super::matches::match_row(1);
    old["market"]["id"] = json!(3707656);
    old["settlementId"] = Value::Null;
    *venue.state.match_snapshot.lock().unwrap() = Some(vec![old]);
    let mut binary = instrument(2);
    binary.id = "3707696-2.PREDICT".parse().unwrap();
    binary.expiration_ns = OBSERVED.into();
    let definition = InstrumentAny::BinaryOption(binary);
    let position = Position::new(
        &definition,
        native_fee_paths::fill(
            &definition,
            1,
            OrderSide::Buy,
            "0.0000920634920634",
            "0.620",
            Money::zero(Currency::USDT()),
        ),
    );
    for _ in 0..2 {
        let (http, ws, public_task) = live_node::public_server(1).await;
        venue.state.rpc_frame.lock().unwrap().2 = nautilus_core::time::get_atomic_clock_realtime()
            .get_time_ns()
            .as_u64()
            / 1_000_000_000;
        let mut execution = config(&venue);
        execution.account_state_mode = PredictAccountStateMode::Reported;
        execution.chain = Some(PredictChainClientConfig::new(
            PredictEnvironment::BnbMainnet,
            format!("{}/rpc", venue.http).into(),
        ));
        let mut live = node::build_node_with_engine(
            trader_id(),
            nautilus_predict::PredictDataClientConfig {
                api_key: KEY.into(),
                market_ids: vec![42],
                base_url_http: Some(http),
                base_url_ws: Some(ws),
                http_timeout_secs: 2,
                ws_timeout_secs: 2,
                dynamic_refresh_secs: 30,
                transport_backend: TransportBackend::Tungstenite,
            },
            execution,
            nautilus_live::node::config::LiveExecutionEngineConfig {
                reconciliation: true,
                reconciliation_startup_delay_secs: 0.0,
                inflight_check_interval_ms: 0,
                ..Default::default()
            },
        )
        .unwrap();
        let cache = live.kernel().cache.clone();
        cache
            .borrow_mut()
            .add_instrument(definition.clone())
            .unwrap();
        cache
            .borrow_mut()
            .add_position_without_order(&position, OmsType::Netting)
            .unwrap();
        let started = Rc::new(std::cell::Cell::new(false));
        live.add_strategy(RestartObserver {
            core: nautilus_trading::StrategyCore::new(nautilus_trading::StrategyConfig {
                strategy_id: Some("RESTART-OBSERVER-001".into()),
                ..Default::default()
            }),
            started: started.clone(),
        })
        .unwrap();
        let handle = live.handle();
        let driver = async {
            exit_orders::barrier(
                "strategy starts after Native startup reconciliation",
                || started.get(),
            )
            .await;
            let cache = cache.borrow();
            assert!(
                cache
                    .instrument(&"3707656-1.PREDICT".parse().unwrap())
                    .is_none()
            );
            assert_eq!(
                cache.position(&position.id).unwrap().quantity,
                position.quantity
            );
            assert!(cache.position(&position.id).unwrap().is_open());
            assert!(venue.state.match_requests.lock().unwrap().is_empty());
            handle.stop();
        };
        timeout(Duration::from_secs(10), async {
            let (result, ()) = tokio::join!(live.run(), driver);
            result.unwrap();
        })
        .await
        .unwrap();
        live.dispose();
        public_task.abort();
    }
}

#[tokio::test]
async fn bounded_inventory_fill_recovery_omits_retired_tokens_without_hiding_explicit_history() {
    let venue = server(vec![]).await;
    let mut retired = holding(1);
    retired["market"]["id"] = json!(3707656);
    retired["outcome"]["status"] = json!("LOST");
    retired["amount"] = json!("17000000000000000000");
    *venue.state.position_snapshot.lock().unwrap() = Some(vec![retired]);
    let mut old = super::matches::match_row(1);
    old["market"]["id"] = json!(3707656);
    *venue.state.match_snapshot.lock().unwrap() = Some(vec![old]);
    let (mut client, mut rx, _cache, _portfolio, _) = submissions::connected(&venue, 1).await;
    while rx.try_recv().is_ok() {}
    let mut request = GenerateFillReports::new(
        UUID4::new(),
        OBSERVED.into(),
        None,
        None,
        Some(UnixNanos::default()),
        Some(OBSERVED.into()),
        None,
        None,
    );
    assert!(
        client
            .generate_fill_reports(request.clone())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        venue.state.match_requests.lock().unwrap().is_empty(),
        "retired static tokens do not require a history scan"
    );
    assert!(rx.try_recv().is_err());
    request.start = None;
    request.end = None;
    let error = client.generate_fill_reports(request).await.unwrap_err();
    assert!(
        error.to_string().contains("instrument is absent"),
        "{error:#}"
    );
    assert_eq!(
        venue.state.match_requests.lock().unwrap().len(),
        1,
        "explicit history retains its full scope"
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn zero_unknown_holdings_need_no_metadata_and_do_not_hide_active_exposure() {
    let venue = server(vec![]).await;
    let mut empty = holding(2);
    empty["market"]["id"] = json!(3707656);
    empty["amount"] = json!("0");
    *venue.state.position_snapshot.lock().unwrap() = Some(vec![empty.clone(), holding(1)]);
    let (client, mut rx, cache) = client(&venue);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let reports = client
        .generate_position_status_reports(&command())
        .await
        .unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].instrument_id, instrument_id(1));
    assert!(reports[0].quantity.as_decimal() > Decimal::ZERO);
    assert_eq!(*venue.state.market_requests.lock().unwrap(), 0);
    assert!(rx.try_recv().is_err());
    *venue.state.position_snapshot.lock().unwrap() = Some(vec![empty.clone()]);
    assert!(
        nautilus_predict::execution::reconciliation_market_ids(&config(&venue))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(*venue.state.market_requests.lock().unwrap(), 0);
    empty["amount"] = json!("1");
    *venue.state.position_snapshot.lock().unwrap() = Some(vec![empty, holding(1)]);
    assert!(
        client
            .generate_position_status_reports(&command())
            .await
            .is_err()
    );
    assert_eq!(
        *venue.state.market_requests.lock().unwrap(),
        1,
        "nonzero unknown holdings still require reliable metadata"
    );
}
