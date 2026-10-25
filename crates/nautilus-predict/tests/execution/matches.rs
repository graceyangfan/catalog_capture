// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Confirmed REST history, participant scope, fees and all-or-nothing report collection.
use nautilus_model::enums::LiquiditySide;

use super::*;

pub(super) fn match_row(outcome: u8) -> Value {
    let mut market = holding(outcome)["market"].clone();
    market["outcomes"] = json!([holding(1)["outcome"], holding(2)["outcome"]]);
    json!({"market":market,
        "taker":{"quoteType":"Bid","amount":"17000000000000000000","price":"620000000000000000",
            "outcome":holding(outcome)["outcome"],"signer":WALLET,"hash":HASH,
            "fee":{"type":"COLLATERAL","amount":"100000000000000000"}},
        "makers":[{"quoteType":"Bid","amount":"17000000000000000000","price":"380000000000000000",
            "outcome":holding(3-outcome)["outcome"],"signer":"0x2222222222222222222222222222222222222222",
            "hash":"0x2222222222222222222222222222222222222222222222222222222222222222",
            "fee":{"type":"SHARES","amount":"0"}}],
        "amountFilled":"17000000000000000000","priceExecuted":"620000000000000000",
        "transactionHash":"0x3333333333333333333333333333333333333333333333333333333333333333",
        "settlementId":"rest-settlement-1","executedAt":"1970-01-01T00:00:00.001Z"})
}

fn command(outcome: Option<u8>) -> GenerateFillReports {
    GenerateFillReports::new(
        UUID4::new(),
        UnixNanos::default(),
        outcome.map(instrument_id),
        None,
        None,
        None,
        None,
        None,
    )
}

fn add_instruments(cache: &Rc<RefCell<Cache>>) {
    for outcome in [1, 2] {
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(outcome)))
            .unwrap();
    }
}

fn page(server: &Server, mut body: Value) {
    if body.get("cursor").is_none() {
        body["cursor"] = Value::Null;
    }
    server
        .state
        .match_pages
        .lock()
        .unwrap()
        .push_back((200, body, 0));
}

#[tokio::test]
#[ignore = "explicit loopback history scaling; PREDICT_REPORT_STAGES_PATH is create_new JSON"]
async fn benchmark_report_collection_finish_and_drop() {
    use std::time::Instant;

    let mut samples = Vec::new();
    for count in [1usize, 10, 100, 1000, 10000, 100000] {
        let venue = server(vec![]).await;
        let (_, mut events, cache) = client(&venue);
        add_instruments(&cache);
        let mut cfg = config(&venue);
        cfg.max_report_pages = count.div_ceil(100) + 1;
        cfg.report_timeout_secs = 120;
        let core = ExecutionClientCore::new(
            trader_id(),
            ClientId::new("PREDICT"),
            *PREDICT_VENUE,
            OmsType::Netting,
            account_id(),
            AccountType::Cash,
            None,
            cache.clone(),
        );
        let clock = Box::leak(Box::new(AtomicTime::new(false, UnixNanos::from(OBSERVED))));
        let client = PredictExecutionClient::new_with_clock(core, cfg, clock).unwrap();
        let mut template = match_row(1);
        template["taker"]["amount"] = json!("1000000000000000000");
        template["taker"]["price"] = json!("10000000000000000");
        template["taker"]["fee"]["amount"] = json!("0");
        let rows = (0..count)
            .map(|index| {
                let mut row = template.clone();
                row["settlementId"] = json!(format!("profile-{index}"));
                row
            })
            .collect::<Vec<_>>();
        let mut previous = Vec::new();
        for replay in [false, true] {
            for (index, chunk) in rows.chunks(100).enumerate() {
                page(
                    &venue,
                    json!({"success":true,"data":chunk,"cursor":
                    (index + 1 < count.div_ceil(100)).then(||format!("page-{index}"))}),
                );
            }
            let task = client
                .generate_fill_reports_task(&command(Some(1)))
                .unwrap();
            let started = Instant::now();
            tokio::spawn(task.collection).await.unwrap();
            let collection_ns = started.elapsed().as_nanos() as u64;
            let started = Instant::now();
            let reports = (task.finish)().unwrap();
            let finish_ns = started.elapsed().as_nanos() as u64;
            assert_eq!(reports.len(), count);
            assert!(reports.iter().all(|report| {
                report.last_qty == Quantity::from(1)
                    && report.last_px == Price::from("0.010")
                    && report.commission.is_zero()
            }));
            let ids = reports
                .iter()
                .map(|report| report.trade_id)
                .collect::<Vec<_>>();
            assert_eq!(
                ids.iter().collect::<std::collections::HashSet<_>>().len(),
                count
            );
            if replay {
                assert_eq!(ids, previous);
            }
            previous = ids;
            let started = Instant::now();
            drop(reports);
            samples.push(
                json!({"fills":count,"replay":replay,"collection_ns":collection_ns,
                "finish_ns":finish_ns,"result_drop_ns":started.elapsed().as_nanos() as u64}),
            );
            assert!(events.try_recv().is_err());
            assert!(
                cache
                    .borrow()
                    .positions(None, None, None, None, None)
                    .is_empty()
            );
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_REPORT_STAGES_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file, &json!({
        "source":"synthetic loopback; direct Native collection/finish API; historical fills without retained Native order; fixture setup and assertions excluded; no venue writes or Native event application",
        "samples":samples,
    }))
    .unwrap();
}

#[tokio::test]
async fn match_reports_select_each_owned_leg_and_deduplicate_overlapping_pages() {
    let row = match_row(1);
    let mut multi = row.clone();
    multi["makers"][0]["signer"] = json!(WALLET);
    let server = server(vec![]).await;
    page(
        &server,
        json!({"success":true,"data":[row.clone()],"cursor":"+/=&"}),
    );
    page(&server, json!({"success":true,"data":[row,multi]}));
    let (client, mut rx, cache) = client(&server);
    add_instruments(&cache);
    let task = client.generate_fill_reports_task(&command(None)).unwrap();
    tokio::spawn(task.collection).await.unwrap();
    let reports = (task.finish)().unwrap();
    assert_eq!(reports.len(), 2);
    let taker = reports
        .iter()
        .find(|r| r.liquidity_side == LiquiditySide::Taker)
        .unwrap();
    let maker = reports
        .iter()
        .find(|r| r.liquidity_side == LiquiditySide::Maker)
        .unwrap();
    assert_eq!(taker.instrument_id, instrument_id(1));
    assert_eq!(taker.last_qty, Quantity::from(17));
    assert_eq!(taker.last_px, Price::from("0.620"));
    assert_eq!(taker.commission, Money::from("0.1 USDT"));
    assert_eq!(
        taker.trade_id,
        settlement_trade_id("rest-settlement-1", HASH.parse().unwrap())
    );
    assert_eq!(maker.instrument_id, instrument_id(2));
    assert_eq!(maker.order_side, OrderSide::Buy); // complementary token, not the taker's price/side
    assert_eq!(maker.last_px, Price::from("0.380"));
    assert_eq!(maker.commission, Money::from("0 USDT"));
    assert_ne!(taker.trade_id, maker.trade_id);
    assert!(rx.try_recv().is_err());
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    let requests = server.state.match_requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["signerAddress"], WALLET);
    assert_eq!(requests[1]["after"], "+/=&");
    assert!(!requests[0].contains_key("isSignerMaker"));
}

#[tokio::test]
async fn match_reports_apply_instrument_order_and_inclusive_time_scope() {
    let mut old = match_row(1);
    old["executedAt"] = json!("1970-01-01T00:00:00.000Z");
    old["settlementId"] = json!("old-settlement");
    let server = server(vec![]).await;
    page(
        &server,
        json!({"success":true,"data":[old,match_row(1),match_row(2)]}),
    );
    let (client, _, cache) = client(&server);
    add_instruments(&cache);
    let mut cmd = command(Some(1));
    cmd.venue_order_id = Some(HASH.into());
    cmd.start = Some(UnixNanos::from(1_000_000u64));
    cmd.end = cmd.start;
    let reports = client.generate_fill_reports(cmd).await.unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].instrument_id, instrument_id(1));
    assert_eq!(reports[0].ts_event, UnixNanos::from(1_000_000u64));
    assert_eq!(
        server.state.match_requests.lock().unwrap()[0]["marketId"],
        "42"
    );
}

#[tokio::test]
async fn match_history_fails_closed_on_truncation_cycles_errors_and_contradictory_duplicates() {
    for case in ["cycle", "bound", "http", "timeout", "duplicate"] {
        let server = server(vec![]).await;
        let (_, _, cache) = client(&server);
        add_instruments(&cache);
        // Recreate through the ordinary config surface for a short deadline/page bound.
        let mut cfg = config(&server);
        cfg.report_timeout_secs = 1;
        cfg.max_report_pages = 2;
        let core = ExecutionClientCore::new(
            trader_id(),
            ClientId::new("PREDICT"),
            *PREDICT_VENUE,
            OmsType::Netting,
            account_id(),
            AccountType::Cash,
            None,
            cache.clone(),
        );
        let clock = Box::leak(Box::new(AtomicTime::new(false, UnixNanos::from(OBSERVED))));
        let client = PredictExecutionClient::new_with_clock(core, cfg, clock).unwrap();
        let row = match_row(1);
        match case {
            "cycle" | "bound" => {
                page(
                    &server,
                    json!({"success":true,"data":[row.clone()],"cursor":"a"}),
                );
                page(
                    &server,
                    json!({"success":true,"data":[row],"cursor":if case=="cycle" {"a"} else {"b"}}),
                );
            }
            "http" => server.state.match_pages.lock().unwrap().push_back((
                429,
                json!({"success":false}),
                0,
            )),
            "timeout" => server.state.match_pages.lock().unwrap().push_back((
                200,
                json!({"success":true,"data":[row],"cursor":null}),
                1500,
            )),
            _ => {
                let mut contradictory = row.clone();
                contradictory["taker"]["fee"]["amount"] = json!("200000000000000000");
                page(&server, json!({"success":true,"data":[row,contradictory]}));
            }
        }
        let error = client
            .generate_fill_reports(command(Some(1)))
            .await
            .unwrap_err()
            .to_string();
        let expected = match case {
            "cycle" => "repeated a cursor",
            "bound" => "page bound",
            "http" => "rate limit",
            "timeout" => "timed out",
            _ => "contradictory fill facts",
        };
        assert!(error.to_lowercase().contains(expected), "{case}: {error}");
        assert!(
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );
    }
}

#[tokio::test]
async fn match_reports_reject_unknown_identity_context_precision_and_fee_contradictions() {
    for case in [
        "hash",
        "hash_context",
        "settlement",
        "foreign",
        "token",
        "market",
        "wei",
        "price",
        "maker_fee",
        "sell_share_fee",
    ] {
        let mut row = match_row(1);
        match case {
            "hash" => row["taker"]["hash"] = Value::Null,
            "hash_context" => {
                row["makers"][0]["hash"] = row["taker"]["hash"].clone();
                row["makers"][0]["signer"] = json!(WALLET);
            }
            "settlement" => row["settlementId"] = Value::Null,
            "foreign" => row["taker"]["signer"] = row["makers"][0]["signer"].clone(),
            "token" => row["taker"]["outcome"]["onChainId"] = json!("99"),
            "market" => row["market"]["id"] = json!(43),
            "wei" => row["taker"]["amount"] = json!("17.0"),
            "price" => row["taker"]["price"] = json!("1620100000000000000"),
            "future" => row["executedAt"] = json!("2030-01-01T00:00:00Z"),
            "maker_fee" => {
                row["taker"]["signer"] = row["makers"][0]["signer"].clone();
                row["makers"][0]["signer"] = json!(WALLET);
                row["makers"][0]["fee"]["amount"] = json!("1");
            }
            _ => {
                row["taker"]["quoteType"] = json!("Ask");
                row["taker"]["fee"]["type"] = json!("SHARES");
            }
        }
        let server = server(vec![]).await;
        page(&server, json!({"success":true,"data":[row]}));
        let (client, _, cache) = client(&server);
        add_instruments(&cache);
        let error = client
            .generate_fill_reports(command(None))
            .await
            .unwrap_err()
            .to_string();
        let expected = match case {
            "hash" => "lacks order hash",
            "hash_context" => "contradictory instrument",
            "settlement" => "settlement identity",
            "foreign" => "authenticated wallet",
            "token" => "outcome/token",
            "market" => "absent from Nautilus cache",
            "wei" => "integer wei",
            "price" => "outside the outcome range",
            "future" => "future",
            "maker_fee" => "maker fill",
            _ => "fill direction",
        };
        assert!(error.contains(expected), "{case}: {error}");
    }
}

#[tokio::test]
async fn share_fee_reports_require_reported_cash_and_preserve_gross_and_raw_fee_evidence() {
    for outcome in [1, 2] {
        let server = server(vec![]).await;
        let mut row = match_row(outcome);
        row["taker"]["fee"] = json!({"type":"SHARES","amount":"340000000000000001"});
        row["taker"]["price"] = json!("130000000000000000");
        page(&server, json!({"success":true,"data":[row.clone()]}));
        let (client, _, cache) = client(&server);
        add_instruments(&cache);
        seed_account(&cache);
        assert!(
            client
                .generate_fill_reports(command(Some(outcome)))
                .await
                .is_err()
        );
        let (client, _, cache) = reported_client(&server);
        add_instruments(&cache);
        seed_account(&cache); // is_reported alone is insufficient: calculate_account_state is true.
        page(&server, json!({"success":true,"data":[row.clone()]}));
        assert!(
            client
                .generate_fill_reports(command(Some(outcome)))
                .await
                .unwrap_err()
                .to_string()
                .contains("without calculated balances")
        );
        let (mut client, mut rx, cache) = reported_client(&server);
        add_instruments(&cache);
        let _portfolio = portfolio(&cache);
        connect_reported(&mut client, &mut rx).await;
        page(&server, json!({"success":true,"data":[row]}));
        let reports = client
            .generate_fill_reports(command(Some(outcome)))
            .await
            .unwrap();
        assert_eq!(reports.len(), 1);
        let report = &reports[0];
        assert_eq!(report.last_qty, Quantity::from(17));
        {
            assert_eq!(report.commission, Money::zero(Currency::USDT()));
            assert_eq!(
                report.quantity_commission,
                Some(Quantity::from("0.340000000000000001"))
            );
        }
        assert!(
            serde_json::to_value(report).unwrap().get("info").is_none(),
            "venue fee evidence must not extend the native FillReport"
        );
        assert_eq!(
            cache
                .borrow()
                .account(&account_id())
                .unwrap()
                .balance_total(Some(Currency::USDT()))
                .unwrap(),
            Money::from("1000 USDT")
        );
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn match_reports_validate_native_order_context_and_cumulative_size_before_dispatch() {
    for case in [
        "post_only",
        "limit",
        "side",
        "overfill",
        "duplicate_applied",
    ] {
        let server = server(vec![]).await;
        let (client, _, cache) = client(&server);
        add_instruments(&cache);
        let original = order_with_terms(1, OrderSide::Buy, case == "post_only");
        cache
            .borrow_mut()
            .add_order(original.clone(), None, None, false)
            .unwrap();
        cache
            .borrow_mut()
            .add_venue_order_id(&original.client_order_id(), &HASH.into(), false)
            .unwrap();
        let mut row = match_row(1);
        let mut rows = vec![];
        match case {
            "side" => row["taker"]["quoteType"] = json!("Ask"),
            "limit" => row["taker"]["price"] = json!("630000000000000000"),
            "overfill" => {
                row["taker"]["amount"] = json!("60000000000000000000");
                let mut other = row.clone();
                other["settlementId"] = json!("rest-settlement-2");
                other["transactionHash"] =
                    json!("0x4444444444444444444444444444444444444444444444444444444444444444");
                rows.push(other);
            }
            "duplicate_applied" => {
                seed_account(&cache);
                let mut engine = ExecutionEngine::new(
                    Rc::new(RefCell::new(VirtualClock::new())),
                    cache.clone(),
                    None,
                );
                let mut event = fill(1);
                event["settlementId"] = json!("rest-settlement-1");
                let report = parse_wallet_report(
                    &serde_json::from_value(event).unwrap(),
                    &original,
                    &instrument(1),
                    account_id(),
                    WALLET.parse().unwrap(),
                    UnixNanos::from(OBSERVED),
                )
                .unwrap()
                .unwrap();
                engine.reconcile_execution_report(&report);
                assert_eq!(
                    cache
                        .borrow()
                        .order(&original.client_order_id())
                        .unwrap()
                        .filled_qty(),
                    Quantity::from(17)
                );
                // The ID matches the wallet fill, but the reported commission contradicts it.
                row["taker"]["fee"]["amount"] = json!("200000000000000000");
            }
            _ => {}
        }
        rows.push(row);
        page(&server, json!({"success":true,"data":rows}));
        let result = client.generate_fill_reports(command(Some(1))).await;
        if matches!(case, "post_only" | "limit") {
            let fills = result.unwrap();
            assert_eq!(fills.len(), 1);
            assert_eq!(fills[0].last_qty, Quantity::from(17));
            assert_eq!(
                fills[0].last_px,
                Price::from(if case == "limit" { "0.630" } else { "0.620" })
            );
            assert_eq!(fills[0].liquidity_side, LiquiditySide::Taker);
            assert!(
                cache
                    .borrow()
                    .order(&original.client_order_id())
                    .unwrap()
                    .filled_qty()
                    .is_zero()
            );
            continue;
        }
        let error = result.unwrap_err().to_string();
        let expected = match case {
            "overfill" => "exceed original submitted",
            "duplicate_applied" => "applied currency fee lies outside",
            _ => "native order context",
        };
        assert!(error.contains(expected), "{case}: {error}");
        let filled = cache
            .borrow()
            .order_owned(&original.client_order_id())
            .unwrap()
            .filled_qty();
        assert_eq!(
            filled,
            if case == "duplicate_applied" {
                Quantity::from(17)
            } else {
                Quantity::from(0)
            }
        );
    }
}

#[tokio::test]
async fn match_reports_distinguish_complete_empty_from_invalid_scope_without_io() {
    let server = server(vec![]).await;
    let (client, _, _) = client(&server);
    let mut inverted = command(Some(1));
    inverted.start = Some(UnixNanos::from(2u64));
    inverted.end = Some(UnixNanos::from(1u64));
    assert!(
        client
            .generate_fill_reports(inverted)
            .await
            .unwrap_err()
            .to_string()
            .contains("inverted")
    );
    let mut invalid = command(None);
    invalid.venue_order_id = Some("12345".into());
    assert!(
        client
            .generate_fill_reports(invalid)
            .await
            .unwrap_err()
            .to_string()
            .contains("exchange hash")
    );
    assert!(server.state.match_requests.lock().unwrap().is_empty());
    page(&server, json!({"success":true,"data":[]}));
    assert!(
        client
            .generate_fill_reports(command(None))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(server.state.match_requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn rest_full_fill_then_wallet_replay_stays_connected_and_does_not_double_count() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = client(&server);
    add_instruments(&cache);
    seed_account(&cache);
    let original = order(1);
    cache
        .borrow_mut()
        .add_order(original.clone(), None, None, false)
        .unwrap();
    cache
        .borrow_mut()
        .add_venue_order_id(&original.client_order_id(), &HASH.into(), false)
        .unwrap();
    let mut engine = ExecutionEngine::new(
        Rc::new(RefCell::new(VirtualClock::new())),
        cache.clone(),
        None,
    );
    client.start().unwrap();
    client.connect().await.unwrap();
    let mut row = match_row(1);
    row["taker"]["amount"] = json!("100000000000000000000");
    page(
        &server,
        json!({"success":true,"data":[row.clone(),row.clone()]}),
    );
    let reports = client
        .generate_fill_reports(command(Some(1)))
        .await
        .unwrap();
    assert_eq!(reports.len(), 1);
    engine.reconcile_execution_report(&ExecutionReport::Fill(Box::new(reports[0].clone())));
    assert_eq!(
        cache
            .borrow()
            .order(&original.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Filled
    );
    // A second REST query returns the same authoritative fill, not an overfill.
    page(&server, json!({"success":true,"data":[row]}));
    let repeated = client
        .generate_fill_reports(command(Some(1)))
        .await
        .unwrap();
    assert_eq!(repeated.len(), 1);
    engine.reconcile_execution_report(&ExecutionReport::Fill(Box::new(repeated[0].clone())));
    let mut duplicate = fill(1);
    duplicate["settlementId"] = json!("rest-settlement-1");
    duplicate["fill"]["executedSizeWei"] = json!("100000000000000000000");
    duplicate["fill"]["executedValueWei"] = json!("62000000000000000000");
    server.wallet(duplicate);
    let mut ack = event("orderAccepted", 1);
    ack["details"]["quantityFilled"] = json!("100");
    let (control_hash, control) = super::wallet_lifecycle::control(&mut engine, &ack);
    server.wallet(ack);
    server.wallet(control);
    let ExecutionEvent::Order(native @ nautilus_model::events::OrderEventAny::Canceled(_)) =
        next(&mut rx).await
    else {
        panic!("replayed fill and historical ACK must be suppressed before control event");
    };
    assert_eq!(native.venue_order_id(), Some(control_hash));
    engine.process(&native);
    assert!(client.is_connected());
    assert_eq!(
        cache
            .borrow()
            .order(&original.client_order_id())
            .unwrap()
            .filled_qty(),
        Quantity::from(100)
    );
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None)[0].quantity,
        Quantity::from(100)
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn captured_real_match_precision_preserves_atomic_gross_and_share_fees() {
    let mut history: Value =
        serde_json::from_str(include_str!("../fixtures/private_api/matches.json")).unwrap();
    for row in history["data"].as_array_mut().unwrap() {
        row["taker"]["signer"] = json!(WALLET);
    }
    let market: nautilus_predict::http::account_models::PredictExecutionMarket =
        serde_json::from_value(history["data"][0]["market"].clone()).unwrap();
    let server = server(vec![]).await;
    page(&server, history);
    let (_, _, cache) = client(&server);
    for outcome in &market.outcomes {
        let mut instrument = instrument(u8::try_from(outcome.index_set).unwrap());
        instrument.id = format!("{}-{}.PREDICT", market.id, outcome.index_set)
            .parse()
            .unwrap();
        instrument.raw_symbol = Symbol::new(outcome.on_chain_id.to_string());
        instrument.price_precision = market.decimal_precision;
        instrument.price_increment = Price::from("0.01");
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument))
            .unwrap();
    }
    let core = ExecutionClientCore::new(
        trader_id(),
        ClientId::new("PREDICT"),
        *PREDICT_VENUE,
        OmsType::Netting,
        account_id(),
        AccountType::Cash,
        None,
        cache.clone(),
    );
    let observed = chrono::DateTime::parse_from_rfc3339("2026-10-15T00:00:00Z")
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap() as u64;
    let clock = Box::leak(Box::new(AtomicTime::new(false, UnixNanos::from(observed))));
    let mut cfg = config(&server);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", server.http).into(),
    ));
    // Fixture account uses the same native reported/calculate=false contract as the factory.
    let state = AccountState::new(
        account_id(),
        AccountType::Cash,
        vec![AccountBalance::new(
            Money::from("1000 USDT"),
            Money::from("0 USDT"),
            Money::from("1000 USDT"),
        )],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        None,
    );
    cache
        .borrow_mut()
        .add_account(AccountAny::Cash(CashAccount::new(state, false, false)))
        .unwrap();
    let client = PredictExecutionClient::new_with_clock(core, cfg, clock).unwrap();
    let reports = client.generate_fill_reports(command(None)).await.unwrap();
    assert_eq!(reports.len(), 2);
    assert_eq!(
        reports
            .iter()
            .map(|r| r.last_qty.as_decimal())
            .sum::<Decimal>(),
        Decimal::from_str_exact("33.398999999999999999").unwrap()
    );
    assert!(
        reports
            .iter()
            .all(|r| r.quantity_commission.is_some() && r.commission.is_zero())
    );
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
}

#[tokio::test]
async fn millisecond_match_timestamp_does_not_reject_known_hash_submitted_within_same_millisecond()
{
    let server = server(vec![]).await;
    let (client, _, cache) = client(&server);
    add_instruments(&cache);
    seed_account(&cache);
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    let mut own = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .instrument_id(instrument_id(1))
        .side(OrderSide::Buy)
        .quantity(Quantity::from(100))
        .price(Price::from("0.620"))
        .build();
    own.apply(factory.generate_order_submitted(&own, 1_000_999.into()))
        .unwrap();
    own.apply(factory.generate_order_accepted(
        &own,
        HASH.into(),
        1_001_100.into(),
        1_001_100.into(),
    ))
    .unwrap();
    cache
        .borrow_mut()
        .add_order(own.clone(), None, None, false)
        .unwrap();
    cache
        .borrow_mut()
        .add_venue_order_id(&own.client_order_id(), &HASH.into(), false)
        .unwrap();
    page(&server, json!({"success":true,"data":[match_row(1)]}));
    let reports = client
        .generate_fill_reports(command(Some(1)))
        .await
        .unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].client_order_id, Some(own.client_order_id()));
    assert_eq!(reports[0].ts_event, UnixNanos::from(1_000_000u64));
    assert_eq!(reports[0].last_qty, Quantity::from(17));
    let mut engine = cancellations::engine(&cache);
    engine.reconcile_execution_report(&ExecutionReport::Fill(Box::new(reports[0].clone())));
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::PartiallyFilled
    );
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None)[0].quantity,
        Quantity::from(17)
    );
    // Exact ownership and quantity checks remain necessary, independent of clocks.
    let mut overfill = match_row(1);
    overfill["taker"]["amount"] = json!("101000000000000000000");
    overfill["settlementId"] = json!("oversized-settlement");
    page(&server, json!({"success":true,"data":[overfill]}));
    assert!(
        client
            .generate_fill_reports(command(Some(1)))
            .await
            .is_err()
    );
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .filled_qty(),
        Quantity::from(17)
    );
}
