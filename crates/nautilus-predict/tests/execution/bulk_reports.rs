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

//! Native bulk/mass contracts and all-or-nothing failure cases, on loopback only.
use nautilus_common::messages::execution::GenerateOrderStatusReports;

use super::*;

fn command(open: bool) -> GenerateOrderStatusReports {
    GenerateOrderStatusReports::new(
        UUID4::new(),
        UnixNanos::from(OBSERVED),
        open,
        None,
        None,
        None,
        None,
        None,
    )
}
#[tokio::test]
async fn bulk_and_mass_reports_preserve_precision_and_duplicate_trade_identity() {
    run("valid").await;
}
#[tokio::test]
async fn bulk_and_mass_fail_closed_on_incomplete_conflicting_and_unverified_sources() {
    for case in [
        "duplicate_order",
        "cursor_cycle",
        "page_failure",
        "page_cap",
        "holdings_duplicate",
        "generation",
        "bad_external_hash",
        "conflicting",
        "unstable",
        "incomplete",
    ] {
        run(case).await;
    }
}
#[tokio::test]
#[ignore = "explicit offline report fixture recording; never overwrite final JSON"]
async fn record_bulk_report_json() {
    let mut sessions = vec![];
    for case in [
        "valid",
        "duplicate_order",
        "cursor_cycle",
        "page_failure",
        "page_cap",
        "holdings_duplicate",
        "generation",
        "bad_external_hash",
        "conflicting",
        "unstable",
        "incomplete",
    ] {
        sessions.push(run(case).await);
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/bulk_mass_reports_synthetic_20261016.json");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic offline native report contract; not mainnet observations","sessions":sessions})).unwrap();
}
async fn run(case: &str) -> Value {
    let private = server(vec![]).await;
    private
        .state
        .create_replies
        .lock()
        .unwrap()
        .push_back((500, 0, vec![]));
    let (mut client, mut rx, cache, _portfolio, order, clock) =
        submissions::connected_with_clock(&private, 1).await;
    let mut engine = submissions::engine(&cache);
    client.submit_order(submissions::submit(&order)).unwrap();
    submissions::apply(&mut engine, &next(&mut rx).await);
    super::exit_orders::barrier("unknown POST", || {
        !private.state.creations.lock().unwrap().is_empty()
    })
    .await;
    let body = private.state.creations.lock().unwrap()[0].clone();
    let source: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/matches_gap_native_feedback_synthetic_20261015.json"
    ))
    .unwrap();
    let mut matches = source["sessions"][0]["matches"].as_array().unwrap().clone();
    for row in &mut matches {
        row["executedAt"] = json!("1970-01-01T00:00:00.002Z");
        for leg in row["makers"].as_array_mut().unwrap() {
            if leg["signer"] == WALLET {
                leg["hash"] = body["data"]["order"]["hash"].clone();
            }
        }
    }
    let mut row = rest_order_data("FILLED", 1, OrderSide::Buy);
    row["order"] = body["data"]["order"].clone();
    row["amountFilled"] = json!("100000000000000000000");
    let hash = body["data"]["order"]["hash"].as_str().unwrap().to_owned();
    private
        .state
        .orders_by_hash
        .lock()
        .unwrap()
        .insert(hash, json!({"success":true,"data":row.clone()}));
    let mut position = holding(1);
    position["amount"] = json!("100000000000000000000");
    *private.state.position_snapshot.lock().unwrap() = Some(vec![position]);
    let good_matches = matches.clone();
    match case {
        // Account refresh reads the same OPEN endpoint. Keep invalid data persistent so
        // it cannot consume a one-shot fault intended for the mass-status collector.
        "duplicate_order" => private
            .state
            .open_orders
            .lock()
            .unwrap()
            .extend([row.clone(), row.clone()]),
        "cursor_cycle" => private.state.open_pages.lock().unwrap().extend([
            json!({"success":true,"cursor":"same","data":[]}),
            json!({"success":true,"cursor":"same","data":[]}),
        ]),
        "page_failure" => {
            private
                .state
                .match_pages
                .lock()
                .unwrap()
                .push_back((503, json!({"success":false}), 0))
        }
        "page_cap" => {
            for i in 0..100 {
                private
                    .state
                    .open_pages
                    .lock()
                    .unwrap()
                    .push_back(json!({"success":true,"cursor":format!("page-{i}"),"data":[]}));
            }
        }
        "holdings_duplicate" => {
            let mut entries = private.state.position_snapshot.lock().unwrap();
            let rows = entries.as_mut().unwrap();
            rows.push(rows[0].clone());
        }
        "bad_external_hash" => {
            let mut extra = row.clone();
            extra["order"]["hash"] = json!(format!("0x{}", "77".repeat(32)));
            private
                .state
                .open_orders
                .lock()
                .unwrap()
                .extend([row.clone(), extra]);
        }
        "conflicting" => {
            let mut bad = matches[0].clone();
            bad["makers"][0]["amount"] = json!("32000000000000000000");
            matches.push(bad);
        }
        "unstable" => {
            let mut changed = row.clone();
            changed["status"] = json!("CANCELLED");
            private
                .state
                .orders
                .lock()
                .unwrap()
                .push_back(json!({"success":true,"data":changed}));
        }
        "incomplete" => {
            matches.pop();
        }
        "generation" => {}
        "valid" => {
            matches.push(matches[0].clone());
        }
        _ => unreachable!(),
    }
    *private.state.match_snapshot.lock().unwrap() = Some(matches.clone());
    let result = if case == "generation" {
        let task = client
            .generate_order_status_reports_task(&command(false))
            .unwrap();
        tokio::spawn(task.collection).await.unwrap();
        client.disconnect().await.unwrap();
        let error = task.result.await.unwrap_err();
        while rx.try_recv().is_ok() {}
        connect_reported(&mut client, &mut rx).await;
        Err(error)
    } else {
        client.generate_mass_status(None).await
    };
    let mut output = json!({"case":case,"request":body,"matches":matches,"nativeStatusBefore":cache.borrow().order(&order.client_order_id()).unwrap().status().to_string(),"mainnetWrites":0});
    if case == "valid" {
        let mass = result.unwrap().unwrap();
        assert!(mass.reports_complete());
        let reports = mass.order_reports();
        let fills = mass.fill_reports();
        assert_eq!(reports.len(), 1);
        assert_eq!(fills.values().map(Vec::len).sum::<usize>(), 2);
        let report = reports.values().next().unwrap();
        assert_eq!(report.order_status, OrderStatus::Filled);
        assert_eq!(report.filled_qty, Quantity::from(100));
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Submitted,
            "collection cannot mutate Cache"
        );
        output["massStatus"] = serde_json::to_value(&mass).unwrap();
        let deferred = client
            .generate_order_status_reports(&command(false))
            .await
            .unwrap_err();
        assert!(deferred.to_string().contains("defer status until applied"));
        output["deferredStatus"] = json!(deferred.to_string());
        let packet = timeout(Duration::from_secs(3), async {
            loop {
                if let ExecutionEvent::Report(ExecutionReport::OrderWithFills(report, fills)) =
                    next(&mut rx).await
                {
                    break (report, fills);
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(packet.1.len(), 2);
        output["nativeFillPacket"] = json!({"order":packet.0,"fills":packet.1});
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Submitted
        );
        engine.reconcile_execution_report(&ExecutionReport::OrderWithFills(packet.0, packet.1));
        let applied = client
            .generate_order_status_reports(&command(false))
            .await
            .unwrap();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].order_status, OrderStatus::Filled);
        let repeated = client.generate_mass_status(None).await.unwrap().unwrap();
        let report = repeated.order_reports().values().next().unwrap().clone();
        engine.reconcile_execution_report(&ExecutionReport::OrderWithFills(
            Box::new(report),
            repeated.fill_reports().values().next().unwrap().clone(),
        ));
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .trade_ids()
                .len(),
            2
        );
        assert_eq!(
            cache.borrow().positions_open(None, None, None, None, None)[0].quantity,
            Quantity::from(100)
        );
        assert!(
            client
                .generate_order_status_reports(&command(true))
                .await
                .unwrap()
                .is_empty()
        );
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(2)))
            .unwrap();
        let mut other_outcome = command(false);
        other_outcome.instrument_id = Some(instrument_id(2));
        assert!(
            client
                .generate_order_status_reports(&other_outcome)
                .await
                .unwrap()
                .is_empty()
        );
        let mut historical = command(false);
        historical.start = Some(UnixNanos::from(1));
        assert!(
            client
                .generate_order_status_reports(&historical)
                .await
                .is_ok()
        );
        // Fill timestamps remain 2ms; the window starts later, at 3ms.
        clock.set_time(UnixNanos::from(3_000_000));
        let bounded = client.generate_mass_status(Some(0)).await.unwrap().unwrap();
        assert!(bounded.lookback_start().is_some());
        assert_eq!(
            bounded.fill_reports().values().map(Vec::len).sum::<usize>(),
            2
        );
    } else {
        let error = result
            .expect_err(&format!("bulk case {case} must fail closed"))
            .to_string();
        let expected = match case {
            "duplicate_order" => "duplicate hash",
            "cursor_cycle" => "repeated a cursor",
            "page_failure" => "503",
            "page_cap" => "exceeded configured bound",
            "holdings_duplicate" => "duplicate outcome holdings",
            "generation" => "generation stopped",
            "bad_external_hash" => "signed hash",
            "conflicting" => "contradictory",
            "unstable" => "changed during bulk",
            "incomplete" => "complete confirmed inventory prefix",
            _ => unreachable!(),
        };
        assert!(error.contains(expected), "{case}: {error}");
        output["error"] = json!(error);
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Submitted
        );
        assert!(
            cache
                .borrow()
                .positions(None, None, None, None, None)
                .is_empty()
        );
        *private.state.match_snapshot.lock().unwrap() = Some(good_matches);
        private.state.open_pages.lock().unwrap().clear();
        private.state.open_orders.lock().unwrap().clear();
        private.state.orders.lock().unwrap().clear();
        let mut repaired_holding = holding(1);
        repaired_holding["amount"] = json!("100000000000000000000");
        *private.state.position_snapshot.lock().unwrap() = Some(vec![repaired_holding]);
        let repaired = client.generate_mass_status(None).await.unwrap().unwrap();
        assert_eq!(
            repaired
                .fill_reports()
                .values()
                .map(Vec::len)
                .sum::<usize>(),
            2
        );
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Submitted
        );
        output["repairedMassStatus"] = serde_json::to_value(repaired).unwrap();
    }
    output["matchResponses"] = json!(private.state.match_response_log.lock().unwrap().clone());
    output["positionResponses"] =
        json!(private.state.position_response_log.lock().unwrap().clone());
    output["orderLists"] = json!(private.state.order_list_log.lock().unwrap().clone());
    output["orderResponses"] = json!(private.state.order_response_log.lock().unwrap().clone());
    client.disconnect().await.unwrap();
    output
}
