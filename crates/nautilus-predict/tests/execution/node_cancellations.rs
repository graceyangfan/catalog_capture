// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Cancellation command and reconciliation scenarios on the product host.

use super::{super::live_node::strategy, *};
#[path = "../support/batch_probe.rs"]
mod batch_probe;
#[path = "../support/native_input_journal.rs"]
mod native_input_journal;

async fn replay_cancel_reports(
    fixture: &HostFixture,
    rows: &Rc<RefCell<Vec<Value>>>,
    wire: &[Value],
) {
    let count = |event: &Value| {
        rows.borrow()
            .iter()
            .filter(|row| {
                row["type"] == "RawOrderStatusReport"
                    && row["report"]["venue_order_id"] == event["orderHash"]
                    && row["report"]["order_status"] == "CANCELED"
            })
            .count()
    };
    let before = wire.iter().map(&count).collect::<Vec<_>>();
    for event in wire {
        let mut event = event.clone();
        event["timestamp"] = json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
        fixture.backing.wallet(event.clone());
        fixture.backing.wallet(event);
    }
    barrier("both wallet duplicates observed for each order", || {
        wire.iter()
            .zip(&before)
            .all(|(event, before)| count(event) >= before + 2)
    })
    .await;
}

async fn batch_probe_case(mode: &str) -> Value {
    eprintln!("offline batch mode: {mode}");
    use std::cell::Cell;

    use nautilus_trading::StrategyCore;
    let f = fixture(false, 2).await;
    let gap = mode.ends_with("_gap");
    let batch_timeout = mode == "batch_timeout";
    let recovery = mode.starts_with("recovery_") || batch_timeout;
    let single = mode == "single_locked_gap" || (recovery && !batch_timeout);
    let release = Arc::new(tokio::sync::Notify::new());
    if gap || recovery {
        *f.backing.state.remove_gate.lock().unwrap() = Some(release.clone());
    }
    *f.backing.state.remove_replies.lock().unwrap() = [match mode {
        "removed" | "preflight" => (200, json!({"echoRemoved":true}), 0),
        "mixed" => (200, json!({"echoMixedTerminal":true}), 0),
        "locked" | "locked_gap" => (200, json!({"echoRemovalLocked":true}), 0),
        "locked_all" | "locked_all_gap" | "single_locked_gap" => {
            (200, json!({"echoAllRemovalLocked":true}), 0)
        }
        "rejected" => (
            400,
            json!({"success":false,"message":"fixture explicit batch rejection"}),
            0,
        ),
        "unknown" => (
            500,
            json!({"success":false,"message":"fixture unknown batch outcome"}),
            0,
        ),
        "recovery_removed_gap" => (200, json!({"echoRemoved":true}), 0),
        "recovery_timeout_query" | "recovery_exhausted_timeout" | "batch_timeout" => (
            200,
            json!({"echoNoop":true}),
            2000, // Withhold the entire response beyond the real client's one-second deadline.
        ),
        "recovery_noop" | "recovery_noop_query" => (200, json!({"echoNoop":true}), 0),
        "recovery_unknown_query"
        | "recovery_unknown_mass"
        | "recovery_exhausted_open"
        | "recovery_exhausted_error" => (
            500,
            json!({"success":false,"message":"fixture unknown cancel outcome"}),
            0,
        ),
        _ => panic!("unsupported batch fixture"),
    }]
    .into();
    let mut wire = vec![];
    for (index, quantity) in [100u64, 50].into_iter().enumerate() {
        let mut row = rest_order_data("OPEN", 1, OrderSide::Buy);
        row["id"] = json!((12345 + index).to_string());
        row["order"]["salt"] = json!((index + 1).to_string());
        row["order"]["makerAmount"] = json!("1000000000000000000");
        row["order"]["takerAmount"] = json!((u128::from(quantity) * 10u128.pow(18)).to_string());
        row["order"]["expiration"] = json!("4102444800");
        let fields = serde_json::from_value(row["order"].clone()).unwrap();
        let hash =
            nautilus_predict::signing::PredictOrderHasher::new(PredictEnvironment::BnbMainnet)
                .order_hash(
                    &fields,
                    nautilus_predict::common::contracts::PredictExchangeType::Standard,
                );
        row["order"]["hash"] = json!(format!("{hash:#x}"));
        f.backing
            .state
            .open_orders
            .lock()
            .unwrap()
            .push(row.clone());
        f.backing
            .state
            .orders_by_hash
            .lock()
            .unwrap()
            .insert(format!("{hash:#x}"), json!({"success":true,"data":row}));
        let body = json!({"data":{"strategy":"LIMIT","pricePerShare":if index == 0 {"10000000000000000"} else {"20000000000000000"},"order":row["order"]}});
        let mut event = exit_orders::wire_event(&body, "orderCancelled", 1, Decimal::ZERO);
        event["orderId"] = row["id"].clone();
        wire.push(event);
    }
    let mut profile = f.config.replace("PREDICT-fixture", "PREDICT-001").replace(
        "orderbook_removal_enabled = false",
        "orderbook_removal_enabled = true",
    );
    if recovery {
        // Configure only existing Native checks. Query and batch-report recovery are
        // isolated so either path must supply the missing terminal evidence itself.
        profile = if mode.starts_with("recovery_exhausted_") {
            profile.replace(
                "inflight_check_threshold_ms = 5000",
                "inflight_check_threshold_ms = 200\ninflight_check_retries = 3",
            )
        } else if mode == "recovery_unknown_mass" {
            profile.replace("open_check_interval_secs = 10.0", "open_check_interval_secs = 0.2\nopen_check_threshold_ms = 0\nopen_check_open_only = false")
        } else {
            profile.replace(
                "inflight_check_threshold_ms = 5000",
                "inflight_check_threshold_ms = 200\ninflight_check_retries = 100",
            )
        };
    }
    if mode.contains("timeout") {
        profile = profile
            .replace(
                "orderbook_removal_enabled = true",
                "orderbook_removal_enabled = true\ntimeout_secs = 1",
            )
            // Query after HTTP timeout, with room for three Native checks before
            // the fixture barrier expires. Only existing Native scheduling runs.
            .replace(
                "inflight_check_threshold_ms = 200",
                "inflight_check_threshold_ms = 1200",
            );
    }
    let signer = Arc::new(
        PredictAuthSigner::new(
            submissions::PUBLIC_KEY.into(),
            PredictEnvironment::BnbMainnet,
            Some(WALLET),
        )
        .unwrap(),
    );
    let mut prepared = host::prepare(
        host::NodeProfile::parse(&profile).unwrap(),
        KEY.into(),
        signer,
    )
    .await
    .unwrap();
    let rows = Rc::new(RefCell::new(vec![]));
    let audit = native_input_journal::NativeInputJournal::attach("recover", rows.clone());
    let complete = Rc::new(Cell::new(false));
    let resume = Rc::new(Cell::new(false));
    let engine = prepared.node.kernel().exec_engine.clone();
    let handle = prepared.node.handle();
    prepared
        .node
        .add_strategy(batch_probe::BatchProbe {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::new("RESTING-BATCH-001")),
                external_order_instrument_ids: Some(vec![prepared.instruments[0]]),
                ..Default::default()
            }),
            instrument: prepared.instruments[0],
            market_id: 42,
            outcome: 1,
            phase: "recover",
            place: false,
            handle: handle.clone(),
            confirmation: Rc::new(RefCell::new(None)),
            journal: rows.clone(),
            complete: complete.clone(),
            ids: vec![],
            book: None,
            modified: false,
            batch_sent: false,
            single_cancel: single,
            resume_cancel: (gap || recovery).then(|| resume.clone()),
            query_closed_before_cancel: mode.starts_with("recovery_exhausted_"),
            modify_rejections: 0,
            last_account: None,
        })
        .unwrap();
    let driver = async {
        barrier("batch probe startup finished", || {
            rows.borrow().iter().any(|r| r["type"] == "StrategyOnStart")
        })
        .await;
        if mode == "preflight" {
            f.backing
                .state
                .orders_by_hash
                .lock()
                .unwrap()
                .values_mut()
                .next()
                .unwrap()["data"]["currency"] = json!("invalid-currency");
        }

        let socket_count = f.backing.state.sockets.lock().unwrap().len();
        if gap {
            barrier("cancel write reached venue", || {
                f.backing.state.removals.lock().unwrap().len() == 1
            })
            .await;
            f.backing
                .state
                .sockets
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .send(Message::Close(None))
                .unwrap();
            barrier("Native execution client observes wallet gap", || {
                !engine
                    .borrow_mut()
                    .get_client_adapter_mut(&"PREDICT".into())
                    .unwrap()
                    .is_connected()
            })
            .await;
            rows.borrow_mut()
                .push(json!({"type":"WalletGapBeforeCancelReceipt"}));
            if !recovery {
                release.notify_one();
            }
        }
        if batch_timeout {
            recover_batch_timeout_probe(&f, &rows, &wire).await;
            barrier("Native account unlocked after batch timeout", || {
                complete.get()
            })
            .await;
            handle.stop();
            return;
        }
        if recovery {
            recover_cancel_probe(&f, mode, &rows, &resume, &wire, || {
                engine
                    .borrow_mut()
                    .get_client_adapter_mut(&"PREDICT".into())
                    .unwrap()
                    .is_connected()
            })
            .await;
            barrier("Native account unlocked after recovery", || complete.get()).await;
            handle.stop();
            return;
        }
        barrier("one actual HTTP batch response", || {
            f.backing.state.removal_responses.lock().unwrap().len() == 1
        })
        .await;
        let posted = f.backing.state.removals.lock().unwrap()[0].clone();
        assert_eq!(
            posted["data"]["ids"].as_array().unwrap().len(),
            if mode == "preflight" || single { 1 } else { 2 }
        );
        if mode == "removed" || mode == "mixed" {
            barrier("two independently resolved canceled callbacks", || {
                rows.borrow()
                    .iter()
                    .filter(|r| r["type"] == "StrategyOrderCallback" && r["kind"] == "Canceled")
                    .count()
                    == 2
            })
            .await;
        } else if mode == "unknown" {
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(
                !rows
                    .borrow()
                    .iter()
                    .any(|r| r["type"] == "StrategyOrderCallback"
                        && (r["kind"] == "Canceled" || r["kind"] == "CancelRejected"))
            );
        } else {
            barrier("individual standard cancel rejections", || {
                rows.borrow()
                    .iter()
                    .filter(|r| {
                        r["type"] == "StrategyOrderCallback" && r["kind"] == "CancelRejected"
                    })
                    .count()
                    == if mode == "rejected" || mode == "locked_all" || mode == "locked_all_gap" {
                        2
                    } else {
                        1
                    }
            })
            .await;
        }
        if mode == "locked" || mode == "locked_all" || gap {
            let expected = if mode == "locked" || mode == "locked_gap" || single {
                1
            } else {
                2
            };
            let rejected = rows
                .borrow()
                .iter()
                .filter(|r| r["type"] == "StrategyOrderCallback" && r["kind"] == "CancelRejected")
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(rejected.len(), expected);
            assert!(rejected.iter().all(|r| r["cachedStatus"] == "ACCEPTED"));
            assert!(rejected.iter().all(|r| {
                r["reason"]
                    .as_str()
                    .unwrap()
                    .contains("removal lock window")
            }));
            assert_eq!(
                f.backing.state.open_orders.lock().unwrap().len(),
                if single { 2 } else { expected }
            );
            // Rejection leaves the order live and Native collateral reserved. It is
            // neither a terminal cancellation nor an invitation to retry the write.
            let account = rows
                .borrow()
                .iter()
                .rfind(|r| r["type"] == "AccountStateApplied")
                .unwrap()
                .clone();
            assert!(
                account["balances"][0]["locked"]
                    .as_str()
                    .unwrap()
                    .parse::<rust_decimal::Decimal>()
                    .unwrap()
                    > rust_decimal::Decimal::ZERO
            );
        }
        if gap {
            assert!(
                !rows
                    .borrow()
                    .iter()
                    .any(|r| r["type"] == "StrategyOrderCallback" && r["kind"] == "Canceled")
            );
            let rejected_ids =
                f.backing.state.removal_responses.lock().unwrap()[0]["body"]["rejected"]
                    .as_array()
                    .unwrap()
                    .clone();
            let expected = wire
                .iter()
                .filter(|event| rejected_ids.contains(&event["orderId"]))
                .map(|event| event["orderHash"].as_str().unwrap().to_string())
                .collect::<std::collections::BTreeSet<_>>();
            let actual = rows
                .borrow()
                .iter()
                .filter(|r| r["type"] == "StrategyOrderCallback" && r["kind"] == "CancelRejected")
                .map(|r| r["venueOrderId"].as_str().unwrap().to_string())
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(actual, expected);
            *f.backing.state.remove_gate.lock().unwrap() = None;
            barrier("wallet transport resubscribes", || {
                f.backing.state.sockets.lock().unwrap().len() > socket_count
            })
            .await;
            let removed = f.backing.state.removal_responses.lock().unwrap()[0]["body"]["removed"]
                .as_array()
                .unwrap()
                .clone();
            for row in f.backing.state.orders_by_hash.lock().unwrap().values_mut() {
                if removed.contains(&row["data"]["id"]) {
                    row["data"]["status"] = json!("CANCELLED");
                }
            }
            let removed_events = wire
                .iter()
                .filter(|event| removed.contains(&event["orderId"]))
                .cloned()
                .collect::<Vec<_>>();
            replay_cancel_reports(&f, &rows, &removed_events).await;
            barrier("Native account recovery permits a new command", || {
                engine
                    .borrow_mut()
                    .get_client_adapter_mut(&"PREDICT".into())
                    .unwrap()
                    .is_connected()
            })
            .await;
            rows.borrow_mut()
                .push(json!({"type":"NativeReadyAfterWalletGap"}));
            assert_eq!(
                f.backing.state.removals.lock().unwrap().len(),
                1,
                "no automatic cancel retry"
            );
            f.backing.state.remove_replies.lock().unwrap().push_back((
                200,
                json!({"echoRemoved":true}),
                0,
            ));
            resume.set(true);
            barrier(
                "explicit strategy cancellation reaches HTTP after rejection",
                || f.backing.state.removal_responses.lock().unwrap().len() == 2,
            )
            .await;
            barrier("both orders canceled through Native", || {
                rows.borrow()
                    .iter()
                    .filter(|row| {
                        row["type"] == "StrategyOrderCallback" && row["kind"] == "Canceled"
                    })
                    .count()
                    == 2
            })
            .await;
        }
        f.backing.state.open_orders.lock().unwrap().clear();
        replay_cancel_reports(&f, &rows, &wire).await;
        barrier("Native account unlocked after batch", || complete.get()).await;
        handle.stop();
    };
    let (result, ()) = tokio::join!(prepared.node.run(), driver);
    result.unwrap();
    prepared.node.dispose();
    audit.detach();
    let rows = rows.borrow();
    assert_eq!(
        rows.iter()
            .filter(|r| r["type"] == "StrategyOrderCallback" && r["kind"] == "Canceled")
            .count(),
        2
    );
    assert_eq!(
        rows.iter()
            .filter(|r| r["type"] == "ModifyTermsPreserved")
            .count(),
        2
    );
    if gap && !recovery {
        assert_eq!(
            rows.iter()
                .filter(
                    |row| row["type"] == "StrategyOrderCallback" && row["kind"] == "CancelRejected"
                )
                .count(),
            if mode == "locked_all_gap" { 2 } else { 1 }
        );
    }
    assert!(f.backing.state.creations.lock().unwrap().is_empty());
    assert!(f.backing.state.open_orders.lock().unwrap().is_empty());
    let result = json!({"source":"offline synthetic Rust strategy + Native LiveNode; loopback venue only","mode":mode,"removals":f.backing.state.removals.lock().unwrap().clone(),"receipts":f.backing.state.removal_responses.lock().unwrap().clone(),"orderQueries":f.backing.state.order_response_log.lock().unwrap().clone(),"orderLists":f.backing.state.order_list_log.lock().unwrap().clone(),"native":rows.clone()});
    assert_eq!(
        f.backing.state.removals.lock().unwrap().len(),
        if (gap || recovery) && !batch_timeout {
            2
        } else {
            1
        },
        "only an explicit strategy command may retry cancellation"
    );
    f.task.abort();
    result
}

/// Both cancellations time out in one HTTP write; Native queries resolve each ID independently.
async fn recover_batch_timeout_probe(
    fixture: &HostFixture,
    rows: &Rc<RefCell<Vec<Value>>>,
    wire: &[Value],
) {
    let state = &fixture.backing.state;
    barrier("two-ID removal reached venue", || {
        state.removals.lock().unwrap().len() == 1
    })
    .await;
    let request = state.removals.lock().unwrap()[0].clone();
    let ids = request["data"]["ids"].as_array().unwrap();
    assert_eq!(ids.len(), 2);
    assert!(wire.iter().all(|event| ids.contains(&event["orderId"])));
    let accepted_count = |hash: &str| {
        rows.borrow()
            .iter()
            .filter(|row| {
                row["type"] == "RawOrderStatusReport"
                    && row["report"]["venue_order_id"] == hash
                    && row["report"]["order_status"] == "ACCEPTED"
            })
            .count()
    };
    let canceled_count = |hash: &str| {
        rows.borrow()
            .iter()
            .filter(|row| {
                row["type"] == "StrategyOrderCallback"
                    && row["venueOrderId"] == hash
                    && row["kind"] == "Canceled"
            })
            .count()
    };
    let locked = || {
        rows.borrow()
            .iter()
            .rfind(|row| row["type"] == "AccountStateApplied")
            .and_then(|row| row["balances"][0]["locked"].as_str())
            .map(|value| value.parse::<Decimal>().unwrap())
    };
    let before = wire
        .iter()
        .map(|event| accepted_count(event["orderHash"].as_str().unwrap()))
        .collect::<Vec<_>>();
    rows.borrow_mut().push(json!({"type":"HttpBatchCancelResponseWithheld","httpTimeoutSecs":1,"responseDelayMillis":2000,"request":request}));
    state
        .remove_gate
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .notify_one();
    barrier(
        "Native queries both OPEN orders after HTTP deadline",
        || {
            wire.iter()
                .zip(&before)
                .all(|(event, count)| accepted_count(event["orderHash"].as_str().unwrap()) > *count)
        },
    )
    .await;
    *state.remove_gate.lock().unwrap() = None;
    for event in wire {
        let hash = event["orderHash"].as_str().unwrap();
        let pending = rows
            .borrow()
            .iter()
            .rfind(|row| row["type"] == "StrategyOrderCallback" && row["venueOrderId"] == hash)
            .unwrap()
            .clone();
        assert_eq!(pending["cachedStatus"], "PENDING_CANCEL");
        assert_eq!(canceled_count(hash), 0);
        assert_eq!(
            state.orders_by_hash.lock().unwrap()[hash]["data"]["status"],
            "OPEN"
        );
    }
    assert_eq!(locked(), Some(Decimal::from(2)));
    assert_eq!(state.open_orders.lock().unwrap().len(), 2);
    assert_eq!(state.removals.lock().unwrap().len(), 1);
    rows.borrow_mut().push(
        json!({"type":"BatchTimeoutPreservesBothPendingOrders","lockedUSDT":"2","removalWrites":1}),
    );

    for (index, event) in wire.iter().enumerate() {
        let hash = event["orderHash"].as_str().unwrap();
        // Simulate venue cancellation independently; no additional adapter write is sent.
        state.orders_by_hash.lock().unwrap().get_mut(hash).unwrap()["data"]["status"] =
            json!("CANCELLED");
        state
            .open_orders
            .lock()
            .unwrap()
            .retain(|row| row["id"] != event["orderId"]);
        barrier("Native query supplies each terminal callback", || {
            canceled_count(hash) == 1
        })
        .await;
        barrier("reported account releases only the removed order", || {
            locked() == Some([Decimal::ONE, Decimal::ZERO][index])
        })
        .await;
        if index == 0 {
            let other = wire[1]["orderHash"].as_str().unwrap();
            assert_eq!(canceled_count(other), 0);
            let pending = rows
                .borrow()
                .iter()
                .rfind(|row| row["type"] == "StrategyOrderCallback" && row["venueOrderId"] == other)
                .unwrap()
                .clone();
            assert_eq!(pending["cachedStatus"], "PENDING_CANCEL");
            assert_eq!(state.open_orders.lock().unwrap().len(), 1);
            rows.borrow_mut().push(json!({"type":"BatchTimeoutOneOrderResolved","venueOrderId":hash,"otherPendingOrder":other,"lockedUSDT":"1"}));
        }
        replay_cancel_reports(fixture, rows, std::slice::from_ref(event)).await;
    }
    assert!(
        wire.iter()
            .all(|event| canceled_count(event["orderHash"].as_str().unwrap()) == 1)
    );
    assert!(
        !rows
            .borrow()
            .iter()
            .any(|row| row["type"] == "StrategyOrderCallback" && row["kind"] == "CancelRejected")
    );
    assert_eq!(
        state.removals.lock().unwrap().len(),
        1,
        "unknown batch outcome must not automatically resend the write"
    );
}

/// Withhold wallet cancellation events until Native's read paths resolve the write.
/// The second resting order is a control: unresolved cancellation must not lock the account.
async fn recover_cancel_probe(
    fixture: &HostFixture,
    mode: &str,
    rows: &Rc<RefCell<Vec<Value>>>,
    resume: &std::cell::Cell<bool>,
    wire: &[Value],
    ready: impl Fn() -> bool,
) {
    let state = &fixture.backing.state;
    barrier("single cancel reached venue", || {
        !state.removals.lock().unwrap().is_empty()
    })
    .await;
    let request = state.removals.lock().unwrap()[0].clone();
    let ids = request["data"]["ids"].as_array().unwrap();
    assert_eq!(ids.len(), 1);
    let id = &ids[0];
    let target = wire.iter().find(|event| &event["orderId"] == id).unwrap();
    let hash = target["orderHash"].as_str().unwrap();
    let terminal = || {
        state.orders_by_hash.lock().unwrap().get_mut(hash).unwrap()["data"]["status"] =
            json!("CANCELLED");
        state
            .open_orders
            .lock()
            .unwrap()
            .retain(|row| &row["id"] != id);
    };
    if mode == "recovery_noop" || mode == "recovery_removed_gap" {
        terminal();
    }
    let accepted_before = rows
        .borrow()
        .iter()
        .filter(|row| {
            row["type"] == "RawOrderStatusReport"
                && row["report"]["venue_order_id"] == hash
                && row["report"]["order_status"] == "ACCEPTED"
        })
        .count();
    let lists_before = state.order_list_log.lock().unwrap().len();
    let queries_before = state
        .order_response_log
        .lock()
        .unwrap()
        .iter()
        .filter(|response| response["path"].as_str().unwrap().ends_with(hash))
        .count();
    if mode == "recovery_exhausted_error" {
        // The first query already supplied cancellation preflight. Fail both
        // remaining reads in the three-check Native budget, without retries.
        *state.order_replies.lock().unwrap() = (0..2)
            .map(|_| {
                (
                    503,
                    json!({"success":false,"message":"fixture order query unavailable"}),
                )
            })
            .collect();
    }
    if mode.contains("timeout") {
        rows.borrow_mut().push(json!({"type":"HttpCancelResponseWithheld","httpTimeoutSecs":1,"responseDelayMillis":2000,"venueOrderId":hash}));
    }
    state
        .remove_gate
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .notify_one();
    barrier("single cancel receipt", || {
        state.removal_responses.lock().unwrap().len() == 1
    })
    .await;
    *state.remove_gate.lock().unwrap() = None;
    if mode == "recovery_removed_gap" {
        barrier("wallet resubscribed without cancellation feedback", || {
            state.sockets.lock().unwrap().len() > 1
        })
        .await;
    }
    if mode.starts_with("recovery_exhausted_") {
        barrier(
            "Native exhausts cancellation without venue terminal evidence",
            || {
                rows.borrow().iter().any(|row| {
                    row["type"] == "StrategyOrderCallback"
                        && row["kind"] == "Canceled"
                        && row["venueOrderId"] == hash
                })
            },
        )
        .await;
        let queried = state
            .order_response_log
            .lock()
            .unwrap()
            .iter()
            .filter(|response| response["path"].as_str().unwrap().ends_with(hash))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            queried.len(),
            queries_before + 2,
            "exactly two Native recovery queries after preflight"
        );
        assert!(
            queried
                .iter()
                .all(|response| response["body"]["data"]["status"] != "CANCELLED")
        );
        if mode == "recovery_exhausted_error" {
            assert!(
                queried[queries_before..]
                    .iter()
                    .all(|response| response["status"] == 503)
            );
        } else {
            assert!(
                queried
                    .iter()
                    .all(|response| response["body"]["data"]["status"] == "OPEN")
            );
        }
        let callback = rows
            .borrow()
            .iter()
            .find(|row| {
                row["type"] == "StrategyOrderCallback"
                    && row["kind"] == "Canceled"
                    && row["venueOrderId"] == hash
            })
            .unwrap()
            .clone();
        assert_eq!(callback["cachedStatus"], "CANCELED");
        assert_eq!(callback["reconciliation"], true);
        assert_eq!(
            state.orders_by_hash.lock().unwrap()[hash]["data"]["status"],
            "OPEN"
        );
        assert_eq!(state.open_orders.lock().unwrap().len(), 2);
        assert_eq!(state.removals.lock().unwrap().len(), 1);
        assert!(
            !rows
                .borrow()
                .iter()
                .any(|row| row["type"] == "RawOrderStatusReport"
                    && row["report"]["order_status"] == "CANCELED")
        );
        barrier(
            "fresh reported account follows synthetic cancellation",
            || {
                let rows = rows.borrow();
                let canceled = rows
                    .iter()
                    .position(|row| {
                        row["type"] == "StrategyOrderCallback"
                            && row["kind"] == "Canceled"
                            && row["venueOrderId"] == hash
                    })
                    .unwrap();
                rows[canceled + 1..]
                    .iter()
                    .any(|row| row["type"] == "AccountStateApplied")
            },
        )
        .await;
        let account = rows
            .borrow()
            .iter()
            .rfind(|row| row["type"] == "AccountStateApplied")
            .unwrap()
            .clone();
        assert_eq!(
            account["balances"][0]["locked"]
                .as_str()
                .unwrap()
                .parse::<Decimal>()
                .unwrap(),
            Decimal::from(2)
        );
        rows.borrow_mut().push(json!({"type":"NativeCancelExhaustedWithoutVenueEvidence","venueOrderId":hash,"venueStatus":"OPEN","callback":callback,"account":account,"orderQueries":queried,"queriesBeforeCancelResponse":queries_before,"venueOpenOrderCount":2,"removalWrites":1}));
    } else if mode.ends_with("_query") || mode.ends_with("_mass") {
        barrier(
            "Native read reports OPEN while cancel remains unresolved",
            || {
                if mode.ends_with("_mass") {
                    return state.order_list_log.lock().unwrap().len() > lists_before;
                }
                rows.borrow()
                    .iter()
                    .filter(|row| {
                        row["type"] == "RawOrderStatusReport"
                            && row["report"]["venue_order_id"] == hash
                            && row["report"]["order_status"] == "ACCEPTED"
                    })
                    .count()
                    > accepted_before
            },
        )
        .await;
        if mode.ends_with("_mass") {
            assert!(state.order_list_log.lock().unwrap().len() > lists_before);
            // Bulk collection may fetch per-hash fences as part of the adapter's
            // report contract. No targeted inflight report has been published.
            assert_eq!(
                rows.borrow()
                    .iter()
                    .filter(|row| row["type"] == "RawOrderStatusReport"
                        && row["report"]["venue_order_id"] == hash
                        && row["report"]["order_status"] == "ACCEPTED")
                    .count(),
                accepted_before
            );
        } else {
            assert!(
                state.order_requests.lock().unwrap().len()
                    >= if mode == "recovery_noop_query" { 3 } else { 2 }
            );
        }
        assert!(
            !rows
                .borrow()
                .iter()
                .any(|row| row["type"] == "StrategyOrderCallback"
                    && (row["kind"] == "Canceled" || row["kind"] == "CancelRejected"))
        );
        let pending = rows
            .borrow()
            .iter()
            .rfind(|row| row["type"] == "StrategyOrderCallback" && row["venueOrderId"] == hash)
            .unwrap()
            .clone();
        assert_eq!(pending["cachedStatus"], "PENDING_CANCEL");
        rows.borrow_mut().push(json!({"type":"NativeOpenPreservesPendingCancel","venueOrderId":hash,"pending":pending}));
    } else {
        barrier(
            "Native terminal evidence resolves first order without wallet feedback",
            || {
                rows.borrow().iter().any(|row| {
                    row["type"] == "StrategyOrderCallback"
                        && row["kind"] == "Canceled"
                        && row["venueOrderId"] == hash
                })
            },
        )
        .await;
    }

    // Cancel only the other acknowledged order, while the unknown leg is still pending.
    // Native commands, queries and reports remain the only execution entry points.
    barrier(
        "Native execution is ready for unrelated cancellation",
        ready,
    )
    .await;
    state
        .remove_replies
        .lock()
        .unwrap()
        .push_back((200, json!({"echoRemoved":true}), 0));
    let accepted_before_retry = rows
        .borrow()
        .iter()
        .filter(|row| {
            row["type"] == "RawOrderStatusReport"
                && row["report"]["venue_order_id"] == hash
                && row["report"]["order_status"] == "ACCEPTED"
        })
        .count();
    let query_count_before_retry = state.order_response_log.lock().unwrap().len();
    resume.set(true);
    barrier("unrelated order cancellation is not blocked", || {
        state.removal_responses.lock().unwrap().len() == 2
    })
    .await;
    let second = state.removals.lock().unwrap()[1].clone();
    let other = second["data"]["ids"].as_array().unwrap();
    assert_eq!(other.len(), 1);
    assert_ne!(&other[0], id);
    if mode.starts_with("recovery_exhausted_") {
        barrier(
            "strategy query observes real OPEN after synthetic Canceled",
            || {
                state.order_response_log.lock().unwrap()[query_count_before_retry..]
                    .iter()
                    .any(|response| {
                        response["path"].as_str().unwrap().ends_with(hash)
                            && response["body"]["data"]["status"] == "OPEN"
                    })
            },
        )
        .await;
        assert_eq!(
            rows.borrow()
                .iter()
                .filter(|row| row["type"] == "RawOrderStatusReport"
                    && row["report"]["venue_order_id"] == hash
                    && row["report"]["order_status"] == "ACCEPTED")
                .count(),
            accepted_before_retry,
            "adapter does not reopen a terminal native order from a conflicting snapshot"
        );
        assert!(
            rows.borrow()
                .iter()
                .any(|row| row["type"] == "StrategyCancelClosedOrderAttempt")
        );
        let callback = rows
            .borrow()
            .iter()
            .rfind(|row| row["type"] == "StrategyOrderCallback" && row["venueOrderId"] == hash)
            .unwrap()
            .clone();
        assert_eq!(callback["cachedStatus"], "CANCELED");
        assert_eq!(
            state.orders_by_hash.lock().unwrap()[hash]["data"]["status"],
            "OPEN"
        );
        rows.borrow_mut().push(json!({"type":"StrategyQueryCannotReopenSyntheticCanceledOrder","venueOrderId":hash,"cachedStatus":"CANCELED","venueStatus":"OPEN","removalWrites":2,"otherOrderCanceled":true}));
    }
    // REST must expose the same final state as the successful removal response.
    for row in state.orders_by_hash.lock().unwrap().values_mut() {
        if other.contains(&row["data"]["id"]) {
            row["data"]["status"] = json!("CANCELLED");
        }
    }
    if mode.ends_with("_query") || mode.ends_with("_mass") {
        barrier("only unresolved order retains collateral", || {
            rows.borrow()
                .iter()
                .rfind(|row| row["type"] == "AccountStateApplied")
                .is_some_and(|row| {
                    row["balances"][0]["locked"]
                        .as_str()
                        .is_some_and(|locked| locked.parse::<Decimal>().unwrap() == Decimal::ONE)
                })
        })
        .await;
        assert!(
            !rows
                .borrow()
                .iter()
                .any(|row| row["type"] == "StrategyOrderCallback"
                    && row["kind"] == "Canceled"
                    && row["venueOrderId"] == hash)
        );
        rows.borrow_mut().push(json!({"type":"UnrelatedCancelCompletedWithOneOrderPending","venueOrderId":hash,"lockedUSDT":"1"}));
    }
    terminal();
    barrier("Native read recovers both terminal orders", || {
        rows.borrow()
            .iter()
            .filter(|row| row["type"] == "StrategyOrderCallback" && row["kind"] == "Canceled")
            .count()
            == 2
    })
    .await;
    assert!(
        !rows
            .borrow()
            .iter()
            .any(|row| row["type"] == "StrategyOrderCallback" && row["kind"] == "CancelRejected")
    );
    replay_cancel_reports(fixture, rows, wire).await;
}

const CANCEL_RECOVERY_MODES: &[&str] = &[
    "recovery_noop",
    "recovery_noop_query",
    "recovery_unknown_query",
    "recovery_unknown_mass",
    "recovery_removed_gap",
];

const CANCEL_EXHAUSTION_MODES: &[&str] = &["recovery_exhausted_open", "recovery_exhausted_error"];

#[tokio::test(flavor = "current_thread")]
async fn native_cancel_exhaustion_is_observed_without_venue_terminal_evidence() {
    // Characterize the pinned dependency, not the desired cancellation contract.
    // No venue cancel is reported before the diagnostic snapshot is collected.
    for mode in CANCEL_EXHAUSTION_MODES {
        batch_probe_case(mode).await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline Native cancel exhaustion capture; create_new only"]
async fn record_native_cancel_exhaustion_probe() {
    save_batch_probe(
        CANCEL_EXHAUSTION_MODES,
        "http_cancel_native_exhaustion_synthetic_20261017.json",
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn batch_http_timeout_recovers_each_order_without_repeating_removal() {
    batch_probe_case("batch_timeout").await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline multi-ID HTTP timeout capture; create_new only"]
async fn record_batch_http_timeout_probe() {
    save_batch_probe(
        &["batch_timeout"],
        "http_batch_transport_timeout_synthetic_20261017.json",
    )
    .await;
}

const CANCEL_TIMEOUT_MODES: &[&str] = &["recovery_timeout_query", "recovery_exhausted_timeout"];

#[tokio::test(flavor = "current_thread")]
async fn http_cancel_timeout_preserves_open_until_native_resolution_or_exhaustion() {
    for mode in CANCEL_TIMEOUT_MODES {
        batch_probe_case(mode).await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline HTTP cancel timeout capture; create_new only"]
async fn record_http_cancel_timeout_probe() {
    save_batch_probe(
        CANCEL_TIMEOUT_MODES,
        "http_cancel_transport_timeout_synthetic_20261017.json",
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_queries_and_batch_reports_recover_cancel_outcomes_without_wallet_feedback() {
    for mode in CANCEL_RECOVERY_MODES {
        batch_probe_case(mode).await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline native cancel recovery capture; create_new only"]
async fn record_native_cancel_recovery_probe() {
    save_batch_probe(
        CANCEL_RECOVERY_MODES,
        "http_cancel_native_recovery_synthetic_20261017.json",
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn empty_cache_batch_probe_claims_modifies_and_cancels_two_orders_once() {
    for mode in ["removed", "mixed", "rejected", "unknown", "preflight"] {
        batch_probe_case(mode).await;
    }
}
#[tokio::test(flavor = "current_thread")]
async fn removal_lock_partitions_preserve_live_orders_and_native_reservations() {
    for mode in ["locked", "locked_all"] {
        batch_probe_case(mode).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn definitive_cancel_rejections_survive_wallet_gaps_and_allow_explicit_retry() {
    for mode in ["single_locked_gap", "locked_gap", "locked_all_gap"] {
        batch_probe_case(mode).await;
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline cancel rejection gap capture; create_new only"]
async fn record_http_cancel_gap_probe() {
    save_batch_probe(
        &["single_locked_gap", "locked_gap", "locked_all_gap"],
        "http_cancel_rejected_gap_native_synthetic_20261017.json",
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline removal lock capture; create_new only"]
async fn record_http_removal_lock_probe() {
    save_batch_probe(
        &["locked", "locked_all"],
        "http_removal_lock_native_synthetic_20261017.json",
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline multi-ID batch capture; create_new only"]
async fn record_http_batch_probe() {
    save_batch_probe(
        &["removed", "mixed", "rejected", "unknown", "preflight"],
        "http_batch_native_synthetic_20261017.json",
    )
    .await;
}

async fn save_batch_probe(modes: &[&str], filename: &str) {
    let mut sessions = vec![];
    for mode in modes {
        sessions.push(batch_probe_case(mode).await);
    }
    let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../data/predict-validation")
        .join(UUID4::new().to_string());
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join(filename);
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap(),
        &json!({"sessions":sessions}),
    )
    .unwrap();
    eprintln!(
        "Saved synthetic cancellation diagnostic: {}",
        path.display()
    );
}
