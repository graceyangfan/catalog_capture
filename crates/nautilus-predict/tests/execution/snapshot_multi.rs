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

//! Two native strategies share one cash account; only the target order is replaced.
use std::sync::atomic::Ordering;

use super::*;

fn path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
        "tests/fixtures/private_api/snapshot_multi_native_reservations_synthetic_20261016.json",
    )
}
fn count(journal: &Rc<RefCell<Vec<Value>>>, callback: &str) -> usize {
    journal
        .borrow()
        .iter()
        .filter(|e| e["callback"] == callback)
        .count()
}
fn balance(cache: &Rc<RefCell<Cache>>) -> Value {
    let cache = cache.borrow();
    let account = cache.account(&account_id()).unwrap();
    json!({"total":account.balance_total(Some(Currency::USDT())).unwrap().as_decimal().to_string(),
        "free":account.balance_free(Some(Currency::USDT())).unwrap().as_decimal().to_string(),
        "locked":account.balance_locked(Some(Currency::USDT())).unwrap().as_decimal().to_string(),
        "open":cache.orders_open(None,None,None,None,None).len()})
}
fn locked(cache: &Rc<RefCell<Cache>>) -> Decimal {
    cache
        .borrow()
        .account(&account_id())
        .unwrap()
        .balance_locked(Some(Currency::USDT()))
        .unwrap()
        .as_decimal()
}
fn book(
    public: &PublicState,
    frames: &mut Vec<Value>,
    saved: Option<&Value>,
    bid: &str,
    ask: &str,
) {
    let ts = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000;
    let mut frame = json!({"type":"M","topic":"predictOrderbook/42","data":{"marketId":42,
        "updateTimestampMs":ts,"orderCount":2,"bids":[[bid,"1000"]],"asks":[[ask,"1000"]]}});
    if let Some(saved) = saved {
        let mut replay = saved["books"][frames.len()].clone();
        replay["data"]["updateTimestampMs"] = json!(ts);
        assert_eq!(replay, frame);
        frame = replay;
    }
    frames.push(frame.clone());
    public.0.lock().unwrap()[0]
        .send(Message::Text(frame.to_string().into()))
        .unwrap();
}
fn accept(server: &Server, body: &Value, saved: Option<&Value>, wallet: &mut Vec<Value>) -> Value {
    let index = server
        .state
        .creations
        .lock()
        .unwrap()
        .iter()
        .position(|r| r == body)
        .unwrap();
    let id = (10001 + index).to_string();
    let mut row = live_node::rest_row(body, "OPEN");
    row["id"] = json!(id);
    server.state.open_orders.lock().unwrap().push(row.clone());
    server.state.orders_by_hash.lock().unwrap().insert(
        body["data"]["order"]["hash"].as_str().unwrap().into(),
        json!({"success":true,"data":row}),
    );
    let outcome = if body["data"]["order"]["tokenId"] == "11" {
        1
    } else {
        2
    };
    let mut ack = exit_orders::wire_event(body, "orderAccepted", outcome, Decimal::ZERO);
    ack["orderId"] = json!(id);
    if let Some(saved) = saved {
        let mut replay = saved["wallet"][wallet.len()].clone();
        for field in ["orderHash", "timestamp", "orderId"] {
            replay[field] = ack[field].clone();
        }
        assert_eq!(replay, ack);
        ack = replay;
    }
    wallet.push(ack.clone());
    server.wallet(ack.clone());
    ack
}
fn request(server: &Server, token: &str, price: &str) -> Value {
    server
        .state
        .creations
        .lock()
        .unwrap()
        .iter()
        .find(|r| r["data"]["order"]["tokenId"] == token && r["data"]["pricePerShare"] == price)
        .unwrap()
        .clone()
}
async fn run(denied: bool, saved: Option<&Value>) -> Value {
    let HostFixture {
        backing,
        public,
        rpc_gate,
        mut config,
        task,
        ..
    } = fixture(true, if denied { 2 } else { 3 }).await;
    config = config.replace("max_total_usdt = \"3\"", "max_total_usdt = \"4\"");
    let total = if denied { 2 } else { 3 };
    backing.state.rpc_frame.lock().unwrap().0 = total * 1_000_000_000_000_000_000;
    *backing.state.create_order_ids.lock().unwrap() = ["10001", "10002", "10003"]
        .into_iter()
        .map(String::from)
        .collect();
    let signer = Arc::new(
        PredictAuthSigner::new(
            submissions::PUBLIC_KEY.into(),
            PredictEnvironment::BnbMainnet,
            Some(WALLET),
        )
        .unwrap(),
    );
    let mut prepared = host::prepare(
        host::NodeProfile::parse(&config).unwrap(),
        KEY.into(),
        signer,
    )
    .await
    .unwrap();
    let target = Rc::new(RefCell::new(vec![]));
    let companion = Rc::new(RefCell::new(vec![]));
    prepared
        .node
        .add_strategy(
            snapshot_maker::SnapshotMaker::new(
                prepared.market_id,
                prepared.instruments[0],
                target.clone(),
            )
            .with_cancel_replace()
            .with_shared_account(),
        )
        .unwrap();
    prepared
        .node
        .add_strategy(snapshot_maker::SnapshotMaker::companion(
            prepared.market_id,
            prepared.instruments[1],
            companion.clone(),
        ))
        .unwrap();
    let cache = prepared.node.kernel().cache.clone();
    let events = Rc::new(RefCell::new(vec![]));
    let eh = TypedHandler::from({
        let events = events.clone();
        move |e: &OrderEventAny| {
            events.borrow_mut().push(json!({"strategy":e.strategy_id().to_string(),"type":format!("{:?}",e.event_type())}));
        }
    });
    msgbus::subscribe_order_events("events.order.*".into(), eh.clone(), None);
    let accounts = Rc::new(RefCell::new(vec![]));
    let ah = TypedHandler::from({
        let accounts = accounts.clone();
        move |e: &AccountState| {
            accounts.borrow_mut().push(json!({"balances":e.balances.iter().map(|b|json!({"currency":b.currency.code,
            "total":b.total.as_decimal().to_string(),"locked":b.locked.as_decimal().to_string(),"free":b.free.as_decimal().to_string()})).collect::<Vec<_>>()}));
        }
    });
    msgbus::subscribe_account_state("events.account.PREDICT-fixture".into(), ah.clone(), None);
    let handle = prepared.node.handle();
    let mut frames = vec![];
    let mut wallet = vec![];
    let mut checkpoints = vec![];
    let driver = async {
        barrier("multi strategy public subscription", || {
            !public.0.lock().unwrap().is_empty()
        })
        .await;
        book(&public, &mut frames, saved, "0.630", "0.640");
        barrier("two initial orders submitted", || {
            backing.state.creations.lock().unwrap().len() == 2
        })
        .await;
        let other = request(&backing, "22", "10000000000000000");
        accept(&backing, &other, saved, &mut wallet);
        barrier("companion native accepted", || {
            count(&companion, "on_order_accepted") == 1
        })
        .await;
        assert_eq!(locked(&cache), Decimal::from(2));
        checkpoints.push(json!({"phase":"two_reserved","account":balance(&cache)}));
        rpc_gate.paused.store(true, Ordering::Release);
        let first = request(&backing, "11", "10000000000000000");
        let old_ack = accept(&backing, &first, saved, &mut wallet);
        barrier("target canceled and account query blocked", || {
            count(&target, "query_account") == 1 && rpc_gate.blocked.load(Ordering::Acquire) > 0
        })
        .await;
        assert_eq!(count(&target, "on_order_modify_rejected"), 1);
        assert_eq!(count(&companion, "on_order_canceled"), 0);
        wallet.push(old_ack.clone());
        backing.wallet(old_ack);
        book(&public, &mut frames, saved, "0.630", "0.640");
        barrier("multi snapshot defers old account", || {
            count(&target, "ReplacementDeferred") == 1
        })
        .await;
        assert_eq!(backing.state.creations.lock().unwrap().len(), 2);
        let next_block = {
            let mut rpc = backing.state.rpc_frame.lock().unwrap();
            rpc.1 += 1;
            rpc.1
        };
        rpc_gate.paused.store(false, Ordering::Release);
        rpc_gate.release.close();
        barrier(
            "target reserve released while companion stays reserved",
            || {
                let c = cache.borrow();
                let account = c.account(&account_id()).unwrap();
                account.balance_locked(Some(Currency::USDT())) == Some(Money::from("1 USDT"))
                    && account.last_event().and_then(|e| {
                        e.info
                            .as_ref()
                            .and_then(|i| i.get("predict_block_number"))
                            .and_then(Value::as_u64)
                    }) == Some(next_block)
            },
        )
        .await;
        checkpoints.push(json!({"phase":"before_replacement","account":balance(&cache)}));
        assert_eq!(
            cache
                .borrow()
                .orders_open(None, None, None, None, None)
                .len(),
            1
        );
        book(&public, &mut frames, saved, "0.631", "0.641");
        barrier("replacement submitted from nonzero locked snapshot", || {
            count(&target, "submit_order") == 2
        })
        .await;
        assert_eq!(count(&target, "ReplacementAccountReady"), 1);
        if denied {
            barrier("native risk denies unfunded replacement", || {
                count(&target, "on_order_denied") == 1
            })
            .await;
            assert_eq!(
                backing.state.creations.lock().unwrap().len(),
                2,
                "native risk blocks HTTP POST"
            );
            assert_eq!(locked(&cache), Decimal::ONE);
            checkpoints.push(json!({"phase":"replacement_denied","account":balance(&cache)}));
        } else {
            barrier("funded replacement reaches venue", || {
                backing.state.creations.lock().unwrap().len() == 3
            })
            .await;
            assert_eq!(locked(&cache), Decimal::new(21, 1));
            checkpoints.push(json!({"phase":"replacement_reserved","account":balance(&cache)}));
            let second = request(&backing, "11", "11000000000000000");
            accept(&backing, &second, saved, &mut wallet);
            barrier("replacement canceled but companion stays open", || {
                count(&target, "on_order_canceled") == 2 && locked(&cache) == Decimal::ONE
            })
            .await;
        }
        assert_eq!(count(&companion, "on_order_canceled"), 0);
        assert_eq!(
            cache
                .borrow()
                .orders_open(
                    None,
                    None,
                    Some(&StrategyId::new("HOST-OTHER-002")),
                    None,
                    None
                )
                .len(),
            1
        );
        checkpoints.push(json!({"phase":"companion_preserved","account":balance(&cache)}));
        book(&public, &mut frames, saved, "0.631", "0.641");
        barrier("duplicate snapshot does not repeat replacement", || {
            count(&target, "on_data") == 4
        })
        .await;
        assert_eq!(count(&target, "submit_order"), 2);
        book(&public, &mut frames, saved, "0.632", "0.642");
        barrier(
            "companion canceled from final snapshot and all funds released",
            || count(&companion, "on_order_canceled") == 1 && locked(&cache).is_zero(),
        )
        .await;
        checkpoints.push(json!({"phase":"cleanup","account":balance(&cache)}));
        handle.stop();
    };
    timeout(Duration::from_secs(20), async {
        tokio::join!(driver, prepared.node.run())
    })
    .await
    .unwrap()
    .1
    .unwrap();
    let expected = if denied { 2 } else { 3 };
    assert_eq!(backing.state.creations.lock().unwrap().len(), expected);
    assert_eq!(backing.state.removals.lock().unwrap().len(), expected);
    {
        let cache = cache.borrow();
        assert!(cache.orders_open(None, None, None, None, None).is_empty());
        assert!(
            cache
                .orders_inflight(None, None, None, None, None)
                .is_empty()
        );
        assert!(cache.positions(None, None, None, None, None).is_empty());
        let orders = cache.orders(None, None, None, None, None);
        assert_eq!(orders.len(), 3);
        assert_eq!(
            orders
                .iter()
                .filter(|o| o.status() == OrderStatus::Denied)
                .count(),
            usize::from(denied)
        );
        assert!(
            orders
                .iter()
                .all(|o| matches!(o.status(), OrderStatus::Denied | OrderStatus::Canceled))
        );
        assert_eq!(
            cache
                .account(&account_id())
                .unwrap()
                .balance_total(Some(Currency::USDT()))
                .unwrap()
                .as_decimal(),
            Decimal::from(total)
        );
    }
    assert!(
        target
            .borrow()
            .iter()
            .any(|e| e["callback"] == "AccountAtAccepted" && e["account"]["open"] == 2),
        "two independent native orders must be open at target acceptance"
    );
    assert_eq!(count(&target, "on_order_rejected"), 0);
    assert_eq!(count(&target, "on_order_cancel_rejected"), 0);
    assert_eq!(count(&companion, "on_order_accepted"), 1);
    assert_eq!(count(&companion, "on_order_canceled"), 1);
    assert_eq!(count(&companion, "submit_order"), 1);
    if let Some(saved) = saved {
        assert_eq!(saved["targetCallbacks"], json!(*target.borrow()));
        assert_eq!(saved["companionCallbacks"], json!(*companion.borrow()));
        assert_eq!(saved["checkpoints"], json!(checkpoints));
    }
    let out = json!({"source":"synthetic two Rust snapshot strategies + actual product host + Native LiveNode; not mainnet",
        "case":if denied {"native_insufficient_free_balance"} else {"replacement_with_other_active_order"},
        "initialCash":total.to_string(),"nativeRiskBypassed":false,"signalInput":"custom_snapshot",
        "books":frames,"wallet":wallet,"requests":backing.state.creations.lock().unwrap().clone(),
        "removalResponses":backing.state.removal_responses.lock().unwrap().clone(),"feedback":events.borrow().clone(),
        "targetCallbacks":target.borrow().clone(),"companionCallbacks":companion.borrow().clone(),
        "accountTopicFeedback":accounts.borrow().clone(),"checkpoints":checkpoints,
        "bothStrategiesCanceledBeforeStop":true,"finalOpen":0,"finalInflight":0,"finalPositions":0,"finalLocked":"0","finalCash":total.to_string()});
    msgbus::unsubscribe_order_events("events.order.*".into(), &eh);
    msgbus::unsubscribe_account_state("events.account.PREDICT-fixture".into(), &ah);
    prepared.node.dispose();
    task.abort();
    out
}
#[tokio::test(flavor = "current_thread")]
async fn multiple_active_orders_replace_without_waiting_for_account_locked_zero() {
    let saved: Value = serde_json::from_slice(&std::fs::read(path()).unwrap()).unwrap();
    for denied in [false, true] {
        let case = if denied {
            "native_insufficient_free_balance"
        } else {
            "replacement_with_other_active_order"
        };
        run(
            denied,
            Some(
                saved["sessions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|s| s["case"] == case)
                    .unwrap(),
            ),
        )
        .await;
    }
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit multi-order synthetic capture; create_new only"]
async fn record_snapshot_multi_reservations_json() {
    let sessions = vec![run(false, None).await, run(true, None).await];
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path())
            .unwrap(),
        &json!({"source":"synthetic loopback, no real balances/orders","sessions":sessions}),
    )
    .unwrap();
}
