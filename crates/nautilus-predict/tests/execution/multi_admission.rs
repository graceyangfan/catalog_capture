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

//! Protocol ownership and concurrent dispatch independent of Native RiskEngine, loopback only.
use nautilus_model::{events::OrderEventAny, identifiers::ClientOrderId};
use nautilus_predict::PredictSubmissionConfig;

use super::*;

fn apply(
    engine: &mut ExecutionEngine,
    cache: &Rc<RefCell<Cache>>,
    event: ExecutionEvent,
    journal: &mut Vec<Value>,
) {
    submissions::apply(engine, &event);
    match event {
        ExecutionEvent::Order(event) => {
            let reason = match &event {
                OrderEventAny::Rejected(e) => Some(e.reason.as_str()),
                OrderEventAny::Denied(e) => Some(e.reason.as_str()),
                _ => None,
            };
            journal.push(json!({"type":format!("{:?}",event.event_type()),"id":event.client_order_id().to_string(),"reason":reason,
                "status":cache.borrow().order(&event.client_order_id()).unwrap().status().to_string()}));
        }
        ExecutionEvent::Account(state) => {
            journal.push(json!({"type":"AccountState","balances":state.balances}))
        }
        _ => {}
    }
}

async fn run(case: &str) -> Value {
    let private = server(vec![]).await;
    let small_funds = matches!(case, "queued_buy_funds" | "unknown_buy_funds");
    let balance = if small_funds {
        1_500_000_000_000_000_000u128
    } else if case == "repeated_maker_taker_quotes" {
        1_000_000_000_000_000_000_000
    } else {
        2_000_000_000_000_000_000
    };
    *private.state.rpc_frame.lock().unwrap() = (balance, 1, 2);
    let (_, mut rx, cache) = client(&private);
    let _portfolio = portfolio(&cache);
    let mut cfg = config(&private);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    cfg.report_timeout_secs = 3;
    cfg.submission = Some(PredictSubmissionConfig {
        signer: Arc::new(
            nautilus_predict::signing::PredictAuthSigner::new(
                submissions::PUBLIC_KEY.into(),
                PredictEnvironment::BnbMainnet,
                Some(WALLET),
            )
            .unwrap(),
        ),

        market_slippage_bps: 100,
    });
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
    let mut client = PredictExecutionClient::new_with_clock(core, cfg, clock).unwrap();
    for index in [1, 2] {
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(index)))
            .unwrap();
    }
    connect_reported(&mut client, &mut rx).await;
    *private.state.rpc_delay_ms.lock().unwrap() = 10;
    *private.state.sell_inventory.lock().unwrap() =
        U256::from(if case == "queued_sell_inventory" {
            150
        } else if case == "represented_sell" {
            200
        } else {
            100
        }) * U256::from(1_000_000_000_000_000_000u64);
    *private.state.sell_approved.lock().unwrap() = true;
    let unknown = matches!(case, "unknown_buy_funds" | "unknown_buy_then_sell");
    if unknown {
        private
            .state
            .create_replies
            .lock()
            .unwrap()
            .push_back((500, 0, vec![]));
    }
    let count = if case == "repeated_maker_taker_quotes" {
        3
    } else {
        2
    };
    // Native RiskEngine is intentionally bypassed by these direct client commands.
    // Funding/inventory is venue-authoritative; Native owns risk, not a client lifetime cap.
    let expected = count;
    let post_gate = Arc::new(tokio::sync::Notify::new());
    if case == "pending_buy_then_sell" {
        *private.state.create_gate.lock().unwrap() = Some(post_gate.clone());
    }
    let mut orders = vec![];
    for n in 0..count {
        let sell = matches!(
            case,
            "queued_sell_inventory" | "represented_sell" | "other_token_sell"
        ) || matches!(case, "unknown_buy_then_sell" | "pending_buy_then_sell") && n == 1;
        let index = if case == "other_token_sell" && n == 1 {
            2
        } else {
            1
        };
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .client_order_id(ClientOrderId::new(format!("MULTI-{n}")))
            .instrument_id(instrument_id(index))
            .side(if sell {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            })
            .quantity(Quantity::from(100))
            .price(Price::from(if sell { "0.020" } else { "0.010" }))
            .post_only(case != "repeated_maker_taker_quotes" || n != 1)
            .build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        orders.push(order);
    }
    let mut engine = submissions::engine(&cache);
    let mut journal = vec![];
    // Queued cases are submitted before polling any event, exercising native-account
    // publication lag without relying on the RiskEngine to hide an adapter race.
    let represented = matches!(case, "represented_buy" | "represented_sell");
    for (n, order) in orders.iter().enumerate() {
        if n == 1 && represented {
            timeout(Duration::from_secs(3), async {
                while private.state.creations.lock().unwrap().is_empty() {
                    if let Ok(event) = rx.try_recv() {
                        apply(&mut engine, &cache, event, &mut journal);
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let body = private.state.creations.lock().unwrap()[0].clone();
            let mut row = live_node::rest_row(&body, "OPEN");
            row["id"] = json!("12345");
            private.state.open_orders.lock().unwrap().push(row);
        }
        client.submit_order(submissions::submit(order)).unwrap();
    }
    timeout(Duration::from_secs(5), async {
        loop {
            while let Ok(event) = rx.try_recv() {
                apply(&mut engine, &cache, event, &mut journal);
            }
            let rejected = orders
                .iter()
                .filter(|order| {
                    cache
                        .borrow()
                        .order(&order.client_order_id())
                        .unwrap()
                        .is_closed()
                })
                .count();
            if private.state.creations.lock().unwrap().len() == expected
                && rejected == count - expected
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("admission case {case}"));
    if case == "pending_buy_then_sell" {
        post_gate.notify_waiters();
    }
    let requests = private.state.creations.lock().unwrap().clone();
    assert_eq!(requests.len(), expected);
    let before_cleanup=orders.iter().map(|order|json!({"id":order.client_order_id().to_string(),"status":cache.borrow().order(&order.client_order_id()).unwrap().status().to_string()})).collect::<Vec<_>>();
    if unknown {
        // Independent requests may complete out of dispatch order. Every transmitted
        // ambiguous attempt stays Submitted; only unsent competing attempts may reject.
        assert_eq!(
            before_cleanup
                .iter()
                .filter(|row| row["status"] == "SUBMITTED")
                .count(),
            expected,
            "{case}: {before_cleanup:?}"
        );
    }
    if case == "repeated_maker_taker_quotes" {
        assert_eq!(
            requests.len(),
            3,
            "repeated quotes have no lifetime adapter budget"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|row| row["data"]["isPostOnly"] == false)
                .count(),
            1
        );
        assert_eq!(
            requests
                .iter()
                .filter(|row| row["data"]["isPostOnly"] == true)
                .count(),
            2
        );
    }
    private.state.open_orders.lock().unwrap().clear();
    for body in &requests {
        let outcome = if body["data"]["order"]["tokenId"] == "11" {
            1
        } else {
            2
        };
        for kind in ["orderAccepted", "orderCancelled"] {
            let mut event = exit_orders::wire_event(body, kind, outcome, Decimal::ZERO);
            event["timestamp"] = json!(2);
            private.wallet(event);
        }
    }
    timeout(Duration::from_secs(3), async {
        loop {
            while let Ok(event) = rx.try_recv() {
                apply(&mut engine, &cache, event, &mut journal);
            }
            if orders.iter().all(|order| {
                cache
                    .borrow()
                    .order(&order.client_order_id())
                    .unwrap()
                    .is_closed()
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client
        .query_account(QueryAccount::new(
            trader_id(),
            Some(ClientId::new("PREDICT")),
            account_id(),
            UUID4::new(),
            UnixNanos::from(OBSERVED),
            None,
            None,
        ))
        .unwrap();
    timeout(Duration::from_secs(3), async {
        loop {
            while let Ok(event) = rx.try_recv() {
                apply(&mut engine, &cache, event, &mut journal);
            }
            if cache
                .borrow()
                .account(&account_id())
                .unwrap()
                .balance_locked(Some(Currency::USDT()))
                == Some(Money::from("0 USDT"))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.disconnect().await.unwrap();
    json!({"case":case,"source":"synthetic loopback adapter admission + native ExecutionEngine/Portfolio; direct client commands intentionally bypass native RiskEngine", "requests":requests,
        "beforeCleanup":before_cleanup,"feedback":journal,"initialBalanceWei":balance.to_string(),"expectedPosts":expected,"orderPosts":private.state.creations.lock().unwrap().len(),
        "finalOpenOrders":cache.borrow().orders_open(None,None,None,None,None).len(),"finalInflightOrders":cache.borrow().orders_inflight(None,None,None,None,None).len(),
        "finalCash":cache.borrow().account(&account_id()).unwrap().balance_total(Some(Currency::USDT())).unwrap().to_string(),"mainnetWrites":0})
}

const CASES: &[&str] = &[
    "queued_buy_funds",
    "unknown_buy_funds",
    "repeated_maker_taker_quotes",
    "represented_buy",
    "unrepresented_buy",
    "queued_sell_inventory",
    "represented_sell",
    "other_token_sell",
    "unknown_buy_then_sell",
    "pending_buy_then_sell",
];
#[tokio::test]
async fn multi_order_dispatch_preserves_ownership_without_adapter_risk_policy() {
    for case in CASES {
        run(case).await;
    }
}
#[tokio::test]
#[ignore = "explicit synthetic multi-order admission JSON, never overwrite evidence"]
async fn record_multi_admission_json() {
    let mut sessions = vec![];
    for case in CASES {
        sessions.push(run(case).await);
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/multi_admission_synthetic_20261016.json");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(
        file,
        &json!({"source":"synthetic offline adapter admission; not mainnet","sessions":sessions}),
    )
    .unwrap();
}

#[tokio::test]
async fn malformed_background_open_rows_do_not_enter_command_admission() {
    for side in [OrderSide::Buy, OrderSide::Sell] {
        for field in ["makerAmount", "takerAmount"] {
            let private = server(vec![]).await;
            let (mut client, _rx, _cache, _portfolio, order) =
                submissions::connected(&private, 1).await;
            let mut row = rest_order_data("OPEN", 1, side);
            row["order"][field] = json!("0");
            private.state.open_orders.lock().unwrap().push(row);
            let open = private.state.requests.lock().unwrap().len();
            client.submit_order(submissions::submit(&order)).unwrap();
            exit_orders::barrier("command does not audit OPEN", || {
                private.state.creations.lock().unwrap().len() == 1
            })
            .await;
            assert_eq!(private.state.requests.lock().unwrap().len(), open);
            client.disconnect().await.unwrap();
        }
    }
}
