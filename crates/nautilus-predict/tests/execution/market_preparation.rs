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

//! Strategy price input and market preparation through the real execution entry.
use nautilus_core::Params;
use nautilus_model::events::OrderEventAny;

use super::*;

pub(super) fn sell(index: u8, number: u32) -> OrderAny {
    OrderTestBuilder::new(OrderType::Market)
        .trader_id(trader_id())
        .instrument_id(instrument_id(index))
        .client_order_id(format!("PREPARED-SELL-{number}").into())
        .side(OrderSide::Sell)
        .quantity(Quantity::from(100))
        .build()
}

pub(super) fn priced(order: &OrderAny, price: Value, slippage_bps: u32) -> SubmitOrder {
    let mut command = submissions::submit(order);
    let params = Params::new(
        [
            ("market_price".into(), price),
            ("market_order_slippage_bps".into(), json!(slippage_bps)),
        ]
        .into(),
    );
    command.params = Some(params);
    command
}

#[tokio::test]
async fn wallet_gap_during_initial_preparation_cannot_publish_a_healthy_connection() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.market_gate.lock().unwrap() = Some(gate.clone());
    client.start().unwrap();
    let interrupt = async {
        // Signing preparation and cash collection start independently. Wait for
        // both facts before interrupting the original acknowledged connection.
        let mut account_applied = false;
        while !account_applied || *server.state.market_requests.lock().unwrap() == 0 {
            if let Ok(ExecutionEvent::Account(state)) = rx.try_recv() {
                msgbus::send_account_state(
                    MessagingSwitchboard::portfolio_update_account(),
                    &state,
                );
                account_applied = true;
            }
            tokio::task::yield_now().await;
        }
        server.state.sockets.lock().unwrap()[0]
            .send(Message::Close(None))
            .unwrap();
        exit_orders::barrier("wallet reconnect during preparation", || {
            server.state.sockets.lock().unwrap().len() >= 2
        })
        .await;
        gate.notify_one();
    };
    let (result, ()) = tokio::join!(client.connect(), interrupt);
    let error = result.expect_err("a setup spanning a wallet gap must fail");
    assert!(
        error
            .to_string()
            .contains("lost its original acknowledged connection")
    );
    assert!(!client.is_connected());
    assert!(server.state.creations.lock().unwrap().is_empty());
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn strategy_market_price_signs_exact_sdk_bounds_without_orderbook_or_funding_reads() {
    for index in [1, 2] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(index)))
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let rpc = server.state.rpc_methods.lock().unwrap().len();
        let open = server.state.requests.lock().unwrap().len();
        let metadata = *server.state.market_requests.lock().unwrap();
        *server.state.rpc_failure.lock().unwrap() = true;
        // No book is configured. An accidental GET would fail parsing before POST.
        let own = sell(index, 1);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        client
            .submit_order(priced(&own, json!("0.020"), 100))
            .unwrap();
        exit_orders::barrier("strategy-priced MARKET POST", || {
            !server.state.creations.lock().unwrap().is_empty()
        })
        .await;
        let body = server.state.creations.lock().unwrap()[0].clone();
        assert_eq!(body["data"]["strategy"], "MARKET");
        assert_eq!(body["data"]["pricePerShare"], "20000000000000000");
        assert_eq!(body["data"]["slippageBps"], "100");
        assert_eq!(body["data"]["isMinAmountOut"], false);
        assert_eq!(
            body["data"]["order"]["makerAmount"],
            "100000000000000000000"
        );
        assert_eq!(body["data"]["order"]["takerAmount"], "1980000000000000000");
        assert_eq!(
            body["data"]["order"]["tokenId"],
            if index == 1 { "11" } else { "22" }
        );
        assert_eq!(*server.state.book_requests.lock().unwrap(), 0);
        assert_eq!(*server.state.market_requests.lock().unwrap(), metadata);
        assert_eq!(server.state.rpc_methods.lock().unwrap().len(), rpc);
        assert_eq!(server.state.requests.lock().unwrap().len(), open);
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn invalid_strategy_price_is_denied_and_slippage_floor_minimum_still_rejects() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let mut engine = super::cancellations::engine(&cache);
    for (n, price) in [
        json!(0.02),
        json!(null),
        json!("0"),
        json!("-0.1"),
        json!("1.1"),
        json!("oops"),
        json!("0.009"),
    ]
    .into_iter()
    .enumerate()
    {
        let own = sell(1, n as u32);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        client.submit_order(priced(&own, price, 100)).unwrap();
        loop {
            let incoming = next(&mut rx).await;
            let terminal = matches!(
                incoming,
                ExecutionEvent::Order(OrderEventAny::Denied(_) | OrderEventAny::Rejected(_))
            );
            submissions::apply(&mut engine, &incoming);
            if terminal {
                break;
            }
        }
        assert!(
            cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .is_closed()
        );
    }
    assert!(server.state.creations.lock().unwrap().is_empty());
    assert_eq!(*server.state.book_requests.lock().unwrap(), 0);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn background_metadata_refresh_does_not_block_valid_commands_and_reuses_inflight_load() {
    let server = server(vec![]).await;
    let ((mut client, mut rx, cache, clock), mut collector) =
        submissions::setup_metadata_probe(&server, 1);
    let _portfolio = portfolio(&cache);
    for index in [1, 2] {
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(index)))
            .unwrap();
    }
    connect_reported(&mut client, &mut rx).await;
    assert_eq!(
        *server.state.market_requests.lock().unwrap(),
        1,
        "connect coalesces YES/NO preparation"
    );
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.market_gate.lock().unwrap() = Some(gate.clone());
    *server.state.market_fee.lock().unwrap() = 200;
    clock.set_time(UnixNanos::from(OBSERVED + 500_000_001));
    exit_orders::barrier("background refresh before hard expiry", || {
        *server.state.market_requests.lock().unwrap() == 2
    })
    .await;
    let first = sell(1, 1);
    cache
        .borrow_mut()
        .add_order(first.clone(), None, None, false)
        .unwrap();
    client
        .submit_order(priced(&first, json!("0.020"), 100))
        .unwrap();
    exit_orders::barrier("valid metadata bypasses blocked refresher", || {
        server.state.creations.lock().unwrap().len() == 1
    })
    .await;
    assert_eq!(
        server.state.creations.lock().unwrap()[0]["data"]["order"]["feeRateBps"],
        "0"
    );
    // Old metadata is now expired, while the refresh's own observation is still fresh.
    clock.set_time(UnixNanos::from(OBSERVED + 1_000_000_000));
    let second = sell(2, 2);
    cache
        .borrow_mut()
        .add_order(second.clone(), None, None, false)
        .unwrap();
    client
        .submit_order(priced(&second, json!("0.020"), 100))
        .unwrap();
    let mut engine = submissions::engine(&cache);
    loop {
        let event = next(&mut rx).await;
        let rejected = matches!(&event,ExecutionEvent::Order(OrderEventAny::Rejected(event)) if event.client_order_id==second.client_order_id());
        submissions::apply(&mut engine, &event);
        if rejected {
            break;
        }
    }
    assert_eq!(server.state.creations.lock().unwrap().len(), 1);
    gate.notify_one();
    submissions::metadata_collected(&mut collector, 2).await;
    let third = sell(2, 3);
    cache
        .borrow_mut()
        .add_order(third.clone(), None, None, false)
        .unwrap();
    client
        .submit_order(priced(&third, json!("0.020"), 100))
        .unwrap();
    exit_orders::barrier("new fee used from prepared shared slot", || {
        server.state.creations.lock().unwrap().len() == 2
    })
    .await;
    assert_eq!(
        server.state.creations.lock().unwrap()[1]["data"]["order"]["feeRateBps"],
        "200"
    );
    assert_eq!(
        *server.state.market_requests.lock().unwrap(),
        2,
        "no third GET behind the refresher"
    );
    assert_eq!(*server.state.book_requests.lock().unwrap(), 0);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn expired_instruments_are_not_prewarmed_and_stopped_load_cannot_repopulate_reconnected_slot()
{
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
    let _portfolio = portfolio(&cache);
    let mut expired = instrument(1);
    expired.expiration_ns = 1.into();
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(expired))
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    assert_eq!(*server.state.market_requests.lock().unwrap(), 0);
    let definition = InstrumentAny::BinaryOption(instrument(1));
    cache
        .borrow_mut()
        .add_instrument(definition.clone())
        .unwrap();
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.market_gate.lock().unwrap() = Some(gate.clone());
    client.on_instrument(definition);
    exit_orders::barrier("new definition starts background preparation", || {
        *server.state.market_requests.lock().unwrap() == 1
    })
    .await;
    client.disconnect().await.unwrap();
    *server.state.market_fee.lock().unwrap() = 200;
    connect_reported(&mut client, &mut rx).await;
    assert_eq!(*server.state.market_requests.lock().unwrap(), 2);
    // The old HTTP handler is released only after the new connection is prepared.
    gate.notify_one();
    let own = sell(1, 1);
    cache
        .borrow_mut()
        .add_order(own.clone(), None, None, false)
        .unwrap();
    client
        .submit_order(priced(&own, json!("0.020"), 100))
        .unwrap();
    exit_orders::barrier("new generation uses its own prepared metadata", || {
        !server.state.creations.lock().unwrap().is_empty()
    })
    .await;
    assert_eq!(
        server.state.creations.lock().unwrap()[0]["data"]["order"]["feeRateBps"],
        "200"
    );
    assert_eq!(*server.state.market_requests.lock().unwrap(), 2);
    assert_eq!(*server.state.book_requests.lock().unwrap(), 0);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn background_validation_rejects_invalid_fee_and_sibling_token_before_caching() {
    for bad_fee in [false, true] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
        let _portfolio = portfolio(&cache);
        if bad_fee {
            *server.state.market_fee.lock().unwrap() = 10001;
        }
        for index in [1, 2] {
            let mut definition = instrument(index);
            if !bad_fee && index == 2 {
                definition.raw_symbol = "999".into();
            }
            cache
                .borrow_mut()
                .add_instrument(InstrumentAny::BinaryOption(definition))
                .unwrap();
        }
        connect_reported(&mut client, &mut rx).await;
        let own = sell(1, 9);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        client
            .submit_order(priced(&own, json!("0.020"), 100))
            .unwrap();
        loop {
            if matches!(
                next(&mut rx).await,
                ExecutionEvent::Order(OrderEventAny::Rejected(_))
            ) {
                break;
            }
        }
        assert!(server.state.creations.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn reconnect_reuses_valid_parameters_while_account_refresh_is_suspended() {
    let server = server(vec![]).await;
    let ((mut client, mut rx, cache, clock), mut collector) =
        submissions::setup_metadata_probe(&server, 30);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    // Eligible for background renewal, still inside the original hard TTL.
    clock.set_time(UnixNanos::from(OBSERVED + 15_000_000_001));
    let cash_gate = Arc::new(tokio::sync::Notify::new());
    *server.state.rpc_gate.lock().unwrap() = Some(cash_gate.clone());
    server
        .state
        .sockets
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(Message::Close(None))
        .unwrap();
    exit_orders::barrier("wallet reconnects", || {
        server.state.sockets.lock().unwrap().len() == 2
    })
    .await;
    wallet_processed(&server).await;
    client
        .query_account(QueryAccount::new(
            trader_id(),
            Some(ClientId::new("PREDICT")),
            account_id(),
            UUID4::new(),
            OBSERVED.into(),
            None,
            None,
        ))
        .unwrap();
    exit_orders::barrier("cash RPC remains suspended", || {
        server.state.rpc_gate.lock().unwrap().is_none()
    })
    .await;
    let before = collector.snapshot();
    let first = sell(1, 1001);
    cache
        .borrow_mut()
        .add_order(first.clone(), None, None, false)
        .unwrap();
    client
        .submit_order(priced(&first, json!("0.020"), 100))
        .unwrap();
    exit_orders::barrier("POST before account refresh completes", || {
        server.state.creations.lock().unwrap().len() == 1
    })
    .await;
    assert!(client.is_submission_ready());
    assert_eq!(
        server.state.creations.lock().unwrap()[0]["data"]["order"]["nonce"],
        "7"
    );
    let after = collector.snapshot();
    assert_eq!(
        after.counters["metadata_hit"],
        before.counters["metadata_hit"] + 1,
        "submission must use the still-valid prepared market"
    );
    assert_eq!(
        after.counters["metadata_miss"], before.counters["metadata_miss"],
        "submission must not request market recovery"
    );
    assert_eq!(*server.state.book_requests.lock().unwrap(), 0);
    cash_gate.notify_one();
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn authenticated_reconnect_is_connected_while_account_refresh_is_suspended() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    connect_reported(&mut client, &mut rx).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.rpc_gate.lock().unwrap() = Some(gate.clone());
    server
        .state
        .sockets
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(Message::Close(None))
        .unwrap();
    exit_orders::barrier("new wallet transport", || {
        server.state.sockets.lock().unwrap().len() == 2
    })
    .await;
    wallet_processed(&server).await;
    client
        .query_account(QueryAccount::new(
            trader_id(),
            Some(ClientId::new("PREDICT")),
            account_id(),
            UUID4::new(),
            OBSERVED.into(),
            None,
            None,
        ))
        .unwrap();
    exit_orders::barrier("account RPC is held", || {
        server.state.rpc_gate.lock().unwrap().is_none()
    })
    .await;
    assert!(
        client.is_connected(),
        "account refresh progress is not execution transport connectivity"
    );
    gate.notify_one();
    client.disconnect().await.unwrap();
}
