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

//! Native MARKET commands require strategy prices in both directions; no book/funds preflight.
use nautilus_common::msgbus::TypedHandler;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::events::OrderEventAny;
use nautilus_testkit::testers::{ExecTester, ExecTesterConfig};
use nautilus_trading::StrategyConfig;

use super::*;

pub(super) fn market(side: OrderSide, index: u8, number: u32, quantity: u64) -> OrderAny {
    OrderTestBuilder::new(OrderType::Market)
        .trader_id(trader_id())
        .instrument_id(instrument_id(index))
        .client_order_id(format!("TAKER-{side}-{number}").into())
        .side(side)
        .quantity(Quantity::from(quantity))
        .build()
}

#[tokio::test]
async fn missing_market_price_denies_both_sides_without_submission_or_queries() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let rpc = server.state.rpc_methods.lock().unwrap().len();
    let open = server.state.requests.lock().unwrap().len();
    let mut engine = submissions::engine(&cache);
    for side in [OrderSide::Sell, OrderSide::Buy] {
        let own = market(side, 1, 1, 100);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        client.submit_order(submissions::submit(&own)).unwrap();
        let event = next(&mut rx).await;
        let ExecutionEvent::Order(OrderEventAny::Denied(denied)) = &event else {
            panic!("missing price must deny before Submitted");
        };
        assert!(
            denied
                .reason
                .as_str()
                .contains("requires strategy market_price")
        );
        submissions::apply(&mut engine, &event);
        assert_eq!(
            cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Denied
        );
    }
    assert!(server.state.creations.lock().unwrap().is_empty());
    assert_eq!(*server.state.book_requests.lock().unwrap(), 0);
    assert_eq!(server.state.rpc_methods.lock().unwrap().len(), rpc);
    assert_eq!(server.state.requests.lock().unwrap().len(), open);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn market_buy_signs_sdk_slippage_for_each_outcome_without_remote_preflight() {
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
        let own = market(OrderSide::Buy, index, 1, 100);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        client
            .submit_order(market_preparation::priced(&own, json!("0.020"), 100))
            .unwrap();
        let mut engine = submissions::engine(&cache);
        timeout(Duration::from_secs(3), async {
            loop {
                if let Ok(event) = rx.try_recv() {
                    submissions::apply(&mut engine, &event);
                }
                if !server.state.creations.lock().unwrap().is_empty()
                    && cache
                        .borrow()
                        .order(&own.client_order_id())
                        .unwrap()
                        .status()
                        == nautilus_model::enums::OrderStatus::Submitted
                    && cache
                        .borrow()
                        .account(&account_id())
                        .unwrap()
                        .balance_locked(Some(Currency::USDT()))
                        .unwrap()
                        .as_decimal()
                        == Decimal::ZERO
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let posts = server.state.creations.lock().unwrap().clone();
        assert_eq!(posts.len(), 1);
        assert_eq!(posts[0]["data"]["strategy"], "MARKET");
        assert_eq!(posts[0]["data"]["order"]["side"], 0);
        assert_eq!(
            posts[0]["data"]["order"]["makerAmount"],
            "2020000000000000000"
        );
        assert_eq!(
            posts[0]["data"]["order"]["takerAmount"],
            "100000000000000000000"
        );
        assert_eq!(
            posts[0]["data"]["order"]["tokenId"],
            if index == 1 { "11" } else { "22" }
        );
        assert_eq!(posts[0]["data"]["pricePerShare"], "20000000000000000");
        assert_eq!(posts[0]["data"]["slippageBps"], "100");
        assert_eq!(posts[0]["data"]["isMinAmountOut"], false);
        assert_eq!(*server.state.book_requests.lock().unwrap(), 0);
        assert_eq!(*server.state.market_requests.lock().unwrap(), metadata);
        assert_eq!(server.state.rpc_methods.lock().unwrap().len(), rpc);
        assert_eq!(server.state.requests.lock().unwrap().len(), open);
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn market_buy_signed_amounts_and_binary_ceiling_do_not_impose_adapter_risk_limits() {
    for (quantity, price, expected) in [
        (5000, "0.020", Some("101000000000000000000")),
        (100, "0.009", None),
        (100, "1", Some("100000000000000000000")),
    ] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let own = market(OrderSide::Buy, 1, 1, quantity);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        client
            .submit_order(market_preparation::priced(&own, json!(price), 100))
            .unwrap();
        let event = next(&mut rx).await;
        if let Some(amount) = expected {
            assert!(matches!(
                event,
                ExecutionEvent::Order(OrderEventAny::Submitted(_))
            ));
            exit_orders::barrier("binary maximum MARKET POST", || {
                !server.state.creations.lock().unwrap().is_empty()
            })
            .await;
            assert_eq!(
                server.state.creations.lock().unwrap()[0]["data"]["order"]["makerAmount"],
                amount
            );
            let next_order = market(OrderSide::Buy, 1, 2, 100);
            cache
                .borrow_mut()
                .add_order(next_order.clone(), None, None, false)
                .unwrap();
            client
                .submit_order(market_preparation::priced(&next_order, json!("0.020"), 100))
                .unwrap();
            exit_orders::barrier("another command has no adapter lifetime budget", || {
                server.state.creations.lock().unwrap().len() == 2
            })
            .await;
        } else {
            assert!(matches!(
                event,
                ExecutionEvent::Order(OrderEventAny::Denied(_))
            ));
            assert!(server.state.creations.lock().unwrap().is_empty());
        }
        assert_eq!(*server.state.book_requests.lock().unwrap(), 0);
        client.disconnect().await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_exec_tester_market_buy_partial_share_fee_and_cancel_release_signed_budget() {
    partial_market_buy(false, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn non_fok_partial_match_plan_never_confirms_fill_or_cancels_remainder() {
    partial_market_buy(true, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn quote_budget_partial_cancel_and_replay_preserve_cash_cap_and_net_shares() {
    partial_market_buy(true, true).await;
}

async fn partial_market_buy(with_plans: bool, quote: bool) {
    let server = server(vec![]).await;
    let (http, ws, public_task) = live_node::public_server(1).await;
    let mut execution = config(&server);
    execution.account_state_mode = PredictAccountStateMode::Reported;
    execution.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", server.http).into(),
    ));
    execution.submission_signer = Some(Arc::new(
        nautilus_predict::signing::PredictAuthSigner::new(
            submissions::PUBLIC_KEY.into(),
            PredictEnvironment::BnbMainnet,
            Some(WALLET),
        )
        .unwrap(),
    ));
    *server.state.rpc_frame.lock().unwrap() = (
        3_000_000_000_000_000_000,
        1,
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000 + 1,
    );
    let mut live = node::build_node(
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
    )
    .unwrap();
    let cache = live.kernel().cache.clone();
    let params = [
        ("market_price".into(), json!("0.020")),
        (
            "market_order_slippage_bps".into(),
            json!(if quote { 0 } else { 100 }),
        ),
    ]
    .into_iter()
    .collect::<indexmap::IndexMap<String, Value>>();
    let params = nautilus_core::Params::new(params);
    // Five significant SDK share digits leave 0.000001 USDT uncommitted.
    let requested = if quote { "2.000001" } else { "100" };
    let signed_cash = Decimal::from_str_exact(if quote { "2" } else { "2.02" }).unwrap();
    let remaining_cash = signed_cash * Decimal::new(83, 2);
    let strategy_id = nautilus_model::identifiers::StrategyId::new("PREDICT-TAKER-001");
    live.add_strategy(ExecTester::new(ExecTesterConfig {
        base: StrategyConfig {
            strategy_id: Some(strategy_id),
            ..Default::default()
        },
        instrument_id: instrument_id(1),
        order_qty: Quantity::from(requested),
        client_id: Some(ClientId::new("PREDICT")),
        order_params: Some(params),
        open_position_on_start_qty: Some(Decimal::from_str_exact(requested).unwrap()),
        use_quote_quantity: quote,
        open_position_on_first_quote: true,
        subscribe_quotes: true,
        subscribe_trades: false,
        enable_limit_buys: false,
        enable_limit_sells: false,
        cancel_orders_on_stop: false,
        close_positions_on_stop: false,
        ..Default::default()
    }))
    .unwrap();
    let journal = Rc::new(RefCell::new(Vec::new()));
    let handler = TypedHandler::from({
        let journal = journal.clone();
        let cache = cache.clone();
        move |event: &OrderEventAny| {
            let cached = cache.borrow();
            let own = cached.order(&event.client_order_id()).unwrap();
            assert_eq!(own.strategy_id(), strategy_id);
            journal
                .borrow_mut()
                .push(json!({"kind":format!("{:?}",event.event_type()),
                "status":own.status().to_string(), "filled":own.filled_qty().to_string()}));
        }
    });
    msgbus::subscribe_order_events(
        "events.order.PREDICT-TAKER-001".into(),
        handler.clone(),
        None,
    );
    let handle = live.handle();
    let driver = async {
        exit_orders::barrier("Native ExecTester MARKET BUY reaches POST", || {
            !server.state.creations.lock().unwrap().is_empty()
        })
        .await;
        let body = server.state.creations.lock().unwrap()[0].clone();
        assert_eq!(body["data"]["strategy"], "MARKET");
        assert!(body["data"].get("isFillOrKill").is_none());
        assert_eq!(
            body["data"]["order"]["makerAmount"],
            if quote {
                "2000000000000000000"
            } else {
                "2020000000000000000"
            }
        );
        assert_eq!(body["data"]["slippageBps"], if quote { "0" } else { "100" });
        assert_eq!(
            body["data"]["order"]["takerAmount"],
            "100000000000000000000"
        );
        let locked = || {
            cache
                .borrow()
                .account(&account_id())
                .and_then(|account| account.balance_locked(Some(Currency::USDT())))
                .map(|value| value.as_decimal())
        };
        assert_eq!(
            locked(),
            Some(Decimal::ZERO),
            "Submitted funds belong to Cash RiskEngine reservations"
        );
        let rpc_before = server.state.rpc_methods.lock().unwrap().len();
        let open_before = server.state.requests.lock().unwrap().len();
        let accepted = exit_orders::wire_event(&body, "orderAccepted", 1, Decimal::ZERO);
        server.wallet(accepted.clone());
        server.wallet(accepted);
        exit_orders::barrier("Native strategy Accepted callback", || {
            journal.borrow().iter().any(|row| row["kind"] == "Accepted")
        })
        .await;
        exit_orders::barrier("OPEN account locks signed collateral, not shares", || {
            locked() == Some(signed_cash)
        })
        .await;
        assert_eq!(server.state.rpc_methods.lock().unwrap().len(), rpc_before);
        assert_eq!(server.state.requests.lock().unwrap().len(), open_before);
        // The envelope is from a recorded full fill. Partial quantities below are
        // deliberately synthetic: this does not establish venue IOC semantics.
        let mut plan = exit_orders::wire_event(&body, "orderMarketMatchPlan", 1, Decimal::ZERO);
        if with_plans {
            let recording: Value = serde_json::from_str(include_str!(
                "../fixtures/private_api/native_market_fill_post_only_rejected_20261025.json"
            ))
            .unwrap();
            let actual = recording["walletEvents"]
                .as_array()
                .unwrap()
                .iter()
                .find(|event| event["type"] == "orderMarketMatchPlan")
                .unwrap();
            for key in [
                "amount",
                "amountFilled",
                "plannedFill",
                "remainderCancelled",
                "settlementIds",
            ] {
                plan[key] = actual[key].clone();
            }
            plan["amount"] = json!("100000000000000000000");
            plan["amountFilled"] = json!("0");
            plan["plannedFill"] = json!("17000000000000000000");
            plan["remainderCancelled"] = json!("83000000000000000000");
            plan["settlementIds"] = json!(["market-buy-share-fee"]);
            server.wallet(plan.clone());
            server.wallet(plan.clone());
            wallet_processed(&server).await;
            assert_eq!(locked(), Some(signed_cash));
            assert!(
                !journal
                    .borrow()
                    .iter()
                    .any(|row| row["kind"] == "Filled" || row["kind"] == "Canceled")
            );
        }
        let mut row = live_node::rest_row(&body, "OPEN");
        row["strategy"] = json!("MARKET");
        row["amountFilled"] = json!("17000000000000000000");
        server.state.open_orders.lock().unwrap().push(row);
        strategy_lifecycle::cash(&server, -Decimal::new(34, 2));
        let mut fill =
            exit_orders::settlement(&body, 1, 17, 17, "0.020", "market-buy-share-fee", false);
        fill["fee"] = json!({"type":"SHARES","amountWei":"340000000000000000"});
        server.wallet(fill.clone());
        server.wallet(fill);
        exit_orders::barrier(
            "partial BUY releases only the filled signed fraction",
            || {
                locked() == Some(remaining_cash)
                    && journal.borrow().iter().any(|row| row["kind"] == "Filled")
            },
        )
        .await;
        let order = cache.borrow().orders(None, None, None, None, None)[0].clone();
        assert_eq!(order.status(), OrderStatus::PartiallyFilled);
        assert!(!journal.borrow().iter().any(|row| row["kind"] == "Canceled"));
        assert_eq!(order.filled_qty().as_decimal(), Decimal::from(17));
        assert_eq!(order.leaves_qty().as_decimal(), Decimal::from(83));
        assert_eq!(
            cache
                .borrow()
                .account(&account_id())
                .unwrap()
                .balance_total(Some(Currency::USDT()))
                .unwrap()
                .as_decimal(),
            Decimal::new(266, 2)
        );
        let position = cache.borrow().positions_open(None, None, None, None, None)[0].clone();
        assert_eq!(position.quantity.as_decimal(), Decimal::new(1666, 2));
        let rpc_after_fill = server.state.rpc_methods.lock().unwrap().len();
        let open_after_fill = server.state.requests.lock().unwrap().len();
        if with_plans {
            // A duplicate old plan and a later zero-planned-fill notice still do
            // not establish the unfilled 83 shares' final remote state.
            server.wallet(plan.clone());
            plan["amountFilled"] = json!("17000000000000000000");
            plan["plannedFill"] = json!("0");
            plan["settlementIds"] = json!([]);
            plan["details"]["quantityFilled"] = json!("17");
            server.wallet(plan);
            wallet_processed(&server).await;
            let own = cache.borrow().orders(None, None, None, None, None)[0].clone();
            assert_eq!(own.status(), OrderStatus::PartiallyFilled);
            assert_eq!(own.filled_qty().as_decimal(), Decimal::from(17));
            assert_eq!(locked(), Some(remaining_cash));
            assert!(!journal.borrow().iter().any(|row| row["kind"] == "Canceled"));
        }
        let canceled = exit_orders::wire_event(&body, "orderCancelled", 1, Decimal::from(17));
        server.wallet(canceled.clone());
        server.wallet(canceled);
        exit_orders::barrier(
            "confirmed market cancellation releases remaining cash",
            || {
                locked() == Some(Decimal::ZERO)
                    && journal.borrow().iter().any(|row| row["kind"] == "Canceled")
            },
        )
        .await;
        assert_eq!(
            server.state.rpc_methods.lock().unwrap().len(),
            rpc_after_fill
        );
        assert_eq!(server.state.requests.lock().unwrap().len(), open_after_fill);
        assert_eq!(*server.state.book_requests.lock().unwrap(), 0);
        assert_eq!(server.state.creations.lock().unwrap().len(), 1);
        assert!(server.state.removals.lock().unwrap().is_empty());
        handle.stop();
    };
    timeout(Duration::from_secs(15), async {
        let (result, ()) = tokio::join!(live.run(), driver);
        result.unwrap();
    })
    .await
    .unwrap();
    for kind in ["Accepted", "Filled", "Canceled"] {
        assert_eq!(
            journal
                .borrow()
                .iter()
                .filter(|row| row["kind"] == kind)
                .count(),
            1,
            "one strategy callback per venue event"
        );
    }
    if quote {
        let journal = journal.borrow();
        let updated = journal
            .iter()
            .position(|row| row["kind"] == "Updated")
            .unwrap();
        let submitted = journal
            .iter()
            .position(|row| row["kind"] == "Submitted")
            .unwrap();
        assert!(updated < submitted);
        assert_eq!(
            journal
                .iter()
                .filter(|row| row["kind"] == "Updated")
                .count(),
            1
        );
        let view = cache.borrow();
        let own = view.orders(None, None, None, None, None)[0].clone();
        let replayed = OrderAny::from_events(own.events().into_iter().cloned().collect()).unwrap();
        assert_eq!(replayed.status(), OrderStatus::Canceled);
        assert_eq!(replayed.quantity(), Quantity::from(100));
        assert_eq!(replayed.filled_qty(), Quantity::from(17));
        assert_eq!(replayed.leaves_qty(), Quantity::from(83));
        assert!(!replayed.is_quote_quantity());
        assert!(replayed.init_event().quote_quantity);
        assert_eq!(replayed.init_event().quantity, Quantity::from(requested));
        let mut row = live_node::rest_row(&server.state.creations.lock().unwrap()[0], "CANCELLED");
        row["strategy"] = json!("MARKET");
        row["amountFilled"] = json!("17000000000000000000");
        let valid = serde_json::from_value(row.clone()).unwrap();
        let terminal = nautilus_predict::execution::parse::parse_rest_order_report(
            &valid,
            &replayed,
            &instrument(1),
            account_id(),
            WALLET.parse().unwrap(),
            get_atomic_clock_realtime().get_time_ns(),
        )
        .unwrap();
        assert_eq!(terminal.order_status, OrderStatus::Canceled);
        assert_eq!(
            terminal.filled_qty,
            replayed.filled_qty(),
            "terminal fact does not invent missing fills"
        );
        // Recovery must retain the original quote cap after quantity normalization.
        row["order"]["makerAmount"] = json!("2000002000000000000");
        let row = serde_json::from_value(row).unwrap();
        let error = nautilus_predict::execution::parse::parse_rest_order_report(
            &row,
            &replayed,
            &instrument(1),
            account_id(),
            WALLET.parse().unwrap(),
            get_atomic_clock_realtime().get_time_ns(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("original Native quote budget"));
    }
    assert!(
        !journal
            .borrow()
            .iter()
            .any(|row| row["kind"] == "Rejected" || row["kind"] == "Denied")
    );
    msgbus::unsubscribe_order_events("events.order.PREDICT-TAKER-001".into(), &handler);
    public_task.abort();
}
