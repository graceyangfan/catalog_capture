// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Quote-budget admission/normalization and zero-fill recovery. Loopback only.
use super::*;
use nautilus_common::messages::execution::QueryOrder;
use nautilus_model::{enums::TimeInForce, events::OrderEventAny, identifiers::ClientOrderId};

fn quote_buy(outcome: u8, tif: TimeInForce, value: &str) -> OrderAny {
    let mut factory = nautilus_common::factories::OrderFactory::new(
        trader_id(),
        "VALUE-001".into(),
        None,
        None,
        Rc::new(RefCell::new(VirtualClock::new())),
        false,
        false,
    );
    factory.market(
        instrument_id(outcome),
        OrderSide::Buy,
        Quantity::from(value),
        Some(tif),
        None,
        Some(true),
        None,
        None,
        None,
        Some(ClientOrderId::new("MARKET-VALUE-001")),
    )
}

fn command(order: &OrderAny, price: &str, bps: u32) -> SubmitOrder {
    let mut cmd = submissions::submit(order);
    cmd.params = Some(nautilus_core::Params::new(
        [
            ("market_price".into(), json!(price)),
            ("market_order_slippage_bps".into(), json!(bps)),
        ]
        .into(),
    ));
    cmd
}

#[tokio::test]
async fn native_quote_buy_normalizes_before_post_and_recovers_zero_fill() {
    for outcome in [1, 2] {
        for (tif, http_status, terminal) in [
            (TimeInForce::Gtc, 201, "OPEN"),
            (TimeInForce::Fok, 500, "CANCELLED"),
            (TimeInForce::Fok, 500, "EXPIRED"),
        ] {
            let server = server(vec![]).await;
            server
                .state
                .create_replies
                .lock()
                .unwrap()
                .push_back((http_status, 0, vec![]));
            let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
            let _portfolio = portfolio(&cache);
            cache
                .borrow_mut()
                .add_instrument(InstrumentAny::BinaryOption(instrument(outcome)))
                .unwrap();
            connect_reported(&mut client, &mut rx).await;
            let original = quote_buy(outcome, tif, "2");
            cache
                .borrow_mut()
                .add_order(original.clone(), None, None, false)
                .unwrap();
            client.submit_order(command(&original, "0.020", 0)).unwrap();
            let mut engine = submissions::engine(&cache);
            let updated = next(&mut rx).await;
            assert!(
                matches!(&updated, ExecutionEvent::Order(OrderEventAny::Updated(e)) if !e.is_quote_quantity && e.quantity == Quantity::from(100))
            );
            submissions::apply(&mut engine, &updated);
            let submitted = next(&mut rx).await;
            assert!(matches!(
                &submitted,
                ExecutionEvent::Order(OrderEventAny::Submitted(_))
            ));
            submissions::apply(&mut engine, &submitted);
            {
                let view = cache.borrow();
                let current = view.order(&original.client_order_id()).unwrap();
                assert_eq!(current.status(), OrderStatus::Submitted);
                assert!(!current.is_quote_quantity());
                assert_eq!(current.quantity(), Quantity::from(100));
                assert!(current.init_event().quote_quantity);
                assert_eq!(current.init_event().quantity, Quantity::from(2));
            }
            exit_orders::barrier("quote MARKET POST", || {
                !server.state.creations.lock().unwrap().is_empty()
            })
            .await;
            let body = server.state.creations.lock().unwrap()[0].clone();
            assert_eq!(body["data"]["strategy"], "MARKET");
            assert_eq!(body["data"]["slippageBps"], "0");
            assert_eq!(body["data"]["isMinAmountOut"], false);
            assert_eq!(body["data"]["order"]["makerAmount"], "2000000000000000000");
            assert_eq!(
                body["data"]["order"]["takerAmount"],
                "100000000000000000000"
            );
            assert_eq!(
                body["data"].get("isFillOrKill"),
                (tif == TimeInForce::Fok).then_some(&json!(true))
            );
            let mut row = live_node::rest_row(&body, terminal);
            row["strategy"] = json!("MARKET");
            server
                .state
                .orders
                .lock()
                .unwrap()
                .push_back(json!({"success":true,"data":row.clone()}));
            client
                .query_order(QueryOrder::new(
                    trader_id(),
                    Some(ClientId::new("PREDICT")),
                    original.strategy_id(),
                    original.instrument_id(),
                    original.client_order_id(),
                    None,
                    UUID4::new(),
                    UnixNanos::from(OBSERVED),
                    None,
                    None,
                ))
                .unwrap();
            let expected = match terminal {
                "OPEN" => OrderStatus::Accepted,
                "EXPIRED" => OrderStatus::Expired,
                _ => OrderStatus::Canceled,
            };
            timeout(Duration::from_secs(3), async {
                loop {
                    if cache
                        .borrow()
                        .order(&original.client_order_id())
                        .unwrap()
                        .status()
                        == expected
                    {
                        break;
                    }
                    submissions::apply(&mut engine, &next(&mut rx).await);
                }
            })
            .await
            .unwrap();
            let locked = if terminal == "OPEN" {
                Money::from("2 USDT")
            } else {
                Money::from("0 USDT")
            };
            timeout(Duration::from_secs(3), async {
                loop {
                    if cache
                        .borrow()
                        .account(&account_id())
                        .unwrap()
                        .balance_locked(Some(Currency::USDT()))
                        == Some(locked)
                    {
                        break;
                    }
                    submissions::apply(&mut engine, &next(&mut rx).await);
                }
            })
            .await
            .unwrap();
            let restored = cache
                .borrow()
                .order_owned(&original.client_order_id())
                .unwrap();
            let mut replayed = original.clone();
            for event in restored.events().iter().skip(1) {
                replayed.apply((*event).clone()).unwrap();
            }
            assert_eq!(replayed.quantity(), Quantity::from(100));
            assert!(!replayed.is_quote_quantity());
            assert_eq!(replayed.init_event().quantity, Quantity::from(2));
            let mut wrong = row.clone();
            wrong["order"]["makerAmount"] = json!("2010000000000000000");
            let wrong: nautilus_predict::http::account_models::PredictOrderData =
                serde_json::from_value(wrong).unwrap();
            let error = nautilus_predict::execution::parse::parse_rest_order_report(
                &wrong,
                &replayed,
                &instrument(outcome),
                account_id(),
                WALLET.parse().unwrap(),
                UnixNanos::from(OBSERVED),
            )
            .unwrap_err();
            assert!(error.to_string().contains("original Native quote budget"));
            assert_eq!(restored.quantity(), Quantity::from(100));
            assert_eq!(restored.time_in_force(), tif);
            assert!(restored.filled_qty().is_zero());
            assert!(
                cache
                    .borrow()
                    .positions(None, None, None, None, None)
                    .is_empty()
            );
            assert_eq!(
                server.state.creations.lock().unwrap().len(),
                1,
                "recovery never resubmits"
            );
            client.disconnect().await.unwrap();
        }
    }
}

#[tokio::test]
async fn unsupported_quote_modes_deny_without_normalization_or_post() {
    for (kind, side, value, price, bps) in [
        (OrderType::Market, OrderSide::Buy, "2", "0.020", 100),
        (OrderType::Market, OrderSide::Buy, "0.99", "0.020", 0),
        (
            OrderType::Market,
            OrderSide::Buy,
            "2.00000000001",
            "0.020",
            0,
        ), // quote precision exceeds native USDT
        (OrderType::Market, OrderSide::Sell, "2", "0.020", 0),
        (OrderType::Limit, OrderSide::Buy, "2", "0.020", 0),
    ] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let own = OrderTestBuilder::new(kind)
            .trader_id(trader_id())
            .instrument_id(instrument_id(1))
            .side(side)
            .quantity(Quantity::from(value))
            .quote_quantity(true)
            .price(Price::from("0.020"))
            .build();
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        let cmd = if kind == OrderType::Market {
            command(&own, price, bps)
        } else {
            submissions::submit(&own)
        };
        client.submit_order(cmd).unwrap();
        let event = next(&mut rx).await;
        assert!(
            matches!(event, ExecutionEvent::Order(OrderEventAny::Denied(_))),
            "unexpected {kind:?}/{side:?}/{value}/{price}/{bps}: {event:?}"
        );
        assert!(
            cache
                .borrow()
                .order(&own.client_order_id())
                .unwrap()
                .is_quote_quantity()
        );
        assert!(server.state.creations.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}

#[derive(Debug)]
struct QuoteBuyer {
    core: nautilus_trading::StrategyCore,
    callbacks: Rc<RefCell<Vec<&'static str>>>,
}
impl nautilus_common::actor::DataActor for QuoteBuyer {
    fn on_start(&mut self) -> anyhow::Result<()> {
        use nautilus_trading::Strategy;
        let order = self.order().market(
            instrument_id(1),
            OrderSide::Buy,
            Quantity::from(2),
            Some(TimeInForce::Fok),
            None,
            Some(true),
            None,
            None,
            None,
            None,
        );
        self.submit_order(
            order,
            None,
            Some(ClientId::new("PREDICT")),
            Some(nautilus_core::Params::new(
                [
                    ("market_price".into(), json!("0.020")),
                    ("market_order_slippage_bps".into(), json!(0)),
                ]
                .into(),
            )),
        )
    }
}
nautilus_trading::nautilus_strategy!(QuoteBuyer, {
    fn on_order_updated(&mut self, event: nautilus_model::events::OrderUpdated) {
        use nautilus_common::actor::DataActor;
        let cache = self.cache();
        let own = cache.order(&event.client_order_id).unwrap();
        assert_eq!(own.status(), OrderStatus::Initialized);
        assert_eq!(own.quantity(), Quantity::from(100));
        assert!(!own.is_quote_quantity());
        assert!(own.init_event().quote_quantity);
        self.callbacks.borrow_mut().push("updated");
    }
    fn on_order_submitted(&mut self, _: nautilus_model::events::OrderSubmitted) {
        assert_eq!(&*self.callbacks.borrow(), &["updated"]);
        self.callbacks.borrow_mut().push("submitted");
    }
    fn on_order_rejected(&mut self, _: nautilus_model::events::OrderRejected) {
        self.callbacks.borrow_mut().push("rejected");
    }
});

#[tokio::test(flavor = "current_thread")]
async fn native_strategy_quote_buy_observes_normalization_before_submitted_and_zero_match() {
    let private = server(vec![]).await;
    let (http, ws, public_task) = live_node::public_server(1).await;
    let now = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
    *private.state.rpc_frame.lock().unwrap() = (
        1_000_000_000_000_000_000_000,
        1,
        now.as_u64() / 1_000_000_000,
    );
    let mut execution = config(&private);
    execution.account_state_mode = PredictAccountStateMode::Reported;
    execution.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    execution.submission_signer = Some(Arc::new(
        nautilus_predict::signing::PredictAuthSigner::new(
            submissions::PUBLIC_KEY.into(),
            PredictEnvironment::BnbMainnet,
            Some(WALLET),
        )
        .unwrap(),
    ));
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
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    cache
        .borrow_mut()
        .add_quote(nautilus_model::data::QuoteTick::new(
            instrument_id(1),
            Price::from("0.010"),
            Price::from("0.020"),
            Quantity::from(1000),
            Quantity::from(1000),
            now,
            now,
        ))
        .unwrap();
    let callbacks = Rc::new(RefCell::new(Vec::new()));
    live.add_strategy(QuoteBuyer {
        core: nautilus_trading::StrategyCore::new(nautilus_trading::StrategyConfig {
            strategy_id: Some("QUOTE-BUY-001".into()),
            ..Default::default()
        }),
        callbacks: callbacks.clone(),
    })
    .unwrap();
    let handle = live.handle();
    let driver = async {
        exit_orders::barrier("native strategy quote POST and ordered callbacks", || {
            private.state.creations.lock().unwrap().len() == 1 && callbacks.borrow().len() == 2
        })
        .await;
        let body = private.state.creations.lock().unwrap()[0].clone();
        assert_eq!(body["data"]["order"]["makerAmount"], "2000000000000000000");
        private.wallet(exit_orders::wire_event(
            &body,
            "orderAccepted",
            1,
            Decimal::ZERO,
        ));
        exit_orders::barrier("normalized Native accepted", || {
            cache
                .borrow()
                .orders(None, None, None, None, None)
                .iter()
                .any(|o| o.status() == OrderStatus::Accepted)
        })
        .await;
        let mut rejected = exit_orders::wire_event(&body, "orderNotAccepted", 1, Decimal::ZERO);
        rejected["reason"] = json!("noMarketMatch");
        private.wallet(rejected);
        exit_orders::barrier("zero-match Native rejection", || {
            callbacks.borrow().len() == 3
        })
        .await;
        assert_eq!(&*callbacks.borrow(), &["updated", "submitted", "rejected"]);
        let view = cache.borrow();
        let orders = view.orders(None, None, None, None, None);
        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].status(), OrderStatus::Rejected);
        assert!(orders[0].filled_qty().is_zero());
        assert!(view.positions(None, None, None, None, None).is_empty());
        drop(orders);
        drop(view);
        handle.stop();
    };
    let (result, ()) = tokio::join!(live.run(), driver);
    result.unwrap();
    public_task.abort();
}

#[tokio::test]
async fn restored_normalization_cannot_loosen_original_quote_budget() {
    for (price, bps, reason) in [
        ("0.021", 0, "original Native quote budget"),
        ("0.020", 100, "market_order_slippage_bps=0"),
    ] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let mut own = quote_buy(1, TimeInForce::Fok, "2");
        // Crash boundary: only Initialized + normalization were persisted, before Submitted.
        own.apply(OrderEventAny::Updated(
            nautilus_model::events::OrderUpdated::new(
                own.trader_id(),
                own.strategy_id(),
                own.instrument_id(),
                own.client_order_id(),
                Quantity::from(100),
                UUID4::new(),
                UnixNanos::from(OBSERVED),
                UnixNanos::from(OBSERVED),
                false,
                None,
                None,
                None,
                None,
                None,
                false,
            ),
        ))
        .unwrap();
        assert!(!own.is_quote_quantity());
        assert_eq!(own.status(), OrderStatus::Initialized);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        client.submit_order(command(&own, price, bps)).unwrap();
        let ExecutionEvent::Order(OrderEventAny::Denied(event)) = next(&mut rx).await else {
            panic!("restored quote boundary denied")
        };
        assert!(event.reason.as_str().contains(reason));
        assert!(server.state.creations.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}
