// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Maker-only venue parameters and receipt metadata, no external services.
use super::*;
use nautilus_common::{live::runner::replace_data_event_sender, messages::DataEvent};
use nautilus_model::{data::Data, events::OrderEventAny, identifiers::ClientOrderId};
use nautilus_predict::{
    PredictLimitOrderReceiptUpdate, PredictSelfTradePrevention,
    predict_limit_order_receipt_data_type,
};
use nautilus_trading::Strategy;

fn maker(side: OrderSide, index: u8) -> OrderAny {
    OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .instrument_id(instrument_id(index))
        .client_order_id(ClientOrderId::new("MAKER-OPTIONS-001"))
        .side(side)
        .quantity(Quantity::from(100))
        .price(Price::from("0.620"))
        .post_only(true)
        .build()
}

#[tokio::test]
async fn native_post_only_stp_and_lock_receipt_are_exact_and_strategy_scoped() {
    for side in [OrderSide::Buy, OrderSide::Sell] {
        for index in [1, 2] {
            for (wire, stp) in [
                ("CANCEL_MAKER", PredictSelfTradePrevention::CancelMaker),
                ("CANCEL_TAKER", PredictSelfTradePrevention::CancelTaker),
                ("CANCEL_BOTH", PredictSelfTradePrevention::CancelBoth),
            ] {
                let server = server(vec![]).await;
                let (data_tx, mut data_rx) = mpsc::unbounded_channel::<DataEvent>();
                replace_data_event_sender(data_tx);
                let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
                let _portfolio = portfolio(&cache);
                cache
                    .borrow_mut()
                    .add_instrument(InstrumentAny::BinaryOption(instrument(index)))
                    .unwrap();
                connect_reported(&mut client, &mut rx).await;
                let own = maker(side, index);
                cache
                    .borrow_mut()
                    .add_order(own.clone(), None, None, false)
                    .unwrap();
                let gate = Arc::new(tokio::sync::Notify::new());
                *server.state.create_gate.lock().unwrap() = Some(gate.clone());
                let mut command = submissions::submit(&own);
                command.params = Some(nautilus_core::Params::new(
                    [("self_trade_prevention".into(), json!(wire))].into(),
                ));
                client.submit_order(command).unwrap();
                let mut engine = submissions::engine(&cache);
                submissions::apply(&mut engine, &next(&mut rx).await);
                exit_orders::barrier("maker options POST", || {
                    !server.state.creations.lock().unwrap().is_empty()
                })
                .await;
                let body = server.state.creations.lock().unwrap()[0].clone();
                assert_eq!(body["data"]["strategy"], "LIMIT");
                assert_eq!(body["data"]["isPostOnly"], true);
                assert_eq!(body["data"]["selfTradePrevention"], wire);
                assert!(body["data"].get("isFillOrKill").is_none());
                assert!(body["data"].get("slippageBps").is_none());
                assert!(body["data"].get("isMinAmountOut").is_none());
                *server.state.create_body.lock().unwrap() = Some(
                    json!({"success":true,"data":{"code":"opaque","orderId":"12345","orderHash":body["data"]["order"]["hash"],"removalLockedUntil":"1970-01-01T00:01:00.123456789Z"}}),
                );
                gate.notify_one();
                let DataEvent::Data(Data::Custom(custom)) =
                    timeout(Duration::from_secs(3), data_rx.recv())
                        .await
                        .unwrap()
                        .unwrap()
                else {
                    panic!("receipt custom data");
                };
                let update = custom
                    .data
                    .as_any()
                    .downcast_ref::<PredictLimitOrderReceiptUpdate>()
                    .unwrap();
                assert_eq!(
                    custom.data_type,
                    predict_limit_order_receipt_data_type(account_id(), own.strategy_id())
                );
                assert_eq!(update.account_id, account_id());
                assert_eq!(update.strategy_id, own.strategy_id());
                assert_eq!(update.instrument_id, own.instrument_id());
                assert_eq!(update.client_order_id, own.client_order_id());
                assert_eq!(
                    update.venue_order_id.as_str(),
                    body["data"]["order"]["hash"].as_str().unwrap()
                );
                assert_eq!(
                    update.removal_locked_until.unwrap().as_u64(),
                    60_123_456_789
                );
                assert_eq!(update.self_trade_prevention, Some(stp));
                assert!(update.is_post_only);
                assert_eq!(
                    cache
                        .borrow()
                        .order(&own.client_order_id())
                        .unwrap()
                        .status(),
                    OrderStatus::Submitted,
                    "receipt is not acceptance"
                );
                assert_eq!(server.state.creations.lock().unwrap().len(), 1);
                client.disconnect().await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn malformed_stp_and_non_limit_combinations_are_denied_without_post() {
    for (kind, value) in [
        (OrderType::Limit, json!("cancel_taker")),
        (OrderType::Limit, json!(true)),
        (OrderType::Limit, Value::Null),
        (OrderType::Limit, json!("unknown")),
        (OrderType::Market, json!("CANCEL_TAKER")),
    ] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let own = if kind == OrderType::Limit {
            maker(OrderSide::Buy, 1)
        } else {
            market_taker::market(OrderSide::Buy, 1, 1, 100)
        };
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        let mut command = submissions::submit(&own);
        command.params = Some(nautilus_core::Params::new(
            [
                ("self_trade_prevention".into(), value),
                ("market_price".into(), json!("0.020")),
            ]
            .into(),
        ));
        if kind == OrderType::Limit {
            command.params = Some(nautilus_core::Params::new(
                [(
                    "self_trade_prevention".into(),
                    command
                        .params
                        .as_ref()
                        .unwrap()
                        .get("self_trade_prevention")
                        .unwrap()
                        .clone(),
                )]
                .into(),
            ));
        }
        client.submit_order(command).unwrap();
        let ExecutionEvent::Order(OrderEventAny::Denied(denied)) = next(&mut rx).await else {
            panic!("invalid STP denied");
        };
        assert!(denied.reason.as_str().contains("self_trade_prevention"));
        assert!(server.state.creations.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}

#[test]
fn receipt_topics_do_not_mix_accounts_or_strategies() {
    let a = predict_limit_order_receipt_data_type("PREDICT-a".into(), "MAKER-001".into());
    assert_ne!(
        a,
        predict_limit_order_receipt_data_type("PREDICT-b".into(), "MAKER-001".into())
    );
    assert_ne!(
        a,
        predict_limit_order_receipt_data_type("PREDICT-a".into(), "MAKER-002".into())
    );
}

#[derive(Debug)]
struct ReceiptSubscriber {
    core: nautilus_trading::StrategyCore,
    active: bool,
    received: Rc<RefCell<Vec<ClientOrderId>>>,
}
impl ReceiptSubscriber {
    fn new(name: &str, active: bool, received: Rc<RefCell<Vec<ClientOrderId>>>) -> Self {
        Self {
            core: nautilus_trading::StrategyCore::new(nautilus_trading::StrategyConfig {
                strategy_id: Some(name.into()),
                ..Default::default()
            }),
            active,
            received,
        }
    }
}
impl nautilus_common::actor::DataActor for ReceiptSubscriber {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_data(
            predict_limit_order_receipt_data_type(account_id(), self.strategy_id().unwrap()),
            None,
            None,
        );
        if self.active {
            let order = self.order().limit(
                instrument_id(1),
                OrderSide::Buy,
                Quantity::from(100),
                Price::from("0.010"),
                Some(nautilus_model::enums::TimeInForce::Gtc),
                None,
                Some(true),
                Some(false),
                Some(false),
                None,
                None,
                None,
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
                    [("self_trade_prevention".into(), json!("CANCEL_TAKER"))].into(),
                )),
            )?;
        }
        Ok(())
    }
    fn on_data(&mut self, data: &nautilus_model::data::CustomData) -> anyhow::Result<()> {
        if let Some(receipt) = data
            .data
            .as_any()
            .downcast_ref::<PredictLimitOrderReceiptUpdate>()
        {
            assert!(self.active, "receipt leaked to another strategy");
            assert_eq!(receipt.strategy_id, self.strategy_id().unwrap());
            assert_eq!(
                data.data_type,
                predict_limit_order_receipt_data_type(account_id(), receipt.strategy_id)
            );
            assert_eq!(
                receipt.self_trade_prevention,
                Some(PredictSelfTradePrevention::CancelTaker)
            );
            assert!(receipt.is_post_only);
            assert!(
                receipt.removal_locked_until.is_none(),
                "absent lock stays unknown"
            );
            self.received.borrow_mut().push(receipt.client_order_id);
        }
        Ok(())
    }
}
nautilus_trading::nautilus_strategy!(ReceiptSubscriber, {});

#[tokio::test(flavor = "current_thread")]
async fn receipt_reaches_only_owning_native_strategy_without_fake_acceptance() {
    let private = server(vec![]).await;
    let (http, ws, public_task) = live_node::public_server(1).await;
    let now = nautilus_core::time::get_atomic_clock_realtime()
        .get_time_ns()
        .as_u64();
    *private.state.rpc_frame.lock().unwrap() =
        (1_000_000_000_000_000_000_000, 1, now / 1_000_000_000);
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
    live.kernel()
        .cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let received = Rc::new(RefCell::new(Vec::new()));
    live.add_strategy(ReceiptSubscriber::new(
        "MAKER-RECEIPT-001",
        true,
        received.clone(),
    ))
    .unwrap();
    live.add_strategy(ReceiptSubscriber::new(
        "MAKER-OTHER-002",
        false,
        Rc::new(RefCell::new(Vec::new())),
    ))
    .unwrap();
    let handle = live.handle();
    let shared_cache = live.kernel().cache.clone();
    let driver = async {
        timeout(Duration::from_secs(8), async {
            while received.borrow().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            shared_cache
                .borrow()
                .order(&received.borrow()[0])
                .unwrap()
                .status(),
            OrderStatus::Submitted,
            "receipt must not fabricate Native acceptance"
        );
        let body = private.state.creations.lock().unwrap()[0].clone();
        for (kind, expected) in [
            ("orderAccepted", OrderStatus::Accepted),
            ("orderCancelled", OrderStatus::Canceled),
        ] {
            private.wallet(exit_orders::wire_event(&body, kind, 1, Decimal::ZERO));
            timeout(Duration::from_secs(3), async {
                while shared_cache
                    .borrow()
                    .order(&received.borrow()[0])
                    .unwrap()
                    .status()
                    != expected
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        handle.stop();
    };
    let (result, ()) = tokio::join!(live.run(), driver);
    result.unwrap();
    assert_eq!(received.borrow().len(), 1);
    let cache = live.kernel().cache.borrow();
    let own = cache.order(&received.borrow()[0]).unwrap();
    assert_eq!(
        own.status(),
        OrderStatus::Canceled,
        "explicit wallet cleanup resolved the submission"
    );
    assert_eq!(private.state.creations.lock().unwrap().len(), 1);
    public_task.abort();
}

#[tokio::test]
async fn http_flag_names_cannot_silently_override_native_maker_intent() {
    for (key, value, reason) in [
        ("isPostOnly", json!(false), "Native LIMIT is_post_only"),
        (
            "selfTradePrevention",
            json!("CANCEL_TAKER"),
            "params[self_trade_prevention]",
        ),
    ] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = submissions::setup_client(&server, OBSERVED);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let own = maker(OrderSide::Buy, 1);
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        let mut command = submissions::submit(&own);
        command.params = Some(nautilus_core::Params::new([(key.into(), value)].into()));
        client.submit_order(command).unwrap();
        let ExecutionEvent::Order(OrderEventAny::Denied(denied)) = next(&mut rx).await else {
            panic!("HTTP flags must not override Native intent");
        };
        assert!(denied.reason.as_str().contains(reason));
        assert!(server.state.creations.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}
