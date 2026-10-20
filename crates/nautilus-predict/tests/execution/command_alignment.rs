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

//! Native strategy commands and callbacks against loopback HTTP/WS/RPC only.
use nautilus_common::{actor::DataActor, msgbus::TypedHandler};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_live::node::LiveNodeHandle;
use nautilus_model::{
    data::CustomData,
    enums::TimeInForce,
    events::{OrderEventAny, OrderModifyRejected},
    identifiers::{ClientOrderId, StrategyId},
};
use nautilus_predict::{
    PredictDataClientConfig, PredictSubmissionConfig, predict_orderbook_data_type,
};
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    PreflightRejected,
    DisconnectedPreflight,
    BatchMixed,
    BatchUnknown,
    CancelAll,
    LiveMultiple,
    LiveUnknown,
    LiveRiskDenied,
}

impl Case {
    fn is_preflight(self) -> bool {
        matches!(self, Self::PreflightRejected | Self::DisconnectedPreflight)
    }
}

#[derive(Debug)]
struct CommandStrategy {
    core: StrategyCore,
    case: Case,
    handle: LiveNodeHandle,
    journal: Rc<RefCell<Vec<Value>>>,
    ids: Vec<ClientOrderId>,
    snapshot: bool,
    command_gate: Rc<std::cell::Cell<bool>>,
    sent: bool,
    cleanup: bool,
    cancel_after_modify: Option<ClientOrderId>,
    account_handler: Option<TypedHandler<AccountState>>,
}
impl CommandStrategy {
    fn drive(&mut self) -> anyhow::Result<()> {
        if !self.snapshot {
            return Ok(());
        }
        if let Some(id) = self.cancel_after_modify.take() {
            self.cancel_order(id, Some(ClientId::new("PREDICT")), None)?;
        }
        let count = if self.case.is_preflight() { 1 } else { 2 };
        let accepted = self.ids.iter().all(|id| {
            self.cache()
                .order(id)
                .is_some_and(|order| order.status() == OrderStatus::Accepted)
        });
        let risk_denied = self.case == Case::LiveRiskDenied
            && self
                .journal
                .borrow()
                .iter()
                .any(|row| row["type"] == "Denied");
        if risk_denied
            && !self.sent
            && self.ids.iter().any(|id| {
                self.cache()
                    .order(id)
                    .is_some_and(|order| order.status() == OrderStatus::Accepted)
            })
        {
            self.sent = true;
            self.cancel_all_orders(
                instrument_id(1),
                None,
                Some(ClientId::new("PREDICT")),
                false,
                None,
            )?;
        }
        if self.ids.len() < count
            && (accepted || matches!(self.case, Case::LiveUnknown | Case::LiveRiskDenied))
        {
            let order = self.order().limit(
                instrument_id(1),
                OrderSide::Buy,
                Quantity::from(100),
                Price::from("0.010"),
                Some(TimeInForce::Gtc),
                None,
                Some(true),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            );
            self.ids.push(order.client_order_id());
            self.submit_order(order, None, Some(ClientId::new("PREDICT")), None)?;
        } else if self.ids.len() == count && accepted && !self.sent && self.command_gate.get() {
            self.sent = true;
            self.journal
                .borrow_mut()
                .push(json!({"type":"CommandDispatch","case":format!("{:?}",self.case)}));
            match self.case {
                Case::PreflightRejected | Case::DisconnectedPreflight => self.modify_order(
                    self.ids[0],
                    None,
                    Some(Price::from("0.009")),
                    None,
                    Some(ClientId::new("PREDICT")),
                    None,
                )?,
                Case::BatchMixed | Case::BatchUnknown => {
                    self.cancel_orders(self.ids.clone(), Some(ClientId::new("PREDICT")), None)?
                }
                Case::CancelAll | Case::LiveMultiple | Case::LiveUnknown | Case::LiveRiskDenied => {
                    self.cancel_all_orders(
                        instrument_id(1),
                        Some(OrderSide::Buy),
                        Some(ClientId::new("PREDICT")),
                        false,
                        None,
                    )?
                }
            }
        }
        let events = self.journal.borrow();
        let rejected = events
            .iter()
            .filter(|row| row["type"] == "CancelRejected")
            .count();
        let canceled = events
            .iter()
            .filter(|row| row["type"] == "Canceled")
            .count();
        drop(events);
        if self.case == Case::BatchMixed && rejected == 2 && canceled == 0 && !self.cleanup {
            self.cleanup = true;
            self.cancel_all_orders(
                instrument_id(1),
                None,
                Some(ClientId::new("PREDICT")),
                false,
                None,
            )?;
        }
        if canceled == count - usize::from(risk_denied)
            && self.cache().account(&account_id()).is_some_and(|account| {
                account.balance_locked(Some(Currency::USDT())) == Some(Money::from("0 USDT"))
            })
        {
            self.handle.stop();
        }
        Ok(())
    }
}
impl DataActor for CommandStrategy {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_data(
            predict_orderbook_data_type(42),
            Some(ClientId::new("PREDICT")),
            None,
        );
        let journal = self.journal.clone();
        let handler = TypedHandler::from(move |state: &AccountState| {
            journal
                .borrow_mut()
                .push(json!({"type":"AccountState","balances":state.balances}));
        });
        msgbus::subscribe_account_state(
            format!("events.account.{}", account_id()).into(),
            handler.clone(),
            None,
        );
        self.account_handler = Some(handler);
        self.clock().set_timer(
            "PREDICT-COMMAND-REPLAY",
            Duration::from_millis(20),
            None,
            None,
            None,
            None,
            None,
        )?;
        Ok(())
    }
    fn on_data(&mut self, _: &CustomData) -> anyhow::Result<()> {
        self.snapshot = true;
        Ok(())
    }
    fn on_time_event(&mut self, _: &nautilus_common::timer::TimeEvent) -> anyhow::Result<()> {
        self.drive()
    }
    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.clock().cancel_timer("PREDICT-COMMAND-REPLAY");
        if let Some(handler) = self.account_handler.take() {
            msgbus::unsubscribe_account_state(
                format!("events.account.{}", account_id()).into(),
                &handler,
            );
        }
        self.unsubscribe_data(
            predict_orderbook_data_type(42),
            Some(ClientId::new("PREDICT")),
            None,
        );
        Ok(())
    }
}
nautilus_strategy!(CommandStrategy, {
    fn on_order_event(&mut self, event: OrderEventAny) {
        let cache = self.cache();
        let order = cache.order(&event.client_order_id()).unwrap();
        let reason = match &event {
            OrderEventAny::CancelRejected(e) => Some(e.reason.as_str()),
            OrderEventAny::ModifyRejected(e) => Some(e.reason.as_str()),
            OrderEventAny::Denied(e) => Some(e.reason.as_str()),
            OrderEventAny::Rejected(e) => Some(e.reason.as_str()),
            _ => None,
        };
        self.journal.borrow_mut().push(json!({"type":format!("{:?}",event.event_type()),
            "id":event.client_order_id().to_string(),"reason":reason,"status":order.status().to_string(),
            "price":order.price().map(|p|p.to_string()),"filled":order.filled_qty().to_string()}));
    }
    fn on_order_modify_rejected(&mut self, event: OrderModifyRejected) {
        self.journal.borrow_mut().push(json!({"type":"ModifyRejectedCallback","status":self.cache().order(&event.client_order_id).unwrap().status().to_string()}));
        self.cancel_after_modify = Some(event.client_order_id);
    }
});

async fn run(case: Case) -> Value {
    run_with_saved(case, None).await
}
async fn run_with_saved(case: Case, saved: Option<&Value>) -> Value {
    let private = server(vec![]).await;
    if case == Case::LiveRiskDenied {
        private.state.rpc_frame.lock().unwrap().0 = 1_500_000_000_000_000_000;
    }
    if case == Case::LiveMultiple {
        private.state.rpc_frame.lock().unwrap().0 = 2_000_000_000_000_000_000;
    }
    let create_gate = Arc::new(tokio::sync::Notify::new());
    if case == Case::LiveRiskDenied {
        *private.state.create_gate.lock().unwrap() = Some(create_gate.clone());
    }
    private.state.rpc_frame.lock().unwrap().2 =
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
    let (http, ws, public_task) = live_node::public_server(1).await;
    let mut exec = config(&private);
    exec.account_state_mode = PredictAccountStateMode::Reported;
    exec.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    exec.orderbook_removal_enabled = !case.is_preflight();
    exec.account_refresh_interval_secs = 1;
    exec.submission = Some(PredictSubmissionConfig {
        signer: Arc::new(
            nautilus_predict::signing::PredictAuthSigner::new(
                submissions::PUBLIC_KEY.into(),
                PredictEnvironment::BnbMainnet,
                Some(WALLET),
            )
            .unwrap(),
        ),
        max_order_usdt: Decimal::ONE,
        max_total_usdt: Decimal::from(2),
        require_post_only: true,
        market_slippage_bps: 100,
    });
    let mut node = node::build_node(
        trader_id(),
        PredictDataClientConfig {
            api_key: KEY.into(),
            market_ids: vec![42],
            base_url_http: Some(http),
            base_url_ws: Some(ws),
            http_timeout_secs: 2,
            ws_timeout_secs: 2,
            dynamic_refresh_secs: 30,
            transport_backend: TransportBackend::Tungstenite,
        },
        exec,
    )
    .unwrap();
    // Historical command fixtures retain their pre-existing cache setup. LiveMultiple
    // and LiveUnknown instead create both orders through the real Rust strategy.
    let mut retained_ids = vec![];
    let mut retained_rows = vec![];
    if !case.is_preflight()
        && !matches!(
            case,
            Case::LiveMultiple | Case::LiveUnknown | Case::LiveRiskDenied
        )
    {
        let cache = node.kernel().cache.clone();
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        for n in 0..2 {
            let mut row = saved
                .map(|session| session["initialCachedOrders"][n].clone())
                .unwrap_or_else(|| rest_order_data("OPEN", 1, OrderSide::Buy));
            row["id"] = json!((12345 + n).to_string());
            row["order"]["salt"] = json!((100 + n).to_string());
            let fields = serde_json::from_value(row["order"].clone()).unwrap();
            let hash =
                nautilus_predict::signing::PredictOrderHasher::new(PredictEnvironment::BnbMainnet)
                    .order_hash(
                        &fields,
                        nautilus_predict::common::contracts::PredictExchangeType::Standard,
                    );
            row["order"]["hash"] = json!(hash);
            let mut order = OrderTestBuilder::new(OrderType::Limit)
                .trader_id(trader_id())
                .strategy_id(StrategyId::new("PREDICT-COMMAND-001"))
                .client_order_id(ClientOrderId::new(format!("RETAINED-CANCEL-{n}")))
                .instrument_id(instrument_id(1))
                .side(OrderSide::Buy)
                .quantity(Quantity::from(100))
                .price(Price::from("0.620"))
                .build();
            let factory =
                OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
            let now = get_atomic_clock_realtime().get_time_ns();
            order
                .apply(factory.generate_order_submitted(&order, now))
                .unwrap();
            order
                .apply(factory.generate_order_accepted(
                    &order,
                    VenueOrderId::new(format!("{hash:#x}")),
                    now,
                    now,
                ))
                .unwrap();
            retained_ids.push(order.client_order_id());
            cache
                .borrow_mut()
                .add_order(
                    OrderAny::try_from(order.init_event().clone()).unwrap(),
                    None,
                    Some(ClientId::new("PREDICT")),
                    false,
                )
                .unwrap();
            for event in order.events().into_iter().skip(1) {
                cache.borrow_mut().update_order(event).unwrap();
            }
            private.state.orders_by_hash.lock().unwrap().insert(
                format!("{hash:#x}"),
                json!({"success":true,"data":row.clone()}),
            );
            private.state.open_orders.lock().unwrap().push(row.clone());
            retained_rows.push(row);
        }
    }
    let command_gate = Rc::new(std::cell::Cell::new(case != Case::DisconnectedPreflight));
    let journal = Rc::new(RefCell::new(vec![]));
    let handle = node.handle();
    let engine = node.kernel().exec_engine.clone();
    let cache = node.kernel().cache.clone();
    node.add_strategy(CommandStrategy {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::new("PREDICT-COMMAND-001")),
            use_uuid_client_order_ids: true,
            ..Default::default()
        }),
        case,
        handle: handle.clone(),
        journal: journal.clone(),
        ids: retained_ids,
        snapshot: false,
        command_gate: command_gate.clone(),
        sent: false,
        cleanup: false,
        cancel_after_modify: None,
        account_handler: None,
    })
    .unwrap();
    if let Some(saved) = saved {
        private.state.remove_replies.lock().unwrap().extend(
            saved["removalResponses"]
                .as_array()
                .unwrap()
                .iter()
                .map(|reply| {
                    let mut body = reply["body"].clone();
                    if body["removed"]
                        .as_array()
                        .is_some_and(|ids| !ids.is_empty())
                    {
                        assert_eq!(body["removed"], reply["request"]["data"]["ids"]);
                        // Saved successes follow the current request's routing ID; concurrent
                        // worker scheduling may choose a different leg for the saved failure.
                        body = json!({"echoRemoved":true});
                    }
                    (reply["status"].as_u64().unwrap() as u16, body, 0)
                }),
        );
    } else {
        if case == Case::BatchMixed {
            private.state.remove_replies.lock().unwrap().push_back((
                400,
                json!({"success":false,"message":"fixture business rejection"}),
                0,
            ));
        }
        if case == Case::BatchUnknown {
            private.state.remove_replies.lock().unwrap().push_back((
                500,
                json!({"success":false,"message":"fixture unknown result"}),
                0,
            ));
        }
        for _ in 0..2 {
            private.state.remove_replies.lock().unwrap().push_back((
                200,
                json!({"echoRemoved":true}),
                0,
            ));
        }
    }
    if case == Case::LiveUnknown {
        private
            .state
            .create_replies
            .lock()
            .unwrap()
            .push_back((500, 0, vec![]));
    }
    let expected_posts = if matches!(case, Case::LiveMultiple | Case::LiveUnknown) {
        2
    } else if case == Case::LiveRiskDenied {
        1
    } else {
        usize::from(case.is_preflight())
    };
    *private.state.create_order_ids.lock().unwrap() = (0..expected_posts)
        .map(|n| (12345 + n).to_string())
        .collect();
    let mut wallet = vec![];
    let driver = async {
        for n in 0..expected_posts {
            exit_orders::barrier("strategy submit", || {
                private.state.creations.lock().unwrap().len() > n
            })
            .await;
            if case == Case::LiveRiskDenied {
                exit_orders::barrier("native risk denies second pending order", || {
                    journal.borrow().iter().any(|row| row["type"] == "Denied")
                })
                .await;
                create_gate.notify_one();
            }
            let body = private.state.creations.lock().unwrap()[n].clone();
            let id = (12345 + n).to_string();
            let mut row = live_node::rest_row(&body, "OPEN");
            row["id"] = json!(id);
            private.state.orders_by_hash.lock().unwrap().insert(
                body["data"]["order"]["hash"].as_str().unwrap().to_owned(),
                json!({"success":true,"data":row.clone()}),
            );
            if !(case == Case::LiveUnknown && n == 0) {
                private.state.open_orders.lock().unwrap().push(row);
            }
            let mut accepted = exit_orders::wire_event(&body, "orderAccepted", 1, Decimal::ZERO);
            accepted["orderId"] = json!(id);
            wallet.push(accepted.clone());
            if !(case == Case::LiveUnknown && n == 0) {
                private.wallet(accepted);
            }
        }
        if case == Case::LiveUnknown {
            let unknown = cache
                .borrow()
                .orders(None, None, None, None, None)
                .into_iter()
                .find(|order| order.status() == OrderStatus::Submitted)
                .unwrap()
                .clone();
            journal.borrow_mut().push(json!({"type":"UnknownRetainedWhileSecondPosted","status":unknown.status().to_string(),"posts":expected_posts}));
            let body = private.state.creations.lock().unwrap()[0].clone();
            private
                .state
                .open_orders
                .lock()
                .unwrap()
                .push(live_node::rest_row(&body, "OPEN"));
            private.wallet(wallet[0].clone());
        }
        if case == Case::DisconnectedPreflight {
            exit_orders::barrier("native entry acceptance", || {
                !cache
                    .borrow()
                    .orders_open(None, None, None, None, None)
                    .is_empty()
            })
            .await;
            private
                .state
                .sockets
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .send(Message::Close(None))
                .unwrap();
            exit_orders::barrier("wallet gap admission closed", || {
                !engine
                    .borrow()
                    .get_client(&ClientId::new("PREDICT"))
                    .unwrap()
                    .is_connected()
            })
            .await;
            command_gate.set(true);
        }
        if case == Case::BatchUnknown {
            exit_orders::barrier("unknown batch response", || {
                private.state.removal_responses.lock().unwrap().len() == 1
            })
            .await;
            assert!(
                !journal
                    .borrow()
                    .iter()
                    .any(|row| row["type"] == "Canceled" || row["type"] == "CancelRejected")
            );
            assert_eq!(private.state.removals.lock().unwrap().len(), 1);
            // A single unknown HTTP batch result leaves both legs unresolved, unlike the
            // historical per-request fixture. Recover each leg through real-shaped WS reports.
            for row in &retained_rows {
                let mut canceled = event("orderCancelled", 1);
                canceled["orderHash"] = row["order"]["hash"].clone();
                canceled["orderId"] = row["id"].clone();
                canceled["timestamp"] =
                    json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
                canceled["details"]["quantity"] = json!("100");
                canceled["details"]["price"] = json!("0.620");
                canceled["details"]["value"] = json!("62");
                private
                    .state
                    .open_orders
                    .lock()
                    .unwrap()
                    .retain(|r| r["id"] != row["id"]);
                wallet.extend([canceled.clone(), canceled.clone()]);
                private.wallet(canceled.clone());
                private.wallet(canceled);
            }
        }
        if case.is_preflight() {
            exit_orders::barrier("strategy cancellation rejection", || {
                journal
                    .borrow()
                    .iter()
                    .any(|row| row["type"] == "CancelRejected")
            })
            .await;
            assert!(private.state.removals.lock().unwrap().is_empty());
            let mut canceled = wallet[0].clone();
            canceled["type"] = json!("orderCancelled");
            private.state.open_orders.lock().unwrap().clear();
            wallet.push(canceled.clone());
            // Venue fixture cleanup after asserting disabled REST removal made no write.
            if case == Case::DisconnectedPreflight {
                exit_orders::barrier("wallet resubscribed for fixture cleanup", || {
                    private.state.sockets.lock().unwrap().len() > 1
                })
                .await;
                let body = private.state.creations.lock().unwrap()[0].clone();
                let mut row = live_node::rest_row(&body, "CANCELLED");
                row["amountFilled"] = json!("0");
                private.state.orders_by_hash.lock().unwrap().insert(
                    body["data"]["order"]["hash"].as_str().unwrap().to_owned(),
                    json!({"success":true,"data":row}),
                );
                *private.state.position_snapshot.lock().unwrap() = Some(vec![]);
                *private.state.match_snapshot.lock().unwrap() = Some(vec![]);
            }
            private.wallet(canceled);
            if case == Case::DisconnectedPreflight {
                exit_orders::barrier("native terminal cleanup applied", || {
                    cache
                        .borrow()
                        .orders_open(None, None, None, None, None)
                        .is_empty()
                })
                .await;
                engine
                    .borrow()
                    .get_client(&ClientId::new("PREDICT"))
                    .unwrap()
                    .query_account(QueryAccount::new(
                        trader_id(),
                        Some(ClientId::new("PREDICT")),
                        account_id(),
                        UUID4::new(),
                        get_atomic_clock_realtime().get_time_ns(),
                        None,
                        None,
                    ))
                    .unwrap();
            }
        }
    };
    timeout(Duration::from_secs(12), async {
        tokio::join!(driver, node.run())
    })
    .await
    .unwrap()
    .1
    .unwrap();
    let events = journal.borrow().clone();
    assert_eq!(
        private.state.creations.lock().unwrap().len(),
        expected_posts
    );
    if case == Case::LiveRiskDenied {
        let denied = events
            .iter()
            .filter(|row| row["type"] == "Denied")
            .collect::<Vec<_>>();
        assert_eq!(denied.len(), 1);
        assert!(
            denied[0]["reason"]
                .as_str()
                .unwrap()
                .starts_with("NOTIONAL_EXCEEDS_FREE_BALANCE")
        );
    }
    let rejects = events
        .iter()
        .filter(|row| row["type"] == "CancelRejected")
        .collect::<Vec<_>>();
    assert_eq!(
        rejects.len(),
        if case == Case::BatchMixed {
            2
        } else {
            usize::from(case.is_preflight())
        }
    );
    for row in rejects {
        assert_eq!(row["status"], "ACCEPTED", "{row}");
    }
    if case.is_preflight() {
        let reject = events
            .iter()
            .find(|row| row["type"] == "ModifyRejected")
            .unwrap();
        assert_eq!(reject["status"], "ACCEPTED");
        assert_eq!(reject["price"], "0.010");
        assert!(private.state.removals.lock().unwrap().is_empty());
    } else {
        assert_eq!(
            private.state.removals.lock().unwrap().len(),
            if case == Case::BatchMixed { 2 } else { 1 }
        );
    }
    assert_eq!(
        cache
            .borrow()
            .orders_open(None, None, None, None, None)
            .len(),
        0
    );
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    assert_eq!(
        cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .balance_total(Some(Currency::USDT())),
        Some(Money::from(if case == Case::LiveRiskDenied {
            "1.5 USDT"
        } else if case == Case::LiveMultiple {
            "2 USDT"
        } else {
            "1000 USDT"
        }))
    );
    assert!(events.iter().any(|row| row["type"] == "AccountState"));
    let output = json!({"case":format!("{case:?}"),"mainnetReads":0,"mainnetWrites":0,"feedback":events,"initialCachedOrders":retained_rows,
        "requests":private.state.creations.lock().unwrap().clone(),"removals":private.state.removals.lock().unwrap().clone(),
        "removalResponses":private.state.removal_responses.lock().unwrap().clone(),"wallet":wallet,
        "finalOpenOrders":0,"finalPositions":0,"finalCash":if case==Case::LiveRiskDenied {"1.5"} else if case==Case::LiveMultiple {"2"} else {"1000"},"finalLocked":"0"});
    node.dispose();
    public_task.abort();
    output
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_native_preflight_rejections_leave_order_accepted() {
    run(Case::PreflightRejected).await;
}
#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_native_batch_mixed_results_then_cancel_all_cleanup() {
    run(Case::BatchMixed).await;
}
#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_native_cancel_all_reports_each_order() {
    run(Case::CancelAll).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline JSON recording; never overwrite historical JSON"]
async fn record_command_alignment_json() {
    let sessions = vec![
        run(Case::PreflightRejected).await,
        run(Case::BatchMixed).await,
        run(Case::BatchUnknown).await,
        run(Case::CancelAll).await,
    ];
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/command_alignment_synthetic_20261016.json");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic offline Rust strategy + LiveNode; not mainnet","sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn saved_command_json_replays_native_strategy_outcomes() {
    let saved: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/command_alignment_synthetic_20261016.json"
    ))
    .unwrap();
    let gap: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/command_gap_rejections_synthetic_20261016.json"
    ))
    .unwrap();
    let multi: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/multi_submission_native_synthetic_20261016.json"
    ))
    .unwrap();
    for session in saved["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .chain(gap["sessions"].as_array().unwrap())
        .chain(multi["sessions"].as_array().unwrap())
    {
        let case = match session["case"].as_str().unwrap() {
            "PreflightRejected" => Case::PreflightRejected,
            "DisconnectedPreflight" => Case::DisconnectedPreflight,
            "BatchMixed" => Case::BatchMixed,
            "BatchUnknown" => Case::BatchUnknown,
            "CancelAll" => Case::CancelAll,
            "LiveMultiple" => Case::LiveMultiple,
            "LiveUnknown" => Case::LiveUnknown,
            "LiveRiskDenied" => Case::LiveRiskDenied,
            _ => panic!("unknown saved case"),
        };
        let actual = run_with_saved(case, Some(session)).await;
        for key in [
            "finalOpenOrders",
            "finalPositions",
            "finalCash",
            "finalLocked",
        ] {
            assert_eq!(actual[key], session[key], "{key}");
        }
        for kind in ["ModifyRejected", "CancelRejected", "Canceled"] {
            let count = |value: &Value| {
                value["feedback"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|event| event["type"] == kind)
                    .count()
            };
            // Historical fixtures used two independent removal requests. The same HTTP400
            // now rejects both children of one batch; preserve the old file as evidence.
            let expected =
                count(session) + usize::from(case == Case::BatchMixed && kind == "CancelRejected");
            assert_eq!(count(&actual), expected, "{kind}");
        }
        assert_eq!(
            actual["removals"].as_array().unwrap().len(),
            session["removals"].as_array().unwrap().len()
                - usize::from(matches!(
                    case,
                    Case::BatchUnknown
                        | Case::BatchMixed
                        | Case::CancelAll
                        | Case::LiveMultiple
                        | Case::LiveUnknown
                ))
        );
        for (actual, saved) in actual["requests"]
            .as_array()
            .unwrap()
            .iter()
            .zip(session["requests"].as_array().unwrap())
        {
            for key in ["strategy", "pricePerShare", "isPostOnly"] {
                assert_eq!(actual["data"][key], saved["data"][key]);
            }
            for key in [
                "makerAmount",
                "takerAmount",
                "feeRateBps",
                "side",
                "tokenId",
            ] {
                assert_eq!(actual["data"]["order"][key], saved["data"]["order"][key]);
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_native_batch_unknown_leg_waits_for_evidence_without_retry() {
    run(Case::BatchUnknown).await;
}

#[tokio::test]
async fn native_cancel_all_filters_side_instrument_and_client_and_batch_checks_scope() {
    use nautilus_common::messages::execution::{BatchCancelOrders, CancelAllOrders};
    let private = server(vec![]).await;
    let (mut client, mut rx, cache) = reported_client(&private);
    let _portfolio = portfolio(&cache);
    for index in [1, 2] {
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(index)))
            .unwrap();
    }
    let mut owned = vec![];
    for (name, index, side, binding, strategy) in [
        ("OWN-1", 1, OrderSide::Buy, "PREDICT", "OWN-001"),
        ("OWN-2", 1, OrderSide::Buy, "PREDICT", "OTHER-001"),
        ("SELL", 1, OrderSide::Sell, "PREDICT", "OWN-001"),
        ("NO", 2, OrderSide::Buy, "PREDICT", "OWN-001"),
        ("CLIENT", 1, OrderSide::Buy, "OTHER", "OWN-001"),
    ] {
        let mut order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .strategy_id(StrategyId::new(strategy))
            .client_order_id(ClientOrderId::new(name))
            .instrument_id(instrument_id(index))
            .side(side)
            .quantity(Quantity::from(100))
            .price(Price::from("0.620"))
            .build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, Some(ClientId::new(binding)), false)
            .unwrap();
        let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
        let submitted = factory.generate_order_submitted(&order, UnixNanos::from(10u64));
        order.apply(submitted.clone()).unwrap();
        cache.borrow_mut().update_order(&submitted).unwrap();
        let accepted = factory.generate_order_accepted(
            &order,
            VenueOrderId::new(format!(
                "{:#x}",
                alloy::primitives::keccak256(name.as_bytes())
            )),
            UnixNanos::from(100u64),
            UnixNanos::from(100u64),
        );
        order.apply(accepted.clone()).unwrap();
        cache.borrow_mut().update_order(&accepted).unwrap();
        owned.push(order);
    }
    connect_reported(&mut client, &mut rx).await;
    assert!(
        client
            .cancel_order(cancellations::cancel(&owned[4]))
            .is_err()
    );
    assert!(rx.try_recv().is_err());
    let mut command = BatchCancelOrders::new(
        trader_id(),
        Some(ClientId::new("PREDICT")),
        owned[0].strategy_id(),
        instrument_id(1),
        vec![
            cancellations::cancel(&owned[0]),
            cancellations::cancel(&owned[1]),
        ],
        UUID4::new(),
        UnixNanos::from(OBSERVED),
        None,
        None,
    );
    // A malformed batch is rejected before even the valid first child publishes or writes.
    assert!(client.batch_cancel_orders(command.clone()).is_err());
    assert!(rx.try_recv().is_err());
    command.cancels = vec![cancellations::cancel(&owned[0]); 2];
    client.batch_cancel_orders(command).unwrap();
    let ExecutionEvent::Order(OrderEventAny::CancelRejected(rejected)) = next(&mut rx).await else {
        panic!("one rejection for duplicate child")
    };
    assert_eq!(rejected.client_order_id, owned[0].client_order_id());
    assert!(rx.try_recv().is_err());
    client
        .cancel_all_orders(CancelAllOrders::new(
            trader_id(),
            Some(ClientId::new("PREDICT")),
            owned[0].strategy_id(),
            instrument_id(1),
            Some(OrderSide::Buy),
            UUID4::new(),
            UnixNanos::from(OBSERVED),
            None,
            None,
        ))
        .unwrap();
    let mut ids = vec![];
    for _ in 0..2 {
        let ExecutionEvent::Order(OrderEventAny::CancelRejected(event)) = next(&mut rx).await
        else {
            panic!("scoped rejection")
        };
        ids.push(event.client_order_id);
    }
    ids.sort();
    assert_eq!(
        ids,
        vec![owned[0].client_order_id(), owned[1].client_order_id()]
    );
    assert!(rx.try_recv().is_err());
    assert!(private.state.removals.lock().unwrap().is_empty());
    client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_disconnected_modify_and_cancel_emit_standard_rejections() {
    run(Case::DisconnectedPreflight).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline gap command capture; never overwrite JSON"]
async fn record_disconnected_command_rejections_json() {
    let session = run(Case::DisconnectedPreflight).await;
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/command_gap_rejections_synthetic_20261016.json");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic offline Rust strategy + LiveNode; wallet epoch gap; not mainnet","sessions":[session]})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_submits_two_active_orders_then_cancel_all() {
    run(Case::LiveMultiple).await;
}
#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_unknown_submission_reserves_capacity_without_blocking_second_order() {
    run(Case::LiveUnknown).await;
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "explicit offline multi-submit recording; never overwrite JSON"]
async fn record_multi_submission_json() {
    let sessions = vec![
        run(Case::LiveMultiple).await,
        run(Case::LiveUnknown).await,
        run(Case::LiveRiskDenied).await,
    ];
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/multi_submission_native_synthetic_20261016.json");
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file,&json!({"source":"synthetic offline Rust Strategy + LiveNode; not mainnet","sessions":sessions})).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_native_risk_reserves_first_pending_order_before_second_submit() {
    run(Case::LiveRiskDenied).await;
}
