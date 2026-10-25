// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  Licensed under the GNU Lesser General Public License Version 3.0.
//  See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

#![cfg(feature = "execution")]

#[path = "execution/quantity_commission.rs"]
mod quantity_commission;

#[path = "execution/quantity_fee_cost.rs"]
mod quantity_fee_cost;

#[path = "execution/ingestion_contract.rs"]
mod ingestion_contract;

#[path = "execution/terminal_recovery.rs"]
mod terminal_recovery;

#[path = "execution/history_survey.rs"]
mod history_survey;

#[path = "execution/factual_precision.rs"]
mod factual_precision;

#[path = "support/fill_session.rs"]
mod fill_session;

#[path = "support/node.rs"]
mod node;

use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy::primitives::U256;
use axum::{
    Router,
    body::Bytes,
    extract::{
        State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, Uri},
    response::Response,
    routing::{get, post},
};
use futures_util::{SinkExt, StreamExt};
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    clock::{Clock, VirtualClock},
    factories::{ExecutionClientFactory, OrderEventFactory},
    live::runner::replace_exec_event_sender,
    messages::{
        ExecutionEvent, ExecutionReport,
        execution::{
            CancelOrder, GenerateFillReports, GenerateOrderStatusReport,
            GeneratePositionStatusReports, QueryAccount, SubmitOrder,
        },
    },
    msgbus::{self, MessageBus, MessagingSwitchboard},
};
use nautilus_core::{UUID4, UnixNanos, time::AtomicTime};
use nautilus_execution::engine::ExecutionEngine;
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    accounts::{Account, AccountAny, cash::CashAccount},
    enums::{AccountType, AssetClass, OmsType, OrderSide, OrderStatus, OrderType},
    events::AccountState,
    identifiers::{AccountId, ClientId, InstrumentId, Symbol, TraderId, VenueOrderId},
    instruments::{BinaryOption, InstrumentAny},
    orders::{Order, OrderAny, builder::OrderTestBuilder},
    types::{AccountBalance, Currency, Money, Price, Quantity, fixed::FIXED_PRECISION},
};
use nautilus_network::websocket::TransportBackend;
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_predict::{
    PredictAccountStateMode, PredictChainClientConfig, PredictEnvironment, PredictExecutionClient,
    PredictExecutionClientConfig, PredictExecutionClientFactory, PredictPrivateClientConfig,
    PredictWalletWebSocketConfig,
    common::{PREDICT_VENUE, credential::PredictAuthToken},
    execution::parse::{parse_rest_order_report, parse_wallet_report, settlement_trade_id},
    http::PredictPrivateHttpClient,
    websocket::private_messages::PredictWalletEvent,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};

const KEY: &str = "execution-api-fixture";
const JWT: &str = "execution-jwt-fixture";
const WALLET: &str = "0x1111111111111111111111111111111111111111";
const HASH: &str = "0x17e3c43e261d9a73f845b38845f06f52589bcb64a82f70831f4acbc818df1dfc";
const OBSERVED: u64 = 2_000_000;

#[path = "execution/mock_venue.rs"]
mod mock_venue;
use mock_venue::*;

fn account_id() -> AccountId {
    AccountId::new("PREDICT-fixture")
}

fn seed_account(cache: &Rc<RefCell<Cache>>) {
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
        .add_account(AccountAny::Cash(CashAccount::new(state, true, false)))
        .unwrap();
}
fn trader_id() -> TraderId {
    TraderId::new("TESTER-001")
}
fn instrument_id(index: u8) -> InstrumentId {
    format!("42-{index}.PREDICT").parse().unwrap()
}

fn instrument(index: u8) -> BinaryOption {
    BinaryOption::builder()
        .instrument_id(instrument_id(index))
        .raw_symbol(Symbol::new(if index == 1 { "11" } else { "22" }))
        .asset_class(AssetClass::Alternative)
        .currency(Currency::USDT())
        .activation_ns(UnixNanos::default())
        .expiration_ns(UnixNanos::from(i64::MAX as u64))
        .price_precision(3)
        .price_increment(Price::from("0.001"))
        .size_precision(FIXED_PRECISION)
        .size_increment(
            Quantity::from_decimal_dp(Decimal::new(1, u32::from(FIXED_PRECISION)), FIXED_PRECISION)
                .unwrap(),
        )
        .ts_event(UnixNanos::default())
        .ts_init(UnixNanos::default())
        .build()
        .unwrap()
}

fn order(index: u8) -> OrderAny {
    order_with_side(index, OrderSide::Buy)
}

fn order_with_side(index: u8, side: OrderSide) -> OrderAny {
    order_with_terms(index, side, false)
}

fn order_with_terms(index: u8, side: OrderSide, post_only: bool) -> OrderAny {
    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .instrument_id(instrument_id(index))
        .side(side)
        .quantity(Quantity::from(100))
        .price(Price::from("0.620"))
        .post_only(post_only)
        .build();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    order
        .apply(factory.generate_order_submitted(&order, UnixNanos::from(10u64)))
        .unwrap();
    order
        .apply(factory.generate_order_accepted(
            &order,
            VenueOrderId::new(HASH),
            UnixNanos::from(100u64),
            UnixNanos::from(100u64),
        ))
        .unwrap();
    order
}

fn config(server: &Server) -> PredictExecutionClientConfig {
    let session = PredictPrivateHttpClient::new(PredictPrivateClientConfig {
        api_key: Some(KEY.into()),
        base_url_http: Some(server.http.clone()),
        ..Default::default()
    })
    .unwrap()
    .authenticated(
        PredictAuthToken::new(JWT.into()).unwrap(),
        WALLET.parse().unwrap(),
    )
    .unwrap();
    let mut config = PredictExecutionClientConfig::new(
        account_id(),
        session,
        PredictWalletWebSocketConfig {
            api_key: Some(KEY.into()),
            base_url_ws: Some(server.ws.clone()),
            transport_backend: TransportBackend::Tungstenite,
            ..Default::default()
        },
    );
    config.account_state_mode = PredictAccountStateMode::Disabled;
    // Legacy explicit-recovery cases control each HTTP response causally.
    config.reconnect_reconciliation_interval_secs = None;
    config
}

fn client(
    server: &Server,
) -> (
    PredictExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
) {
    let cache = Rc::new(RefCell::new(Cache::default()));
    msgbus::set_message_bus(Rc::new(RefCell::new(MessageBus::new(
        trader_id(),
        UUID4::new(),
        None,
        None,
    ))));
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
    let (tx, rx) = mpsc::unbounded_channel();
    replace_exec_event_sender(tx);
    (
        PredictExecutionClient::new_with_clock(core, config(server), clock).unwrap(),
        rx,
        cache,
    )
}

fn lifecycle_fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/private_api/wallet_lifecycle.json")).unwrap()
}

fn holding(index: u8) -> Value {
    let mut row = lifecycle_fixture()["holding"].clone();
    row["id"] = json!(format!("holding-{index}"));
    row["outcome"]["status"] = Value::Null;
    row["outcome"]["indexSet"] = json!(index);
    row["outcome"]["onChainId"] = json!(if index == 1 { "11" } else { "22" });
    row["market"]["outcomes"] = json!([row["outcome"].clone()]);
    row
}

fn event(kind: &str, index: u8) -> Value {
    let mut row = lifecycle_fixture()["accepted"].clone();
    row["type"] = json!(kind);
    row["details"]["outcomeIndex"] = json!(index);
    row["details"]["outcome"] = json!(if index == 1 { "YES" } else { "NO" });
    row
}

fn fill(index: u8) -> Value {
    let mut row = event("orderTransactionSuccess", index);
    row.as_object_mut().unwrap().extend(
        lifecycle_fixture()["confirmedFillFields"]
            .as_object()
            .unwrap()
            .clone(),
    );
    row
}

async fn next(rx: &mut mpsc::UnboundedReceiver<ExecutionEvent>) -> ExecutionEvent {
    timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn owned_position_reports_finalize_against_current_core_cache() {
    let server = server(vec![
        json!({"success":true,"cursor":"+/=&", "data":[holding(1)]}),
        json!({"success":true,"data":[holding(2)]}),
    ])
    .await;
    let (client, mut rx, cache) = client(&server);
    let cmd = GeneratePositionStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
        None,
        None,
        None,
    );
    let task = client.generate_position_status_reports_task(&cmd).unwrap();
    // Collection is Send and runs without any instruments in the core-thread cache.
    tokio::spawn(task.collection).await.unwrap();
    for index in [1, 2] {
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(index)))
            .unwrap();
    }
    let reports = (task.finish)().unwrap();
    assert_eq!(reports.len(), 2);
    assert!(
        reports
            .iter()
            .all(|report| report.quantity.as_decimal() == Decimal::from(17)
                && report.ts_last.as_u64() == OBSERVED)
    );
    assert!(!client.provides_bulk_position_coverage(instrument_id(1)));
    assert!(client.get_account().is_none()); // API points never manufacture an AccountState.
    assert!(rx.try_recv().is_err()); // Report collection must not apply execution events.
    let requests = server.state.requests.lock().unwrap();
    assert_eq!(requests[0]["includeLost"], "true");
    assert_eq!(requests[1]["after"], "+/=&");
}

#[tokio::test]
async fn verified_position_queries_do_not_depend_on_the_identity_endpoint() {
    let server = server(vec![json!({"success":true,"data":[holding(1)]})]).await;
    let (mut client, _rx, cache) = client(&server);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    client.connect().await.unwrap();
    let before = *server.state.account_requests.lock().unwrap();
    assert!(before > 0);
    server
        .state
        .account_unavailable
        .store(true, std::sync::atomic::Ordering::Release);
    let cmd = GeneratePositionStatusReports::new(
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
        None,
        None,
        None,
    );
    let reports = client.generate_position_status_reports(&cmd).await.unwrap();
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].signed_decimal_qty, Decimal::from(17));
    assert_eq!(*server.state.account_requests.lock().unwrap(), before);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn position_pagination_fails_on_cycles_bounds_and_duplicate_outcomes() {
    for (pages, max) in [
        (
            vec![
                json!({"success":true,"cursor":"cycle","data":[]}),
                json!({"success":true,"cursor":"cycle","data":[]}),
            ],
            10,
        ),
        (vec![json!({"success":true,"cursor":"more","data":[]})], 1),
        (
            vec![json!({"success":true,"cursor":null,"data":[holding(1),holding(1)]})],
            10,
        ),
    ] {
        let server = server(pages).await;
        let (mut client, _, cache) = client(&server);
        client.disconnect().await.unwrap();
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        let mut cfg = config(&server);
        cfg.max_report_pages = max;
        let core = ExecutionClientCore::new(
            trader_id(),
            ClientId::new("PREDICT"),
            *PREDICT_VENUE,
            OmsType::Netting,
            account_id(),
            AccountType::Cash,
            None,
            cache,
        );
        let client = PredictExecutionClient::new(core, cfg).unwrap();
        let cmd = GeneratePositionStatusReports::new(
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
            None,
            None,
            None,
        );
        assert!(client.generate_position_status_reports(&cmd).await.is_err());
    }
}

#[tokio::test]
async fn factory_reports_and_commands_use_nautilus_ports_without_trading_or_fake_funds() {
    let server = server(vec![]).await;
    let (_, mut rx, cache) = client(&server);
    let factory = PredictExecutionClientFactory::new();
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(VirtualClock::new()));
    let mut client = factory
        .create(
            trader_id(),
            "PREDICT",
            &config(&server),
            cache.into(),
            clock,
        )
        .unwrap();
    assert_eq!(client.oms_type(), OmsType::Netting);
    assert_eq!(client.account_id(), account_id());
    assert!(
        client
            .query_account(QueryAccount::new(
                trader_id(),
                None,
                account_id(),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None
            ))
            .is_err()
    );
    let order = order(1);
    assert!(
        client
            .submit_order(SubmitOrder::from_order(
                &order,
                trader_id(),
                None,
                None,
                UUID4::new(),
                UnixNanos::default()
            ))
            .is_err()
    );
    assert!(client.generate_mass_status(None).await.is_err());
    client.start().unwrap();
    client.connect().await.unwrap();
    assert!(client.is_connected());
    assert!(client.get_account().is_none());
    assert!(rx.try_recv().is_err());
    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
}

#[tokio::test]
async fn confirmed_wallet_fill_flows_through_emitter_and_real_nautilus_execution_engine() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = client(&server);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let order = order(1);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
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
        .add_account(AccountAny::Cash(CashAccount::new(state, true, false)))
        .unwrap();
    let mut clock = VirtualClock::new();
    clock.advance_time(UnixNanos::from(OBSERVED), true);
    let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(clock));
    let mut engine = ExecutionEngine::new(clock, cache.clone(), None);
    client.start().unwrap();
    client.connect().await.unwrap();
    let mut pending = fill(1);
    pending["type"] = json!("orderTransactionSubmitted");
    pending["kind"] = json!("PURCHASE");
    server.wallet(pending);
    server.wallet(fill(1));
    let ExecutionEvent::Report(report) = next(&mut rx).await else {
        panic!("standard report channel")
    };
    let ExecutionReport::Fill(fill_report) = &report else {
        panic!("pending settlement must not create an order/fill report")
    };
    assert_eq!(fill_report.last_qty.as_decimal(), Decimal::from(17));
    assert_eq!(fill_report.commission.as_decimal(), Decimal::new(1, 1));
    assert_eq!(fill_report.venue_order_id.as_str(), HASH);
    engine.reconcile_execution_report(&report);
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .filled_qty()
            .as_decimal(),
        Decimal::from(17)
    );
    // Native reconciliation owns deduplication and cache mutation; replay remains one fill.
    engine.reconcile_execution_report(&report);
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .filled_qty()
            .as_decimal(),
        Decimal::from(17)
    );
    server.wallet(fill(1));
    let mut cancelled = event("orderCancelled", 1);
    cancelled["details"]["quantityFilled"] = json!("17");
    server.wallet(cancelled);
    let ExecutionEvent::Order(event @ nautilus_model::events::OrderEventAny::Canceled(_)) =
        next(&mut rx).await
    else {
        panic!("duplicate fill must be suppressed before native cancellation")
    };
    engine.process(&event);
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    client.disconnect().await.unwrap();
}

#[test]
fn fill_mapping_keeps_outcome_price_unknown_liquidity_and_exact_representation() {
    let clock = UnixNanos::from(OBSERVED);
    for index in [1, 2] {
        let order = order(index);
        let mut data = fill(index);
        data.as_object_mut().unwrap().remove("isMaker");
        let data: PredictWalletEvent = serde_json::from_value(data).unwrap();
        let Some(ExecutionReport::Fill(report)) = parse_wallet_report(
            &data,
            &order,
            &instrument(index),
            account_id(),
            WALLET.parse().unwrap(),
            clock,
        )
        .unwrap() else {
            panic!("confirmed fill")
        };
        assert_eq!(report.last_px.as_decimal(), Decimal::new(62, 2)); // NO is not complemented to 0.38.
        assert_eq!(
            report.liquidity_side,
            nautilus_model::enums::LiquiditySide::NoLiquiditySide
        );
        assert_eq!(
            report.trade_id,
            settlement_trade_id("settlement-42", HASH.parse().unwrap())
        );
    }
    let order = order(1);
    for field in [0, 1, 2, 3, 4] {
        let mut data = fill(1);
        match field {
            0 => {
                data.as_object_mut().unwrap().remove("fee");
                data.as_object_mut().unwrap().remove("isMaker");
            }
            1 => data["fee"]["type"] = json!("SHARES"),
            2 => data["fill"]["executedSizeWei"] = json!("1"),
            3 => data["fee"]["amountWei"] = json!("1"),
            _ => data["fill"]["executedSizeWei"] = json!(U256::MAX.to_string()),
        }
        let data = serde_json::from_value(data).unwrap();
        assert!(
            parse_wallet_report(
                &data,
                &order,
                &instrument(1),
                account_id(),
                WALLET.parse().unwrap(),
                clock
            )
            .is_err()
        );
    }
}

#[tokio::test]
async fn ambiguous_wallet_evidence_does_not_publish_a_fill_or_disconnect_transport() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = client(&server);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order(1), None, None, false)
        .unwrap();
    seed_account(&cache);
    client.connect().await.unwrap();
    let mut data = fill(1);
    data.as_object_mut().unwrap().remove("fee");
    data.as_object_mut().unwrap().remove("isMaker");
    server.wallet(data);
    wallet_processed(&server).await;
    assert!(rx.try_recv().is_err());
    assert!(
        client.is_connected(),
        "the consumer must remain alive for recovery"
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn burst_fill_and_cancel_reports_preserve_core_cache_ownership_and_causal_fill_evidence() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = client(&server);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let order = order(1);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    seed_account(&cache);
    client.connect().await.unwrap();
    server.wallet(fill(1));
    let mut cancelled = event("orderCancelled", 1);
    cancelled["details"]["quantityFilled"] = json!("17");
    server.wallet(cancelled);
    let ExecutionEvent::Report(first) = next(&mut rx).await else {
        panic!("fill report")
    };
    let ExecutionEvent::Order(second @ nautilus_model::events::OrderEventAny::Canceled(_)) =
        next(&mut rx).await
    else {
        panic!("native cancellation follows confirmed fill")
    };
    assert!(matches!(first, ExecutionReport::Fill(_)));
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .filled_qty()
            .as_decimal(),
        Decimal::ZERO
    );
    let mut clock = VirtualClock::new();
    clock.advance_time(UnixNanos::from(OBSERVED), true);
    let mut engine = ExecutionEngine::new(Rc::new(RefCell::new(clock)), cache.clone(), None);
    engine.reconcile_execution_report(&first);
    engine.process(&second);
    let cached = cache
        .borrow()
        .order_owned(&order.client_order_id())
        .unwrap();
    assert_eq!(cached.filled_qty().as_decimal(), Decimal::from(17));
    assert_eq!(cached.status(), OrderStatus::Canceled);
    assert!(client.is_connected());
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn conflicting_settlement_replay_and_connection_gaps_preserve_stream_recovery() {
    for gap in [false, true] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = client(&server);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        cache
            .borrow_mut()
            .add_order(order(1), None, None, false)
            .unwrap();
        seed_account(&cache);
        client.connect().await.unwrap();
        if gap {
            server
                .state
                .sockets
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .send(Message::Close(None))
                .unwrap();
        } else {
            server.wallet(fill(1));
            let _ = next(&mut rx).await;
            let mut conflict = fill(1);
            conflict["fill"]["executedSizeWei"] = json!("18000000000000000000");
            server.wallet(conflict);
        }
        if gap {
            exit_orders::barrier("wallet reconnect", || {
                server.state.sockets.lock().unwrap().len() >= 2
            })
            .await;
        }
        wallet_processed(&server).await;
        if !gap {
            assert!(
                client.is_connected(),
                "history errors do not disconnect the transport"
            );
        }
        assert!(rx.try_recv().is_err());
        client.disconnect().await.unwrap();
    }
}

fn rest_order_data(status: &str, index: u8, side: OrderSide) -> Value {
    let (maker, taker) = if side == OrderSide::Buy {
        ("62000000000000000000", "100000000000000000000")
    } else {
        ("100000000000000000000", "62000000000000000000")
    };
    json!({"order":{"hash":HASH,"salt":"1","maker":WALLET,"signer":WALLET,
        "taker":"0x0000000000000000000000000000000000000000", "tokenId":if index==1 {"11"} else {"22"},
        "makerAmount":maker,"takerAmount":taker,"expiration":"0","nonce":"0","feeRateBps":"0",
        "side":if side==OrderSide::Buy {0} else {1},"signatureType":0,"signature":"public-fixture"},
        "id":"12345","marketId":42,"currency":"USDT","amount":"unknown-unit",
        "amountFilled":"0","isNegRisk":false,"isYieldBearing":false,"strategy":"LIMIT",
        "status":status,"rewardEarningRate":0})
}

fn order_report_cmd(order: &OrderAny) -> GenerateOrderStatusReport {
    GenerateOrderStatusReport::new(
        UUID4::new(),
        UnixNanos::default(),
        Some(order.instrument_id()),
        Some(order.client_order_id()),
        order.venue_order_id(),
        None,
        None,
    )
}

#[tokio::test]
async fn rest_order_reports_finalize_on_core_and_reconcile_through_native_engine() {
    let server = server(vec![]).await;
    let mut row = rest_order_data("CANCELLED", 1, OrderSide::Buy);
    let fields = serde_json::from_value(row["order"].clone()).unwrap();
    let hasher = nautilus_predict::signing::PredictOrderHasher::new(PredictEnvironment::BnbMainnet);
    let hash = hasher.order_hash(
        &fields,
        nautilus_predict::common::contracts::PredictExchangeType::Standard,
    );
    row["order"]["hash"] = json!(hash);
    server
        .state
        .orders
        .lock()
        .unwrap()
        .push_back(json!({"success":true,"data":row}));
    let (client, mut rx, cache) = client(&server);
    let order = order(1);
    seed_account(&cache);
    cache
        .borrow_mut()
        .add_order(
            OrderAny::try_from(order.init_event().clone()).unwrap(),
            None,
            None,
            false,
        )
        .unwrap();
    for event in order.events().into_iter().skip(1) {
        let mut event = event.clone();
        if let nautilus_model::events::OrderEventAny::Accepted(event) = &mut event {
            event.venue_order_id = VenueOrderId::new(format!("{hash:#x}"));
        }
        cache.borrow_mut().update_order(&event).unwrap();
    }
    let native = cache
        .borrow()
        .order_owned(&order.client_order_id())
        .unwrap();
    let mut cmd = order_report_cmd(&native);
    cmd.client_order_id = None;
    cmd.instrument_id = None;
    let task = client.generate_order_status_report_task(&cmd).unwrap();
    tokio::spawn(task.collection).await.unwrap();
    // The worker neither accesses nor updates Cache, and instrument discovery can finish later.
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Accepted
    );
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let report = (task.finish)().unwrap().unwrap();
    assert_eq!(report.order_status, OrderStatus::Canceled);
    assert_eq!(report.ts_accepted, UnixNanos::from(100u64));
    assert_eq!(report.ts_last, UnixNanos::from(OBSERVED));
    assert_eq!(report.ts_init, UnixNanos::from(OBSERVED));
    assert_eq!(
        report.venue_order_id,
        VenueOrderId::new(format!("{hash:#x}"))
    );
    assert!(rx.try_recv().is_err());
    let mut clock = VirtualClock::new();
    clock.advance_time(UnixNanos::from(OBSERVED), true);
    let mut engine = ExecutionEngine::new(Rc::new(RefCell::new(clock)), cache.clone(), None);
    engine.reconcile_execution_report(&ExecutionReport::Order(Box::new(report)));
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    // submission=None still authenticates the public contract hash; it never skips
    // validation merely because this client has no order signing authority.
    row["order"]["makerAmount"] = json!("63000000000000000000");
    server
        .state
        .orders
        .lock()
        .unwrap()
        .push_back(json!({"success":true,"data":row}));
    assert!(
        client
            .generate_order_status_report(&cmd)
            .await
            .unwrap_err()
            .to_string()
            .contains("do not reproduce signed hash")
    );
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
}

#[test]
fn unfilled_rest_reports_preserve_buy_sell_and_outcome_terms() {
    for index in [1, 2] {
        for side in [OrderSide::Buy, OrderSide::Sell] {
            for (venue_status, native_status) in [
                ("OPEN", OrderStatus::Accepted),
                ("CANCELLED", OrderStatus::Canceled),
                ("EXPIRED", OrderStatus::Expired),
            ] {
                let row =
                    serde_json::from_value(rest_order_data(venue_status, index, side)).unwrap();
                let order = order_with_side(index, side);
                let report = parse_rest_order_report(
                    &row,
                    &order,
                    &instrument(index),
                    account_id(),
                    WALLET.parse().unwrap(),
                    UnixNanos::from(OBSERVED),
                )
                .unwrap();
                assert_eq!(report.order_status, native_status);
                assert_eq!(report.order_side, Some(side));
                assert_eq!(report.quantity, order.quantity());
                assert_eq!(report.filled_qty.as_decimal(), Decimal::ZERO);
                assert_eq!(report.price, Some(Price::from("0.620")));
                assert_eq!(report.time_in_force, order.time_in_force());
            }
        }
    }
}

#[test]
fn rest_reports_reject_ambiguous_fills_and_changed_or_missing_order_evidence() {
    let order = order(1);
    for (pointer, value) in [
        ("/amountFilled", json!("1")),
        ("/order/hash", Value::Null),
        ("/order/takerAmount", json!("99000000000000000000")),
        ("/order/makerAmount", json!("61000000000000000000")),
        ("/order/tokenId", json!("22")),
        ("/order/side", json!(1)),
        ("/marketId", json!(43)),
        ("/currency", json!("USDC")),
        ("/status", json!("INVALIDATED")),
        ("/status", json!("FILLED")),
        ("/strategy", json!("MARKET")),
    ] {
        let mut row = rest_order_data("OPEN", 1, OrderSide::Buy);
        *row.pointer_mut(pointer).unwrap() = value;
        let row = serde_json::from_value(row).unwrap();
        assert!(
            parse_rest_order_report(
                &row,
                &order,
                &instrument(1),
                account_id(),
                WALLET.parse().unwrap(),
                UnixNanos::from(OBSERVED)
            )
            .is_err(),
            "{pointer}"
        );
    }
}

#[tokio::test]
async fn order_report_commands_validate_targets_and_recheck_cache_after_collection() {
    let server = server(vec![]).await;
    let (client, _, cache) = client(&server);
    let order = order(1);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    let mut cmd = order_report_cmd(&order);
    cmd.instrument_id = Some(instrument_id(2));
    assert!(client.generate_order_status_report(&cmd).await.is_err());
    // Failed target resolution performs no order HTTP request and never returns None.
    assert!(server.state.orders.lock().unwrap().is_empty());
    server
        .state
        .orders
        .lock()
        .unwrap()
        .push_back(json!({"success":true,"data":rest_order_data("OPEN", 1, OrderSide::Buy)}));
    let task = client
        .generate_order_status_report_task(&order_report_cmd(&order))
        .unwrap();
    tokio::spawn(task.collection).await.unwrap();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    let canceled = factory.generate_order_canceled(
        &order,
        order.venue_order_id(),
        UnixNanos::from(1000u64),
        UnixNanos::from(1000u64),
    );
    cache.borrow_mut().update_order(&canceled).unwrap();
    assert!(
        (task.finish)().is_err(),
        "a stale OPEN snapshot cannot reopen a terminal native order"
    );
}

#[tokio::test]
async fn execution_collateral_source_uses_session_deposit_address_without_inventing_account_state()
{
    let server = server(vec![]).await;
    let (_, mut rx, cache) = client(&server);
    let mut cfg = config(&server);
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", server.http).into(),
    ));
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
    let client = PredictExecutionClient::new_with_clock(core.clone(), cfg.clone(), clock).unwrap();
    let snapshot = client.request_collateral_snapshot().await.unwrap();
    assert_eq!(snapshot.account_address.to_string().to_lowercase(), WALLET);
    assert_eq!(
        snapshot.balance_wei,
        U256::from(1_000_000_000_000_000_000_000u128)
    );
    assert_eq!(*server.state.rpc_accounts.lock().unwrap(), [WALLET]);
    assert!(client.get_account().is_none());
    assert!(rx.try_recv().is_err());
    cfg.chain.as_mut().unwrap().environment = PredictEnvironment::BnbTestnet;
    assert!(PredictExecutionClient::new_with_clock(core, cfg, clock).is_err());
}

#[tokio::test]
async fn rest_snapshot_cannot_erase_fills_applied_during_collection() {
    let server = server(vec![]).await;
    server
        .state
        .orders
        .lock()
        .unwrap()
        .push_back(json!({"success":true,"data":rest_order_data("CANCELLED", 1, OrderSide::Buy)}));
    let (client, _, cache) = client(&server);
    let order = order(1);
    seed_account(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    let task = client
        .generate_order_status_report_task(&order_report_cmd(&order))
        .unwrap();
    tokio::spawn(task.collection).await.unwrap();
    let event: PredictWalletEvent = serde_json::from_value(fill(1)).unwrap();
    let report = parse_wallet_report(
        &event,
        &order,
        &instrument(1),
        account_id(),
        WALLET.parse().unwrap(),
        UnixNanos::from(OBSERVED),
    )
    .unwrap()
    .unwrap();
    let mut clock = VirtualClock::new();
    clock.advance_time(UnixNanos::from(OBSERVED), true);
    let mut engine = ExecutionEngine::new(Rc::new(RefCell::new(clock)), cache.clone(), None);
    engine.reconcile_execution_report(&report);
    assert!(
        (task.finish)().is_err(),
        "zero REST amount cannot supersede a confirmed fill in current Cache"
    );
    let cache = cache.borrow();
    let cached = cache.order(&order.client_order_id()).unwrap();
    assert_eq!(cached.filled_qty().as_decimal(), Decimal::new(17, 0));
    assert_eq!(cached.status(), OrderStatus::PartiallyFilled);
}

fn reported_client(
    server: &Server,
) -> (
    PredictExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
) {
    reported_client_with_removal(server, false)
}

fn reported_client_with_removal(
    server: &Server,
    enabled: bool,
) -> (
    PredictExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
) {
    let (_, rx, cache) = client(server);
    let mut cfg = config(server);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.orderbook_removal_enabled = enabled;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", server.http).into(),
    ));
    cfg.report_timeout_secs = 1;
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
    (
        PredictExecutionClient::new_with_clock(core, cfg, clock).unwrap(),
        rx,
        cache,
    )
}

fn portfolio(cache: &Rc<RefCell<Cache>>) -> Portfolio {
    let mut clock = VirtualClock::new();
    clock.advance_time(UnixNanos::from(OBSERVED), true);
    Portfolio::new(Rc::new(RefCell::new(clock)), cache.clone(), None)
}

#[tokio::test]
async fn captured_balance_replay_publishes_native_cash_and_retains_exact_wei_evidence() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/private_api/balance.json")).unwrap();
    let wei: u128 = fixture["balanceWei"].as_str().unwrap().parse().unwrap();
    let server = server(vec![]).await;
    server.state.rpc_frame.lock().unwrap().0 = wei;
    let (mut client, mut rx, cache) = reported_client_with_removal(&server, false);
    let _portfolio = portfolio(&cache);
    let state = connect_reported(&mut client, &mut rx).await;
    assert_eq!(
        state.balances[0].total.as_decimal(),
        Decimal::new(4_837_915, 5)
    );
    assert!(state.balances[0].locked.is_zero());
    assert_eq!(state.balances[0].free, state.balances[0].total);
    assert_eq!(
        state.info.as_ref().unwrap()["predict_balance_wei"],
        fixture["balanceWei"]
    );
    let account = cache.borrow().account_owned(&account_id()).unwrap();
    assert_eq!(
        account.balance_total(Some(Currency::USDT())).unwrap(),
        state.balances[0].total
    );
    client.disconnect().await.unwrap();
}

async fn connect_reported(
    client: &mut PredictExecutionClient,
    rx: &mut mpsc::UnboundedReceiver<ExecutionEvent>,
) -> AccountState {
    client.start().unwrap();
    let connection = client.connect();
    tokio::pin!(connection);
    let mut applied = None;
    loop {
        tokio::select! {
            result = &mut connection => {
                result.unwrap();
                return applied.expect("connect must publish its reported account");
            }
            event = rx.recv() => {
                let Some(ExecutionEvent::Account(state)) = event else {
                    panic!("native account channel");
                };
                msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state);
                applied = Some(state);
            }
        }
    }
}

async fn apply_account(rx: &mut mpsc::UnboundedReceiver<ExecutionEvent>) -> AccountState {
    let ExecutionEvent::Account(state) = next(rx).await else {
        panic!("native account channel");
    };
    msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state);
    state
}

#[tokio::test]
async fn reported_account_connects_through_real_portfolio_and_reuses_native_reservation_math() {
    let server = server(vec![]).await;
    server
        .state
        .open_orders
        .lock()
        .unwrap()
        .push(rest_order_data("OPEN", 1, OrderSide::Buy));
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order(1), None, None, false)
        .unwrap();
    assert!(client.get_account().is_none());
    let state = connect_reported(&mut client, &mut rx).await;
    assert!(client.is_connected());
    assert_eq!(state.balances[0].total, Money::from("1000 USDT"));
    assert_eq!(state.balances[0].locked, Money::from("62 USDT"));
    assert_eq!(state.balances[0].free, Money::from("938 USDT"));
    assert!(state.is_reported);
    let account = client.get_account().unwrap();
    assert!(matches!(account, AccountAny::Cash(_)) && !account.calculated_account_state());
    assert_eq!(
        account.balance(Some(Currency::USDT())).unwrap().locked,
        Money::from("62 USDT")
    );
    // Account startup does not read the four exchange allowances or submit transactions.
    assert_eq!(
        server
            .state
            .rpc_methods
            .lock()
            .unwrap()
            .iter()
            .filter(|m| *m == "eth_call")
            .count(),
        2
    );
    client
        .query_account(QueryAccount::new(
            trader_id(),
            None,
            account_id(),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
    let state = apply_account(&mut rx).await;
    assert_eq!(state.balances[0].locked, Money::from("62 USDT"));
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn reported_cash_and_native_positions_follow_confirmed_fills_and_cancel_release() {
    let server = server(vec![]).await;
    server
        .state
        .open_orders
        .lock()
        .unwrap()
        .push(rest_order_data("OPEN", 1, OrderSide::Buy));
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let order = order(1);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let open_before = server
        .state
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|row| row.get("status").is_some_and(|status| status == "OPEN"))
        .count();
    let mut clock = VirtualClock::new();
    clock.advance_time(UnixNanos::from(OBSERVED), true);
    let mut engine = ExecutionEngine::new(Rc::new(RefCell::new(clock)), cache.clone(), None);
    *server.state.rpc_frame.lock().unwrap() = (989_360_000_000_000_000_000u128, 2, 2);
    server.state.open_orders.lock().unwrap()[0]["amountFilled"] = json!("17");
    server.wallet(fill(1));
    let ExecutionEvent::Report(report) = next(&mut rx).await else {
        panic!("fill report");
    };
    engine.reconcile_execution_report(&report);
    let state = apply_account(&mut rx).await;
    assert_eq!(state.balances[0].total, Money::from("989.36 USDT"));
    assert_eq!(state.balances[0].locked, Money::from("51.46 USDT"));
    assert_eq!(state.balances[0].free, Money::from("937.90 USDT"));
    assert_eq!(
        server
            .state
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row.get("status").is_some_and(|status| status == "OPEN"))
            .count(),
        open_before,
        "confirmed cash refresh must not collect OPEN again"
    );
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None)[0]
            .quantity
            .as_decimal(),
        Decimal::new(17, 0)
    );
    server.wallet(fill(1));
    assert!(
        timeout(Duration::from_millis(150), rx.recv())
            .await
            .is_err()
    );
    server.state.open_orders.lock().unwrap().clear();
    *server.state.rpc_frame.lock().unwrap() = (989_360_000_000_000_000_000u128, 3, 2);
    let mut canceled = event("orderCancelled", 1);
    canceled["details"]["quantityFilled"] = json!("17");
    server.wallet(canceled);
    let ExecutionEvent::Order(event @ nautilus_model::events::OrderEventAny::Canceled(_)) =
        next(&mut rx).await
    else {
        panic!("native wallet cancellation");
    };
    engine.process(&event);
    let state = apply_account(&mut rx).await;
    assert_eq!(state.balances[0].locked, Money::from("0 USDT"));
    assert_eq!(state.balances[0].free, Money::from("989.36 USDT"));
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .filled_qty()
            .as_decimal(),
        Decimal::new(17, 0)
    );
    assert_eq!(
        cache.borrow().positions(None, None, None, None, None)[0]
            .quantity
            .as_decimal(),
        Decimal::new(17, 0)
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn full_fill_releases_retained_open_reservation_without_an_open_query() {
    let server = server(vec![]).await;
    server
        .state
        .open_orders
        .lock()
        .unwrap()
        .push(rest_order_data("OPEN", 1, OrderSide::Buy));
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let order = order(1);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let reads = server.state.requests.lock().unwrap().len();
    *server.state.rpc_frame.lock().unwrap() = (938_000_000_000_000_000_000u128, 2, 2);
    let mut full = fill(1);
    full["details"]["quantityFilled"] = json!("100");
    full["details"]["valueFilled"] = json!("62");
    full["fill"]["executedSizeWei"] = json!("100000000000000000000");
    full["fill"]["executedValueWei"] = json!("62000000000000000000");
    full["fee"]["amountWei"] = json!("0");
    let mut clock = VirtualClock::new();
    clock.advance_time(UnixNanos::from(OBSERVED), true);
    let mut engine = ExecutionEngine::new(Rc::new(RefCell::new(clock)), cache.clone(), None);
    server.wallet(full);
    let ExecutionEvent::Report(report) = next(&mut rx).await else {
        panic!("confirmed fill")
    };
    engine.reconcile_execution_report(&report);
    let state = apply_account(&mut rx).await;
    assert_eq!(state.balances[0].total, Money::from("938 USDT"));
    assert_eq!(state.balances[0].locked, Money::from("0 USDT"));
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Filled
    );
    assert_eq!(
        cache.borrow().positions_open(None, None, None, None, None)[0]
            .quantity
            .as_decimal(),
        Decimal::from(100)
    );
    assert_eq!(server.state.requests.lock().unwrap().len(), reads);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn reported_account_requires_framework_registration_and_complete_order_pages() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = reported_client(&server);
    client.start().unwrap();
    let (connected, state) = tokio::join!(client.connect(), next(&mut rx));
    assert!(matches!(state, ExecutionEvent::Account(_)));
    assert!(connected.is_err() && !client.is_connected());
    assert!(cache.borrow().account(&account_id()).is_none());
    client.disconnect().await.unwrap();

    let server = self::server(vec![]).await;
    server.state.open_pages.lock().unwrap().extend([
        json!({"success":true,"cursor":"cycle","data":[]}),
        json!({"success":true,"cursor":"cycle","data":[]}),
    ]);
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    client.start().unwrap();
    assert!(client.connect().await.is_err());
    assert!(rx.try_recv().is_err());
    assert!(cache.borrow().account(&account_id()).is_none());
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn reported_account_keeps_external_buy_capacity_and_registers_unfunded_wallets() {
    for funded in [true, false] {
        let server = server(vec![]).await;
        if funded {
            server
                .state
                .open_orders
                .lock()
                .unwrap()
                .push(rest_order_data("OPEN", 1, OrderSide::Buy));
        } else {
            server.state.rpc_frame.lock().unwrap().0 = 0;
        }
        let (mut client, mut rx, cache) = reported_client(&server);
        let _portfolio = portfolio(&cache);
        let state = connect_reported(&mut client, &mut rx).await;
        assert_eq!(
            state.balances[0].locked,
            Money::from(if funded { "62 USDT" } else { "0 USDT" })
        );
        assert_eq!(
            state.balances[0].free,
            Money::from(if funded { "938 USDT" } else { "0 USDT" })
        );
        assert!(
            cache
                .borrow()
                .orders_refs(None, None, None, None, None)
                .is_empty()
        );
        assert!(client.get_account().is_some());
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn unchanged_command_feedback_does_not_restart_an_inflight_account_collection() {
    use nautilus_model::events::{OrderEventAny, OrderPendingUpdate};

    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let order = order(1);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let mut clock = VirtualClock::new();
    clock.advance_time(UnixNanos::from(OBSERVED), true);
    let mut engine = ExecutionEngine::new(Rc::new(RefCell::new(clock)), cache.clone(), None);
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.rpc_gate.lock().unwrap() = Some(gate.clone());
    let requests_before = server.state.rpc_methods.lock().unwrap().len();
    client
        .query_account(QueryAccount::new(
            trader_id(),
            None,
            account_id(),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
    timeout(Duration::from_secs(2), async {
        while server.state.rpc_methods.lock().unwrap().len() == requests_before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Collection is causally held at RPC while the engine applies real command feedback.
    let pending = OrderEventAny::PendingUpdate(OrderPendingUpdate::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        order.account_id(),
        UUID4::new(),
        UnixNanos::from(300),
        UnixNanos::from(300),
        false,
        order.venue_order_id(),
    ));
    engine.process(&pending);
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    let rejection = factory.generate_order_modify_rejected(
        &cache.borrow().order(&order.client_order_id()).unwrap(),
        order.venue_order_id(),
        "unsupported modification",
        UnixNanos::from(400),
        UnixNanos::from(400),
    );
    engine.process(&rejection);
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Accepted
    );
    gate.notify_one();
    let state = apply_account(&mut rx).await;
    assert_eq!(state.balances[0].locked, Money::from("62 USDT"));
    assert_eq!(state.balances[0].free, Money::from("938 USDT"));
    assert_eq!(
        server.state.rpc_methods.lock().unwrap().len() - requests_before,
        3,
        "unchanged reservation must publish the first collected snapshot without retry"
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn stale_account_collection_cannot_reinstate_a_released_order_reservation() {
    let server = server(vec![]).await;
    server
        .state
        .open_orders
        .lock()
        .unwrap()
        .push(rest_order_data("OPEN", 1, OrderSide::Buy));
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let order = order(1);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let mut clock = VirtualClock::new();
    clock.advance_time(UnixNanos::from(OBSERVED), true);
    let mut engine = ExecutionEngine::new(Rc::new(RefCell::new(clock)), cache.clone(), None);
    let initial = server.state.rpc_methods.lock().unwrap().len();
    server.state.rpc_latest_started.notified().await; // Initial account snapshot.
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.rpc_gate.lock().unwrap() = Some(gate.clone());
    client
        .query_account(QueryAccount::new(
            trader_id(),
            None,
            account_id(),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        ))
        .unwrap();
    timeout(Duration::from_secs(1), async {
        loop {
            if server.state.rpc_methods.lock().unwrap().len() > initial {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    server.state.rpc_latest_started.notified().await; // Held first query.
    server.wallet(event("orderCancelled", 1));
    let ExecutionEvent::Order(event @ nautilus_model::events::OrderEventAny::Canceled(_)) =
        next(&mut rx).await
    else {
        panic!("native wallet cancellation");
    };
    engine.process(&event);
    // Block a second collection: the first balance must be reusable with the
    // latest local reservation view after cancellation, without repeating RPC.
    *server.state.rpc_latest_gate.lock().unwrap() = Some(Arc::new(tokio::sync::Notify::new()));
    // Publish the local release while the explicit remote query is still held.
    let state = timeout(Duration::from_millis(500), apply_account(&mut rx))
        .await
        .unwrap();
    assert_eq!(state.balances[0].locked, Money::from("0 USDT"));
    assert_eq!(state.balances[0].free, Money::from("1000 USDT"));
    gate.notify_one();
    let state = timeout(Duration::from_millis(500), apply_account(&mut rx))
        .await
        .unwrap();
    assert_eq!(state.balances[0].locked, Money::from("0 USDT"));
    assert_eq!(state.balances[0].free, Money::from("1000 USDT"));
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn account_refresh_errors_and_shutdown_preserve_the_last_reported_cash() {
    let server = server(vec![]).await;
    server.state.rpc_frame.lock().unwrap().1 = 2;
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    connect_reported(&mut client, &mut rx).await;
    let query = || {
        QueryAccount::new(
            trader_id(),
            None,
            account_id(),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None,
        )
    };
    let mut foreign = query();
    foreign.account_id = AccountId::new("PREDICT-other");
    assert!(client.query_account(foreign).is_err());
    server.state.rpc_frame.lock().unwrap().1 = 1;
    client.query_account(query()).unwrap();
    assert!(
        timeout(Duration::from_millis(150), rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        client
            .get_account()
            .unwrap()
            .balance(Some(Currency::USDT()))
            .unwrap()
            .total,
        Money::from("1000 USDT")
    );
    *server.state.rpc_failure.lock().unwrap() = true;
    client.query_account(query()).unwrap();
    assert!(
        timeout(Duration::from_millis(150), rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        client
            .get_account()
            .unwrap()
            .balance(Some(Currency::USDT()))
            .unwrap()
            .total,
        Money::from("1000 USDT")
    );
    *server.state.rpc_failure.lock().unwrap() = false;
    server.state.rpc_frame.lock().unwrap().1 = 3;
    *server.state.rpc_delay_ms.lock().unwrap() = 200;
    let requests = server.state.rpc_methods.lock().unwrap().len();
    client.query_account(query()).unwrap();
    timeout(Duration::from_secs(1), async {
        loop {
            if server.state.rpc_methods.lock().unwrap().len() > requests {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    client.disconnect().await.unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(
        cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .balance(Some(Currency::USDT()))
            .unwrap()
            .total,
        Money::from("1000 USDT")
    );
}

#[tokio::test]
async fn normal_execution_rejects_missing_balance_source_and_calculated_cash_accounts() {
    let server = server(vec![]).await;
    let (_, mut rx, cache) = client(&server);
    let mut cfg = config(&server);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
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
    let mut client = PredictExecutionClient::new(core.clone(), cfg.clone()).unwrap();
    assert!(client.connect().await.is_err());
    assert!(!client.is_connected() && rx.try_recv().is_err());
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", server.http).into(),
    ));
    seed_account(&cache);
    let mut client = PredictExecutionClient::new(core, cfg).unwrap();
    assert!(
        client.connect().await.is_err(),
        "a calculated account would double-apply reported collateral changes"
    );
    assert!(rx.try_recv().is_err());
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn reported_account_preserves_native_exact_cash_and_raw_chain_balance() {
    let server = server(vec![]).await;
    let raw = 1_000_000_000_009_000_000_000u128;
    server.state.rpc_frame.lock().unwrap().0 = raw;
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    let state = connect_reported(&mut client, &mut rx).await;
    assert_eq!(
        state.balances[0].total,
        Money::from_str_exact("1000.000000009 USDT").unwrap()
    );
    assert_eq!(state.balances[0].free, state.balances[0].total);
    assert_eq!(
        state.info.as_ref().unwrap()["predict_balance_wei"],
        json!(raw.to_string())
    );
    assert_eq!(state.info.as_ref().unwrap()["predict_chain_id"], json!(56));
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn external_capacity_uses_native_overcommit_clamp_and_conservative_rounding() {
    for (maker, taker, expected_lock, expected_free) in [
        (
            "2000000000000000000000",
            "4000000000000000000000",
            "1000 USDT",
            "0 USDT",
        ),
        (
            "62000000000000000001",
            "100000000000000000000",
            "62.00000001 USDT",
            "937.99999999 USDT",
        ),
    ] {
        let server = server(vec![]).await;
        let mut row = rest_order_data("OPEN", 1, OrderSide::Buy);
        row["order"]["makerAmount"] = json!(maker);
        row["order"]["takerAmount"] = json!(taker);
        server.state.open_orders.lock().unwrap().push(row);
        let (mut client, mut rx, cache) = reported_client(&server);
        let _portfolio = portfolio(&cache);
        let state = connect_reported(&mut client, &mut rx).await;
        assert_eq!(state.balances[0].locked, Money::from(expected_lock));
        assert_eq!(state.balances[0].free, Money::from(expected_free));
        assert!(
            cache
                .borrow()
                .orders_refs(None, None, None, None, None)
                .is_empty()
        );
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn account_balance_waits_for_a_block_covering_confirmed_fills_then_refreshes() {
    let server = server(vec![]).await;
    server
        .state
        .open_orders
        .lock()
        .unwrap()
        .push(rest_order_data("OPEN", 1, OrderSide::Buy));
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let order = order(1);
    cache
        .borrow_mut()
        .add_order(order, None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let mut clock = VirtualClock::new();
    clock.advance_time(UnixNanos::from(OBSERVED), true);
    let mut engine = ExecutionEngine::new(Rc::new(RefCell::new(clock)), cache.clone(), None);
    *server.state.rpc_frame.lock().unwrap() = (989_360_000_000_000_000_000u128, 2, 0);
    server.wallet(fill(1));
    let ExecutionEvent::Report(report) = next(&mut rx).await else {
        panic!("fill report");
    };
    engine.reconcile_execution_report(&report);
    assert!(
        timeout(Duration::from_millis(150), rx.recv())
            .await
            .is_err()
    );
    assert_eq!(
        client
            .get_account()
            .unwrap()
            .balance(Some(Currency::USDT()))
            .unwrap()
            .total,
        Money::from("1000 USDT")
    );
    server.state.rpc_frame.lock().unwrap().2 = 2;
    let state = apply_account(&mut rx).await;
    assert_eq!(state.balances[0].total, Money::from("989.36 USDT"));
    assert_eq!(state.balances[0].locked, Money::from("51.46 USDT"));
    client.disconnect().await.unwrap();
}

#[path = "execution/cancellations.rs"]
mod cancellations;

#[path = "execution/submissions.rs"]
mod submissions;

#[path = "execution/live_node.rs"]
mod live_node;

#[path = "execution/market_replay.rs"]
mod market_replay;

#[path = "execution/maker_lifecycle.rs"]
mod maker_lifecycle;

#[tokio::test]
async fn confirmed_fill_rejects_inconsistent_raw_value_before_native_updates() {
    let native = order(1);
    for (field, value) in [
        ("executedValueWei", "10540000000000000001"),
        ("executedPriceWei", "630000000000000000"),
    ] {
        let mut data = fill(1);
        data["fill"][field] = json!(value);
        let event: PredictWalletEvent = serde_json::from_value(data).unwrap();
        assert!(
            parse_wallet_report(
                &event,
                &native,
                &instrument(1),
                account_id(),
                WALLET.parse().unwrap(),
                UnixNanos::from(OBSERVED)
            )
            .is_err()
        );
        assert!(native.filled_qty().is_zero());
    }
}

#[tokio::test]
async fn targeted_query_reconciles_known_zero_fill_orders_and_releases_native_cash() {
    let server = server(vec![]).await;
    let mut row = rest_order_data("CANCELLED", 1, OrderSide::Buy);
    let hasher = nautilus_predict::signing::PredictOrderHasher::new(PredictEnvironment::BnbMainnet);
    let hash = hasher.order_hash(
        &serde_json::from_value(row["order"].clone()).unwrap(),
        nautilus_predict::common::contracts::PredictExchangeType::Standard,
    );
    row["order"]["hash"] = json!(hash);
    server
        .state
        .orders
        .lock()
        .unwrap()
        .push_back(json!({"success":true,"data":row}));
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let original = order(1);
    let mut order = OrderAny::try_from(original.init_event().clone()).unwrap();
    for event in original.events().into_iter().skip(1) {
        let mut event = event.clone();
        if let nautilus_model::events::OrderEventAny::Accepted(event) = &mut event {
            event.venue_order_id = VenueOrderId::new(format!("{hash:#x}"));
        }
        order.apply(event).unwrap();
    }
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let query = nautilus_common::messages::execution::QueryOrder::new(
        trader_id(),
        Some(ClientId::new("PREDICT")),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        order.venue_order_id(),
        UUID4::new(),
        UnixNanos::from(OBSERVED),
        None,
        None,
    );
    client.query_order(query).unwrap();
    let mut engine = cancellations::engine(&cache);
    timeout(Duration::from_secs(3), async {
        loop {
            match next(&mut rx).await {
                ExecutionEvent::Report(report) => {
                    engine.reconcile_execution_report(&report);
                    break;
                }
                ExecutionEvent::Account(state) => msgbus::send_account_state(
                    MessagingSwitchboard::portfolio_update_account(),
                    &state,
                ),
                _ => panic!("unexpected query event"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
    let state = apply_account(&mut rx).await;
    assert_eq!(state.balances[0].locked, Money::from("0 USDT"));
    assert_eq!(server.state.order_requests.lock().unwrap().len(), 1);
    assert!(server.state.match_requests.lock().unwrap().is_empty());
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    client.disconnect().await.unwrap();
}

#[test]
fn maker_fee_evidence_preserves_native_fill_quantity_and_rejects_contradictions() {
    let order = order_with_terms(1, OrderSide::Buy, true);
    for fee_type in [None, Some("COLLATERAL"), Some("SHARES")] {
        let mut data = fill(1);
        data["isMaker"] = json!(true);
        if let Some(kind) = fee_type {
            data["fee"] = json!({"amountWei":"0","type":kind});
        } else {
            data.as_object_mut().unwrap().remove("fee");
        }
        let event = serde_json::from_value(data.clone()).unwrap();
        let Some(ExecutionReport::Fill(report)) = parse_wallet_report(
            &event,
            &order,
            &instrument(1),
            account_id(),
            WALLET.parse().unwrap(),
            UnixNanos::from(OBSERVED),
        )
        .unwrap() else {
            panic!("explicit maker fill")
        };
        assert_eq!(report.last_qty.as_decimal(), Decimal::from(17));
        assert_eq!(report.commission, Money::from("0 USDT"));
        assert_eq!(
            report.liquidity_side,
            nautilus_model::enums::LiquiditySide::Maker
        );
        for identity in [None, Some(false)] {
            if let Some(identity) = identity {
                data["isMaker"] = json!(identity);
            } else {
                data.as_object_mut().unwrap().remove("isMaker");
            }
            if fee_type.is_none() {
                let event = serde_json::from_value(data.clone()).unwrap();
                assert!(
                    parse_wallet_report(
                        &event,
                        &order,
                        &instrument(1),
                        account_id(),
                        WALLET.parse().unwrap(),
                        UnixNanos::from(OBSERVED)
                    )
                    .is_err()
                );
            }
        }
    }
    for kind in ["COLLATERAL", "SHARES"] {
        let mut data = fill(1);
        data["isMaker"] = json!(true);
        data["fee"] = json!({"amountWei":"100000000000000000","type":kind});
        let event = serde_json::from_value(data).unwrap();
        assert!(
            parse_wallet_report(
                &event,
                &order,
                &instrument(1),
                account_id(),
                WALLET.parse().unwrap(),
                UnixNanos::from(OBSERVED)
            )
            .is_err()
        );
    }
}

#[path = "execution/fee_models.rs"]
mod fee_models;

#[path = "execution/native_fee_paths.rs"]
mod native_fee_paths;

#[path = "execution/share_fee_reconciliation.rs"]
mod share_fee_reconciliation;

#[path = "execution/matches.rs"]
mod matches;

#[path = "execution/precision.rs"]
mod precision;

#[path = "execution/exit_orders.rs"]
mod exit_orders;

#[path = "execution/maker_options.rs"]
mod maker_options;
#[path = "execution/market_fok.rs"]
mod market_fok;
#[path = "execution/market_preparation.rs"]
mod market_preparation;
#[path = "execution/market_taker.rs"]
mod market_taker;
#[path = "execution/market_value.rs"]
mod market_value;

#[path = "execution/wallet_lifecycle.rs"]
mod wallet_lifecycle;

#[path = "execution/strategy_lifecycle.rs"]
mod strategy_lifecycle;

#[path = "execution/query_recovery.rs"]
mod query_recovery;

#[path = "execution/recovery_live.rs"]
mod recovery_live;

#[path = "execution/recovery_rejections.rs"]
mod recovery_rejections;

#[path = "execution/bulk_reports.rs"]
mod bulk_reports;

#[path = "execution/account_recovery_timer.rs"]
mod account_recovery_timer;

#[path = "execution/restart_live.rs"]
mod restart_live;

#[path = "execution/command_alignment.rs"]
mod command_alignment;

#[path = "execution/multi_admission.rs"]
mod multi_admission;

#[tokio::test]
async fn reported_reconnect_drains_old_account_reports_before_new_registration() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    let previous = connect_reported(&mut client, &mut rx).await;
    client.disconnect().await.unwrap();
    nautilus_common::live::runner::get_exec_event_sender()
        .send(ExecutionEvent::Account(previous.clone()))
        .unwrap();
    let current = connect_reported(&mut client, &mut rx).await;
    assert_ne!(current.event_id, previous.event_id);
    assert_eq!(
        cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .last_event()
            .unwrap()
            .event_id,
        current.event_id
    );
    client.disconnect().await.unwrap();
}

#[path = "execution/early_cancel.rs"]
mod early_cancel;

#[path = "execution/native_tester.rs"]
mod native_tester;

#[path = "execution/node_host.rs"]
mod node_host;

#[path = "execution/recorded_startup.rs"]
mod recorded_startup;

#[path = "execution/cold_start.rs"]
mod cold_start;

#[path = "execution/query_recorded.rs"]
mod query_recorded;

#[path = "execution/batch_recorded.rs"]
mod batch_recorded;

#[tokio::test]
async fn unknown_wallet_order_does_not_prevent_consuming_owned_fills() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = client(&server);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(order(1), None, None, false)
        .unwrap();
    seed_account(&cache);
    client.connect().await.unwrap();
    let mut unknown = fill(1);
    unknown["orderHash"] = json!(format!("0x{}", "77".repeat(32)));
    server.wallet(unknown);
    server.wallet(fill(1));
    let ExecutionEvent::Report(ExecutionReport::Fill(fill)) = next(&mut rx).await else {
        panic!("owned fill must still arrive")
    };
    assert_eq!(fill.last_qty, Quantity::from(17));
    assert!(
        client.is_connected(),
        "transport remains available for Native recovery"
    );
    client.disconnect().await.unwrap();
}

#[allow(dead_code)]
#[path = "execution/entry_benchmark.rs"]
pub(crate) mod entry_benchmark;

#[tokio::test]
async fn account_snapshot_collects_open_and_cash_concurrently_and_publishes_native_balance() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    connect_reported(&mut client, &mut rx).await;
    let rendezvous = Arc::new(tokio::sync::Barrier::new(3));
    *server.state.open_rpc_barrier.lock().unwrap() = Some(rendezvous.clone());
    client
        .query_account(QueryAccount::new(
            trader_id(),
            None,
            account_id(),
            UUID4::new(),
            UnixNanos::from(OBSERVED),
            None,
            None,
        ))
        .unwrap();
    timeout(Duration::from_millis(500), rendezvous.wait())
        .await
        .unwrap();
    *server.state.open_rpc_barrier.lock().unwrap() = None;
    let state = apply_account(&mut rx).await;
    assert_eq!(state.balances[0].total, Money::from("1000 USDT"));
    assert_eq!(state.balances[0].free, Money::from("1000 USDT"));
    assert_eq!(
        cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .balance_total(Some(Currency::USDT())),
        Some(Money::from("1000 USDT"))
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn explicit_account_query_during_io_keeps_a_followup_fresh_read() {
    for fail_first in [false, true] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = reported_client(&server);
        let _portfolio = portfolio(&cache);
        connect_reported(&mut client, &mut rx).await;
        server.state.rpc_latest_started.notified().await;
        let initial = server.state.rpc_methods.lock().unwrap().len();
        let query = || {
            QueryAccount::new(
                trader_id(),
                None,
                account_id(),
                UUID4::new(),
                UnixNanos::from(OBSERVED),
                None,
                None,
            )
        };
        let first = Arc::new(tokio::sync::Notify::new());
        *server.state.rpc_gate.lock().unwrap() = Some(first.clone());
        client.query_account(query()).unwrap();
        server.state.rpc_latest_started.notified().await;
        let second = Arc::new(tokio::sync::Notify::new());
        *server.state.rpc_latest_gate.lock().unwrap() = Some(second.clone());
        client.query_account(query()).unwrap();
        *server.state.rpc_failure.lock().unwrap() = fail_first;
        first.notify_one();
        if !fail_first {
            let state = apply_account(&mut rx).await;
            assert_eq!(state.balances[0].total, Money::from("1000 USDT"));
        }
        timeout(
            Duration::from_millis(500),
            server.state.rpc_latest_started.notified(),
        )
        .await
        .unwrap();
        *server.state.rpc_frame.lock().unwrap() = (900_000_000_000_000_000_000, 2, 2);
        *server.state.rpc_failure.lock().unwrap() = false;
        second.notify_one();
        let state = apply_account(&mut rx).await;
        assert_eq!(state.balances[0].total, Money::from("900 USDT"));
        assert_eq!(
            server.state.rpc_methods.lock().unwrap().len() - initial,
            if fail_first { 4 } else { 6 }
        );
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn local_lifecycle_reuses_cash_without_rest_and_preserves_external_open_reservations() {
    let server = server(vec![]).await;
    let mut external = rest_order_data("OPEN", 1, OrderSide::Buy);
    external["order"]["hash"] = json!(format!("0x{}", "aa".repeat(32)));
    external["id"] = json!("12346");
    *server.state.open_orders.lock().unwrap() =
        vec![rest_order_data("OPEN", 1, OrderSide::Buy), external];
    let (mut client, mut rx, cache) = reported_client(&server);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let own = order(1);
    cache
        .borrow_mut()
        .add_order(own.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    assert_eq!(
        client
            .get_account()
            .unwrap()
            .balance(Some(Currency::USDT()))
            .unwrap()
            .locked,
        Money::from("124 USDT")
    );
    let rpc_before = server.state.rpc_methods.lock().unwrap().len();
    let open_before = server.state.requests.lock().unwrap().len();
    *server.state.rpc_latest_gate.lock().unwrap() = Some(Arc::new(tokio::sync::Notify::new()));
    let mut clock = VirtualClock::new();
    clock.advance_time(UnixNanos::from(OBSERVED), true);
    let mut engine = ExecutionEngine::new(Rc::new(RefCell::new(clock)), cache.clone(), None);
    server.wallet(event("orderCancelled", 1));
    let ExecutionEvent::Order(event @ nautilus_model::events::OrderEventAny::Canceled(_)) =
        next(&mut rx).await
    else {
        panic!("native wallet cancellation");
    };
    engine.process(&event);
    let canceled = apply_account(&mut rx).await;
    assert_eq!(canceled.balances[0].locked, Money::from("62 USDT"));
    assert_eq!(canceled.balances[0].free, Money::from("938 USDT"));
    let replacement = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .client_order_id("LOCAL-REPLACEMENT".into())
        .instrument_id(instrument_id(1))
        .side(OrderSide::Buy)
        .quantity(Quantity::from(100))
        .price(Price::from("0.010"))
        .build();
    cache
        .borrow_mut()
        .add_order(replacement.clone(), None, None, false)
        .unwrap();
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    engine.process(&factory.generate_order_submitted(&replacement, 400.into()));
    // Native risk owns the local in-flight amount; no duplicate reported-lock event.
    assert!(rx.try_recv().is_err());
    assert_eq!(
        client
            .get_account()
            .unwrap()
            .balance(Some(Currency::USDT()))
            .unwrap()
            .locked,
        Money::from("62 USDT")
    );
    engine.process(&factory.generate_order_accepted(
        &replacement,
        VenueOrderId::new(format!("0x{}", "bb".repeat(32))),
        500.into(),
        500.into(),
    ));
    let accepted = apply_account(&mut rx).await;
    assert_eq!(accepted.balances[0].locked, Money::from("63 USDT"));
    assert_eq!(server.state.rpc_methods.lock().unwrap().len(), rpc_before);
    assert_eq!(server.state.requests.lock().unwrap().len(), open_before);
    client.disconnect().await.unwrap();
}

#[path = "execution/expiry_scope.rs"]
mod expiry_scope;

/// Same-socket round trip proves earlier wallet messages were consumed without using a trading gate.
async fn wallet_processed(venue: &Server) {
    let marker = UUID4::new().to_string();
    venue
        .state
        .sockets
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .send(Message::Text(
            json!({"type":"M","topic":"heartbeat","data":marker})
                .to_string()
                .into(),
        ))
        .unwrap();
    exit_orders::barrier("wallet messages consumed", || {
        venue
            .state
            .ws_requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r["method"] == "heartbeat" && r["data"] == marker)
    })
    .await;
}

#[path = "execution/feedback_delivery.rs"]
mod feedback_delivery;
