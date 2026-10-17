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

use nautilus_model::{events::OrderEventAny, identifiers::ClientOrderId};
use nautilus_predict::{PredictSubmissionConfig, signing::PredictAuthSigner};

use super::*;

pub(super) const PUBLIC_KEY: &str =
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

fn setup_client_at(
    server: &Server,
    now: u64,
) -> (
    PredictExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
) {
    setup_client_with_policy(server, now, true)
}

pub(super) fn setup_client_with_policy(
    server: &Server,
    now: u64,
    require_post_only: bool,
) -> (
    PredictExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
) {
    let (client, rx, cache, _) = setup_client_with_policy_and_clock(server, now, require_post_only);
    (client, rx, cache)
}

fn setup_client_with_policy_and_clock(
    server: &Server,
    now: u64,
    require_post_only: bool,
) -> (
    PredictExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
    &'static AtomicTime,
) {
    let (_, rx, cache) = client(server);
    let mut cfg = config(server);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", server.http).into(),
    ));
    cfg.report_timeout_secs = 1;
    cfg.submission = Some(PredictSubmissionConfig {
        signer: Arc::new(
            PredictAuthSigner::new(
                PUBLIC_KEY.into(),
                PredictEnvironment::BnbMainnet,
                Some(WALLET),
            )
            .unwrap(),
        ),
        max_order_usdt: Decimal::from(100),
        max_total_usdt: Decimal::from(100),
        require_post_only,
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
    let clock = Box::leak(Box::new(AtomicTime::new(false, UnixNanos::from(now))));
    (
        PredictExecutionClient::new_with_clock(core, cfg, clock).unwrap(),
        rx,
        cache,
        clock,
    )
}

fn initialized(index: u8) -> OrderAny {
    OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .instrument_id(instrument_id(index))
        .side(OrderSide::Buy)
        .quantity(Quantity::from(100))
        .price(Price::from("0.620"))
        .post_only(true)
        .build()
}
pub(super) fn submit(order: &OrderAny) -> SubmitOrder {
    SubmitOrder::from_order(
        order,
        trader_id(),
        Some(ClientId::new("PREDICT")),
        None,
        UUID4::new(),
        UnixNanos::from(OBSERVED),
    )
}
pub(super) fn engine(cache: &Rc<RefCell<Cache>>) -> ExecutionEngine {
    cancellations::engine(cache)
}

pub(super) fn apply(engine: &mut ExecutionEngine, event: &ExecutionEvent) {
    match event {
        ExecutionEvent::Order(event) => engine.process(event),
        ExecutionEvent::Report(report) => {
            engine.reconcile_execution_report(report);
        }
        ExecutionEvent::Account(state) => {
            msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), state);
        }
        _ => panic!("unexpected native execution event"),
    }
}

pub(super) async fn connected(
    server: &Server,
    index: u8,
) -> (
    PredictExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
    Portfolio,
    OrderAny,
) {
    let (client, rx, cache, portfolio, order, _) = connected_with_clock(server, index).await;
    (client, rx, cache, portfolio, order)
}

pub(super) async fn connected_with_clock(
    server: &Server,
    index: u8,
) -> (
    PredictExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
    Portfolio,
    OrderAny,
    &'static AtomicTime,
) {
    let (mut client, mut rx, cache, clock) =
        setup_client_with_policy_and_clock(server, OBSERVED, true);
    let portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(index)))
        .unwrap();
    let order = initialized(index);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    (client, rx, cache, portfolio, order, clock)
}

#[tokio::test]
async fn wallet_before_http_accepts_yes_and_no_and_preserves_burst_fill_cancel() {
    for (index, fee) in [(1, 0), (2, 0), (1, 200), (2, 200)] {
        let server = server(vec![]).await;
        *server.state.market_fee.lock().unwrap() = fee;
        server.state.create_replies.lock().unwrap().push_back((
            201,
            300,
            vec![
                "orderAccepted".into(),
                "orderTransactionSuccess".into(),
                "orderCancelled".into(),
            ],
        ));
        let (mut client, mut rx, cache, _portfolio, order) = connected(&server, index).await;
        client.submit_order(submit(&order)).unwrap();
        assert!(client.submit_order(submit(&order)).is_err());
        let first = next(&mut rx).await;
        assert!(matches!(
            first,
            ExecutionEvent::Order(OrderEventAny::Submitted(_))
        ));
        let mut engine = engine(&cache);
        apply(&mut engine, &first);
        timeout(Duration::from_secs(3), async {
            while cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status()
                != OrderStatus::Canceled
            {
                let event = next(&mut rx).await;
                apply(&mut engine, &event);
            }
        })
        .await
        .unwrap();
        let cached = cache
            .borrow()
            .order_owned(&order.client_order_id())
            .unwrap();
        assert_eq!(cached.filled_qty().as_decimal(), Decimal::from(17));
        assert_eq!(
            cache.borrow().positions(None, None, None, None, None)[0]
                .quantity
                .as_decimal(),
            Decimal::from(17)
        );
        {
            let bodies = server.state.creations.lock().unwrap();
            assert_eq!(bodies.len(), 1);
            let body = &bodies[0]["data"];
            assert_eq!(body["pricePerShare"], "620000000000000000");
            assert_eq!(body["isPostOnly"], true);
            assert_eq!(body["order"]["makerAmount"], "62000000000000000000");
            assert_eq!(body["order"]["takerAmount"], "100000000000000000000");
            assert_eq!(body["order"]["nonce"], "7");
            assert_eq!(body["order"]["feeRateBps"], fee.to_string());
            assert_eq!(body["order"]["maker"], WALLET);
            assert_eq!(body["order"]["signer"], WALLET);
            assert_eq!(
                body["order"]["tokenId"],
                if index == 1 { "11" } else { "22" }
            );
        }
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn opaque_receipt_does_not_accept_and_timeout_never_resubmits() {
    for delay in [0, 1500] {
        let server = server(vec![]).await;
        server
            .state
            .create_replies
            .lock()
            .unwrap()
            .push_back((201, delay, vec![]));
        let (mut client, mut rx, cache, _portfolio, order) = connected(&server, 1).await;
        client.submit_order(submit(&order)).unwrap();
        let first = next(&mut rx).await;
        let mut engine = engine(&cache);
        apply(&mut engine, &first);
        timeout(Duration::from_secs(2), async {
            while server.state.creations.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
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
            OrderStatus::Submitted
        );
        assert!(client.submit_order(submit(&order)).is_err());
        assert!(client.reset().is_err());
        assert_eq!(server.state.creations.lock().unwrap().len(), 1);
        client.disconnect().await.unwrap();
        while let Ok(event) = rx.try_recv() {
            assert!(!matches!(
                event,
                ExecutionEvent::Order(OrderEventAny::Rejected(_))
            ));
        }
    }
}

#[tokio::test]
async fn admission_failures_reject_without_post_or_approvals() {
    for (fee, allowance) in [(10_001, U256::MAX), (0, U256::ZERO)] {
        let server = server(vec![]).await;
        *server.state.market_fee.lock().unwrap() = fee;
        *server.state.admission_allowance.lock().unwrap() = allowance;
        let (mut client, mut rx, cache, _portfolio, order) = connected(&server, 1).await;
        client.submit_order(submit(&order)).unwrap();
        let mut engine = engine(&cache);
        timeout(Duration::from_secs(2), async {
            while cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status()
                != OrderStatus::Rejected
            {
                let event = next(&mut rx).await;
                apply(&mut engine, &event);
            }
        })
        .await
        .unwrap();
        assert!(server.state.creations.lock().unwrap().is_empty());
        assert!(
            !server
                .state
                .rpc_methods
                .lock()
                .unwrap()
                .iter()
                .any(|method| method.contains("send"))
        );
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn explicit_http_rejection_flows_to_native_rejected_state() {
    for status in [400, 401, 403, 404, 410, 422, 429] {
        let server = server(vec![]).await;
        server
            .state
            .create_replies
            .lock()
            .unwrap()
            .push_back((status, 0, vec![]));
        let (mut client, mut rx, cache, _portfolio, order) = connected(&server, 1).await;
        client.submit_order(submit(&order)).unwrap();
        let mut engine = engine(&cache);
        timeout(Duration::from_secs(3), async {
            while cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status()
                != OrderStatus::Rejected
            {
                let event = next(&mut rx).await;
                if status == 400
                    && let ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) = &event
                {
                    assert!(rejected.reason.as_str().contains("InvalidSignature"));
                    assert!(rejected.reason.as_str().contains("API code Some(400)"));
                }
                apply(&mut engine, &event);
            }
        })
        .await
        .unwrap();
        assert_eq!(server.state.creations.lock().unwrap().len(), 1);
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn native_gtc_and_gtd_submit_exact_official_sdk_bodies_and_preserve_account_capacity() {
    use nautilus_model::enums::TimeInForce;
    let fixtures: Value =
        serde_json::from_str(include_str!("../fixtures/submission_matrix.json")).unwrap();
    for vector in fixtures["vectors"].as_array().unwrap() {
        let server = server(vec![]).await;
        *server.state.market_fee.lock().unwrap() = vector["fee_bps"].as_u64().unwrap() as u32;
        server.state.create_replies.lock().unwrap().push_back((
            201,
            0,
            vec!["orderAccepted".into()],
        ));
        let (mut client, mut rx, cache, _portfolio, _) = connected(&server, 1).await;
        let tif = if vector["time_in_force"] == "GTC" {
            TimeInForce::Gtc
        } else {
            TimeInForce::Gtd
        };
        let mut builder = OrderTestBuilder::new(OrderType::Limit);
        builder
            .trader_id(trader_id())
            .instrument_id(instrument_id(1))
            .client_order_id(ClientOrderId::new("TYPE-001"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from("1000"))
            .price(Price::from("0.001"))
            .post_only(true)
            .time_in_force(tif);
        if tif == TimeInForce::Gtd {
            builder.expire_time(UnixNanos::from(300_000_000_000));
        }
        let order = builder.build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        let mut command = submit(&order);
        command.command_id = UUID4::from("00000000-0000-4000-8000-000000000001");
        client.submit_order(command).unwrap();
        let mut engine = engine(&cache);
        timeout(Duration::from_secs(3), async {
            while cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status()
                != OrderStatus::Accepted
            {
                apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .unwrap();
        assert_eq!(
            server.state.creations.lock().unwrap().as_slice(),
            &[vector["body"].clone()]
        );
        let cached = cache
            .borrow()
            .order_owned(&order.client_order_id())
            .unwrap();
        assert_eq!(cached.time_in_force(), tif);
        assert_eq!(cached.price(), order.price());
        assert_eq!(cached.quantity(), order.quantity());
        // Acceptance and the authoritative cash report are separate events.
        timeout(Duration::from_secs(3), async {
            while cache
                .borrow()
                .account(&account_id())
                .unwrap()
                .balance_locked(Some(Currency::USDT()))
                .unwrap()
                .as_decimal()
                < Decimal::new(1, 5)
            {
                apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .unwrap();
        let account = cache.borrow().account_owned(&account_id()).unwrap();
        assert_eq!(
            account
                .balance_total(Some(Currency::USDT()))
                .unwrap()
                .as_decimal(),
            Decimal::from(1000)
        );
        assert!(
            account
                .balance_locked(Some(Currency::USDT()))
                .unwrap()
                .as_decimal()
                >= Decimal::new(1, 5)
        );
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn unsupported_market_buy_stop_and_immediate_time_in_force_are_native_denied_before_post() {
    use nautilus_model::enums::TimeInForce;
    for (kind, tif) in [
        (OrderType::Market, TimeInForce::Gtc),
        (OrderType::StopMarket, TimeInForce::Gtc),
        (OrderType::Limit, TimeInForce::Ioc),
        (OrderType::Limit, TimeInForce::Fok),
    ] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache, _portfolio, _) = connected(&server, 1).await;
        let denied = OrderTestBuilder::new(kind)
            .trader_id(trader_id())
            .instrument_id(instrument_id(1))
            .client_order_id(ClientOrderId::new("TYPE-DENIED-001"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .price(Price::from("0.001"))
            .trigger_price(Price::from("0.002"))
            .time_in_force(tif)
            .build();
        cache
            .borrow_mut()
            .add_order(denied.clone(), None, None, false)
            .unwrap();
        client.submit_order(submit(&denied)).unwrap();
        let native = next(&mut rx).await;
        assert!(matches!(
            native,
            ExecutionEvent::Order(OrderEventAny::Denied(_))
        ));
        let ExecutionEvent::Order(OrderEventAny::Denied(event)) = &native else {
            unreachable!()
        };
        let reason = match kind {
            OrderType::Limit => "LIMIT requires GTC/GTD",
            OrderType::Market => "MARKET submission supports SELL only",
            _ => "order type",
        };
        assert!(event.reason.as_str().contains(reason));
        apply(&mut engine(&cache), &native);
        assert_eq!(
            cache
                .borrow()
                .order(&denied.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Denied
        );
        assert!(server.state.creations.lock().unwrap().is_empty());
        assert!(server.state.removals.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn transmitted_timeout_stays_submitted_then_late_wallet_evidence_resolves_without_replay() {
    let server = server(vec![]).await;
    server
        .state
        .create_replies
        .lock()
        .unwrap()
        .push_back((201, 1500, vec![]));
    let (mut client, mut rx, cache, _portfolio, order) = connected(&server, 1).await;
    client.submit_order(submit(&order)).unwrap();
    let mut engine = engine(&cache);
    apply(&mut engine, &next(&mut rx).await);
    timeout(Duration::from_secs(2), async {
        while server.state.creations.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // The actual one-second write deadline passes; no fake reject or automatic POST retry.
    assert!(
        timeout(Duration::from_millis(1250), async {
            loop {
                let event = rx.recv().await.unwrap();
                assert!(!matches!(
                    event,
                    ExecutionEvent::Order(OrderEventAny::Rejected(_))
                ));
                apply(&mut engine, &event);
            }
        })
        .await
        .is_err()
    );
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Submitted
    );
    assert_eq!(
        cache
            .borrow()
            .account_owned(&account_id())
            .unwrap()
            .balance_locked(Some(Currency::USDT()))
            .unwrap()
            .as_decimal(),
        Decimal::from(62)
    );
    assert!(client.submit_order(submit(&order)).is_err());
    assert!(client.reset().is_err());
    let hash = server.state.creations.lock().unwrap()[0]["data"]["order"]["hash"].clone();
    for kind in ["orderAccepted", "orderCancelled"] {
        let mut payload = event(kind, 1);
        payload["orderHash"] = hash.clone();
        server.wallet(payload);
        timeout(Duration::from_secs(2), async {
            let status = if kind == "orderAccepted" {
                OrderStatus::Accepted
            } else {
                OrderStatus::Canceled
            };
            while cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status()
                != status
            {
                apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .unwrap();
    }
    assert_eq!(server.state.creations.lock().unwrap().len(), 1);
    assert!(
        cache
            .borrow()
            .positions(None, None, None, None, None)
            .is_empty()
    );
    client.disconnect().await.unwrap();
    client.reset().unwrap();
}

#[tokio::test]
async fn gtd_precision_quantity_and_post_only_failures_have_specific_native_denial_reasons() {
    use nautilus_model::enums::TimeInForce;
    for (price, qty, post_only, expiry, reason) in [
        ("0.001", "0.001", true, None, "quantity minimum"),
        ("0.0015", "0.01", true, None, "instrument increment"),
        ("0.001", "0.01", false, None, "requires post-only"),
        (
            "0.001",
            "0.01",
            true,
            Some(5_000_000_001u64),
            "whole seconds",
        ),
        ("0.001", "0.01", true, Some(1_000_000_000u64), "expired"),
        (
            "0.001",
            "1000",
            true,
            Some(122_000_000_000u64),
            "more than 2 minutes",
        ),
    ] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache) = setup_client_at(&server, 2_000_000_000);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        connect_reported(&mut client, &mut rx).await;
        let mut builder = OrderTestBuilder::new(OrderType::Limit);
        builder
            .trader_id(trader_id())
            .instrument_id(instrument_id(1))
            .client_order_id(ClientOrderId::new("TERM-DENIED-001"))
            .side(OrderSide::Buy)
            .price(Price::from(price))
            .quantity(Quantity::from(qty))
            .post_only(post_only);
        if let Some(expiry) = expiry {
            builder
                .time_in_force(TimeInForce::Gtd)
                .expire_time(UnixNanos::from(expiry));
        }
        let order = builder.build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        client.submit_order(submit(&order)).unwrap();
        let native = next(&mut rx).await;
        let ExecutionEvent::Order(OrderEventAny::Denied(event)) = &native else {
            panic!("expected native denial")
        };
        assert!(
            event.reason.as_str().contains(reason),
            "unexpected denial reason"
        );
        apply(&mut engine(&cache), &native);
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Denied
        );
        assert!(server.state.creations.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn unsupported_native_terms_emit_denied_and_never_reach_private_post() {
    for side in [OrderSide::Sell, OrderSide::Buy] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache, _portfolio, old) = connected(&server, 1).await;
        let denied = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .instrument_id(instrument_id(1))
            .client_order_id(nautilus_model::identifiers::ClientOrderId::new(
                "DENIED-001",
            ))
            .side(side)
            .reduce_only(side == OrderSide::Sell)
            .quantity(Quantity::from(1000))
            .price(Price::from("0.620"))
            .post_only(true)
            .build();
        cache
            .borrow_mut()
            .add_order(denied.clone(), None, None, false)
            .unwrap();
        client.submit_order(submit(&denied)).unwrap();
        let denied_event = next(&mut rx).await;
        assert!(matches!(
            denied_event,
            ExecutionEvent::Order(OrderEventAny::Denied(_))
        ));
        let mut engine = engine(&cache);
        apply(&mut engine, &denied_event);
        assert_eq!(
            cache
                .borrow()
                .order(&denied.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Denied
        );
        assert_eq!(
            cache
                .borrow()
                .order(&old.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Initialized
        );
        assert!(server.state.creations.lock().unwrap().is_empty());
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn shutdown_during_admission_retains_submitted_command_without_any_private_write() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache, _portfolio, order) = connected(&server, 1).await;
    *server.state.rpc_delay_ms.lock().unwrap() = 2000;
    client.submit_order(submit(&order)).unwrap();
    let submitted = next(&mut rx).await;
    let mut engine = engine(&cache);
    apply(&mut engine, &submitted);
    client.disconnect().await.unwrap();
    assert!(server.state.creations.lock().unwrap().is_empty());
    assert!(client.reset().is_err());
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Submitted
    );
    while let Ok(event) = rx.try_recv() {
        assert!(!matches!(
            event,
            ExecutionEvent::Order(OrderEventAny::Rejected(_))
        ));
    }
}

#[tokio::test]
async fn fee_bearing_non_post_only_limit_buy_retains_signed_fee_and_native_submitted_state() {
    let server = server(vec![]).await;
    *server.state.market_fee.lock().unwrap() = 200;
    let (mut client, mut rx, cache) = setup_client_with_policy(&server, OBSERVED, false);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .instrument_id(instrument_id(1))
        .side(OrderSide::Buy)
        .quantity(Quantity::from(100))
        .price(Price::from("0.620"))
        .post_only(false)
        .build();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    client.submit_order(submit(&order)).unwrap();
    let mut engine = engine(&cache);
    timeout(Duration::from_secs(3), async {
        loop {
            if cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status()
                == OrderStatus::Submitted
            {
                break;
            }
            apply(&mut engine, &next(&mut rx).await);
        }
    })
    .await
    .unwrap();
    // HTTP completion need not emit another native event after Submitted.
    super::exit_orders::barrier("fee-bearing BUY POST", || {
        server.state.creations.lock().unwrap().len() == 1
    })
    .await;
    {
        let bodies = server.state.creations.lock().unwrap();
        assert_eq!(bodies.len(), 1);
        assert_eq!(bodies[0]["data"]["isPostOnly"], false);
        assert_eq!(bodies[0]["data"]["order"]["feeRateBps"], "200");
    }
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
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn fee_bearing_maker_without_fee_field_updates_native_position_and_reported_cash() {
    let server = server(vec![]).await;
    *server.state.market_fee.lock().unwrap() = 200;
    server
        .state
        .create_replies
        .lock()
        .unwrap()
        .push_back((201, 0, vec!["orderAccepted".into()]));
    let (mut client, mut rx, cache, _portfolio, order) = connected(&server, 1).await;
    client.submit_order(submit(&order)).unwrap();
    let mut engine = engine(&cache);
    while cache
        .borrow()
        .order(&order.client_order_id())
        .unwrap()
        .status()
        != OrderStatus::Accepted
    {
        apply(&mut engine, &next(&mut rx).await);
    }
    let hash = server.state.creations.lock().unwrap()[0]["data"]["order"]["hash"].clone();
    *server.state.rpc_frame.lock().unwrap() = (989_460_000_000_000_000_000u128, 2, 2);
    let mut data = fill(1);
    data["orderHash"] = hash;
    data["timestamp"] = json!(2);
    data["isMaker"] = json!(true);
    data.as_object_mut().unwrap().remove("fee");
    server.wallet(data);
    let mut observed_cash = false;
    timeout(Duration::from_secs(3), async {
        loop {
            let event = next(&mut rx).await;
            if let ExecutionEvent::Account(state) = &event {
                observed_cash |= state.balances[0].total == Money::from("989.46 USDT");
            }
            apply(&mut engine, &event);
            if observed_cash
                && cache
                    .borrow()
                    .order(&order.client_order_id())
                    .unwrap()
                    .filled_qty()
                    .as_decimal()
                    == Decimal::from(17)
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(observed_cash);
    let account = cache.borrow().account_owned(&account_id()).unwrap();
    assert_eq!(
        account.balance_total(Some(Currency::USDT())).unwrap(),
        Money::from("989.46 USDT")
    );
    assert_eq!(
        account.balance_locked(Some(Currency::USDT())).unwrap(),
        Money::from("51.46 USDT")
    );
    {
        let cache = cache.borrow();
        let positions = cache.positions(None, None, None, None, None);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].quantity.as_decimal(), Decimal::from(17));
        assert!(positions[0].commissions.values().all(|fee| fee.is_zero()));
    }
    assert!(client.is_connected());
    assert_eq!(server.state.creations.lock().unwrap().len(), 1);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn worker_minimum_resting_requests_match_official_sdk_with_runtime_salt() {
    let fixture: Value =
        serde_json::from_str(include_str!("../fixtures/runtime_limit_requests.json")).unwrap();
    let vectors = fixture["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| {
            row["predictAccount"] == WALLET
                && row["isNegRisk"] == false
                && row["isYieldBearing"] == false
                && row["saltProfile"] == "runtime_sdk_range"
        })
        .collect::<Vec<_>>();
    assert_eq!(vectors.len(), 2);
    for vector in vectors {
        let server = server(vec![]).await;
        *server.state.market_fee.lock().unwrap() = 200;
        server.state.create_replies.lock().unwrap().push_back((
            201,
            0,
            vec!["orderAccepted".into()],
        ));
        let (mut client, mut rx, cache, _portfolio, _) = connected(&server, 1).await;
        let data = &vector["body"]["data"];
        let price = if data["pricePerShare"] == "10000000000000000" {
            "0.010"
        } else {
            "0.020"
        };
        let quantity = if price == "0.010" { 100 } else { 50 };
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .instrument_id(instrument_id(1))
            .client_order_id(ClientOrderId::new("MINIMUM-001"))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(quantity))
            .price(Price::from(price))
            .post_only(true)
            .build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        let mut command = submit(&order);
        command.command_id = UUID4::from(vector["commandId"].as_str().unwrap());
        client.submit_order(command).unwrap();
        let mut engine = engine(&cache);
        timeout(Duration::from_secs(3), async {
            while cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status()
                != OrderStatus::Accepted
            {
                apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .unwrap();
        assert_eq!(
            server.state.creations.lock().unwrap().as_slice(),
            &[vector["body"].clone()]
        );
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .price(),
            order.price()
        );
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .filled_qty()
                .as_decimal(),
            Decimal::ZERO
        );
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn sdk_range_salt_collision_preserves_original_owner_and_never_reposts_hash() {
    let server = server(vec![]).await;
    server
        .state
        .create_replies
        .lock()
        .unwrap()
        .push_back((400, 0, vec![]));
    let (mut client, mut rx, cache, _portfolio, _) = connected(&server, 1).await;
    let mut engine = engine(&cache);
    for (number, command_id) in [
        (1, "00000000-0000-4000-8000-000000000001"),
        (2, "10000000-0000-4000-8000-000000000001"),
    ] {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .instrument_id(instrument_id(1))
            .client_order_id(ClientOrderId::new(format!("SALT-COLLISION-{number}")))
            .side(OrderSide::Buy)
            .price(Price::from("0.010"))
            .quantity(Quantity::from(100))
            .post_only(true)
            .build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        let mut command = submit(&order);
        command.command_id = UUID4::from(command_id);
        client.submit_order(command).unwrap();
        timeout(Duration::from_secs(3), async {
            loop {
                let event = next(&mut rx).await;
                if let ExecutionEvent::Order(OrderEventAny::Rejected(rejected)) = &event {
                    if number == 2 {
                        assert!(
                            rejected
                                .reason
                                .as_str()
                                .contains("already belongs to another native order")
                        );
                    }
                    apply(&mut engine, &event);
                    break;
                }
                apply(&mut engine, &event);
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
            OrderStatus::Rejected
        );
    }
    assert_eq!(server.state.creations.lock().unwrap().len(), 1);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn sdk_valid_subminimum_orders_are_denied_without_scaling_and_one_usdt_is_admitted() {
    for quantity in ["0.01", "99", "100"] {
        let server = server(vec![]).await;
        server.state.create_replies.lock().unwrap().push_back((
            201,
            0,
            vec!["orderAccepted".into()],
        ));
        let (mut client, mut rx, cache, _portfolio, _) = connected(&server, 1).await;
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .instrument_id(instrument_id(1))
            .client_order_id(ClientOrderId::new("MINIMUM-NOTIONAL"))
            .side(OrderSide::Buy)
            .price(Price::from("0.010"))
            .quantity(Quantity::from(quantity))
            .post_only(true)
            .build();
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        client.submit_order(submit(&order)).unwrap();
        let mut engine = engine(&cache);
        let expected = if quantity == "100" {
            OrderStatus::Accepted
        } else {
            OrderStatus::Denied
        };
        timeout(Duration::from_secs(3), async {
            while cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status()
                != expected
            {
                let event = next(&mut rx).await;
                if let ExecutionEvent::Order(OrderEventAny::Denied(denied)) = &event {
                    assert!(denied.reason.as_str().contains("venue minimum of 1 USDT"));
                }
                apply(&mut engine, &event);
            }
        })
        .await
        .unwrap();
        assert_eq!(
            cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .quantity(),
            order.quantity()
        );
        {
            let posts = server.state.creations.lock().unwrap();
            assert_eq!(posts.len(), usize::from(quantity == "100"));
            if quantity == "100" {
                assert_eq!(
                    posts[0]["data"]["order"]["makerAmount"],
                    "1000000000000000000"
                );
            }
        }
        client.disconnect().await.unwrap();
    }
}
