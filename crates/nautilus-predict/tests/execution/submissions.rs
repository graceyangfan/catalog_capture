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
    setup_client(server, now)
}

pub(super) fn setup_client(
    server: &Server,
    now: u64,
) -> (
    PredictExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
) {
    let (client, rx, cache, _) = setup_client_with_clock(server, now);
    (client, rx, cache)
}

pub(super) fn setup_client_with_clock(
    server: &Server,
    now: u64,
) -> (
    PredictExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
    &'static AtomicTime,
) {
    setup_with_metadata_ttl(server, now, 30)
}

pub(super) fn setup_with_metadata_ttl(
    server: &Server,
    now: u64,
    ttl: u64,
) -> (
    PredictExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
    &'static AtomicTime,
) {
    setup_with_account_interval(server, now, ttl, 30)
}

type Setup = (
    PredictExecutionClient,
    mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
    &'static AtomicTime,
);
fn setup_with_account_interval(
    server: &Server,
    now: u64,
    ttl: u64,
    account_interval: u64,
) -> Setup {
    try_setup(server, now, ttl, account_interval, None).unwrap()
}
pub(super) fn setup_metadata_probe(
    server: &Server,
    ttl: u64,
) -> (Setup, nautilus_predict::latency::PredictLatencyCollector) {
    let (probe, collector) =
        nautilus_predict::latency::PredictLatency::channel(trader_id(), account_id(), 128).unwrap();
    (
        try_setup(server, OBSERVED, ttl, 30, Some(probe)).unwrap(),
        collector,
    )
}
pub(super) async fn metadata_collected(
    collector: &mut nautilus_predict::latency::PredictLatencyCollector,
    count: u64,
) {
    exit_orders::barrier("background market collection finished", || {
        collector
            .snapshot()
            .distributions
            .get(&nautilus_predict::latency::LatencyMetric::MarketGet)
            .is_some_and(|metric| metric.completed_observations >= count)
    })
    .await;
}
pub(super) fn try_setup(
    server: &Server,
    now: u64,
    ttl: u64,
    account_interval: u64,
    latency: Option<nautilus_predict::latency::PredictLatency>,
) -> anyhow::Result<Setup> {
    let (_, rx, cache) = client(server);
    let mut cfg = config(server);
    cfg.account_state_mode = PredictAccountStateMode::Reported;
    cfg.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", server.http).into(),
    ));
    cfg.report_timeout_secs = 1;
    cfg.latency = latency;
    cfg.market_metadata_ttl_secs = ttl;
    cfg.account_refresh_interval_secs = account_interval;
    cfg.submission = Some(PredictSubmissionConfig {
        signer: Arc::new(
            PredictAuthSigner::new(
                PUBLIC_KEY.into(),
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
    let clock = Box::leak(Box::new(AtomicTime::new(false, UnixNanos::from(now))));
    Ok((
        PredictExecutionClient::new_with_clock(core, cfg, clock)?,
        rx,
        cache,
        clock,
    ))
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
    let (mut client, mut rx, cache, clock) = setup_client_with_clock(server, OBSERVED);
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
        let mut own_fills = 0;
        timeout(Duration::from_secs(3), async {
            while cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status()
                != OrderStatus::Canceled
            {
                let event = next(&mut rx).await;
                if let ExecutionEvent::Order(OrderEventAny::Filled(fill)) = &event {
                    assert!(!fill.reconciliation);
                    assert_eq!(fill.client_order_id, order.client_order_id());
                    own_fills += 1;
                }
                assert!(
                    !matches!(event, ExecutionEvent::Report(ExecutionReport::Fill(_))),
                    "own live fill bypasses reconciliation"
                );
                apply(&mut engine, &event);
            }
        })
        .await
        .unwrap();
        assert_eq!(own_fills, 1);
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
async fn invalid_market_is_local_and_allowance_failure_is_a_venue_rejection() {
    for (fee, allowance) in [(10_001, U256::MAX), (0, U256::ZERO)] {
        let server = server(vec![]).await;
        *server.state.market_fee.lock().unwrap() = fee;
        *server.state.admission_allowance.lock().unwrap() = allowance;
        let (mut client, mut rx, cache, _portfolio, order) = connected(&server, 1).await;
        if allowance.is_zero() {
            server
                .state
                .create_replies
                .lock()
                .unwrap()
                .push_back((400, 0, vec![]));
        }
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
        assert_eq!(
            server.state.creations.lock().unwrap().len(),
            usize::from(allowance.is_zero())
        );
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
async fn unsupported_stop_and_immediate_time_in_force_are_native_denied_before_post() {
    use nautilus_model::enums::TimeInForce;
    for (kind, tif) in [
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
        Decimal::ZERO // Native Cash RiskEngine owns in-flight funding.
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
async fn gtd_precision_and_quantity_failures_have_specific_native_denial_reasons() {
    use nautilus_model::enums::TimeInForce;
    for (price, qty, post_only, expiry, reason) in [
        ("0.001", "0.001", true, None, "quantity minimum"),
        ("0.0015", "0.01", true, None, "instrument increment"),
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
            .quote_quantity(side == OrderSide::Buy)
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
    let (mut client, mut rx, cache) = setup_client(&server, OBSERVED);
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

fn metadata_order(n: usize, index: u8) -> OrderAny {
    OrderTestBuilder::new(OrderType::Limit)
        .trader_id(trader_id())
        .client_order_id(ClientOrderId::new(format!("METADATA-{n}")))
        .instrument_id(instrument_id(index))
        .side(OrderSide::Buy)
        .quantity(Quantity::from(100))
        .price(Price::from("0.010"))
        .post_only(true)
        .build()
}

async fn metadata_outcome(
    client: &PredictExecutionClient,
    server: &Server,
    cache: &Rc<RefCell<Cache>>,
    rx: &mut mpsc::UnboundedReceiver<ExecutionEvent>,
    engine: &mut ExecutionEngine,
    order: &OrderAny,
    expected_posts: usize,
) {
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    client.submit_order(submit(order)).unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            while let Ok(event) = rx.try_recv() {
                apply(engine, &event);
            }
            if server.state.creations.lock().unwrap().len() == expected_posts
                || cache
                    .borrow()
                    .order(&order.client_order_id())
                    .unwrap()
                    .status()
                    == OrderStatus::Rejected
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn market_metadata_reuse_ignores_funding_reads_and_invalidates_at_its_boundaries() {
    for case in [
        "warm",
        "other_outcome",
        "rejected",
        "allowance",
        "balance",
        "rpc_failure",
    ] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache, _clock) = setup_with_metadata_ttl(&server, OBSERVED, 30);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        if case == "other_outcome" {
            cache
                .borrow_mut()
                .add_instrument(InstrumentAny::BinaryOption(instrument(2)))
                .unwrap();
        }
        connect_reported(&mut client, &mut rx).await;
        let mut engine = engine(&cache);
        if case == "rejected" {
            server
                .state
                .create_replies
                .lock()
                .unwrap()
                .push_back((400, 0, vec![]));
        }
        let first = metadata_order(0, 1);
        metadata_outcome(&client, &server, &cache, &mut rx, &mut engine, &first, 1).await;
        assert_eq!(*server.state.market_requests.lock().unwrap(), 1, "{case}");
        if case == "rejected" {
            timeout(Duration::from_secs(2), async {
                while cache
                    .borrow()
                    .order(&first.client_order_id())
                    .unwrap()
                    .status()
                    != OrderStatus::Rejected
                {
                    apply(&mut engine, &next(&mut rx).await);
                }
            })
            .await
            .unwrap();
        }
        if case == "allowance" {
            *server.state.admission_allowance.lock().unwrap() = U256::ZERO;
        }
        if case == "balance" {
            server.state.rpc_frame.lock().unwrap().0 = 0;
        }
        if case == "rpc_failure" {
            *server.state.rpc_failure.lock().unwrap() = true;
        }
        let second = metadata_order(1, if case == "other_outcome" { 2 } else { 1 });
        metadata_outcome(&client, &server, &cache, &mut rx, &mut engine, &second, 2).await;
        assert_eq!(server.state.creations.lock().unwrap().len(), 2, "{case}");
        assert_eq!(
            server.state.creations.lock().unwrap()[1]["data"]["order"]["feeRateBps"],
            "0",
            "{case}"
        );
        assert_eq!(*server.state.market_requests.lock().unwrap(), 1, "{case}");
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn warm_limit_submits_with_open_and_rpc_unavailable() {
    let server = server(vec![]).await;
    let (mut client, _rx, cache, _portfolio, warm) = connected(&server, 1).await;
    client.submit_order(submit(&warm)).unwrap();
    super::exit_orders::barrier("market warmed", || {
        server.state.creations.lock().unwrap().len() == 1
    })
    .await;
    let rpc = server.state.rpc_methods.lock().unwrap().len();
    let open = server.state.requests.lock().unwrap().len();
    let market = *server.state.market_requests.lock().unwrap();
    *server.state.rpc_failure.lock().unwrap() = true;
    *server.state.open_rpc_barrier.lock().unwrap() = Some(Arc::new(tokio::sync::Barrier::new(2)));
    let order = metadata_order(90, 1);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    client.submit_order(submit(&order)).unwrap();
    super::exit_orders::barrier("POST without any pre-order reads", || {
        server.state.creations.lock().unwrap().len() == 2
    })
    .await;
    assert_eq!(server.state.rpc_methods.lock().unwrap().len(), rpc);
    assert_eq!(server.state.requests.lock().unwrap().len(), open);
    assert_eq!(*server.state.market_requests.lock().unwrap(), market);
    assert_eq!(
        server.state.creations.lock().unwrap()[1]["data"]["order"]["nonce"],
        "7"
    );
    // No Submitted application: an account refresh cannot hide command-path IO.
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn cold_commands_reject_before_held_background_metadata_and_never_replay() {
    let server = server(vec![]).await;
    let ((mut client, mut rx, cache, clock), mut collector) = setup_metadata_probe(&server, 30);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.market_gate.lock().unwrap() = Some(gate.clone());
    clock.set_time(UnixNanos::from(OBSERVED + 30_000_000_000));
    let own = metadata_order(1, 1);
    let mut engine = engine(&cache);
    metadata_outcome(&client, &server, &cache, &mut rx, &mut engine, &own, 1).await;
    assert_eq!(
        cache
            .borrow()
            .order(&own.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Rejected
    );
    assert!(server.state.creations.lock().unwrap().is_empty());
    exit_orders::barrier("cold miss wakes background preparation", || {
        *server.state.market_requests.lock().unwrap() == 2
    })
    .await;
    *server.state.market_fee.lock().unwrap() = 200;
    // The response belongs to the old instrument slot and must be discarded.
    let definition = InstrumentAny::BinaryOption(instrument(1));
    cache
        .borrow_mut()
        .add_instrument(definition.clone())
        .unwrap();
    client.on_instrument(definition);
    gate.notify_one();
    metadata_collected(&mut collector, 3).await;
    assert!(server.state.creations.lock().unwrap().is_empty());
    let fresh = metadata_order(2, 1);
    metadata_outcome(&client, &server, &cache, &mut rx, &mut engine, &fresh, 1).await;
    assert_eq!(
        server.state.creations.lock().unwrap()[0]["data"]["order"]["feeRateBps"],
        "200"
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn zero_metadata_ttl_is_a_configuration_error() {
    let server = server(vec![]).await;
    assert!(
        try_setup(&server, OBSERVED, 0, 30, None)
            .unwrap_err()
            .to_string()
            .contains("market metadata TTL")
    );
}

#[tokio::test]
async fn concurrent_commands_do_not_wait_for_funding_or_a_prior_post() {
    let server = server(vec![]).await;
    let (mut client, _rx, cache, _portfolio, _) = connected(&server, 1).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *server.state.create_gate.lock().unwrap() = Some(gate.clone());
    *server.state.rpc_failure.lock().unwrap() = true;
    let rpc = server.state.rpc_methods.lock().unwrap().len();
    let open = server.state.requests.lock().unwrap().len();
    for order in [metadata_order(10, 1), metadata_order(11, 1)] {
        cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        client.submit_order(submit(&order)).unwrap();
    }
    super::exit_orders::barrier("both POSTs reach venue while replies are held", || {
        server.state.creations.lock().unwrap().len() == 2
    })
    .await;
    assert_eq!(server.state.rpc_methods.lock().unwrap().len(), rpc);
    assert_eq!(server.state.requests.lock().unwrap().len(), open);
    gate.notify_waiters();
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn concurrent_yes_no_submissions_share_one_prepared_market() {
    let server = server(vec![]).await;
    let ((mut client, mut rx, cache, _clock), mut collector) = setup_metadata_probe(&server, 30);
    let _portfolio = portfolio(&cache);
    for index in [1, 2] {
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(index)))
            .unwrap();
    }
    connect_reported(&mut client, &mut rx).await;
    metadata_collected(&mut collector, 1).await;
    for own in [metadata_order(20, 1), metadata_order(21, 2)] {
        cache
            .borrow_mut()
            .add_order(own.clone(), None, None, false)
            .unwrap();
        client.submit_order(submit(&own)).unwrap();
    }
    exit_orders::barrier("both prepared outcomes POST", || {
        server.state.creations.lock().unwrap().len() == 2
    })
    .await;
    assert_eq!(*server.state.market_requests.lock().unwrap(), 1);
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn restored_order_does_not_trigger_an_adapter_funds_audit() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache) = setup_client_at(&server, OBSERVED);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let mut restored = metadata_order(50, 1);
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    restored
        .apply(factory.generate_order_submitted(&restored, 1.into()))
        .unwrap();
    restored
        .apply(factory.generate_order_accepted(&restored, HASH.into(), 2.into(), 2.into()))
        .unwrap();
    cache
        .borrow_mut()
        .add_order(restored.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    // Direct adapter calls bypass Native risk deliberately. The adapter neither
    // re-audits funds nor changes the restored Native order before transmission.
    server.state.rpc_frame.lock().unwrap().0 = 1_500_000_000_000_000_000;
    let order = metadata_order(51, 1);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    client.submit_order(submit(&order)).unwrap();
    super::exit_orders::barrier("new order sent beside restored order", || {
        server.state.creations.lock().unwrap().len() == 1
    })
    .await;
    assert_eq!(
        cache
            .borrow()
            .order(&restored.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Accepted
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn funding_rejection_reaches_native_while_account_io_is_blocked() {
    for reason in ["insufficient balance", "insufficient allowance"] {
        let server = server(vec![]).await;
        let (mut client, mut rx, cache, _portfolio, order) = connected(&server, 1).await;
        server.state.rpc_latest_started.notified().await; // Drain initialization.
        *server.state.rpc_gate.lock().unwrap() = Some(Arc::new(tokio::sync::Notify::new()));
        *server.state.create_body.lock().unwrap() =
            Some(json!({"success":false,"code":400,"error":"bad_request","message":reason}));
        server
            .state
            .create_replies
            .lock()
            .unwrap()
            .push_back((400, 0, vec![]));
        client.submit_order(submit(&order)).unwrap();
        super::exit_orders::barrier("POST does not wait for a funds read", || {
            server.state.creations.lock().unwrap().len() == 1
        })
        .await;
        let mut engine = engine(&cache);
        timeout(Duration::from_secs(1), async {
            while cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status()
                != OrderStatus::Rejected
            {
                apply(&mut engine, &next(&mut rx).await);
            }
        })
        .await
        .unwrap();
        assert_eq!(server.state.creations.lock().unwrap().len(), 1);
        // A separate command uses the same market metadata even while the account
        // collector is stuck; neither an error callback nor the next send waits.
        let next_order = metadata_order(96, 1);
        cache
            .borrow_mut()
            .add_order(next_order.clone(), None, None, false)
            .unwrap();
        client.submit_order(submit(&next_order)).unwrap();
        super::exit_orders::barrier("next command is independent of the funds rejection", || {
            server.state.creations.lock().unwrap().len() == 2
        })
        .await;
        assert_eq!(*server.state.market_requests.lock().unwrap(), 1);
        client.disconnect().await.unwrap();
    }
}

#[tokio::test]
async fn prepared_signing_nonces_are_scoped_to_each_exchange() {
    let server = server(vec![]).await;
    *server.state.nonce_values.lock().unwrap() = [7, 8, 9, 10].map(U256::from);
    let ((mut client, mut rx, cache, _clock), mut collector) = setup_metadata_probe(&server, 30);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    let order = metadata_order(96, 1);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    client.submit_order(submit(&order)).unwrap();
    super::exit_orders::barrier("standard exchange POST", || {
        server.state.creations.lock().unwrap().len() == 1
    })
    .await;
    assert_eq!(
        server.state.creations.lock().unwrap()[0]["data"]["order"]["nonce"],
        "7"
    );
    // New market context selects a different prepared exchange, without a nonce RPC.
    *server.state.market_yield_bearing.lock().unwrap() = true;
    let definition = InstrumentAny::BinaryOption(instrument(1));
    cache
        .borrow_mut()
        .add_instrument(definition.clone())
        .unwrap();
    client.on_instrument(definition);
    metadata_collected(&mut collector, 2).await;
    let order = metadata_order(97, 1);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    client.submit_order(submit(&order)).unwrap();
    super::exit_orders::barrier("yield-bearing exchange POST", || {
        server.state.creations.lock().unwrap().len() == 2
    })
    .await;
    assert_eq!(
        server.state.creations.lock().unwrap()[1]["data"]["order"]["nonce"],
        "9"
    );
    // Consume both Submitted events only after both requests have arrived.
    let mut engine = engine(&cache);
    while let Ok(event) = rx.try_recv() {
        apply(&mut engine, &event);
    }
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn invalid_nonce_rejection_cannot_be_undone_by_an_older_periodic_collection() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache, _) = setup_with_account_interval(&server, OBSERVED, 30, 1);
    let _portfolio = portfolio(&cache);
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
        .unwrap();
    let order = metadata_order(95, 1);
    cache
        .borrow_mut()
        .add_order(order.clone(), None, None, false)
        .unwrap();
    connect_reported(&mut client, &mut rx).await;
    // Hold the final block check of a periodic snapshot: it has already read
    // nonce 7, but must not restore it after the venue has explicitly rejected it.
    let final_gate = Arc::new(tokio::sync::Notify::new());
    *server.state.rpc_final_gate.lock().unwrap() = Some(final_gate.clone());
    timeout(
        Duration::from_secs(2),
        server.state.rpc_final_started.notified(),
    )
    .await
    .unwrap();
    *server.state.create_body.lock().unwrap() =
        Some(json!({"success":false,"code":400,"error":"bad_request","message":"invalid nonce"}));
    server
        .state
        .create_replies
        .lock()
        .unwrap()
        .push_back((400, 0, vec![]));
    *server.state.nonce_values.lock().unwrap() = [17, 18, 19, 20].map(U256::from);
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
            apply(&mut engine, &next(&mut rx).await);
        }
    })
    .await
    .unwrap();
    *server.state.create_body.lock().unwrap() = None;
    let fresh_gate = Arc::new(tokio::sync::Notify::new());
    *server.state.rpc_final_gate.lock().unwrap() = Some(fresh_gate.clone());
    final_gate.notify_one();
    // Local projections can also publish cash while IO is held. Fence the
    // fresh nonce collection itself, rather than counting account events.
    timeout(
        Duration::from_secs(2),
        server.state.rpc_final_started.notified(),
    )
    .await
    .unwrap();
    while let Ok(event) = rx.try_recv() {
        apply(&mut engine, &event);
    }
    fresh_gate.notify_one();
    let _ = super::apply_account(&mut rx).await;
    let fresh = metadata_order(98, 1);
    cache
        .borrow_mut()
        .add_order(fresh.clone(), None, None, false)
        .unwrap();
    client.submit_order(submit(&fresh)).unwrap();
    super::exit_orders::barrier("new command uses refreshed nonce", || {
        server.state.creations.lock().unwrap().len() == 2
    })
    .await;
    {
        let bodies = server.state.creations.lock().unwrap();
        assert_eq!(bodies[0]["data"]["order"]["nonce"], "7");
        assert_eq!(bodies[1]["data"]["order"]["nonce"], "17");
    }
    assert_eq!(
        cache
            .borrow()
            .order(&order.client_order_id())
            .unwrap()
            .status(),
        OrderStatus::Rejected
    );
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn reconnect_prepares_nonces_when_the_new_transport_epoch_restarts_at_zero() {
    let server = server(vec![]).await;
    let (mut client, mut rx, _cache, _portfolio, order) = connected(&server, 1).await;
    client.disconnect().await.unwrap();
    *server.state.nonce_values.lock().unwrap() = [17, 18, 19, 20].map(U256::from);
    connect_reported(&mut client, &mut rx).await;
    client.submit_order(submit(&order)).unwrap();
    super::exit_orders::barrier("reconnected command uses fresh prepared nonce", || {
        server.state.creations.lock().unwrap().len() == 1
    })
    .await;
    assert_eq!(
        server.state.creations.lock().unwrap()[0]["data"]["order"]["nonce"],
        "17"
    );
    client.disconnect().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn accepted_lock_is_queued_on_native_application_before_background_tasks_run() {
    let server = server(vec![]).await;
    let (mut client, mut rx, cache, _portfolio, order) = connected(&server, 1).await;
    let mut engine = engine(&cache);
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    let reads = server.state.requests.lock().unwrap().len();
    let submitted = factory.generate_order_submitted(&order, OBSERVED.into());
    engine.process(&submitted);
    assert!(
        rx.try_recv().is_err(),
        "Submitted capacity belongs to Native risk, not reported locks"
    );
    let accepted = factory.generate_order_accepted(
        &cache.borrow().order(&order.client_order_id()).unwrap(),
        VenueOrderId::new(format!("0x{}", "11".repeat(32))),
        OBSERVED.into(),
        OBSERVED.into(),
    );
    engine.process(&accepted);
    // No await: a background projection cannot have run. Native's live sender
    // treats this AccountState as a child before the strategy's next command.
    let ExecutionEvent::Account(state) =
        rx.try_recv().expect("Native Accepted must queue its lock")
    else {
        panic!("expected reported lock projection");
    };
    assert!(state.balances[0].locked > Money::zero(Currency::USDT()));
    msgbus::send_account_state(MessagingSwitchboard::portfolio_update_account(), &state);
    assert_eq!(
        cache
            .borrow()
            .account(&account_id())
            .unwrap()
            .balance_locked(Some(Currency::USDT()))
            .unwrap(),
        state.balances[0].locked
    );
    assert_eq!(server.state.requests.lock().unwrap().len(), reads);
    assert!(server.state.creations.lock().unwrap().is_empty());
    client.disconnect().await.unwrap();
}
