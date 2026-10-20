// -------------------------------------------------------------------------------------------------
// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
// See https://www.gnu.org/licenses/LICENSE-3.0.html for license details.
// -------------------------------------------------------------------------------------------------
//! Maker-only LiveNode regressions using retained mainnet wallet shapes and account evidence.
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_predict::{PredictDataClientConfig, PredictSubmissionConfig};

use super::{
    live_node::{
        captured_resting_public_server, rest_row,
        strategy::{CalibrationInputs, CalibrationStrategy, Observations, Phase, StrategySettings},
    },
    node, *,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    CapturedDuplicates,
    GtdDuplicates,
    HttpRejected,
    ExpiryRejected,
    WalletRejected,
    CancelRejected,
}

#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_captured_maker_duplicates_preserve_orders_and_account() {
    run(Scenario::CapturedDuplicates).await;
}
#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_gtd_maker_cancel_replace_preserves_expiration() {
    run(Scenario::GtdDuplicates).await;
}
#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_http_rejection_unlocks_account_before_stopping() {
    run(Scenario::HttpRejected).await;
}
#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_captured_expiry_rejection_preserves_reason_and_unlocks_account() {
    run(Scenario::ExpiryRejected).await;
}
#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_wallet_post_only_rejection_preserves_reason_and_account() {
    run(Scenario::WalletRejected).await;
}
#[tokio::test(flavor = "current_thread")]
async fn rust_strategy_livenode_cancel_rejection_retains_original_order_and_reservation() {
    run(Scenario::CancelRejected).await;
}

async fn run(scenario: Scenario) {
    let recorded: Value = serde_json::from_str(include_str!(
        "../fixtures/private_api/live_resting_fixed_20261015/calibration.json"
    ))
    .unwrap();
    let wallet = recorded["wallet"]["events"].as_array().unwrap();
    assert_eq!(wallet.len(), 4);
    let cash: u128 = recorded["before"]["cashWei"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let total = Decimal::from_str_exact("48.37915").unwrap();
    let private = server(vec![]).await;
    if scenario == Scenario::ExpiryRejected {
        let captured: Value = serde_json::from_str(include_str!(
            "../fixtures/private_api/order_create_expiry_error_20261015.json"
        ))
        .unwrap();
        *private.state.create_body.lock().unwrap() =
            Some(captured["response"]["errorBody"].clone());
    }
    let http_rejected = matches!(scenario, Scenario::HttpRejected | Scenario::ExpiryRejected);
    let rejection_gate = Arc::new(tokio::sync::Notify::new());
    if http_rejected {
        *private.state.create_gate.lock().unwrap() = Some(rejection_gate.clone());
    }
    *private.state.market_fee.lock().unwrap() = 200;
    *private.state.rpc_frame.lock().unwrap() = (
        cash,
        1,
        get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000,
    );
    let signer = Arc::new(
        nautilus_predict::signing::PredictAuthSigner::new(
            submissions::PUBLIC_KEY.into(),
            PredictEnvironment::BnbMainnet,
            Some(WALLET),
        )
        .unwrap(),
    );
    let mut execution = config(&private);
    execution.account_state_mode = PredictAccountStateMode::Reported;
    execution.chain = Some(PredictChainClientConfig::new(
        PredictEnvironment::BnbMainnet,
        format!("{}/rpc", private.http).into(),
    ));
    execution.orderbook_removal_enabled = true;
    execution.submission = Some(PredictSubmissionConfig {
        signer,
        max_order_usdt: Decimal::ONE,
        max_total_usdt: Decimal::from(2),
        require_post_only: true,
        market_slippage_bps: 100,
    });
    let (http, ws, public_task) = captured_resting_public_server().await;
    let mut live = node::build_node(
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
        execution,
    )
    .unwrap();
    let handle = live.handle();
    let observed = Rc::new(RefCell::new(Observations::default()));
    live.add_strategy(
        CalibrationStrategy::new(
            CalibrationInputs {
                account_id: account_id(),
                settings: StrategySettings {
                    phase: Phase::Resting,
                    run_secs: 10,
                    stale_after_ms: 2000,
                    minimum_rebound_ticks: 1,
                },
                market_id: 42,
                instrument_id: instrument_id(2),
                outcome: 2,
                quantity: Quantity::from(100),
                maximum_price: Decimal::new(1, 2),
                maximum_order_cost: Decimal::ONE,
                maximum_total_cost: Decimal::from(2),
                execution_enabled: true,
                fee_rate_bps: 200,
            },
            observed.clone(),
            handle.clone(),
        )
        .with_resting_price(Decimal::new(1, 2))
        .with_resting_time_in_force(
            if matches!(scenario, Scenario::GtdDuplicates | Scenario::ExpiryRejected) {
                nautilus_model::enums::TimeInForce::Gtd
            } else {
                nautilus_model::enums::TimeInForce::Gtc
            },
        ),
    )
    .unwrap();
    let duplicates = matches!(
        scenario,
        Scenario::CapturedDuplicates | Scenario::GtdDuplicates
    );
    let count = if duplicates { 2 } else { 1 };
    for index in 0..count {
        private
            .state
            .create_order_ids
            .lock()
            .unwrap()
            .push_back(wallet[index * 2]["orderId"].as_str().unwrap().to_owned());
        private.state.create_replies.lock().unwrap().push_back((
            if http_rejected { 400 } else { 201 },
            0,
            vec![],
        ));
        private.state.remove_replies.lock().unwrap().push_back((
            if scenario==Scenario::CancelRejected {400} else {200},
            if scenario==Scenario::CancelRejected {json!({"success":false,"code":400,"error":"bad_request","message":"cancel rejected"})}
            else {json!({"success":true,"removed":[wallet[index*2]["orderId"]],"noop":[]})},50,
        ));
    }
    let driver = async {
        for index in 0..count {
            timeout(Duration::from_secs(6), async {
                while private.state.creations.lock().unwrap().len() <= index {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let body = private.state.creations.lock().unwrap()[index].clone();
            assert_eq!(body["data"]["strategy"], "LIMIT");
            assert_eq!(body["data"]["isPostOnly"], true);
            assert_eq!(body["data"]["order"]["makerAmount"], "1000000000000000000");
            let expiration = body["data"]["order"]["expiration"].as_u64().unwrap();
            let now = get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000_000;
            if matches!(scenario, Scenario::GtdDuplicates | Scenario::ExpiryRejected) {
                assert!((now + 280..=now + 300).contains(&expiration));
            } else {
                assert_eq!(expiration, 4_102_444_800);
            }
            if http_rejected {
                timeout(Duration::from_secs(3), async {
                    while observed.borrow().last_cash_locked != Some(Decimal::ONE) {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                rejection_gate.notify_one();
                return;
            }
            let bind = |source: &Value| {
                // Recorded order terms/outcome remain unchanged; only loopback identity,
                // signed hash, market and clock bindings differ from the immutable recording.
                let mut event = source.clone();
                event["walletAddress"] = json!(WALLET);
                event["orderHash"] = body["data"]["order"]["hash"].clone();
                event["details"]["marketId"] = json!(42);
                event["timestamp"] =
                    json!(get_atomic_clock_realtime().get_time_ns().as_u64() / 1_000_000);
                event
            };
            let mut accepted = bind(&wallet[index * 2]);
            assert_eq!(accepted["details"]["outcomeIndex"], 2);
            if scenario == Scenario::WalletRejected {
                timeout(Duration::from_secs(3), async {
                    while observed.borrow().last_cash_locked != Some(Decimal::ONE) {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                // Synthetic rejection branch on the captured envelope, not a real rejection.
                accepted["type"] = json!("orderNotAccepted");
                accepted["reason"] = json!("rejectedPostOnly");
                private.wallet(accepted);
                return;
            }
            let mut row = rest_row(&body, "OPEN");
            row["id"] = wallet[index * 2]["orderId"].clone();
            private
                .state
                .orders
                .lock()
                .unwrap()
                .push_back(json!({"success":true,"data":row}));
            private.wallet(accepted.clone());
            if duplicates {
                private.wallet(accepted);
            }
            timeout(Duration::from_secs(6), async {
                while private.state.removals.lock().unwrap().len() <= index {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if scenario == Scenario::CancelRejected {
                timeout(Duration::from_secs(3), async {
                    while observed.borrow().cancel_rejected == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                handle.stop();
                return;
            }
            // Race a real cancellation shape and its duplicate with the HTTP removal reply.
            let canceled = bind(&wallet[index * 2 + 1]);
            private.wallet(canceled.clone());
            private.wallet(canceled);
        }
    };
    let deadline = async {
        if timeout(Duration::from_secs(9), async {
            while !handle.should_stop() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_err()
        {
            handle.stop();
        }
    };
    let (result, (), ()) = tokio::join!(live.run(), driver, deadline);
    result.unwrap();
    let observed = observed.borrow();
    assert!(observed.snapshots > 0);
    assert_eq!(observed.submitted, count);
    assert_eq!(observed.fills, 0);
    assert_eq!(observed.position_events, 0);
    assert_eq!(observed.last_cash_total, Some(total));
    assert_eq!(private.state.creations.lock().unwrap().len(), count);
    let cache = live.kernel().cache.borrow();
    assert!(cache.positions(None, None, None, None, None).is_empty());
    let orders = cache.orders(None, None, None, None, None);
    assert_eq!(orders.len(), count);
    for order in &orders {
        assert_eq!(order.filled_qty().as_decimal(), Decimal::ZERO);
        assert!(order.is_post_only());
        if scenario == Scenario::ExpiryRejected {
            // Replay the captured HTTP error on an otherwise admissible GTD request;
            // the original short expiration is now denied locally before HTTP.
            assert_eq!(
                order.time_in_force(),
                nautilus_model::enums::TimeInForce::Gtd
            );
            assert!(order.expire_time().is_some());
        }
        if scenario == Scenario::GtdDuplicates {
            assert_eq!(
                order.time_in_force(),
                nautilus_model::enums::TimeInForce::Gtd
            );
            let bodies = private.state.creations.lock().unwrap();
            let venue_order_id = order.venue_order_id().unwrap();
            let body = bodies
                .iter()
                .find(|body| {
                    body["data"]["order"]["hash"].as_str() == Some(venue_order_id.as_str())
                })
                .unwrap();
            assert_eq!(
                order.expire_time().unwrap().as_u64(),
                body["data"]["order"]["expiration"].as_u64().unwrap() * 1_000_000_000
            );
        }
    }
    if duplicates {
        assert_eq!(observed.accepted, 2);
        assert_eq!(observed.modify_rejected, 2);
        assert_eq!(observed.modify_terms_preserved, 2);
        assert_eq!(observed.canceled, 2);
        assert_eq!(observed.rejected, 0);
        assert_eq!(private.state.removals.lock().unwrap().len(), 2);
        assert!(orders.iter().all(|o| o.status() == OrderStatus::Canceled));
        assert_eq!(observed.last_cash_locked, Some(Decimal::ZERO));
        assert!(observed.recorded_feedback.iter().any(|row| {
            row["type"] == "AccountStateApplied"
                && row["balances"][0]["locked"]
                    .as_str()
                    .is_some_and(|v| Decimal::from_str_exact(v).unwrap() == Decimal::ONE)
        }));
    } else if scenario == Scenario::CancelRejected {
        assert_eq!(observed.cancel_rejected, 1);
        assert_eq!(observed.accepted, 1);
        assert_eq!(observed.canceled, 0);
        assert_eq!(orders[0].status(), OrderStatus::Accepted);
        assert_eq!(orders[0].price(), orders[0].init_event().price);
        assert_eq!(private.state.removals.lock().unwrap().len(), 1);
        assert_eq!(
            cache
                .account(&account_id())
                .unwrap()
                .balance_locked(Some(Currency::USDT()))
                .unwrap()
                .as_decimal(),
            Decimal::ONE
        );
    } else {
        assert_eq!(observed.rejected, 1);
        assert_eq!(observed.accepted, 0);
        assert_eq!(observed.canceled, 0);
        assert!(private.state.removals.lock().unwrap().is_empty());
        assert_eq!(orders[0].status(), OrderStatus::Rejected);
        let event = observed
            .recorded_feedback
            .iter()
            .find(|row| row["type"] == "Rejected")
            .unwrap();
        if scenario == Scenario::WalletRejected {
            assert!(
                event["rejectionReason"]
                    .as_str()
                    .unwrap()
                    .contains("post-only")
            );
            assert_eq!(event["duePostOnly"], true);
        } else {
            assert!(event["rejectionReason"].as_str().unwrap().contains(
                if scenario == Scenario::ExpiryRejected {
                    "ExpiryTooSoon"
                } else {
                    "InvalidSignature"
                }
            ));
        }
        assert_eq!(observed.last_cash_locked, Some(Decimal::ZERO));
        assert_eq!(
            cache
                .account(&account_id())
                .unwrap()
                .balance_locked(Some(Currency::USDT()))
                .unwrap()
                .as_decimal(),
            Decimal::ZERO
        );
    }
    public_task.abort();
}

#[test]
fn wallet_rejection_and_cancellation_reasons_reach_native_report_fields() {
    use nautilus_model::enums::OrderStatus;
    for (kind, reason, expected_status, expected_reason) in [
        (
            "orderNotAccepted",
            "noMarketMatch",
            OrderStatus::Rejected,
            "Predict no market match",
        ),
        (
            "orderNotAccepted",
            "rejectedDuplicate",
            OrderStatus::Rejected,
            "Predict duplicate order rejected",
        ),
        (
            "orderNotAccepted",
            "rejectedPostOnly",
            OrderStatus::Rejected,
            "Predict post-only order rejected",
        ),
        (
            "orderNotAccepted",
            "sensitive-unknown-value",
            OrderStatus::Rejected,
            "Predict unknown order rejection",
        ),
        (
            "orderCancelled",
            "selfTradePrevention",
            OrderStatus::Canceled,
            "Predict self-trade prevention",
        ),
        (
            "orderCancelled",
            "sensitive-unknown-value",
            OrderStatus::Canceled,
            "Predict unknown cancellation reason",
        ),
    ] {
        let mut payload = event(kind, 1);
        payload["reason"] = json!(reason);
        let event: PredictWalletEvent = serde_json::from_value(payload).unwrap();
        let report = parse_wallet_report(
            &event,
            &order(1),
            &instrument(1),
            account_id(),
            WALLET.parse().unwrap(),
            UnixNanos::from(OBSERVED),
        )
        .unwrap()
        .unwrap();
        let ExecutionReport::Order(report) = report else {
            panic!("expected lifecycle report")
        };
        assert_eq!(report.order_status, expected_status);
        assert_eq!(report.cancel_reason.as_deref(), Some(expected_reason));
        assert!(!format!("{report:?}").contains("sensitive-unknown-value"));
    }
}
