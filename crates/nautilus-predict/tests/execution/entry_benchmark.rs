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

//! Setup and network I/O are outside the timed synchronous command-validation path.
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion};

use super::*;

fn retain_orders(cache: &Rc<RefCell<Cache>>, retained: usize) {
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    for n in 0..retained {
        let mut old = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .client_order_id(format!("RETAINED-{n}").into())
            .instrument_id(instrument_id(1))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .price(Price::from("0.010"))
            .build();
        old.apply(factory.generate_order_submitted(&old, 1.into()))
            .unwrap();
        let hash = VenueOrderId::new(format!("0x{:064x}", n + 1));
        old.apply(factory.generate_order_accepted(&old, hash, 2.into(), 2.into()))
            .unwrap();
        old.apply(factory.generate_order_canceled(&old, Some(hash), 3.into(), 3.into()))
            .unwrap();
        cache
            .borrow_mut()
            .add_order(old, None, None, false)
            .unwrap();
    }
}

pub(crate) fn run(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _runtime = runtime.enter();
    let mut group = c.benchmark_group("execution/submit_identity_validation");
    group.sample_size(30);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(1));
    for retained in [0, 1_000, 10_000] {
        let server = runtime.block_on(server(vec![]));
        let (mut client, mut rx, cache, _) =
            submissions::setup_client_with_clock(&server, OBSERVED);
        let _portfolio = portfolio(&cache);
        cache
            .borrow_mut()
            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
            .unwrap();
        retain_orders(&cache, retained);
        let target = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(trader_id())
            .client_order_id("BENCH-TARGET".into())
            .instrument_id(instrument_id(1))
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .price(Price::from("0.010"))
            .post_only(true)
            .build();
        cache
            .borrow_mut()
            .add_order(target.clone(), None, None, false)
            .unwrap();
        runtime.block_on(connect_reported(&mut client, &mut rx));
        let mut command = submissions::submit(&target);
        command.strategy_id = "DIFFERENT-001".into();
        assert!(client.submit_order(command.clone()).is_err());
        group.bench_with_input(BenchmarkId::from_parameter(retained), &retained, |b, _| {
            b.iter(|| {
                assert!(
                    black_box(&client)
                        .submit_order(black_box(command.clone()))
                        .is_err()
                )
            });
        });
        assert!(server.state.creations.lock().unwrap().is_empty());
        runtime.block_on(client.disconnect()).unwrap();
    }
    group.finish();
}

/// Actual warm LIMIT command -> loopback POST arrival. Each RPC request is delayed
/// by 5 ms to make network-stage serialization reproducible; this is not venue latency.
pub(crate) fn flow(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _runtime = runtime.enter();
    let mut group = c.benchmark_group("execution/warm_limit_batch_rpc_5ms");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));
    for batch in [1, 4, 8] {
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            b.iter_custom(|iterations| {
                let mut measured = Duration::ZERO;
                for _ in 0..iterations {
                    measured += runtime.block_on(async {
                        let server = server(vec![]).await;
                        let (mut client, _rx, cache, _portfolio, warm) =
                            submissions::connected(&server, 1).await;
                        client.submit_order(submissions::submit(&warm)).unwrap();
                        timeout(Duration::from_secs(2), async {
                            while server.state.creations.lock().unwrap().is_empty() {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .unwrap();
                        let mut orders = Vec::new();
                        for n in 0..batch {
                            let order = OrderTestBuilder::new(OrderType::Limit)
                                .trader_id(trader_id())
                                .client_order_id(format!("FLOW-{n}").into())
                                .instrument_id(instrument_id(1))
                                .side(OrderSide::Buy)
                                .quantity(Quantity::from(100))
                                .price(Price::from("0.010"))
                                .post_only(true)
                                .build();
                            cache
                                .borrow_mut()
                                .add_order(order.clone(), None, None, false)
                                .unwrap();
                            orders.push(order);
                        }
                        *server.state.rpc_delay_ms.lock().unwrap() = 5;
                        let rpc_before = server.state.rpc_methods.lock().unwrap().len();
                        let open_before = server.state.requests.lock().unwrap().len();
                        let started = std::time::Instant::now();
                        for order in &orders {
                            client.submit_order(submissions::submit(order)).unwrap();
                        }
                        timeout(Duration::from_secs(3), async {
                            while server.state.creations.lock().unwrap().len() < batch + 1 {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .unwrap();
                        let elapsed = started.elapsed();
                        assert_eq!(server.state.creations.lock().unwrap().len(), batch + 1);
                        assert_eq!(
                            server.state.rpc_methods.lock().unwrap().len() - rpc_before,
                            0
                        );
                        assert_eq!(server.state.requests.lock().unwrap().len() - open_before, 0);
                        client.disconnect().await.unwrap();
                        elapsed
                    });
                }
                measured
            });
        });
    }
    group.finish();
}

/// Successful command with real local REST/RPC/signing and retained history.
/// The setup is intentionally untimed and each sample uses a fresh client.
pub(crate) fn retained_flow(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _runtime = runtime.enter();
    let mut group = c.benchmark_group("execution/warm_limit_retained_rpc_0ms");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(50));
    group.measurement_time(Duration::from_millis(100));
    for retained in [0, 10_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(retained),
            &retained,
            |b, &retained| {
                b.iter_custom(|iterations| {
                    let mut measured = Duration::ZERO;
                    for _ in 0..iterations {
                        measured += runtime.block_on(async {
                            let server = server(vec![]).await;
                            let (mut client, mut rx, cache, _) =
                                submissions::setup_client_with_clock(&server, OBSERVED);
                            let _portfolio = portfolio(&cache);
                            cache
                                .borrow_mut()
                                .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
                                .unwrap();
                            retain_orders(&cache, retained);
                            let warm = OrderTestBuilder::new(OrderType::Limit)
                                .trader_id(trader_id())
                                .client_order_id("WARM".into())
                                .instrument_id(instrument_id(1))
                                .side(OrderSide::Buy)
                                .quantity(Quantity::from(100))
                                .price(Price::from("0.010"))
                                .post_only(true)
                                .build();
                            cache
                                .borrow_mut()
                                .add_order(warm.clone(), None, None, false)
                                .unwrap();
                            connect_reported(&mut client, &mut rx).await;
                            client.submit_order(submissions::submit(&warm)).unwrap();
                            timeout(Duration::from_secs(2), async {
                                while server.state.creations.lock().unwrap().is_empty() {
                                    tokio::task::yield_now().await;
                                }
                            })
                            .await
                            .unwrap();
                            let order = OrderTestBuilder::new(OrderType::Limit)
                                .trader_id(trader_id())
                                .client_order_id("TARGET".into())
                                .instrument_id(instrument_id(1))
                                .side(OrderSide::Buy)
                                .quantity(Quantity::from(100))
                                .price(Price::from("0.010"))
                                .post_only(true)
                                .build();
                            cache
                                .borrow_mut()
                                .add_order(order.clone(), None, None, false)
                                .unwrap();
                            let rpc_before = server.state.rpc_methods.lock().unwrap().len();
                            let open_before = server.state.requests.lock().unwrap().len();
                            let started = std::time::Instant::now();
                            client.submit_order(submissions::submit(&order)).unwrap();
                            timeout(Duration::from_secs(2), async {
                                while server.state.creations.lock().unwrap().len() != 2 {
                                    tokio::task::yield_now().await;
                                }
                            })
                            .await
                            .unwrap();
                            let elapsed = started.elapsed();
                            assert_eq!(
                                server.state.rpc_methods.lock().unwrap().len() - rpc_before,
                                0
                            );
                            assert_eq!(
                                server.state.requests.lock().unwrap().len() - open_before,
                                0
                            );
                            client.disconnect().await.unwrap();
                            elapsed
                        });
                    }
                    measured
                });
            },
        );
    }
    group.finish();
}

/// Known-ID removal -> Native Canceled -> subscriber-visible reported free cash.
/// A 5 ms delay on each RPC makes the former remote dependency measurable.
pub(crate) fn cancel_feedback(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _runtime = runtime.enter();
    let expected_reads: usize = std::env::var("PREDICT_BENCH_EXPECT_ACCOUNT_READS")
        .unwrap_or_else(|_| "0".into())
        .parse()
        .unwrap();
    let mut group = c.benchmark_group("execution/cancel_to_reported_cash_rpc_5ms");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(200));
    group.bench_function("known_id", |b| {
        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iterations {
                elapsed += runtime.block_on(async {
                    let server = server(vec![]).await;
                    server
                        .state
                        .open_orders
                        .lock()
                        .unwrap()
                        .push(rest_order_data("OPEN", 1, OrderSide::Buy));
                    server.state.remove_replies.lock().unwrap().push_back((
                        200,
                        json!({"success":true,"removed":["12345"],"noop":[]}),
                        0,
                    ));
                    let (mut client, mut rx, cache) = reported_client_with_removal(&server, true);
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
                    *server.state.rpc_delay_ms.lock().unwrap() = 5;
                    let rpc_before = server.state.rpc_methods.lock().unwrap().len();
                    let open_before = server.state.requests.lock().unwrap().len();
                    let mut engine = cancellations::engine(&cache);
                    let start = std::time::Instant::now();
                    client.cancel_order(cancellations::cancel(&order)).unwrap();
                    let ExecutionEvent::Order(event) = next(&mut rx).await else {
                        panic!("native cancel");
                    };
                    engine.process(&event);
                    let state = apply_account(&mut rx).await;
                    let elapsed = start.elapsed();
                    assert_eq!(state.balances[0].locked, Money::zero(Currency::USDT()));
                    assert_eq!(state.balances[0].free, Money::from("1000 USDT"));
                    assert_eq!(server.state.removals.lock().unwrap().len(), 1);
                    assert!(server.state.order_requests.lock().unwrap().is_empty());
                    assert_eq!(
                        server.state.rpc_methods.lock().unwrap().len() - rpc_before,
                        3 * expected_reads
                    );
                    assert_eq!(
                        server.state.requests.lock().unwrap().len() - open_before,
                        expected_reads
                    );
                    client.disconnect().await.unwrap();
                    elapsed
                });
            }
            elapsed
        })
    });
    group.finish();
}

/// Shared parsing precedes both paths. Measure normalized wallet status -> actual
/// Native msgbus callback, including event construction on each path. No network IO.
pub(crate) fn wallet_dispatch(c: &mut Criterion) {
    use std::cell::Cell;

    use nautilus_common::msgbus::{TypedHandler, switchboard};
    use nautilus_model::events::OrderEventAny;

    let own = order(1);
    let wire: PredictWalletEvent = serde_json::from_value(event("orderCancelled", 1)).unwrap();
    let report = parse_wallet_report(
        &wire,
        &own,
        &instrument(1),
        account_id(),
        WALLET.parse().unwrap(),
        OBSERVED.into(),
    )
    .unwrap()
    .unwrap();
    let ExecutionReport::Order(status) = &report else {
        unreachable!()
    };
    let factory = OrderEventFactory::new(trader_id(), account_id(), AccountType::Cash, None);
    let mut group = c.benchmark_group("execution/wallet_cancel_dispatch");
    group.sample_size(50);
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_millis(500));
    for native in [false, true] {
        group.bench_function(
            if native {
                "native_event"
            } else {
                "reconciliation"
            },
            |b| {
                b.iter_custom(|iterations| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        msgbus::set_message_bus(Rc::new(RefCell::new(MessageBus::new(
                            trader_id(),
                            UUID4::new(),
                            None,
                            None,
                        ))));
                        let cache = Rc::new(RefCell::new(Cache::default()));
                        cache
                            .borrow_mut()
                            .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
                            .unwrap();
                        cache
                            .borrow_mut()
                            .add_order(own.clone(), None, None, false)
                            .unwrap();
                        seed_account(&cache);
                        let mut engine = cancellations::engine(&cache);
                        let callback = Rc::new(Cell::new(None));
                        let handler = TypedHandler::from({
                            let callback = callback.clone();
                            let cache = cache.clone();
                            move |event: &OrderEventAny| {
                                let at = std::time::Instant::now();
                                assert!(matches!(event, OrderEventAny::Canceled(_)));
                                assert_eq!(
                                    cache
                                        .borrow()
                                        .order(&event.client_order_id())
                                        .unwrap()
                                        .status(),
                                    OrderStatus::Canceled
                                );
                                assert!(callback.replace(Some(at)).is_none());
                            }
                        });
                        let topic = switchboard::get_event_order_topic(own.strategy_id());
                        msgbus::subscribe_order_events(topic.into(), handler.clone(), None);
                        let start = std::time::Instant::now();
                        if native {
                            let event = factory.generate_order_canceled(
                                black_box(&own),
                                Some(status.venue_order_id),
                                status.ts_last,
                                status.ts_init,
                            );
                            engine.process(black_box(&event));
                        } else {
                            engine.reconcile_execution_report(black_box(&report));
                        }
                        elapsed += callback
                            .get()
                            .expect("Native must deliver cancellation")
                            .duration_since(start);
                        msgbus::unsubscribe_order_events(topic.into(), &handler);
                    }
                    elapsed
                });
            },
        );
    }
    group.finish();
}

/// Priced MARKET command -> loopback create arrival. Connection and preparation
/// are untimed. Both directions require exactly one POST and zero preflight reads.
pub(crate) fn market_preparation(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _runtime = runtime.enter();
    let mut group = c.benchmark_group("execution/market_to_post_strategy_price");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_millis(500));
    for side in [OrderSide::Sell, OrderSide::Buy] {
        group.bench_function(
            if side == OrderSide::Buy {
                "buy"
            } else {
                "sell"
            },
            |b| {
                b.iter_custom(|iterations| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        elapsed += runtime.block_on(async {
                            let server = server(vec![]).await;
                            let (mut client, mut rx, cache) =
                                submissions::setup_client(&server, OBSERVED);
                            let _portfolio = portfolio(&cache);
                            cache
                                .borrow_mut()
                                .add_instrument(InstrumentAny::BinaryOption(instrument(1)))
                                .unwrap();
                            connect_reported(&mut client, &mut rx).await;
                            let rpc = server.state.rpc_methods.lock().unwrap().len();
                            let open = server.state.requests.lock().unwrap().len();
                            let metadata = *server.state.market_requests.lock().unwrap();
                            let own = market_taker::market(side, 1, 1, 100);
                            cache
                                .borrow_mut()
                                .add_order(own.clone(), None, None, false)
                                .unwrap();
                            let command = market_preparation::priced(&own, json!("0.020"), 100);
                            let start = std::time::Instant::now();
                            client.submit_order(command).unwrap();
                            exit_orders::barrier("benchmark priced MARKET arrival", || {
                                !server.state.creations.lock().unwrap().is_empty()
                            })
                            .await;
                            let elapsed = start.elapsed();
                            let body = server.state.creations.lock().unwrap()[0].clone();
                            assert_eq!(
                                body["data"]["order"]["makerAmount"],
                                if side == OrderSide::Buy {
                                    "2020000000000000000"
                                } else {
                                    "100000000000000000000"
                                }
                            );
                            assert_eq!(
                                body["data"]["order"]["takerAmount"],
                                if side == OrderSide::Buy {
                                    "100000000000000000000"
                                } else {
                                    "1980000000000000000"
                                }
                            );
                            assert_eq!(*server.state.book_requests.lock().unwrap(), 0);
                            assert_eq!(*server.state.market_requests.lock().unwrap(), metadata);
                            assert_eq!(server.state.rpc_methods.lock().unwrap().len(), rpc);
                            assert_eq!(server.state.requests.lock().unwrap().len(), open);
                            assert_eq!(server.state.creations.lock().unwrap().len(), 1);
                            client.disconnect().await.unwrap();
                            elapsed
                        });
                    }
                    elapsed
                });
            },
        );
    }
    group.finish();
}
