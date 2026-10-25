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

//! Applied Native account projections, not an independent funds model.
use super::*;
use crate::{
    common::{
        PREDICT_VENUE, contracts::collateral_contract, credential::PredictAuthToken,
        environment::PredictEnvironment,
    },
    config::{
        PredictAccountStateMode, PredictChainClientConfig, PredictPrivateClientConfig,
        PredictWalletWebSocketConfig,
    },
    execution::{PredictExecutionClient, context::WorkerOrder},
    http::{
        PredictPrivateHttpClient,
        models::{PredictMarket, PredictResponse},
    },
    providers::instruments_from_market,
};
use alloy::primitives::U256;
use nautilus_common::{
    cache::Cache,
    live::runner::replace_exec_event_sender,
    msgbus::{self, MessageBus},
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    enums::{OmsType, OrderSide, OrderType},
    events::{AccountState, OrderEventAny},
    identifiers::{AccountId, ClientId, TraderId},
    instruments::Instrument,
    orders::{Order, OrderAny, builder::OrderTestBuilder},
    types::Quantity,
};
use rust_decimal::Decimal;
use std::{cell::RefCell, rc::Rc};

struct Fixture {
    client: PredictExecutionClient,
    cache: Rc<RefCell<Cache>>,
    rx: tokio::sync::mpsc::UnboundedReceiver<nautilus_common::messages::ExecutionEvent>,
    factory: OrderEventFactory,
    instrument: InstrumentAny,
}
impl Fixture {
    fn new(total: u64, requested: u64) -> Self {
        let session = PredictPrivateHttpClient::new(PredictPrivateClientConfig {
            api_key: Some("offline".into()),
            ..Default::default()
        })
        .unwrap()
        .authenticated(
            PredictAuthToken::new("offline".into()).unwrap(),
            "0x0000000000000000000000000000000000000022"
                .parse()
                .unwrap(),
        )
        .unwrap();
        let account_id = AccountId::from("PREDICT-projection");
        let mut config = PredictExecutionClientConfig::new(
            account_id,
            session,
            PredictWalletWebSocketConfig {
                api_key: Some("offline".into()),
                ..Default::default()
            },
        );
        config.account_state_mode = PredictAccountStateMode::Reported;
        config.chain = Some(PredictChainClientConfig::new(
            PredictEnvironment::BnbMainnet,
            "http://127.0.0.1:1".into(),
        ));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let core = ExecutionClientCore::new(
            TraderId::from("TESTER-001"),
            ClientId::from("PREDICT"),
            *PREDICT_VENUE,
            OmsType::Netting,
            account_id,
            AccountType::Cash,
            None,
            cache.clone(),
        );
        msgbus::set_message_bus(Rc::new(RefCell::new(MessageBus::new(
            core.trader_id,
            UUID4::new(),
            None,
            None,
        ))));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        replace_exec_event_sender(tx);
        let mut client = PredictExecutionClient::new(core, config).unwrap();
        let auth = AuthTracker::new();
        auth.succeed();
        client.wallet_auth = Some(auth);
        client.wallet_epoch = Some(Arc::new(AtomicU64::new(0)));
        let mut response: PredictResponse<PredictMarket> =
            serde_json::from_str(include_str!("../../test_data/market_2859346.json")).unwrap();
        response.data.id = 42;
        let (_, definitions) = instruments_from_market(&response.data, 1.into()).unwrap();
        let instrument = definitions[0].clone();
        cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        let snapshot = crate::blockchain::models::PredictBalanceSnapshot {
            environment: PredictEnvironment::BnbMainnet,
            account_address: client.config.session.account_address(),
            collateral: collateral_contract(PredictEnvironment::BnbMainnet),
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            block_timestamp: 1.into(),
            decimals: 18,
            balance_wei: U256::from(total) * U256::from(10u64.pow(18)),
            ts_observed: 1.into(),
        };
        let factory =
            OrderEventFactory::new(client.core.trader_id, account_id, AccountType::Cash, None);
        let info = Params::new(
            [(
                "predict_requested_lock".into(),
                serde_json::json!(requested.to_string()),
            )]
            .into(),
        );
        let locked = Money::from_decimal_exact(Decimal::from(requested), Currency::USDT()).unwrap();
        let event = factory.generate_account_state(
            vec![reported_balance(&snapshot, locked).unwrap()],
            vec![],
            true,
            1.into(),
            1.into(),
            Some(info),
        );
        let event_id = event.event_id;
        cache
            .borrow_mut()
            .add_account(AccountAny::Cash(CashAccount::new(event, false, false)))
            .unwrap();
        {
            let mut state = client.account_sync.state.lock().unwrap();
            state.confirmed = Some(Arc::new(ConfirmedAccount {
                epoch: 0,
                rows: Arc::default(),
                balance: snapshot,
            }));
            state.projected_revision = Some(0);
            state.last_event = Some(event_id);
        }
        FULL_ORDER_READS.with(|n| n.set(0));
        Self {
            client,
            cache,
            rx,
            factory,
            instrument,
        }
    }
    fn order(&self, id: &str, price: &str, accepted: bool) -> OrderAny {
        let mut order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(self.client.core.trader_id)
            .client_order_id(id.into())
            .instrument_id(self.instrument.id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .price(price.parse().unwrap())
            .build();
        order
            .apply(self.factory.generate_order_submitted(&order, 1.into()))
            .unwrap();
        if accepted {
            order
                .apply(
                    self.factory
                        .generate_order_accepted(&order, hash(id), 2.into(), 2.into()),
                )
                .unwrap();
        }
        order
    }
    fn watch(&mut self, orders: &[OrderAny]) {
        for order in orders {
            self.cache
                .borrow_mut()
                .add_order(order.clone(), None, None, false)
                .unwrap();
        }
        self.cache.borrow_mut().build_index();
        self.client.prepare_lookup().unwrap();
    }
    fn apply(&self, event: &OrderEventAny) {
        self.cache.borrow_mut().update_order(event).unwrap();
        msgbus::publish_order_event("events.order.TESTER".into(), event);
    }
    fn account(&mut self) -> AccountState {
        let nautilus_common::messages::ExecutionEvent::Account(event) = self
            .rx
            .try_recv()
            .expect("Native projection must be queued without async work")
        else {
            panic!("account event")
        };
        event
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.client.clear_observer();
    }
}
fn hash(id: &str) -> VenueOrderId {
    VenueOrderId::new(format!("0x{:064x}", if id == "TARGET" { 1 } else { 2 }))
}

#[test]
fn accepted_uses_native_submitted_view_not_signing_context_and_never_scans_other_orders() {
    let mut f = Fixture::new(2, 0);
    let submitted = f.order("TARGET", "0.010", false);
    // Hash registration captures the original Initialized command; Native has
    // already applied Submitted. The two views share identity, not scalar age.
    let initialized = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(f.client.core.trader_id)
        .client_order_id(submitted.client_order_id())
        .instrument_id(f.instrument.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from(100))
        .price("0.010".parse().unwrap())
        .build();
    f.watch(std::slice::from_ref(&submitted));
    let context = Arc::new(WorkerOrder::capture(&initialized));
    context.record_acceptance(2.into());
    f.client
        .orders
        .lock()
        .unwrap()
        .insert(hash("TARGET"), context);
    f.client.pending_submissions.lock().unwrap().entries.insert(
        submitted.client_order_id(),
        crate::execution::orders::Submission {
            venue_id: Some(hash("TARGET")),
            wallet_epoch: Some(0),
            market_buy_collateral: None,
        },
    );
    f.apply(
        &f.factory
            .generate_order_accepted(&submitted, hash("TARGET"), 3.into(), 3.into()),
    );
    let event = f.account();
    assert_eq!(event.balances[0].locked, Money::from("1 USDT"));
    FULL_ORDER_READS
        .with(|n| assert_eq!(n.get(), 0, "Accepted must not scan the active portfolio"));
    assert_eq!(
        f.client.orders.lock().unwrap()[&hash("TARGET")].queued_acceptance(),
        Some(2.into())
    );
}

#[test]
fn confirmed_cancel_releases_from_uncapped_native_request_not_clamped_balance() {
    let mut f = Fixture::new(100, 150);
    let target = f.order("TARGET", "0.700", true);
    let other = f.order("OTHER", "0.800", true);
    f.watch(&[target.clone(), other]);
    f.client
        .account_sync
        .confirm_terminal(hash("TARGET"), Some(target.client_order_id()));
    f.apply(
        &f.factory
            .generate_order_canceled(&target, Some(hash("TARGET")), 3.into(), 3.into()),
    );
    let event = f.account();
    assert_eq!(event.balances[0].locked, Money::from("80 USDT"));
    assert_eq!(event.balances[0].free, Money::from("20 USDT"));
    assert_eq!(
        Decimal::from_str_exact(
            event.info.unwrap()["predict_requested_lock"]
                .as_str()
                .unwrap()
        )
        .unwrap(),
        Decimal::from(80)
    );
    FULL_ORDER_READS.with(|n| assert_eq!(n.get(), 0));
}

#[test]
fn unproven_native_cancel_does_not_release_venue_open_capacity() {
    let mut f = Fixture::new(100, 70);
    let target = f.order("TARGET", "0.700", true);
    f.watch(std::slice::from_ref(&target));
    f.apply(
        &f.factory
            .generate_order_canceled(&target, Some(hash("TARGET")), 3.into(), 3.into()),
    );
    assert!(f.rx.try_recv().is_err());
    assert!(
        f.client
            .account_sync
            .state
            .lock()
            .unwrap()
            .confirmed
            .is_none()
    );
}

#[test]
fn missed_projection_revision_falls_back_to_current_native_portfolio() {
    let mut f = Fixture::new(100, 100);
    let target = f.order("TARGET", "0.700", true);
    let other = f.order("OTHER", "0.800", true);
    f.watch(&[target.clone(), other]);
    f.client
        .account_sync
        .state
        .lock()
        .unwrap()
        .projected_revision = None;
    f.client
        .account_sync
        .confirm_terminal(hash("TARGET"), Some(target.client_order_id()));
    f.apply(
        &f.factory
            .generate_order_canceled(&target, Some(hash("TARGET")), 3.into(), 3.into()),
    );
    assert_eq!(f.account().balances[0].locked, Money::from("80 USDT"));
    FULL_ORDER_READS
        .with(|n| assert_eq!(n.get(), 1, "full fallback reads the remaining active owner"));
}

#[test]
fn pending_native_account_event_prevents_overtaking_even_on_order_local_path() {
    let mut f = Fixture::new(100, 80);
    let target = f.order("TARGET", "0.700", true);
    let other = f.order("OTHER", "0.100", true);
    f.watch(&[target.clone(), other]);
    f.client.account_sync.state.lock().unwrap().last_event = Some(UUID4::new());
    f.client
        .account_sync
        .confirm_terminal(hash("TARGET"), Some(target.client_order_id()));
    f.apply(
        &f.factory
            .generate_order_canceled(&target, Some(hash("TARGET")), 3.into(), 3.into()),
    );
    assert!(f.rx.try_recv().is_err());
    assert_eq!(
        FULL_ORDER_READS.with(|n| n.get()),
        0,
        "known pending Native account event must not trigger a full order projection"
    );
    assert_eq!(
        f.cache
            .borrow()
            .account_ref(&f.client.core.account_id)
            .unwrap()
            .balance_locked(Some(Currency::USDT()))
            .unwrap(),
        Money::from("80 USDT")
    );
}

#[test]
fn concurrent_projection_of_same_base_and_revision_is_published_once() {
    let mut f = Fixture::new(100, 0);
    f.watch(&[]);
    f.client
        .account_sync
        .state
        .lock()
        .unwrap()
        .projected_revision = None;
    let base = f
        .client
        .account_sync
        .state
        .lock()
        .unwrap()
        .confirmed
        .clone()
        .unwrap();
    let reporter = f.client.account_reporter().unwrap();
    assert!(
        reporter
            .publish_projection(&base, 0, Money::zero(Currency::USDT()), &[], None)
            .unwrap()
    );
    assert!(
        reporter
            .publish_projection(&base, 0, Money::zero(Currency::USDT()), &[], None)
            .unwrap()
    );
    let event = f.account();
    assert_eq!(event.balances[0].free, Money::from("100 USDT"));
    assert!(f.rx.try_recv().is_err());
}

#[test]
fn market_buy_uses_signed_budget_for_acceptance_and_cancel_after_registry_retirement() {
    let mut f = Fixture::new(100, 0);
    let mut order = OrderTestBuilder::new(OrderType::Market)
        .trader_id(f.client.core.trader_id)
        .client_order_id("TARGET".into())
        .instrument_id(f.instrument.id())
        .side(OrderSide::Buy)
        .quantity(Quantity::from(100))
        .build();
    order
        .apply(f.factory.generate_order_submitted(&order, 1.into()))
        .unwrap();
    f.watch(std::slice::from_ref(&order));
    f.client.pending_submissions.lock().unwrap().entries.insert(
        order.client_order_id(),
        crate::execution::orders::Submission {
            venue_id: Some(hash("TARGET")),
            wallet_epoch: Some(0),
            market_buy_collateral: Some(Decimal::from(2)),
        },
    );
    f.apply(
        &f.factory
            .generate_order_accepted(&order, hash("TARGET"), 2.into(), 2.into()),
    );
    let accepted = f.account();
    assert_eq!(accepted.balances[0].locked, Money::from("2 USDT"));
    f.cache
        .borrow_mut()
        .account_mut(&f.client.core.account_id)
        .unwrap()
        .apply(accepted)
        .unwrap();
    let order = f
        .cache
        .borrow()
        .order_owned(&order.client_order_id())
        .unwrap();
    f.client
        .account_sync
        .confirm_terminal(hash("TARGET"), Some(order.client_order_id()));
    f.apply(
        &f.factory
            .generate_order_canceled(&order, Some(hash("TARGET")), 3.into(), 3.into()),
    );
    let canceled = f.account();
    assert_eq!(canceled.balances[0].locked, Money::from("0 USDT"));
    assert!(
        f.client
            .pending_submissions
            .lock()
            .unwrap()
            .entries
            .is_empty()
    );
    FULL_ORDER_READS.with(|n| assert_eq!(n.get(), 0));
}

#[test]
fn order_local_release_preserves_confirmed_fill_block_freshness() {
    let mut f = Fixture::new(100, 70);
    let target = f.order("TARGET", "0.700", true);
    f.watch(std::slice::from_ref(&target));
    f.client.account_sync.state.lock().unwrap().latest_fill = 2.into();
    f.client
        .account_sync
        .confirm_terminal(hash("TARGET"), Some(target.client_order_id()));
    f.apply(
        &f.factory
            .generate_order_canceled(&target, Some(hash("TARGET")), 3.into(), 3.into()),
    );
    assert!(f.rx.try_recv().is_err());
    assert_eq!(
        f.cache
            .borrow()
            .account_ref(&f.client.core.account_id)
            .unwrap()
            .balance_locked(Some(Currency::USDT()))
            .unwrap(),
        Money::from("70 USDT")
    );
}

fn remote_row(index: u32) -> Arc<PredictOrderData> {
    Arc::new(serde_json::from_value(serde_json::json!({
        "order":{"hash":format!("0x{index:064x}"),"salt":"1","maker":"0x0000000000000000000000000000000000000022",
        "signer":"0x0000000000000000000000000000000000000022","taker":"0x0000000000000000000000000000000000000000",
        "tokenId":"11","makerAmount":"10000000000000000000","takerAmount":"100000000000000000000",
        "expiration":"0","nonce":"0","feeRateBps":"0","side":0,"signatureType":0,"signature":"offline"},
        "id":index.to_string(),"marketId":42,"currency":"USDT","amount":"unknown-unit","amountFilled":"0",
        "isNegRisk":false,"isYieldBearing":false,"strategy":"LIMIT","status":"OPEN","rewardEarningRate":0
    })).unwrap())
}

#[test]
fn indexed_open_retirement_preserves_old_readers_and_unrelated_external_rows() {
    let f = Fixture::new(1000, 20);
    let rows = Arc::new(
        (1..=3)
            .map(|n| {
                let row = remote_row(n);
                (row.order.hash.unwrap(), row)
            })
            .collect::<AHashMap<_, _>>(),
    );
    let mut state = f.client.account_sync.state.lock().unwrap();
    Arc::make_mut(state.confirmed.as_mut().unwrap()).rows = rows.clone();
    let old = state.confirmed.clone().unwrap();
    state.confirmed_terminal.insert(hash("TARGET"));
    state.retire_orders(&HashSet::from([VenueOrderId::new(format!("0x{:064x}", 4))]));
    assert!(Arc::ptr_eq(state.confirmed.as_ref().unwrap(), &old));
    state.retire_orders(&HashSet::from([hash("TARGET")]));
    assert_eq!(old.rows.len(), 3);
    assert_eq!(state.confirmed.as_ref().unwrap().rows.len(), 2);
    assert!(
        state
            .confirmed
            .as_ref()
            .unwrap()
            .rows
            .contains_key(&B256::from(U256::from(2)))
    );
    assert!(
        state
            .confirmed
            .as_ref()
            .unwrap()
            .rows
            .contains_key(&B256::from(U256::from(3)))
    );
    assert_eq!(state.resource_revision, 0);
    assert_eq!(state.projected_revision, Some(0));
}

#[test]
#[ignore = "explicit OPEN retirement timing; PREDICT_OPEN_RETIRE_PATH is create_new JSON"]
fn benchmark_open_owner_retirement() {
    let mut cases = Vec::new();
    for count in [1000u32, 10000] {
        for shared in [false, true] {
            let mut timings = Vec::new();
            for iteration in 0..23 {
                let rows = Arc::new(
                    (1..=count)
                        .map(|n| {
                            let row = remote_row(n);
                            (row.order.hash.unwrap(), row)
                        })
                        .collect(),
                );
                let snapshot = crate::blockchain::models::PredictBalanceSnapshot {
                    environment: PredictEnvironment::BnbMainnet,
                    account_address: alloy::primitives::Address::repeat_byte(0x22),
                    collateral: collateral_contract(PredictEnvironment::BnbMainnet),
                    block_number: 1,
                    block_hash: B256::repeat_byte(1),
                    block_timestamp: 1.into(),
                    decimals: 18,
                    balance_wei: U256::from(count) * U256::from(10u64.pow(18)),
                    ts_observed: 1.into(),
                };
                let ids = ((count - 9)..=count)
                    .map(|n| VenueOrderId::new(format!("0x{n:064x}")))
                    .collect::<HashSet<_>>();
                let mut state = AccountSyncState {
                    confirmed: Some(Arc::new(ConfirmedAccount {
                        epoch: 0,
                        rows,
                        balance: snapshot,
                    })),
                    confirmed_terminal: ids.clone(),
                    ..Default::default()
                };
                let reader = shared.then(|| state.confirmed.clone().unwrap());
                let started = std::time::Instant::now();
                state.retire_orders(&ids);
                let elapsed = started.elapsed().as_nanos() as u64;
                assert_eq!(
                    state.confirmed.as_ref().unwrap().rows.len(),
                    count as usize - 10
                );
                assert!(state.confirmed_terminal.is_empty());
                if let Some(reader) = reader {
                    assert_eq!(reader.rows.len(), count as usize);
                }
                if iteration >= 3 {
                    timings.push(elapsed);
                }
            }
            cases.push(serde_json::json!({"open_rows":count,"shared_reader":shared,"retired_owners":10,"samples_ns":timings}));
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_OPEN_RETIRE_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file,&serde_json::json!({"source":"production AccountSyncState::retire_orders on Native thread; 3 warmups + 20 samples; setup excluded; 10 proven terminal keys selected at end of remote OPEN baseline; shared reader preserved; p99 is max","cases":cases})).unwrap();
}

#[test]
fn recovery_queries_coalesce_without_copying_native_history() {
    use nautilus_common::{clients::ExecutionClient, messages::execution::QueryAccount};
    let mut f = Fixture::new(1000, 0);
    f.watch(&[]);
    f.client.core.set_connected();
    f.client
        .account_refresh_required
        .store(true, Ordering::Release);
    f.client.account_query_active.store(true, Ordering::Release);
    f.client
        .query_account(QueryAccount::new(
            f.client.core.trader_id,
            Some(f.client.core.client_id),
            f.client.core.account_id,
            UUID4::new(),
            1.into(),
            None,
            None,
        ))
        .unwrap();
    assert!(f.client.account_query_active.load(Ordering::Acquire));
    f.client
        .account_query_active
        .store(false, Ordering::Release);
}

#[test]
#[ignore = "explicit deferred Native projection bench; PREDICT_DEFERRED_PROJECTION_PATH is create_new JSON"]
fn benchmark_pending_account_projection() {
    use std::{hint::black_box, time::Instant};
    let mut rows = Vec::new();
    for active in [1, 1000, 10000] {
        let mut f = Fixture::new(100000, 0);
        let orders = (0..active)
            .map(|i| {
                let mut order = f.order(&format!("BG-{i}"), "0.010", false);
                order
                    .apply(f.factory.generate_order_accepted(
                        &order,
                        VenueOrderId::new(format!("0x{:064x}", i + 1000)),
                        2.into(),
                        2.into(),
                    ))
                    .unwrap();
                order
            })
            .collect::<Vec<_>>();
        f.watch(&orders);
        {
            let mut state = f.client.account_sync.state.lock().unwrap();
            state.last_event = Some(UUID4::new());
            state.projected_revision = None;
        }
        let reporter = f.client.account_reporter().unwrap();
        let mut samples = Vec::new();
        for iteration in 0..110 {
            FULL_ORDER_READS.with(|n| n.set(0));
            let started = Instant::now();
            assert!(!reporter.project_on_core(black_box(&f.client.core)).unwrap());
            let ns = started.elapsed().as_nanos() as u64;
            if iteration >= 10 {
                samples.push(ns);
            }
        }
        samples.sort_unstable();
        assert!(f.rx.try_recv().is_err());
        rows.push(serde_json::json!({"active_orders":active,"samples_ns":samples,"last_full_order_reads":FULL_ORDER_READS.with(|n|n.get()),"p50_ns":samples[49],"p95_ns":samples[94],"p99_ns":samples[98]}));
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_DEFERRED_PROJECTION_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file,&serde_json::json!({"source":"production core projection with known unapplied Native event; 10 warmups + 100 samples; setup excluded; no publication/REST/RPC", "rows":rows})).unwrap();
}

#[test]
fn applied_account_event_flushes_deferred_cancel_projection_without_a_remote_query() {
    use nautilus_common::{
        messages::ExecutionEvent,
        msgbus::{MessagingSwitchboard, TypedHandler},
    };
    let mut f = Fixture::new(100, 80);
    let target = f.order("TARGET", "0.700", true);
    let other = f.order("OTHER", "0.100", true);
    f.watch(&[target.clone(), other]);
    f.client
        .account_sync
        .state
        .lock()
        .unwrap()
        .projected_revision = None;
    assert!(
        f.client
            .account_reporter()
            .unwrap()
            .project_on_core(&f.client.core)
            .unwrap()
    );
    let ExecutionEvent::Account(first) = f.rx.try_recv().unwrap() else {
        panic!("account projection");
    };
    f.client
        .account_sync
        .confirm_terminal(hash("TARGET"), Some(target.client_order_id()));
    f.apply(
        &f.factory
            .generate_order_canceled(&target, Some(hash("TARGET")), 3.into(), 3.into()),
    );
    assert!(
        f.rx.try_recv().is_err(),
        "cancel cannot overtake the pending snapshot"
    );
    let topic = format!("events.account.{}", f.client.core.account_id);
    msgbus::publish_account_state(topic.as_str().into(), &first);
    assert!(
        f.rx.try_recv().is_err(),
        "an unapplied topic message is not an ACK"
    );
    let cache = f.cache.clone();
    msgbus::register_account_state_endpoint(
        MessagingSwitchboard::portfolio_update_account(),
        TypedHandler::from(move |event: &AccountState| {
            cache.borrow_mut().update_account_state(event).unwrap();
            msgbus::publish_account_state(
                format!("events.account.{}", event.account_id).into(),
                event,
            );
        }),
    );
    // Use the actual Runner routing, not a direct call to the adapter's callback.
    nautilus_live::runner::AsyncRunner::handle_exec_event(ExecutionEvent::Account(first.clone()));
    let ExecutionEvent::Account(released) =
        f.rx.try_recv()
            .expect("Native application must flush the latest reservation")
    else {
        panic!("account projection");
    };
    assert_eq!(released.balances[0].locked, Money::from("10 USDT"));
    assert_eq!(released.balances[0].free, Money::from("90 USDT"));
    msgbus::publish_account_state(topic.as_str().into(), &first);
    assert!(
        f.rx.try_recv().is_err(),
        "old publication must not duplicate or relock"
    );
    nautilus_live::runner::AsyncRunner::handle_exec_event(ExecutionEvent::Account(
        released.clone(),
    ));
    assert!(
        f.rx.try_recv().is_err(),
        "current projection must not publish recursively"
    );
    assert_eq!(
        f.cache
            .borrow()
            .account(&f.client.core.account_id)
            .unwrap()
            .balance_locked(Some(Currency::USDT())),
        Some(Money::from("10 USDT"))
    );
    f.client.clear_observer();
    f.client
        .account_sync
        .state
        .lock()
        .unwrap()
        .resource_revision += 1;
    msgbus::publish_account_state(topic.as_str().into(), &released);
    assert!(
        f.rx.try_recv().is_err(),
        "retired account observers must not publish"
    );
}

#[test]
fn unresolved_commands_do_not_prevent_account_refresh() {
    use nautilus_common::{
        clock::VirtualClock,
        messages::execution::TradingCommand,
        msgbus::{MessagingSwitchboard, TypedIntoHandler},
    };
    for cancel in [false, true] {
        let mut f = Fixture::new(1000, 0);
        let order = f.order("TARGET", "0.010", cancel);
        f.watch(std::slice::from_ref(&order));
        if cancel {
            f.client
                .pending_cancels
                .lock()
                .unwrap()
                .insert(order.client_order_id());
        }
        f.client.core.set_connected();
        f.client.healthy.store(true, Ordering::Release);
        f.client
            .account_refresh_required
            .store(true, Ordering::Release);
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        f.client.recovery_clock = Some(clock.clone());
        let queries = Rc::new(std::cell::Cell::new(0));
        let seen = queries.clone();
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::exec_engine_queue_execute(),
            TypedIntoHandler::from(move |_: TradingCommand| seen.set(seen.get() + 1)),
        );
        f.client.start_recovery_timer().unwrap();
        for second in 1..=3 {
            let events = clock
                .borrow_mut()
                .advance_time((second * 1_000_000_000u64).into(), true);
            for handler in clock.borrow().match_handlers(events) {
                handler.run();
            }
        }
        assert_eq!(
            queries.get(),
            3,
            "account refresh is independent of unresolved orders and cancels"
        );
        if cancel {
            f.client.pending_cancels.lock().unwrap().clear();
        } else {
            f.apply(
                &f.factory
                    .generate_order_accepted(&order, hash("TARGET"), 2.into(), 2.into()),
            );
        }
        let events = clock
            .borrow_mut()
            .advance_time(4_000_000_000u64.into(), true);
        for handler in clock.borrow().match_handlers(events) {
            handler.run();
        }
        assert_eq!(
            queries.get(),
            4,
            "the existing timer keeps refreshing after command resolution"
        );
        f.client.stop_recovery_timer();
    }
}

#[test]
fn reset_preserves_unresolved_cancel_ownership_until_reconciled() {
    use nautilus_common::clients::ExecutionClient;
    let mut f = Fixture::new(100, 0);
    let order = f.order("TARGET", "0.010", true);
    f.watch(std::slice::from_ref(&order));
    assert!(
        f.client
            .pending_submissions
            .lock()
            .unwrap()
            .entries
            .is_empty(),
        "recovered order need not have a local submission"
    );
    f.client
        .pending_cancels
        .lock()
        .unwrap()
        .insert(order.client_order_id());
    assert!(
        f.client
            .reset()
            .unwrap_err()
            .to_string()
            .contains("cancellations")
    );
    assert!(
        f.client
            .pending_cancels
            .lock()
            .unwrap()
            .contains(&order.client_order_id())
    );
    assert!(
        f.client
            .account_orders
            .lock()
            .unwrap()
            .contains_key(&order.client_order_id())
    );
    f.apply(
        &f.factory
            .generate_order_canceled(&order, order.venue_order_id(), 3.into(), 3.into()),
    );
    assert!(f.client.pending_cancels.lock().unwrap().is_empty());
    f.client.reset().unwrap();
    assert!(f.client.account_orders.lock().unwrap().is_empty());
}
