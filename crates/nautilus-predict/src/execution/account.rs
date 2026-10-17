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

//! Reported cash accounts: protocol collection, native reservation math and account-state emission.
//! This is not a second order/position manager and does not submit chain transactions.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use alloy::primitives::B256;
use nautilus_common::factories::OrderEventFactory;
use nautilus_core::{AtomicMap, Params, UUID4, time::AtomicTime};
use nautilus_live::ExecutionEventEmitter;
use nautilus_model::{
    accounts::cash::CashAccount,
    enums::AccountType,
    events::OrderEventAny,
    identifiers::{ClientOrderId, InstrumentId},
    instruments::InstrumentAny,
    orders::{Order, OrderAny},
    types::{Currency, Money},
};
use nautilus_network::websocket::AuthTracker;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::parse::validate_rest_order_terms;
use crate::{
    blockchain::{PredictChainClient, parse::parse_balance_snapshot},
    config::PredictExecutionClientConfig,
    http::account_models::PredictOrderData,
};

#[derive(Debug, Default)]
pub(super) struct AccountSync {
    pub state: Mutex<AccountSyncState>,
    pub notify: Notify,
}

#[derive(Debug, Default)]
pub(super) struct AccountSyncState {
    pub revision: u64,
    // Chain freshness must outlive Native's retention of individual settled orders.
    pub latest_fill: nautilus_core::UnixNanos,
    pub last_event: Option<UUID4>,
    block: Option<(u64, B256)>,
}

#[derive(Debug, thiserror::Error)]
#[error("Predict balance block predates confirmed native fills")]
struct BalanceBlockLag;

#[derive(Clone, Debug)]
pub(super) struct AccountReporter {
    pub config: PredictExecutionClientConfig,
    pub chain: PredictChainClient,
    pub instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
    pub orders: Arc<AtomicMap<ClientOrderId, Arc<OrderAny>>>,
    pub sync: Arc<AccountSync>,
    pub emitter: ExecutionEventEmitter,
    pub clock: &'static AtomicTime,
    pub authentication: AuthTracker,
    pub connection_epoch: Arc<AtomicU64>,
    pub allow_stream_gap: bool,
}

impl AccountReporter {
    /// Returns false when native context changed during collection. Only a fresh source may emit.
    pub async fn refresh(&self) -> anyhow::Result<bool> {
        let epoch = self.connection_epoch.load(Ordering::Acquire);
        anyhow::ensure!(
            self.allow_stream_gap || self.authentication.is_authenticated(),
            "Predict account reporting requires an acknowledged wallet connection"
        );
        let (revision, latest_fill, instruments, orders) = {
            let state = self.sync.state.lock().expect("account sync poisoned");
            (
                state.revision,
                state.latest_fill,
                self.instruments.load(),
                self.orders.load(),
            )
        };
        let (rows, snapshot) = tokio::time::timeout(
            Duration::from_secs(self.config.report_timeout_secs),
            async {
                self.config.session.request_account().await?;
                let rows = collect_open_orders(&self.config).await?;
                let snapshot = self
                    .chain
                    .request_balance_snapshot(self.config.session.account_address(), self.clock)
                    .await?;
                Ok::<_, anyhow::Error>((rows, snapshot))
            },
        )
        .await
        .map_err(|_| anyhow::anyhow!("Predict account collection timed out"))??;

        // A lagging RPC block must not undo collateral changes of already confirmed native fills.
        let newest_fill = orders
            .values()
            .flat_map(|order| order.events())
            .filter_map(|event| {
                if let OrderEventAny::Filled(fill) = event {
                    Some(fill.ts_event)
                } else {
                    None
                }
            })
            .max()
            .unwrap_or_default()
            .max(latest_fill);
        if snapshot.block_timestamp < newest_fill {
            return Err(BalanceBlockLag.into());
        }
        let currency = Currency::USDT();
        let balance = reported_balance(&snapshot, Money::zero(currency))?;
        let factory = OrderEventFactory::new(
            self.emitter.trader_id(),
            self.config.account_id,
            AccountType::Cash,
            None,
        );
        let initial = factory.generate_account_state(
            vec![balance],
            vec![],
            true,
            snapshot.ts_observed,
            self.clock.get_time_ns(),
            None,
        );
        // Native cash-account math only. This temporary model is never inserted into Cache.
        let cash = CashAccount::new(initial, false, false);
        let mut native_by_hash = HashMap::new();
        for order in orders.values() {
            if let Some(hash) = order
                .venue_order_id()
                .and_then(|id| id.as_str().parse::<B256>().ok())
            {
                anyhow::ensure!(
                    native_by_hash.insert(hash, order).is_none(),
                    "Predict native order context has duplicate venue hashes"
                );
            }
        }
        let mut locked = Money::zero(currency);
        let mut represented = HashSet::new();
        for row in rows {
            let hash = super::resources::open_order_hash(&row)?;
            anyhow::ensure!(
                represented.insert(hash),
                "Predict open-order snapshot has an invalid status, currency or duplicate hash"
            );
            let native = native_by_hash.get(&hash).copied();
            let reservation = if let Some(order) =
                native.filter(|order| order.is_open() || order.is_inflight())
            {
                let instrument = instruments.get(&order.instrument_id()).ok_or_else(|| {
                    anyhow::anyhow!("Predict reservation instrument is absent from native context")
                })?;
                let InstrumentAny::BinaryOption(binary) = instrument else {
                    anyhow::bail!("Predict reservations require native binary instruments");
                };
                validate_rest_order_terms(
                    &row,
                    order,
                    binary,
                    self.config.account_id,
                    self.config.session.account_address(),
                )?;
                if order.order_type() == nautilus_model::enums::OrderType::Market {
                    super::resources::reported_signed_reservation(&row)?
                } else {
                    super::resources::native_reservation(&cash, order, instrument)?
                }
            } else {
                // Unmanaged or stale external BUYs still consume capacity. The full signed maker
                // budget is conservative and independent of ambiguous REST filled-amount units.
                super::resources::reported_signed_reservation(&row)?
            };
            locked = locked
                .checked_add(reservation)
                .ok_or_else(|| anyhow::anyhow!("Predict reservation aggregate overflow"))?;
        }
        for order in orders
            .values()
            .filter(|order| order.is_open() || order.is_inflight())
        {
            if order
                .venue_order_id()
                .and_then(|id| id.as_str().parse::<B256>().ok())
                .is_some_and(|hash| represented.contains(&hash))
            {
                continue;
            }
            let instrument = instruments.get(&order.instrument_id()).ok_or_else(|| {
                anyhow::anyhow!(
                    "Predict local reservation instrument is absent from native context"
                )
            })?;
            locked = locked
                .checked_add(super::resources::native_reservation(
                    &cash, order, instrument,
                )?)
                .ok_or_else(|| anyhow::anyhow!("Predict reservation aggregate overflow"))?;
        }
        let balance = reported_balance(&snapshot, locked)?;
        let mut info = Params::new();
        info.insert(
            "predict_block_number".into(),
            serde_json::json!(snapshot.block_number),
        );
        info.insert(
            "predict_block_hash".into(),
            serde_json::json!(format!("{:#x}", snapshot.block_hash)),
        );
        info.insert(
            "predict_account_address".into(),
            serde_json::json!(format!("{:#x}", snapshot.account_address)),
        );
        info.insert(
            "predict_balance_wei".into(),
            serde_json::json!(snapshot.balance_wei.to_string()),
        );
        info.insert(
            "predict_chain_id".into(),
            serde_json::json!(snapshot.environment.chain_id()),
        );
        info.insert(
            "predict_requested_lock".into(),
            serde_json::json!(locked.as_decimal().to_string()),
        );
        let event = factory.generate_account_state(
            vec![balance],
            vec![],
            true,
            snapshot.ts_observed,
            self.clock.get_time_ns(),
            Some(info),
        );
        let mut state = self.sync.state.lock().expect("account sync poisoned");
        anyhow::ensure!(
            self.connection_epoch.load(Ordering::Acquire) == epoch
                && (self.allow_stream_gap || self.authentication.is_authenticated()),
            "Predict wallet connection changed during account collection"
        );
        if state.revision != revision {
            self.sync.notify.notify_one();
            return Ok(false);
        }
        if let Some((number, hash)) = state.block {
            anyhow::ensure!(
                snapshot.block_number >= number
                    && (snapshot.block_number != number || snapshot.block_hash == hash),
                "Predict account snapshot block regressed or changed at the same height"
            );
        }
        self.emitter.try_send_account_state(event.clone())?;
        state.last_event = Some(event.event_id);
        state.block = Some((snapshot.block_number, snapshot.block_hash));
        Ok(true)
    }
}

// On-chain USDT has 18 decimals; canonical Nautilus USDT has fewer. Floor total at the
// account boundary so representable free funds never exceed the chain balance. Keep the full
// raw amount in AccountState info; the strict standalone conversion API remains unchanged.
pub(super) fn reported_balance(
    snapshot: &crate::blockchain::models::PredictBalanceSnapshot,
    locked: Money,
) -> anyhow::Result<nautilus_model::types::AccountBalance> {
    let precision = u32::from(Currency::USDT().precision);
    anyhow::ensure!(precision <= 18, "unsupported native USDT precision");
    let unit = alloy::primitives::U256::from(10u64.pow(18 - precision));
    let mut represented = snapshot.clone();
    represented.balance_wei -= represented.balance_wei % unit;
    let total = parse_balance_snapshot(&represented, Money::zero(Currency::USDT()))?;
    // Native cash clamps overcommitted capacity to total and reports zero free funds. Preserve
    // the uncapped request in event info instead of rejecting otherwise valid funding snapshots.
    nautilus_model::types::AccountBalance::from_total_and_locked(
        total.total.as_decimal(),
        locked.as_decimal(),
        Currency::USDT(),
    )
    .map_err(|_| anyhow::anyhow!("Predict reported balance exceeds native monetary range"))
}

pub(super) async fn collect_open_orders(
    config: &PredictExecutionClientConfig,
) -> anyhow::Result<Vec<PredictOrderData>> {
    super::reconciliation::collect_orders(config, true).await
}

pub(super) async fn run(reporter: AccountReporter, cancellation: CancellationToken) {
    let mut interval = tokio::time::interval(Duration::from_secs(
        reporter.config.account_refresh_interval_secs,
    ));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    loop {
        tokio::select! { biased;
            _ = cancellation.cancelled() => break,
            _ = reporter.sync.notify.notified() => {},
            _ = interval.tick() => {},
        }
        if !reporter.authentication.is_authenticated() {
            // The private transport owns resubscription; retain this generation across gaps.
            continue;
        }
        // Wallet success time can slightly follow its mined block timestamp. Retry this
        // read-only causal lag briefly so cash does not wait for the normal funding poll.
        for attempt in 0..5 {
            let result = tokio::select! { biased;
                _ = cancellation.cancelled() => return,
                result = reporter.refresh() => result,
            };
            match result {
                Err(error) if error.is::<BalanceBlockLag>() && attempt < 4 => {
                    tokio::select! { biased;
                        _ = cancellation.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                    }
                }
                Err(error) => {
                    log::warn!(
                        "Predict account refresh failed; retaining the last reported state: {error}"
                    );
                    break;
                }
                Ok(_) => break,
            }
        }
    }
}

/// Bounds explicit and automatic gap checks to one account collection per generation.
pub(super) struct AccountQueryGuard(Arc<std::sync::atomic::AtomicBool>);
impl AccountQueryGuard {
    pub fn acquire(active: &Arc<std::sync::atomic::AtomicBool>) -> Option<Self> {
        active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| Self(active.clone()))
    }
}
impl Drop for AccountQueryGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
