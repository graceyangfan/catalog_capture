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

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use ahash::AHashMap;
use alloy::primitives::B256;
use anyhow::Context;
use nautilus_common::{
    clients::ExecutionClient, factories::OrderEventFactory, messages::execution::QueryAccount,
};
use nautilus_core::{AtomicMap, Params, UUID4, UnixNanos, time::AtomicTime};
use nautilus_live::ExecutionEventEmitter;
#[cfg(test)]
use nautilus_model::orders::Order;
use nautilus_model::{
    accounts::{Account, AccountAny, cash::CashAccount},
    enums::{AccountType, OrderSide, OrderType},
    events::AccountState,
    identifiers::{ClientOrderId, InstrumentId, VenueOrderId},
    instruments::InstrumentAny,
    types::{Currency, Money, Price},
};
use nautilus_network::websocket::AuthTracker;
use rust_decimal::Decimal;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::execution::{context, metadata, orders, parse};
use crate::{
    blockchain::{PredictChainClient, parse::parse_balance_snapshot},
    common::{enums::PredictOrderSide, parse::exact_wei},
    config::{PredictAccountStateMode, PredictExecutionClientConfig},
    http::account_models::{PredictOrderData, PredictOrderStatus},
};

// This is not a second order/position manager and does not submit chain transactions.

#[cfg(test)]
thread_local! {
    static FULL_ORDER_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) fn latest_venue_fill(order: &impl context::OrderView) -> UnixNanos {
    if order.is_synthetic() {
        UnixNanos::default()
    } else {
        order.latest_fill()
    }
}

// Compare reservation inputs; transient command status alone does not change reported locks.
pub(super) fn order_resources_changed<O: context::OrderView, P: context::OrderView>(
    previous: Option<&P>,
    current: &O,
) -> bool {
    // Submitted capacity belongs to Native RiskEngine. Only OPEN reservations
    // affect reported locked cash; ordinary in-flight transitions need no projection.
    let Some(previous) = previous else {
        return current.is_open();
    };
    (previous.is_open() || current.is_open())
        && (previous.account_id() != current.account_id()
            || previous.instrument_id() != current.instrument_id()
            || previous.order_side() != current.order_side()
            || previous.order_type() != current.order_type()
            || previous.venue_order_id() != current.venue_order_id()
            || previous.quantity() != current.quantity()
            || previous.filled_qty() != current.filled_qty()
            || previous.price() != current.price()
            || previous.is_open() != current.is_open())
}

#[derive(Debug, Default)]
pub(super) struct AccountSync {
    pub state: Mutex<AccountSyncState>,
    pub notify: Notify,
}

#[derive(Debug, Default)]
pub(super) struct AccountSyncState {
    // Local projection depends on reservation inputs, not transient command statuses.
    pub resource_revision: u64,
    // An index into Native worker snapshots, not a second order or balance model.
    pub active_orders: HashMap<ClientOrderId, Option<VenueOrderId>>,
    // Chain freshness must outlive Native's retention of individual settled orders.
    pub latest_fill: UnixNanos,
    pub last_event: Option<UUID4>,
    pub block: Option<(u64, B256)>,
    remote_requested: u64,
    remote_collected: u64,
    open_requested: u64,
    open_collected: u64,
    confirmed: Option<Arc<ConfirmedAccount>>,
    projected_revision: Option<u64>,
    // Venue facts only: an unresolved local command cannot release remote OPEN cash.
    pub confirmed_terminal: HashSet<VenueOrderId>,
}

#[derive(Clone, Debug)]
pub(super) struct ConfirmedAccount {
    pub epoch: u64,
    pub rows: Arc<AHashMap<B256, Arc<PredictOrderData>>>,
    pub balance: crate::blockchain::models::PredictBalanceSnapshot,
}

impl AccountSync {
    pub fn request_remote(&self) {
        let mut state = self.state.lock().expect("account sync poisoned");
        state.request_cash();
        state.open_requested = state.open_requested.wrapping_add(1);
        drop(state);
        self.notify.notify_one();
    }

    pub fn confirm_terminal(&self, id: VenueOrderId, client: Option<ClientOrderId>) {
        let mut state = self.state.lock().expect("account sync poisoned");
        if state.confirmed_terminal.insert(id)
            && client.is_none_or(|id| !state.active_orders.contains_key(&id))
        {
            state.resource_revision = state.resource_revision.wrapping_add(1);
            drop(state);
            self.notify.notify_one();
        }
    }

    pub fn confirm_report(&self, report: &nautilus_model::reports::OrderStatusReport) {
        use nautilus_model::enums::OrderStatus;
        // Unclaimed external reports have no Native retention owner. Periodic
        // OPEN collection, rather than an unbounded terminal registry, covers them.
        if report.client_order_id.is_none() {
            return;
        }
        if matches!(
            report.order_status,
            OrderStatus::Canceled
                | OrderStatus::Expired
                | OrderStatus::Filled
                | OrderStatus::Rejected
        ) {
            self.confirm_terminal(report.venue_order_id, report.client_order_id);
        }
    }
}

impl AccountSyncState {
    pub fn record_projection(&mut self, base: Arc<ConfirmedAccount>, event_id: UUID4) {
        self.block = Some((base.balance.block_number, base.balance.block_hash));
        self.confirmed = Some(base);
        self.projected_revision = Some(self.resource_revision);
        self.last_event = Some(event_id);
    }

    pub fn request_cash(&mut self) {
        self.remote_requested = self.remote_requested.wrapping_add(1);
    }

    pub fn retire_orders(&mut self, hashes: &HashSet<VenueOrderId>) {
        if hashes.is_empty() {
            return;
        }
        if let Some(base) = &mut self.confirmed
            && hashes.iter().any(|id| {
                self.confirmed_terminal.contains(id)
                    && id
                        .as_str()
                        .parse::<B256>()
                        .is_ok_and(|hash| base.rows.contains_key(&hash))
            })
        {
            let rows = Arc::make_mut(&mut Arc::make_mut(base).rows);
            for id in hashes {
                if self.confirmed_terminal.contains(id)
                    && let Ok(hash) = id.as_str().parse::<B256>()
                {
                    rows.remove(&hash);
                }
            }
        }
        for hash in hashes {
            self.confirmed_terminal.remove(hash);
        }
    }

    pub fn invalidate_cash(&mut self) {
        self.confirmed = None;
        self.projected_revision = None;
        self.request_cash();
    }

    pub fn clear_connection(&mut self) {
        self.confirmed = None;
        self.projected_revision = None;
        self.confirmed_terminal.clear();
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Predict balance block predates confirmed native fills")]
struct BalanceBlockLag;

#[derive(Clone, Debug)]
pub(super) struct AccountReporter {
    pub config: PredictExecutionClientConfig,
    pub chain: PredictChainClient,
    pub instruments: Arc<AtomicMap<InstrumentId, metadata::ExecutionInstrument>>,
    pub orders: context::OrderSnapshots<ClientOrderId>,
    pub pending: orders::PendingSubmissions,
    pub rest_ids: orders::RestOrderIds,
    pub sync: Arc<AccountSync>,
    pub emitter: ExecutionEventEmitter,
    pub clock: &'static AtomicTime,
    pub authentication: AuthTracker,
    pub connection_epoch: Arc<AtomicU64>,
    pub allow_stream_gap: bool,
}

impl AccountReporter {
    /// Explicit initialization/recovery reads. Ordinary order events only reproject
    /// the last confirmed remote snapshot with current Native reservations.
    pub async fn refresh(&self) -> anyhow::Result<bool> {
        Ok(self.collect_account(false).await?.is_some())
    }

    async fn collect_account(
        &self,
        periodic: bool,
    ) -> anyhow::Result<Option<Arc<ConfirmedAccount>>> {
        let epoch = self.connection_epoch.load(Ordering::Acquire);
        anyhow::ensure!(
            self.allow_stream_gap || self.authentication.is_authenticated(),
            "Predict account reporting requires an acknowledged wallet connection"
        );
        let (requested, open_requested, verify_open) = {
            let state = self.sync.state.lock().expect("account sync poisoned");
            (
                state.remote_requested,
                state.open_requested,
                periodic
                    || self.allow_stream_gap
                    || state
                        .confirmed
                        .as_ref()
                        .is_none_or(|base| base.epoch != epoch)
                    || state.open_requested != state.open_collected,
            )
        };
        let collection = tokio::time::timeout(
            Duration::from_secs(self.config.request_timeout_secs),
            async {
                tokio::try_join!(
                    async {
                        if verify_open {
                            collect_open_orders(&self.config).await.map(Some)
                        } else {
                            Ok(None)
                        }
                    },
                    self.chain.request_balance_snapshot(
                        self.config.session.account_address(),
                        self.clock,
                    ),
                )
            },
        );
        tokio::pin!(collection);
        // A slow periodic/query read must not hold up a confirmed cancellation's
        // local account publication. Explicit queries remain counted across IO.
        let collected = loop {
            tokio::select! {
                biased;
                result = &mut collection => break result,
                _ = self.sync.notify.notified() => {
                    if let Err(error) = self.project() {
                        log::debug!("Predict local account projection deferred: {error}");
                    }
                }
            }
        };
        // Preserve a new explicit query/fill request even when this read fails.
        if self
            .sync
            .state
            .lock()
            .expect("account sync poisoned")
            .remote_requested
            != requested
        {
            self.sync.notify.notify_one();
        }
        let (rows, snapshot) = collected.context("Predict account collection timed out")??;
        let rows = rows
            .map(|rows| {
                let mut by_hash = AHashMap::with_capacity(rows.len());
                for row in rows {
                    let hash = open_order_hash(&row)?;
                    reported_signed_reservation(&row)?;
                    anyhow::ensure!(
                        by_hash.insert(hash, Arc::new(row)).is_none(),
                        "Predict duplicate OPEN hash"
                    );
                }
                Ok::<_, anyhow::Error>(Arc::new(by_hash))
            })
            .transpose()?;
        let collected = {
            let mut state = self.sync.state.lock().expect("account sync poisoned");
            anyhow::ensure!(
                self.connection_epoch.load(Ordering::Acquire) == epoch
                    && (self.allow_stream_gap || self.authentication.is_authenticated()),
                "Predict wallet connection changed during account collection"
            );
            // A Native-only cancellation can require a newer OPEN read to resolve
            // uncertainty. A collection already underway is not that evidence.
            if state.confirmed.is_none() && state.remote_requested != requested {
                self.sync.notify.notify_one();
                return Ok(None);
            }
            if snapshot.block_timestamp < state.latest_fill {
                return Err(BalanceBlockLag.into());
            }
            if let Some((number, hash)) = state.block {
                anyhow::ensure!(
                    snapshot.block_number >= number
                        && (snapshot.block_number != number || snapshot.block_hash == hash),
                    "Predict account snapshot block regressed or changed at the same height"
                );
            }
            let rows = if let Some(rows) = rows {
                state.open_collected = open_requested;
                rows
            } else {
                // Use the latest retained OPEN view: Native purge may have trimmed it during IO.
                state
                    .confirmed
                    .as_ref()
                    .filter(|base| base.epoch == epoch)
                    .ok_or_else(|| anyhow::anyhow!("Predict cash refresh lost its OPEN baseline"))?
                    .rows
                    .clone()
            };
            let collected = Arc::new(ConfirmedAccount {
                epoch,
                rows,
                balance: snapshot,
            });
            state.confirmed = Some(Arc::clone(&collected));
            state.projected_revision = None;
            state.remote_collected = requested;
            collected
        };
        let result = self.project();
        if self.needs_remote() {
            self.sync.notify.notify_one();
        }
        result.map(|projected| projected.then_some(collected))
    }

    fn needs_remote(&self) -> bool {
        let state = self.sync.state.lock().expect("account sync poisoned");
        state
            .confirmed
            .as_ref()
            .is_none_or(|base| base.epoch != self.connection_epoch.load(Ordering::Acquire))
            || state.remote_requested != state.remote_collected
            || state.open_requested != state.open_collected
    }

    fn project(&self) -> anyhow::Result<bool> {
        self.project_with_core(None)
    }

    /// On the Native owner thread, send local locks as callback children before
    /// strategy follow-up commands. Never overtake an older queued account snapshot.
    pub(super) fn project_on_core(
        &self,
        core: &nautilus_live::ExecutionClientCore,
    ) -> anyhow::Result<bool> {
        self.project_with_core(Some(core))
    }

    pub(super) fn on_account_state(
        &self,
        core: &nautilus_live::ExecutionClientCore,
        event: &AccountState,
    ) -> anyhow::Result<()> {
        if event.account_id != core.account_id {
            return Ok(());
        }
        let dirty = {
            let state = self.sync.state.lock().expect("account sync poisoned");
            state.last_event == Some(event.event_id)
                && state.projected_revision != Some(state.resource_revision)
                // Portfolio can republish the previous account event after a
                // fill. Its application is not confirmation of the new cash.
                && state.remote_requested == state.remote_collected
        };
        if dirty {
            // The core fence verifies Cache application; a topic alone
            // must never release locks from an unapplied account snapshot.
            self.project_on_core(core)?;
        }
        Ok(())
    }

    /// Update one reservation from the last applied Native projection. The
    /// account remains authoritative; no per-order balance/lock cache is kept.
    pub(super) fn project_order_on_core(
        &self,
        core: &nautilus_live::ExecutionClientCore,
        previous: Option<&context::WorkerOrder>,
        current: &context::WorkerOrder,
        registered: Option<VenueOrderId>,
        market_buy_collateral: Option<rust_decimal::Decimal>,
    ) -> anyhow::Result<bool> {
        let projection = (|| {
            let state = self.sync.state.lock().expect("account sync poisoned");
            let Some(base) = state.confirmed.as_ref() else {
                return Ok(None);
            };
            let Some(previous) = previous else {
                return Ok(None);
            };
            // A concurrent refresh or unapplied account event needs full projection.
            if state.projected_revision != Some(state.resource_revision.wrapping_sub(1))
                || base.epoch != self.connection_epoch.load(Ordering::Acquire)
                || !(previous.is_open() || previous.is_inflight())
            {
                return Ok(None);
            }
            if base.balance.block_timestamp < state.latest_fill {
                return Err(BalanceBlockLag.into());
            }
            let id = current.venue_order_id().or(registered);
            if previous.venue_order_id().or(registered) != id {
                return Ok(None);
            }
            let Some(id) = id else { return Ok(None) };
            let Ok(hash) = id.as_str().parse::<B256>() else {
                return Ok(None);
            };
            let cache = core.cache();
            let Some(account) = cache.account_ref(&self.config.account_id) else {
                return Ok(None);
            };
            let AccountAny::Cash(cash) = &*account else {
                return Ok(None);
            };
            let Some(applied) = cash.last_event() else {
                return Ok(None);
            };
            if state.last_event != Some(applied.event_id) {
                return Ok(None);
            }
            let Some(requested) = applied
                .info
                .as_ref()
                .and_then(|info| info.get("predict_requested_lock"))
                .and_then(serde_json::Value::as_str)
                .and_then(|value| rust_decimal::Decimal::from_str_exact(value).ok())
                .and_then(|value| Money::from_decimal_exact(value, Currency::USDT()).ok())
            else {
                return Ok(None);
            };
            let instruments = self.instruments.load();
            let Some(definition) = instruments.get(&current.instrument_id()) else {
                return Ok(None);
            };
            let row = base.rows.get(&hash);
            let terminal = state.confirmed_terminal.contains(&id);
            if terminal && current.is_open() {
                return Ok(None);
            }
            if let Some(row) =
                row.filter(|_| !terminal && (current.is_open() || current.is_inflight()))
            {
                let InstrumentAny::BinaryOption(binary) = &definition.instrument else {
                    anyhow::bail!("Predict reservations require native binary instruments");
                };
                parse::validate_rest_order_terms_for_hash(
                    row,
                    current,
                    binary,
                    self.config.account_id,
                    self.config.session.account_address(),
                    id,
                )?;
            }
            let collateral = row
                .map(|row| crate::common::parse::exact_wei(row.order.fields.maker_amount))
                .transpose()?
                .or(market_buy_collateral);
            let before = if previous.is_open() {
                native_reservation(cash, previous, &definition.instrument, collateral)?
            } else {
                Money::zero(Currency::USDT())
            };
            let after = if current.is_open() {
                native_reservation(cash, current, &definition.instrument, collateral)?
            } else if let Some(row) = row.filter(|_| !terminal && !current.is_inflight()) {
                reported_signed_reservation(row)?
            } else {
                Money::zero(Currency::USDT())
            };
            let Some(locked) = requested
                .checked_sub(before)
                .filter(|value| *value >= Money::zero(Currency::USDT()))
                .and_then(|value| value.checked_add(after))
            else {
                return Ok(None);
            };
            Ok::<_, anyhow::Error>(Some((Arc::clone(base), state.resource_revision, locked)))
        })()?;
        if let Some((base, revision, locked)) = projection {
            if self.publish_projection(&base, revision, locked, &[], Some(core))? {
                Ok(true)
            } else {
                self.project_on_core(core)
            }
        } else {
            self.project_on_core(core)
        }
    }

    fn project_with_core(
        &self,
        core: Option<&nautilus_live::ExecutionClientCore>,
    ) -> anyhow::Result<bool> {
        // Order/definition changes during network IO do not invalidate chain cash.
        // Compute locks from the latest Native view after collection. Confirmed fills
        // still fence the block below, and the final revision check fences publication.
        let (base, revision, latest_fill, instruments, mut orders, terminal) = {
            let state = self.sync.state.lock().expect("account sync poisoned");
            let Some(base) = state
                .confirmed
                .as_ref()
                .filter(|base| base.epoch == self.connection_epoch.load(Ordering::Acquire))
            else {
                return Ok(false);
            };
            if state.projected_revision == Some(state.resource_revision) {
                return Ok(true);
            }
            if let Some(core) = core {
                let applied = core
                    .cache()
                    .account_ref(&self.config.account_id)
                    .and_then(|account| account.last_event().map(|event| event.event_id));
                // The final publish fence remains mandatory. A known pending
                // event cannot be overtaken, so do not first rebuild all locks.
                if state.last_event != applied {
                    return Ok(false);
                }
            }
            (
                Arc::clone(base),
                state.resource_revision,
                state.latest_fill,
                self.instruments.load(),
                {
                    let orders = self.orders.lock().expect("order snapshots poisoned");
                    #[cfg(test)]
                    FULL_ORDER_READS
                        .with(|count| count.set(count.get() + state.active_orders.len()));
                    state
                        .active_orders
                        .iter()
                        .filter_map(|(id, registered)| {
                            orders
                                .get(id)
                                .map(|order| (Arc::clone(order), *registered, None))
                        })
                        .collect::<Vec<_>>()
                },
                base.rows
                    .keys()
                    .copied()
                    .filter(|hash| {
                        state
                            .confirmed_terminal
                            .contains(&VenueOrderId::new(format!("{hash:#x}")))
                    })
                    .collect::<HashSet<_>>(),
            )
        };
        // Do not wait for a command's registry while holding account sync: Native
        // observers also need that lock before strategy callbacks. The final revision
        // fence rejects a projection if Native reservations changed during this read.
        {
            let mut market_buys = orders.iter_mut().filter(|(order, _, _)| {
                order.order_type() == nautilus_model::enums::OrderType::Market
                    && order.order_side() == nautilus_model::enums::OrderSide::Buy
            });
            if let Some(first) = market_buys.next() {
                let pending = self.pending.lock().expect("submission registry poisoned");
                for (order, _, collateral) in std::iter::once(first).chain(market_buys) {
                    *collateral = pending
                        .entries
                        .get(&order.client_order_id())
                        .and_then(|entry| entry.market_buy_collateral);
                }
            }
        }
        let snapshot = &base.balance;

        // A lagging RPC block must not undo collateral changes of already confirmed native fills.
        let newest_fill = latest_fill;
        if snapshot.block_timestamp < newest_fill {
            return Err(BalanceBlockLag.into());
        }
        let currency = Currency::USDT();
        let balance = reported_balance(snapshot, Money::zero(currency))?;
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
        // Only a nonempty remote OPEN baseline needs Native hash correlation.
        if !base.rows.is_empty() {
            for (order, registered, _) in &orders {
                if let Some(hash) = order
                    .venue_order_id()
                    .or(*registered)
                    .and_then(|id| id.as_str().parse::<B256>().ok())
                {
                    anyhow::ensure!(
                        native_by_hash.insert(hash, order).is_none(),
                        "Predict native order context has duplicate venue hashes"
                    );
                }
            }
        }
        let mut locked = Money::zero(currency);
        let mut routes = Vec::new();
        let mut represented = HashSet::new();
        for (&hash, row) in base.rows.iter() {
            if terminal.contains(&hash) {
                continue;
            }
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
                let instrument = &instrument.instrument;
                let InstrumentAny::BinaryOption(binary) = instrument else {
                    anyhow::bail!("Predict reservations require native binary instruments");
                };
                parse::validate_rest_order_terms_for_hash(
                    row,
                    order,
                    binary,
                    self.config.account_id,
                    self.config.session.account_address(),
                    VenueOrderId::new(format!("{hash:#x}")),
                )?;
                routes.push((
                    VenueOrderId::new(format!("{hash:#x}")),
                    orders::parse_id(&row.id)?,
                ));
                if order.is_open() {
                    native_reservation(
                        &cash,
                        order,
                        instrument,
                        Some(crate::common::parse::exact_wei(
                            row.order.fields.maker_amount,
                        )?),
                    )?
                } else {
                    // Cash RiskEngine retains the local Submitted reservation until
                    // OPEN/terminal. Reporting it locked too would deduct it twice.
                    Money::zero(currency)
                }
            } else {
                // Unmanaged or stale external BUYs still consume capacity. The full signed maker
                // budget is conservative and independent of ambiguous REST filled-amount units.
                reported_signed_reservation(row)?
            };
            locked = locked
                .checked_add(reservation)
                .ok_or_else(|| anyhow::anyhow!("Predict reservation aggregate overflow"))?;
        }
        for (order, registered, market_buy_collateral) in
            orders.iter().filter(|(order, _, _)| order.is_open())
        {
            if !represented.is_empty()
                && order
                    .venue_order_id()
                    .or(*registered)
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
                .checked_add(native_reservation(
                    &cash,
                    order,
                    &instrument.instrument,
                    *market_buy_collateral,
                )?)
                .ok_or_else(|| anyhow::anyhow!("Predict reservation aggregate overflow"))?;
        }
        self.publish_projection(&base, revision, locked, &routes, core)
    }

    fn publish_projection(
        &self,
        base: &Arc<ConfirmedAccount>,
        revision: u64,
        locked: Money,
        routes: &[(VenueOrderId, alloy::primitives::U256)],
        core: Option<&nautilus_live::ExecutionClientCore>,
    ) -> anyhow::Result<bool> {
        let snapshot = &base.balance;
        let epoch = base.epoch;
        let factory = OrderEventFactory::new(
            self.emitter.trader_id(),
            self.config.account_id,
            AccountType::Cash,
            None,
        );
        let balance = reported_balance(snapshot, locked)?;
        let info = Params::new(
            [
                ("predict_connection_epoch".into(), serde_json::json!(epoch)),
                (
                    "predict_block_number".into(),
                    serde_json::json!(snapshot.block_number),
                ),
                (
                    "predict_block_hash".into(),
                    serde_json::json!(format!("{:#x}", snapshot.block_hash)),
                ),
                (
                    "predict_account_address".into(),
                    serde_json::json!(format!("{:#x}", snapshot.account_address)),
                ),
                (
                    "predict_balance_wei".into(),
                    serde_json::json!(snapshot.balance_wei.to_string()),
                ),
                (
                    "predict_chain_id".into(),
                    serde_json::json!(snapshot.environment.chain_id()),
                ),
                (
                    "predict_requested_lock".into(),
                    serde_json::json!(locked.as_decimal().to_string()),
                ),
            ]
            .into(),
        );
        let event = factory.generate_account_state(
            vec![balance],
            vec![],
            true,
            self.clock.get_time_ns().max(snapshot.ts_observed),
            self.clock.get_time_ns(),
            Some(info),
        );
        let mut state = self.sync.state.lock().expect("account sync poisoned");
        anyhow::ensure!(
            self.connection_epoch.load(Ordering::Acquire) == epoch
                && (self.allow_stream_gap || self.authentication.is_authenticated()),
            "Predict wallet connection changed during account collection"
        );
        if state.resource_revision != revision
            || !state
                .confirmed
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, base))
        {
            self.sync.notify.notify_one();
            return Ok(false);
        }
        if state.projected_revision == Some(revision) {
            return Ok(true);
        }
        if let Some((number, hash)) = state.block {
            anyhow::ensure!(
                snapshot.block_number >= number
                    && (snapshot.block_number != number || snapshot.block_hash == hash),
                "Predict account snapshot block regressed or changed at the same height"
            );
        }
        if let Some(core) = core {
            let applied = core
                .cache()
                .account_ref(&self.config.account_id)
                .and_then(|account| account.last_event().map(|event| event.event_id));
            if state.last_event != applied {
                return Ok(false);
            }
        }
        orders::register_many(&self.rest_ids, routes)?;
        let event_id = event.event_id;
        self.emitter.try_send_account_state(event)?;
        state.record_projection(Arc::clone(base), event_id);
        Ok(true)
    }
}

// On-chain USDT has 18 decimals; Native fixed-point may have fewer. Floor total at the
// account boundary so representable free funds never exceed the chain balance. Keep the full
// raw amount in AccountState info for exact diagnostics.
pub(super) fn reported_balance(
    snapshot: &crate::blockchain::models::PredictBalanceSnapshot,
    locked: Money,
) -> anyhow::Result<nautilus_model::types::AccountBalance> {
    let precision = u32::from(nautilus_model::types::fixed::FIXED_PRECISION);
    anyhow::ensure!(precision <= 18, "unsupported native USDT precision");
    let unit = alloy::primitives::U256::from(10u64.pow(18 - precision));
    let mut represented = snapshot.clone();
    represented.balance_wei -= represented.balance_wei % unit;
    let total = parse_balance_snapshot(&represented, Money::zero(Currency::USDT()))?;
    // Native cash clamps overcommitted capacity to total and reports zero free funds. Preserve
    // the uncapped request in event info instead of rejecting otherwise valid funding snapshots.
    let locked = locked.clamp(Money::zero(Currency::USDT()), total.total);
    let free = total
        .total
        .checked_sub(locked)
        .ok_or_else(|| anyhow::anyhow!("Predict reported free balance exceeds native range"))?;
    nautilus_model::types::AccountBalance::new_checked(total.total, locked, free)
        .map_err(|_| anyhow::anyhow!("Predict reported balance exceeds native monetary range"))
}

pub(super) async fn collect_open_orders(
    config: &PredictExecutionClientConfig,
) -> anyhow::Result<Vec<PredictOrderData>> {
    let timing = config
        .latency
        .as_ref()
        .map(|probe| probe.timer(crate::latency::LatencyMetric::OpenCollection));
    let result = config
        .session
        .collect_open_orders(config.report_page_size, config.max_report_pages, || {
            if let Some(probe) = &config.latency {
                probe.count(crate::latency::LatencyCounter::OpenPageRequests);
            }
        })
        .await;
    if let Some(timing) = timing {
        timing.finish();
    }
    result
}

pub(super) async fn run(
    reporter: AccountReporter,
    cancellation: CancellationToken,
    active: Arc<AtomicBool>,
    refreshed_epoch: Arc<AtomicU64>,
    refresh_required: Arc<AtomicBool>,
) {
    run_account(
        reporter,
        &cancellation,
        &active,
        &refreshed_epoch,
        &refresh_required,
    )
    .await;
}

async fn run_account(
    mut reporter: AccountReporter,
    cancellation: &CancellationToken,
    active: &Arc<AtomicBool>,
    refreshed_epoch: &AtomicU64,
    refresh_required: &AtomicBool,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(
        reporter.config.account_refresh_interval_secs,
    ));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    loop {
        let periodic = tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            _ = reporter.sync.notify.notified() => false,
            _ = interval.tick() => true,
        };
        // Only explicit Native QueryAccount may read through an unacknowledged wallet gap.
        // The request counters survive IO and coalesce with the same account collector.
        reporter.allow_stream_gap = {
            let state = reporter.sync.state.lock().expect("account sync poisoned");
            state.open_requested != state.open_collected
        };
        if !reporter.authentication.is_authenticated() && !reporter.allow_stream_gap {
            continue;
        }
        let Some(_guard) = AccountQueryGuard::acquire(active) else {
            continue;
        };
        // Wallet success time can slightly follow its mined block timestamp. Retry this
        // read-only causal lag briefly so cash does not wait for the normal funding poll.
        for attempt in 0..5 {
            let result = tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                result = async {
                    if periodic || reporter.needs_remote() {
                        let snapshot = reporter.collect_account(periodic).await?;
                        if let Some(snapshot) = &snapshot
                            && reporter.allow_stream_gap
                            && reporter.connection_epoch.load(Ordering::Acquire) == snapshot.epoch
                        {
                            refreshed_epoch.store(snapshot.epoch, Ordering::Release);
                            refresh_required.store(false, Ordering::Release);
                        }
                        Ok(snapshot.is_some())
                    } else {
                        reporter.project()
                    }
                } => result,
            };
            match result {
                Err(error)
                    if (error.is::<BalanceBlockLag>()
                        || error.is::<crate::blockchain::rpc::PredictBlockChanged>())
                        && attempt < 4 =>
                {
                    let retry = tokio::time::sleep(Duration::from_secs(1));
                    tokio::pin!(retry);
                    loop {
                        tokio::select! {
                            biased;
                            () = cancellation.cancelled() => return,
                            _ = &mut retry => break,
                            _ = reporter.sync.notify.notified() => {
                                if let Err(error) = reporter.project() {
                                    log::debug!("Predict local account projection deferred: {error}");
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    log::warn!(
                        "Predict account refresh failed; retaining the last reported state: {error}"
                    );
                    break;
                }
                Ok(false) if reporter.allow_stream_gap && attempt < 4 => continue,
                Ok(_) => break,
            }
        }
    }
}

impl crate::execution::PredictExecutionClient {
    pub(super) fn query_account_command(&self, cmd: QueryAccount) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.account_id == self.core.account_id
                && cmd.trader_id == self.core.trader_id
                && cmd.client_id.is_none_or(|id| id == self.core.client_id),
            "Predict account query identity differs from execution client"
        );
        anyhow::ensure!(
            self.config.account_state_mode == PredictAccountStateMode::Reported
                && self.core.is_connected()
                && self.tasks.is_open(),
            "Predict reported account client generation is not active"
        );
        self.prune_mirrors_on_request();
        if self.account_refresh_required.load(Ordering::Acquire) || !self.is_connected() {
            self.account_refresh_required.store(true, Ordering::Release);
        }
        self.account_sync.request_remote();
        Ok(())
    }
}

/// Marks the single account collector active so maintenance does not dispatch redundant queries.
struct AccountQueryGuard(Arc<AtomicBool>);
impl AccountQueryGuard {
    fn acquire(active: &Arc<AtomicBool>) -> Option<Self> {
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

// Reported cash uses Native leaves notional; command admission does not audit funds.

/// Validate the supported OPEN resource view before account reporting.
/// These are original signed amounts, not remaining quantity: partial fills do not
/// reduce them. A zero amount cannot describe a supported priced order. Refuse an
/// unaccountable resource snapshot rather than treating it as available capacity.
pub(super) fn open_order_hash(row: &PredictOrderData) -> anyhow::Result<B256> {
    anyhow::ensure!(
        row.status == PredictOrderStatus::Open
            && row.currency == "USDT"
            && !row.order.fields.maker_amount.is_zero()
            && !row.order.fields.taker_amount.is_zero(),
        "Predict open-order snapshot has an invalid status, currency or zero signed amount"
    );
    row.order
        .hash
        .ok_or_else(|| anyhow::anyhow!("Predict open order lacks its exchange hash"))
}

fn signed_collateral(row: &PredictOrderData) -> anyhow::Result<Decimal> {
    match row.order.fields.side {
        PredictOrderSide::Buy => exact_wei(row.order.fields.maker_amount),
        PredictOrderSide::Sell => Ok(Decimal::ZERO),
    }
}

/// Unknown/market BUY budgets are not reduced using ambiguous REST filled-amount units.
pub(super) fn reported_signed_reservation(row: &PredictOrderData) -> anyhow::Result<Money> {
    external_reservation(signed_collateral(row)?)
}

fn native_buy_price(order: &impl context::OrderView) -> anyhow::Result<Price> {
    if order.order_type() == OrderType::Market {
        // No signed budget is available: binary BUY cost is bounded by one USDT/share.
        Ok(Price::from("1"))
    } else {
        order.price().ok_or_else(|| {
            anyhow::anyhow!("Predict local BUY reservation requires an explicit price")
        })
    }
}

#[cfg(test)]
pub(super) fn native_inventory(order: &impl context::OrderView) -> Decimal {
    if order.order_side() == OrderSide::Sell {
        order.leaves_qty().as_decimal()
    } else {
        Decimal::ZERO
    }
}

pub(super) fn native_reservation(
    cash: &CashAccount,
    order: &impl context::OrderView,
    instrument: &InstrumentAny,
    market_buy_collateral: Option<Decimal>,
) -> anyhow::Result<Money> {
    anyhow::ensure!(
        matches!(instrument, InstrumentAny::BinaryOption(_)) && !order.is_quote_quantity(),
        "Predict reservations require native base-quantity binary orders"
    );
    if order.order_side() == OrderSide::Sell {
        return Ok(Money::zero(Currency::USDT()));
    }
    if order.order_type() == OrderType::Market {
        if let Some(collateral) = market_buy_collateral.or_else(|| order.quote_buy_budget()) {
            // Native filled/leaves quantities are gross shares. The immutable signed
            // budget bounds the unfilled fraction; REST amountFilled units are unused.
            let quantity = crate::common::parse::decimal_to_wei(order.quantity().as_decimal())?;
            let leaves = crate::common::parse::decimal_to_wei(order.leaves_qty().as_decimal())?;
            let budget = crate::common::parse::decimal_to_wei(collateral)?;
            let numerator = budget
                .checked_mul(leaves)
                .and_then(|value| value.checked_add(quantity - alloy::primitives::U256::from(1)))
                .ok_or_else(|| anyhow::anyhow!("Predict MARKET reservation overflow"))?;
            return external_reservation(exact_wei(numerator / quantity)?);
        }
        // Recovery can observe a MARKET before the authenticated OPEN snapshot. With
        // no original signed budget in the native order, reserve the binary maximum
        // of one USDT/share. Never infer the budget from a current/average quote.
        return external_reservation(order.leaves_qty().as_decimal());
    }
    let price = native_buy_price(order)?;
    let reservation = cash.calculate_balance_locked(
        instrument,
        order.order_side(),
        order.leaves_qty(),
        price,
        None,
    )?;
    anyhow::ensure!(
        reservation.currency == Currency::USDT()
            && reservation.currency.precision == Currency::USDT().precision,
        "Predict native reservation has an unexpected currency scale"
    );
    Ok(reservation)
}

fn external_reservation(amount: Decimal) -> anyhow::Result<Money> {
    let currency = Currency::USDT();
    let amount = amount.round_dp_with_strategy(
        u32::from(currency.precision),
        rust_decimal::RoundingStrategy::ToPositiveInfinity,
    );
    Money::from_decimal(amount, currency)
        .map_err(|_| anyhow::anyhow!("Predict external reservation exceeds native money range"))
}

#[cfg(test)]
#[path = "../../tests/unit/resources.rs"]
mod reservation_tests;

#[cfg(test)]
#[path = "../../tests/unit/account.rs"]
mod tests;
