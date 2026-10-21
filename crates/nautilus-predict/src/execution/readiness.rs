// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.
//! Read-only verification of already-applied native state before releasing a stream-gap gate.
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::Ordering},
};

use nautilus_common::messages::execution::GenerateFillReports;
use nautilus_core::UUID4;
use nautilus_model::{
    accounts::{Account, AccountAny, cash::CashAccount},
    enums::{OrderStatus, OrderType, PositionSide},
    identifiers::{InstrumentId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    types::{Currency, Money},
};
use rust_decimal::Decimal;

use super::{PredictExecutionClient, account::AccountReporter, matches::MatchContext};

#[cfg(test)]
thread_local! {
    pub(super) static SNAPSHOT_CAPTURES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) struct ReadinessCheck {
    reporter: AccountReporter,
    context: MatchContext<Arc<super::context::WorkerOrder>>,
    positions: HashMap<InstrumentId, Decimal>,
    pub(super) account: CashAccount,
    revision: u64,
    epoch: u64,
    verified_epoch: Arc<std::sync::atomic::AtomicU64>,
    healthy: Arc<std::sync::atomic::AtomicBool>,
    required: Arc<std::sync::atomic::AtomicBool>,
    pending: super::orders::PendingSubmissions,
    cancels: super::cancellations::PendingCancels,
    evidence: super::precision::SharedFillEvidence,
    hasher: Arc<crate::signing::PredictOrderHasher>,
}
// Compare two fully parsed collections by stable trade identity. Parsing and
// Native validation still establish facts; no retained recovery cache is added.
pub(super) fn verify_match_fence<O: super::context::OrderView>(
    fills: &[super::precision::BufferedFill],
    fence: &[super::precision::BufferedFill],
    orders: &HashMap<VenueOrderId, O>,
) -> anyhow::Result<()> {
    let mut ids = fills
        .iter()
        .map(|fill| fill.report.trade_id)
        .collect::<ahash::AHashSet<_>>();
    anyhow::ensure!(
        ids.len() == fills.len() && fence.len() == fills.len(),
        "Predict readiness match fence changed"
    );
    for fill in fence {
        let order = orders
            .get(&fill.report.venue_order_id)
            .ok_or_else(|| anyhow::anyhow!("Predict readiness match fence has new ownership"))?;
        anyhow::ensure!(
            super::parse::validate_applied_fill(&fill.report, order)?
                && ids.remove(&fill.report.trade_id),
            "Predict readiness match fence changed"
        );
    }
    Ok(())
}

impl ReadinessCheck {
    /// Cheap Native/local command gate, shared by automatic retries and capture.
    /// Retained closed histories are inspected only once these commands resolve.
    pub fn commands_resolved(
        core: &nautilus_live::ExecutionClientCore,
        pending: &super::orders::PendingSubmissions,
        cancels: &super::cancellations::PendingCancels,
    ) -> bool {
        if !cancels.lock().expect("pending cancels poisoned").is_empty() {
            return false;
        }
        let cache = core.cache();
        if cache
            .iter_client_order_ids_inflight(Some(&core.venue), None, None, Some(&core.account_id))
            .next()
            .is_some()
        {
            return false;
        }
        let acknowledged = |id| {
            cache.order_ref(&id).is_some_and(|order| {
                order.is_closed()
                    || (order.ts_accepted().is_some()
                        && matches!(
                            order.status(),
                            OrderStatus::Accepted | OrderStatus::PartiallyFilled
                        )
                        && order.order_type() == OrderType::Limit)
            })
        };
        cache
            .iter_client_order_ids_open(Some(&core.venue), None, None, Some(&core.account_id))
            .all(acknowledged)
            && pending
                .lock()
                .expect("submission registry poisoned")
                .entries
                .keys()
                .copied()
                .all(acknowledged)
    }

    pub fn new(client: &PredictExecutionClient) -> anyhow::Result<Self> {
        anyhow::ensure!(
            Self::commands_resolved(
                &client.core,
                &client.pending_submissions,
                &client.pending_cancels
            ),
            "Predict readiness has unresolved commands"
        );
        #[cfg(test)]
        SNAPSHOT_CAPTURES.with(|n| n.set(n.get() + 1));
        let cache = client.core.cache();
        // Bounded cleanup can leave ordinary retired contexts behind. Only real
        // unapplied/undecided venue evidence can keep recovery gated.
        {
            let orders = client.orders.lock().expect("order snapshots poisoned");
            let mut facts = client.fill_evidence.lock().expect("fill evidence poisoned");
            anyhow::ensure!(
                orders.iter().all(|(hash, order)| {
                    !order.is_closed()
                        || cache.order(&order.client_order_id()).is_some()
                        || !facts.has_unapplied(*hash, order)
                }),
                "Predict readiness has unapplied fills for a purged native order"
            );
        }
        let account = cache
            .account_ref(&client.core.account_id)
            .ok_or_else(|| anyhow::anyhow!("Predict readiness lacks native account"))?;
        let AccountAny::Cash(cash) = &*account else {
            anyhow::bail!("Predict readiness requires a native cash account");
        };
        let mut event = cash
            .last_event()
            .ok_or_else(|| anyhow::anyhow!("Predict readiness lacks account provenance"))?;
        // Native balances may have changed without appending an AccountState.
        // Capture current values and provenance, not the full retained event log.
        event.balances = cash.balances().into_values().collect();
        let snapshot =
            CashAccount::new(event, cash.calculated_account_state(), cash.allow_borrowing);
        drop(account);
        let account = snapshot;
        let orders = cache.iter_client_order_ids(
            Some(&client.core.venue),
            None,
            None,
            Some(&client.core.account_id),
        );
        // Unknown native IDs must not disappear in a filter_map and accidentally permit trading.
        let mut by_hash = HashMap::new();
        let observed = client
            .account_orders
            .lock()
            .expect("order snapshots poisoned");
        for client_id in orders {
            let order = cache.try_order_ref(&client_id)?;
            // REST cannot restore post-only intent. Both regular and post-only LIMITs
            // can remain live; unresolved MARKET executions still require reconciliation.
            anyhow::ensure!(
                order.is_closed()
                    || (matches!(
                        order.status(),
                        OrderStatus::Accepted | OrderStatus::PartiallyFilled
                    ) && order.order_type() == OrderType::Limit),
                "Predict readiness requires terminal orders or acknowledged LIMIT orders"
            );
            // Position reconciliation orders are local native adjustments, never venue orders.
            if order.tags().is_some_and(|tags| {
                tags.iter().any(|tag| {
                    tag.as_str() == nautilus_execution::reconciliation::RECONCILIATION_ORDER_TAG
                })
            }) && order
                .venue_order_id()
                .is_some_and(|id| id.as_str().parse::<alloy::primitives::B256>().is_err())
                && order.status() == OrderStatus::Filled
            {
                continue;
            }
            let Some(id) = order.venue_order_id() else {
                anyhow::ensure!(
                    matches!(order.status(), OrderStatus::Rejected | OrderStatus::Denied)
                        && order.filled_qty().is_zero(),
                    "Predict readiness lacks native venue hash"
                );
                continue;
            };
            anyhow::ensure!(
                by_hash
                    .insert(
                        id,
                        super::context::WorkerOrder::snapshot(&order, observed.get(&client_id))
                    )
                    .is_none(),
                "Predict readiness has duplicate hash ownership"
            );
        }
        drop(observed);
        let mut positions = HashMap::new();
        for p in cache.positions_open(
            Some(&client.core.venue),
            None,
            None,
            Some(&client.core.account_id),
            None,
        ) {
            let qty = match p.side {
                PositionSide::Long => p.quantity.as_decimal(),
                PositionSide::Short => -p.quantity.as_decimal(),
                _ => Decimal::ZERO,
            };
            *positions.entry(p.instrument_id).or_insert(Decimal::ZERO) += qty;
        }
        let mut reserved: HashMap<InstrumentId, Decimal> = HashMap::new();
        for order in by_hash
            .values()
            .filter(|o| !o.is_closed() && o.order_side() == nautilus_model::enums::OrderSide::Sell)
        {
            *reserved.entry(order.instrument_id()).or_default() +=
                super::resources::native_inventory(order);
        }
        for (instrument, quantity) in reserved {
            anyhow::ensure!(
                positions.get(&instrument).copied().unwrap_or_default()
                    + super::precision::POSITION_RECONCILIATION_TOLERANCE
                    >= quantity,
                "Predict readiness active SELL exceeds reconciled net holdings"
            );
        }
        let revision = client
            .account_sync
            .state
            .lock()
            .expect("account sync poisoned")
            .revision;
        Ok(Self {
            reporter: client.account_reporter()?,
            context: MatchContext {
                account_id: client.core.account_id,
                wallet: client.config.session.account_address(),
                shares_allowed: !account.calculated_account_state(),
                orders: by_hash,
                instruments: cache
                    .instruments(&client.core.venue, None)
                    .into_iter()
                    .map(|i| (i.id(), i.clone()))
                    .collect(),
            },
            positions,
            account,
            revision,
            epoch: client
                .wallet_epoch
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Predict readiness lacks epoch"))?
                .load(Ordering::Acquire),
            verified_epoch: client.verified_epoch.clone(),
            healthy: client.healthy.clone(),
            required: client.reconciliation_required.clone(),
            pending: client.pending_submissions.clone(),
            cancels: client.pending_cancels.clone(),
            evidence: client.fill_evidence.clone(),
            hasher: client.order_hasher.clone(),
        })
    }
    fn pending_resolved(&self) -> bool {
        self.pending
            .lock()
            .expect("submission registry poisoned")
            .entries
            .iter()
            .all(|(client, hash)| {
                hash.venue_id
                    .and_then(|id| self.context.orders.get(&id))
                    .is_some_and(|order| {
                        order.client_order_id() == *client
                            && !order.is_closed()
                            && order.ts_accepted().is_some()
                    })
            })
    }
    fn verify_open<'a>(
        &self,
        rows: impl Iterator<Item = &'a crate::http::account_models::PredictOrderData>,
    ) -> anyhow::Result<()> {
        let expected = self
            .context
            .orders
            .iter()
            .filter(|(_, o)| !o.is_closed())
            .map(|(id, _)| *id)
            .collect::<HashSet<_>>();
        let mut seen = HashSet::new();
        for row in rows {
            anyhow::ensure!(
                row.status == crate::http::account_models::PredictOrderStatus::Open,
                "Predict readiness OPEN contains a terminal row"
            );
            let hash = row
                .order
                .hash
                .ok_or_else(|| anyhow::anyhow!("Predict readiness OPEN lacks hash"))?;
            let id = VenueOrderId::new(format!("{hash:#x}"));
            anyhow::ensure!(
                expected.contains(&id) && seen.insert(id),
                "Predict readiness found unclaimed or duplicate venue OPEN order"
            );
            let hasher = &self.hasher;
            anyhow::ensure!(
                hasher.order_hash(
                    &row.order.fields,
                    crate::common::contracts::PredictExchangeType::from_market_flags(
                        row.is_neg_risk,
                        row.is_yield_bearing
                    )
                ) == hash,
                "Predict readiness OPEN signed fields differ from hash"
            );
            let order = &self.context.orders[&id];
            let Some(InstrumentAny::BinaryOption(instrument)) =
                self.context.instruments.get(&order.instrument_id())
            else {
                anyhow::bail!("Predict readiness lacks OPEN instrument");
            };
            super::parse::validate_rest_order_terms(
                row,
                order,
                instrument,
                self.context.account_id,
                self.context.wallet,
            )?;
        }
        anyhow::ensure!(
            seen == expected,
            "Predict readiness OPEN coverage differs from native active orders"
        );
        Ok(())
    }
    pub async fn confirm(
        self,
        snapshot: Arc<super::account::ConfirmedAccount>,
    ) -> anyhow::Result<()> {
        let stage = std::sync::atomic::AtomicUsize::new(0);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(self.reporter.config.report_timeout_secs),
            self.verify(&stage, &snapshot),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Predict readiness verification timed out"))
        .and_then(std::convert::identity);
        result.map_err(|error| {
            // Cold-path failure context only; preserve the original error for downcasting.
            let (revision, latest_fill) = {
                let sync = self.reporter.sync.state.lock().expect("account sync poisoned");
                (sync.revision, sync.latest_fill)
            };
            let name = ["session", "account_identity", "open", "matches", "settlement_coverage",
                "holdings", "balance", "open_fence", "match_fence", "native_fence"][stage.load(Ordering::Relaxed)];
            let context = format!("Predict recovery stage={name} epoch={}/{} revision={}/{} latest_fill={} native_orders={} native_positions={}: {error:#}",
                self.epoch, self.reporter.connection_epoch.load(Ordering::Acquire), self.revision, revision,
                latest_fill, self.context.orders.len(), self.positions.len());
            error.context(context)
        })
    }
    async fn verify(
        &self,
        stage: &std::sync::atomic::AtomicUsize,
        snapshot: &super::account::ConfirmedAccount,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.healthy.load(Ordering::Acquire)
                && self.reporter.authentication.is_authenticated()
                && self.reporter.connection_epoch.load(Ordering::Acquire) == self.epoch,
            "Predict readiness requires a fresh uninterrupted wallet session"
        );
        anyhow::ensure!(
            self.pending_resolved()
                && self
                    .cancels
                    .lock()
                    .expect("pending cancels poisoned")
                    .is_empty(),
            "Predict readiness has unresolved commands"
        );
        let cfg = &self.reporter.config;
        // Pagination limits bound actual REST collections, not how many Native
        // owners can be retained in the configured cache window.
        stage.store(1, Ordering::Relaxed);
        cfg.session.request_account().await?;
        stage.store(2, Ordering::Relaxed);
        anyhow::ensure!(
            snapshot.epoch == self.epoch,
            "Predict readiness OPEN belongs to another wallet epoch"
        );
        let open = &snapshot.rows;
        self.verify_open(open.values().map(AsRef::as_ref))?;
        stage.store(3, Ordering::Relaxed);
        let rows = super::matches::collect_matches(cfg, None).await?;
        // Verify settlements for native-owned orders. Historical flat cycles do not
        // become new strategy activity; current OPEN orders and holdings are fenced below.
        let staged = Arc::new(std::sync::Mutex::new(
            self.evidence
                .lock()
                .map_err(|_| anyhow::anyhow!("Predict evidence poisoned"))?
                .snapshot_for_orders(self.context.orders.keys().copied()),
        ));
        stage.store(4, Ordering::Relaxed);
        let now = self.reporter.clock.get_time_ns();
        let scope = GenerateFillReports::new(UUID4::new(), now, None, None, None, None, None, None);
        let fills =
            super::matches::parse_cached_match_reports(rows, now, &self.context, &scope, &staged)?;
        let mut unapplied = Vec::new();
        let mut applied_quantities: HashMap<VenueOrderId, Decimal> = HashMap::new();
        for fill in &fills {
            if super::parse::validate_applied_fill(
                &fill.report,
                &self.context.orders[&fill.report.venue_order_id],
            )? {
                let quantity = applied_quantities
                    .entry(fill.report.venue_order_id)
                    .or_default();
                *quantity = quantity
                    .checked_add(fill.report.last_qty.as_decimal())
                    .ok_or_else(|| anyhow::anyhow!("Predict readiness fill quantity overflow"))?;
            } else {
                unapplied.push(fill.clone());
            }
        }
        if !unapplied.is_empty() {
            // A definitive REST cancellation can close an order during a WS gap before
            // its settlement arrives. Native inflight checks no longer cover that order.
            // Reuse the confirmed match facts already collected here; Native still owns
            // applying late fills, positions, deduplication and strategy callbacks.
            let state = self
                .reporter
                .sync
                .state
                .lock()
                .expect("account sync poisoned");
            anyhow::ensure!(
                state.revision == self.revision
                    && self.healthy.load(Ordering::Acquire)
                    && self.reporter.authentication.is_authenticated()
                    && self.reporter.connection_epoch.load(Ordering::Acquire) == self.epoch,
                "Predict readiness changed before settlement publication"
            );
            let mut facts = self
                .evidence
                .lock()
                .map_err(|_| anyhow::anyhow!("Predict evidence poisoned"))?;
            facts.validate_commit(&unapplied)?;
            for fill in &unapplied {
                self.reporter.emitter.try_send_execution_report(
                    nautilus_common::messages::ExecutionReport::Fill(Box::new(fill.report.clone())),
                )?;
                facts.queue(std::slice::from_ref(fill))?;
            }
            anyhow::bail!("Predict confirmed settlements queued; wait for Native application");
        }
        // Native owns terminal order states. Confirm their complete applied settlement
        // prefix, including late fills, without querying every retained historical hash.
        // A missing or unapplied fill keeps the gate closed for Native reconciliation.
        for (id, order) in &self.context.orders {
            anyhow::ensure!(
                applied_quantities.get(id).copied().unwrap_or_default()
                    == order.filled_qty().as_decimal(),
                "Predict readiness lacks complete applied settlement coverage"
            );
        }
        // OPEN already contains the signed terms and cumulative fill quantity used by
        // single-order reports. Reuse the standard parser; the second OPEN read below
        // fences changes while matches, holdings and account state are checked.
        let grouped = super::reconciliation::fills_by_order(&fills);
        for row in open.values() {
            let id = VenueOrderId::new(format!(
                "{:#x}",
                row.order.hash.expect("verified OPEN hash")
            ));
            let order = &self.context.orders[&id];
            let Some(InstrumentAny::BinaryOption(instrument)) =
                self.context.instruments.get(&order.instrument_id())
            else {
                anyhow::bail!("Predict readiness lacks binary instrument");
            };
            let report = super::reconciliation::status_report(
                row,
                order,
                instrument,
                &self.context,
                &self.hasher,
                grouped.get(&id).map_or(&[], Vec::as_slice),
                now,
            )?;
            anyhow::ensure!(
                report.order_status == order.status() && report.filled_qty == order.filled_qty(),
                "Predict readiness order state differs from applied native state"
            );
        }
        stage.store(5, Ordering::Relaxed);
        let holdings = super::reports::collect_positions(cfg, None).await?;
        let mut venue_positions = HashMap::new();
        for row in holdings {
            let id = crate::common::instrument_id_from_outcome(
                row.market.id,
                u8::try_from(row.outcome.index_set)?,
            )?;
            let Some(InstrumentAny::BinaryOption(i)) = self.context.instruments.get(&id) else {
                anyhow::bail!("Predict readiness has unknown holding instrument");
            };
            let report = crate::http::parse::parse_position_status_report(
                &row,
                i,
                self.context.account_id,
                now,
            )?;
            anyhow::ensure!(
                venue_positions
                    .insert(id, report.signed_decimal_qty)
                    .is_none(),
                "Predict readiness has duplicate holdings"
            );
        }
        let keys = self
            .positions
            .keys()
            .chain(venue_positions.keys())
            .copied()
            .collect::<HashSet<_>>();
        for id in keys {
            anyhow::ensure!(
                (self.positions.get(&id).copied().unwrap_or_default()
                    - venue_positions.get(&id).copied().unwrap_or_default())
                .abs()
                    <= super::precision::POSITION_RECONCILIATION_TOLERANCE,
                "Predict readiness net position mismatch"
            );
        }
        stage.store(6, Ordering::Relaxed);
        let balance = self
            .reporter
            .chain
            .request_balance_snapshot(self.context.wallet, self.reporter.clock)
            .await?;
        let cash = &self.account;
        // Reuse the verified OPEN response; no per-order scan or new remote read.
        let market_budgets = open
            .values()
            .map(|row| {
                let hash = row
                    .order
                    .hash
                    .ok_or_else(|| anyhow::anyhow!("Predict readiness OPEN lacks hash"))?;
                Ok((
                    VenueOrderId::new(format!("{hash:#x}")),
                    crate::common::parse::exact_wei(row.order.fields.maker_amount)?,
                ))
            })
            .collect::<anyhow::Result<std::collections::HashMap<_, _>>>()?;
        let mut locked = Money::zero(Currency::USDT());
        for order in self.context.orders.values().filter(|o| !o.is_closed()) {
            let instrument = self
                .context
                .instruments
                .get(&order.instrument_id())
                .ok_or_else(|| anyhow::anyhow!("Predict readiness lacks reservation instrument"))?;
            locked = locked
                .checked_add(super::resources::native_reservation(
                    cash,
                    order,
                    instrument,
                    order
                        .venue_order_id()
                        .and_then(|id| market_budgets.get(&id).copied())
                        .or_else(|| {
                            self.pending
                                .lock()
                                .expect("submission registry poisoned")
                                .entries
                                .get(&order.client_order_id())
                                .and_then(|entry| entry.market_buy_collateral)
                        }),
                )?)
                .ok_or_else(|| {
                    anyhow::anyhow!("Predict readiness reservation aggregate overflow")
                })?;
        }
        let expected = super::account::reported_balance(&balance, locked)?;
        anyhow::ensure!(
            self.account.balance_total(Some(Currency::USDT())) == Some(expected.total)
                && self.account.balance_locked(Some(Currency::USDT())) == Some(expected.locked),
            "Predict readiness cash differs from applied native account: actual={:?}/{:?}, expected={}/{}",
            self.account
                .balance_total(Some(Currency::USDT()))
                .map(|m| m.as_decimal()),
            self.account
                .balance_locked(Some(Currency::USDT()))
                .map(|m| m.as_decimal()),
            expected.total.as_decimal(),
            expected.locked.as_decimal()
        );
        let latest_fill = self
            .reporter
            .sync
            .state
            .lock()
            .expect("account sync poisoned")
            .latest_fill;
        let newest = self
            .context
            .orders
            .values()
            .map(super::account::latest_venue_fill)
            .max()
            .unwrap_or_default()
            .max(latest_fill);
        anyhow::ensure!(
            balance.block_timestamp >= newest,
            "Predict readiness balance block predates confirmed settlement: block_time={}, newest_fill={}",
            balance.block_timestamp,
            newest
        );
        stage.store(7, Ordering::Relaxed);
        let fence = super::account::collect_open_orders(cfg).await?;
        self.verify_open(fence.iter())?;
        let by_id = fence
            .iter()
            .map(|row| (row.id.as_str(), row))
            .collect::<HashMap<_, _>>();
        anyhow::ensure!(
            open.len() == fence.len()
                && by_id.len() == fence.len()
                && open.values().all(|row| by_id
                    .get(row.id.as_str())
                    .is_some_and(|current| super::reconciliation::same_order(row, current))),
            "Predict readiness OPEN fence changed"
        );
        stage.store(8, Ordering::Relaxed);
        let fence_rows = super::matches::collect_matches(cfg, None).await?;
        let fence_fills = super::matches::parse_cached_match_reports(
            fence_rows,
            self.reporter.clock.get_time_ns(),
            &self.context,
            &scope,
            &staged,
        )?;
        verify_match_fence(&fills, &fence_fills, &self.context.orders)?;
        // Recovery starts a fresh metadata lifetime even if the transport epoch did not change.
        self.reporter.instruments.rcu(|map| {
            let definitions = map
                .values()
                .map(|entry| entry.instrument.clone())
                .collect::<Vec<_>>();
            for instrument in definitions {
                super::types::upsert_instrument(map, instrument);
            }
        });
        // Prepare before publishing readiness; command tasks only read prepared slots.
        super::metadata::prepare(
            &self.reporter.config,
            &self.reporter.instruments,
            self.reporter.clock,
            &self.reporter.connection_epoch,
        )
        .await;
        stage.store(9, Ordering::Relaxed);
        let mut sync = self
            .reporter
            .sync
            .state
            .lock()
            .expect("account sync poisoned");
        anyhow::ensure!(
            sync.revision == self.revision
                && self.healthy.load(Ordering::Acquire)
                && self.reporter.authentication.is_authenticated()
                && self.reporter.connection_epoch.load(Ordering::Acquire) == self.epoch
                && self.pending_resolved()
                && self
                    .cancels
                    .lock()
                    .expect("pending cancels poisoned")
                    .is_empty(),
            "Predict readiness changed during verification"
        );
        anyhow::ensure!(
            sync.block
                .is_none_or(|(number, hash)| balance.block_number >= number
                    && (balance.block_number != number || balance.block_hash == hash)),
            "Predict readiness balance regressed during verification"
        );
        let mut state = self
            .account
            .last_event()
            .ok_or_else(|| anyhow::anyhow!("Predict readiness lacks account provenance"))?;
        state.event_id = UUID4::new();
        state.ts_event = self.reporter.clock.get_time_ns();
        state.ts_init = state.ts_event;
        let info = state.info.get_or_insert_with(Default::default);
        info.insert(
            "predict_connection_epoch".into(),
            serde_json::json!(self.epoch),
        );
        info.insert(
            "predict_reconciliation_ready".into(),
            serde_json::json!(true),
        );
        info.insert(
            "predict_balance_wei".into(),
            serde_json::json!(balance.balance_wei.to_string()),
        );
        info.insert(
            "predict_block_number".into(),
            serde_json::json!(balance.block_number),
        );
        info.insert(
            "predict_block_hash".into(),
            serde_json::json!(format!("{:#x}", balance.block_hash)),
        );
        info.insert(
            "predict_requested_lock".into(),
            serde_json::json!(locked.as_decimal().to_string()),
        );
        let event_id = state.event_id;
        self.verified_epoch.store(self.epoch, Ordering::Release);
        self.required.store(false, Ordering::Release);
        if !self.healthy.load(Ordering::Acquire)
            || !self.reporter.authentication.is_authenticated()
            || self.reporter.connection_epoch.load(Ordering::Acquire) != self.epoch
        {
            self.required.store(true, Ordering::Release);
            anyhow::bail!("Predict readiness session changed at publication");
        }
        if let Err(error) = self.reporter.emitter.try_send_account_state(state) {
            self.required.store(true, Ordering::Release);
            return Err(error);
        }
        // The readiness AccountState is the next applied Native projection too.
        // Keep its provenance and verified cash together for callback-local deltas.
        sync.record_projection(
            Arc::new(super::account::ConfirmedAccount {
                epoch: self.epoch,
                rows: Arc::clone(open),
                balance,
            }),
            event_id,
        );
        log::info!("Predict applied native state verified; stream-gap submission gate released");
        Ok(())
    }
}
