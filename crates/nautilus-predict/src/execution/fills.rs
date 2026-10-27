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

//! Venue settlement evidence and queued delivery; Native owns applied quantities.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use alloy::primitives::{B256, U256};
use nautilus_core::UnixNanos;
#[cfg(test)]
use nautilus_model::orders::{Order, OrderAny};
use nautilus_model::{
    identifiers::{AccountId, TradeId, VenueOrderId},
    reports::FillReport,
    types::{Currency, Money, Quantity},
};
use rust_decimal::Decimal;

use crate::execution::{context, parse};
use crate::{
    common::parse::{decimal_to_wei, floor_wei_quantity},
    execution::{
        context::WorkerOrder,
        observations::{PredictSettlementStatus, PredictSettlementUpdate},
    },
    http::account_models::PredictFeeType,
    websocket::private_messages::{
        PredictWalletEvent, PredictWalletEventKind, PredictWalletFill, PredictWalletTransactionKind,
    },
};

// Preserve the established reconciliation tolerance for historical projected holdings.
// Exact atomic fees, raw settlement identity and gross order quantities never use
// this tolerance; their validation remains exact.
pub(super) const POSITION_RECONCILIATION_TOLERANCE: Decimal =
    Decimal::from_parts(1, 0, 0, false, 12);

#[cfg(test)]
thread_local! {
    static COMMIT_BATCH_IDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) type SharedFillEvidence = Arc<Mutex<FillEvidence>>;

/// Venue facts stay in the adapter, alongside (not inside) the standard native report.
/// This follows Polymarket's BufferedFill/private metadata boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct BufferedFill {
    pub report: FillReport,
    pub raw: RawFillFacts,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RawFillFacts {
    pub settlement_id: String,
    pub gross_shares: U256,
    pub price: U256,
    pub fee: U256,
    pub fee_type: PredictFeeType,
    pub transaction_hash: Option<B256>,
}

impl BufferedFill {
    pub fn from_wallet(
        report: FillReport,
        event: &crate::websocket::private_messages::PredictWalletEvent,
    ) -> anyhow::Result<Self> {
        use crate::websocket::private_messages::PredictWalletEventKind;
        let PredictWalletEventKind::OrderTransactionSuccess {
            settlement_id,
            fill,
            fee,
            is_maker,
        } = &event.kind
        else {
            anyhow::bail!("Predict raw fill requires a confirmed wallet settlement");
        };
        let (amount, fee_type) = match fee {
            Some(fee) => (fee.amount_wei, fee.fee_type),
            None if *is_maker == Some(true) => (U256::ZERO, PredictFeeType::Collateral),
            None => anyhow::bail!("Predict fill lacks fee evidence"),
        };
        Ok(Self {
            report,
            raw: RawFillFacts {
                settlement_id: settlement_id.clone(),
                gross_shares: fill.executed_size_wei,
                price: fill.executed_price_wei,
                fee: amount,
                fee_type,
                transaction_hash: None,
            },
        })
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct FillEvidence {
    // Native owns applied allocations; its owner retention/purge also bounds these facts.
    entries: ahash::AHashMap<TradeId, BufferedFill>,
    pub notifications: SettlementNotifications,
    by_order: ahash::AHashMap<VenueOrderId, OrderEvidence>,
    queued: ahash::AHashMap<VenueOrderId, HashMap<TradeId, Option<nautilus_core::UnixNanos>>>,
}

// Reuses immutable committed facts only; Native still supplies allocated quantity.
// No memo survives evidence cloning, Native history replacement or owner retirement.
#[derive(Debug, Default)]
struct OrderEvidence {
    ids: HashSet<TradeId>,
    prefix: Option<VerifiedPrefix>,
}
impl Clone for OrderEvidence {
    fn clone(&self) -> Self {
        Self {
            ids: self.ids.clone(),
            prefix: None,
        }
    }
}
#[derive(Debug)]
struct VerifiedPrefix {
    cursor: context::NativeFillCursor,
    // Retirement can prove complete ID membership without reading economic fields.
    // Normalization must still compute raw totals before reusing this prefix.
    raw: Option<U256>,
    collateral_fee: U256,
}

// One operation's raw prefix, not a retained balance or order model. Each Native
// trade ID is visited once; queued and incoming allocations extend it in order.
#[derive(Default)]
struct FillPrefix {
    ids: HashSet<TradeId>,
    allocated: Decimal,
    raw: U256,
    collateral_fee: U256,
    complete: bool,
}
impl FillPrefix {
    fn capture<'a>(
        order: Option<&impl context::OrderView>,
        raw: &impl Fn(TradeId) -> Option<&'a RawFillFacts>,
    ) -> anyhow::Result<Self> {
        let mut prefix = Self {
            complete: true,
            ..Self::default()
        };
        if let Some(order) = order {
            prefix.allocated = order.filled_qty().as_decimal();
            // Native's applied-ID index is already unique. Do not copy it into
            // another membership set; only queued/incoming facts need local dedup.
            order.for_each_trade_id(|id| prefix.include_raw(raw(id)))?;
        }
        Ok(prefix)
    }
    fn capture_committed(
        order: &impl context::OrderView,
        entries: &ahash::AHashMap<TradeId, BufferedFill>,
        mut memo: Option<&mut Option<VerifiedPrefix>>,
    ) -> anyhow::Result<Self> {
        let previous = memo
            .as_deref()
            .and_then(Option::as_ref)
            .filter(|prefix| prefix.raw.is_some());
        let cursor = previous.map(|prefix| prefix.cursor.clone());
        let mut prefix = Self {
            allocated: order.filled_qty().as_decimal(),
            raw: previous.and_then(|p| p.raw).unwrap_or(U256::ZERO),
            collateral_fee: previous.map_or(U256::ZERO, |p| p.collateral_fee),
            complete: true,
            ..Self::default()
        };
        let mut next = order.for_each_trade_id_since(cursor.as_ref(), |id| {
            prefix.include_raw(entries.get(&id).map(|fill| &fill.raw))
        })?;
        if cursor.is_some() && next.is_none() {
            prefix.raw = U256::ZERO;
            prefix.collateral_fee = U256::ZERO;
            prefix.complete = true;
            next = order.for_each_trade_id_since(None, |id| {
                prefix.include_raw(entries.get(&id).map(|fill| &fill.raw))
            })?;
        }
        if let Some(memo) = memo.as_mut() {
            **memo = next
                .filter(|_| prefix.complete)
                .map(|cursor| VerifiedPrefix {
                    cursor,
                    raw: Some(prefix.raw),
                    collateral_fee: prefix.collateral_fee,
                });
        }
        Ok(prefix)
    }
    fn include(
        &mut self,
        id: TradeId,
        quantity: Decimal,
        raw: Option<&RawFillFacts>,
    ) -> anyhow::Result<()> {
        if !self.ids.insert(id) {
            return Ok(());
        }
        self.include_allocation(quantity, raw)
    }
    fn include_allocation(
        &mut self,
        quantity: Decimal,
        raw: Option<&RawFillFacts>,
    ) -> anyhow::Result<()> {
        self.allocated = self
            .allocated
            .checked_add(quantity)
            .ok_or_else(|| anyhow::anyhow!("Predict allocated quantity overflow"))?;
        self.include_raw(raw)
    }
    fn include_raw(&mut self, raw: Option<&RawFillFacts>) -> anyhow::Result<()> {
        if let Some(raw) = raw {
            self.raw = self
                .raw
                .checked_add(raw.gross_shares)
                .ok_or_else(|| anyhow::anyhow!("Predict raw cumulative quantity overflow"))?;
            if raw.fee_type == PredictFeeType::Collateral {
                self.collateral_fee = self
                    .collateral_fee
                    .checked_add(raw.fee)
                    .ok_or_else(|| anyhow::anyhow!("Predict raw currency fee overflow"))?;
            }
        } else {
            self.complete = false;
        }
        Ok(())
    }
    fn validate(&self, order: &impl context::OrderView) -> anyhow::Result<()> {
        let quantity = order.quantity().as_decimal();
        let raw_quantity = decimal_to_wei(quantity)?;
        anyhow::ensure!(
            self.raw <= decimal_to_wei(order.submitted_quantity().as_decimal())?,
            "Predict raw fills exceed original submitted order quantity"
        );
        anyhow::ensure!(
            self.allocated <= quantity,
            "Predict normalized fills exceed cached native order quantity"
        );
        if self.allocated == quantity {
            anyhow::ensure!(
                self.complete
                    && (self.raw == raw_quantity
                        || order.status() == nautilus_model::enums::OrderStatus::Filled
                            && order.quantity() != order.submitted_quantity()),
                "Predict full fill requires complete raw evidence or verified terminal normalization"
            );
        }
        Ok(())
    }
}

fn same_facts(left: &BufferedFill, right: &BufferedFill) -> bool {
    let (a, b) = (&left.report, &right.report);
    a.account_id == b.account_id
        && a.instrument_id == b.instrument_id
        && a.venue_order_id == b.venue_order_id
        && a.order_side == b.order_side
        && a.last_px == b.last_px
        && (a.commission == b.commission
            || (left.raw.fee_type == PredictFeeType::Collateral
                && right.raw.fee_type == PredictFeeType::Collateral))
        && a.quantity_commission == b.quantity_commission
        && a.liquidity_side == b.liquidity_side
        && left.raw.gross_shares == right.raw.gross_shares
        && left.raw.fee == right.raw.fee
        && (left.raw.fee.is_zero() || left.raw.fee_type == right.raw.fee_type)
        && left.raw.price == right.raw.price
}

impl FillEvidence {
    /// Recovery's cross-IO comparison snapshot contains only relevant raw facts,
    /// not another account's orders or custom-data notification/queue state.
    #[cfg(test)]
    pub fn snapshot_for_orders(&self, owners: impl Iterator<Item = VenueOrderId>) -> Self {
        let mut snapshot = Self::default();
        for owner in owners {
            if let Some(ids) = self.by_order.get(&owner) {
                for id in &ids.ids {
                    if let Some(fill) = self.entries.get(id) {
                        snapshot.entries.insert(*id, fill.clone());
                    }
                }
                snapshot.by_order.insert(owner, ids.clone());
            }
        }
        snapshot
    }

    /// Native has purged these terminal orders and every retained fill was applied.
    /// Other orders keep their full atomic prefix, including queued settlements.
    pub fn retire_orders(&mut self, retired: &HashSet<VenueOrderId>) {
        self.notifications.retire_orders(retired);
        for hash in retired {
            self.queued.remove(hash);
            if let Some(ids) = self.by_order.remove(hash) {
                for id in ids.ids {
                    self.entries.remove(&id);
                }
            }
        }
    }

    /// Borrow this owner's queued, not-yet-applied facts at the caller's order cutoff.
    /// The Native observer removes applied markers; restored snapshots may already
    /// include a queued ID, which must not be counted twice.
    pub fn pending_for<'a>(
        &'a self,
        order: &'a impl context::OrderView,
        hash: VenueOrderId,
    ) -> impl Iterator<Item = &'a BufferedFill> {
        self.queued.get(&hash).into_iter().flat_map(|pending| {
            pending.keys().filter_map(|id| {
                if order.has_applied_fill(*id) {
                    None
                } else {
                    self.entries.get(id)
                }
            })
        })
    }

    /// Mark only successfully published reports, never a read-only REST collection.
    pub fn queue(&mut self, fills: &[BufferedFill]) -> anyhow::Result<()> {
        self.commit(fills)?;
        for fill in fills {
            self.queued
                .entry(fill.report.venue_order_id)
                .or_default()
                .entry(fill.report.trade_id)
                .or_insert(None);
        }
        Ok(())
    }

    pub fn queue_wallet(&mut self, fill: &BufferedFill) -> anyhow::Result<()> {
        self.commit(std::slice::from_ref(fill))?;
        self.queued
            .entry(fill.report.venue_order_id)
            .or_default()
            .insert(fill.report.trade_id, Some(fill.report.ts_event));
        Ok(())
    }

    pub fn validate_wallet_timestamp(&self, fill: &BufferedFill) -> anyhow::Result<()> {
        if let Some(Some(timestamp)) = self
            .queued
            .get(&fill.report.venue_order_id)
            .and_then(|pending| pending.get(&fill.report.trade_id))
        {
            anyhow::ensure!(
                *timestamp == fill.report.ts_event,
                "Predict wallet settlement replay timestamp changed"
            );
        }
        Ok(())
    }

    pub fn applied(&mut self, trade_id: TradeId) {
        self.notifications.applied(trade_id);
        if let Some(fill) = self.entries.get(&trade_id) {
            let id = fill.report.venue_order_id;
            if let Some(pending) = self.queued.get_mut(&id) {
                pending.remove(&trade_id);
                if pending.is_empty() {
                    self.queued.remove(&id);
                }
            }
        }
    }

    /// Reuse complete ID membership only for the same immutable Native history.
    /// Economic totals remain optional; new facts and replaced histories are rechecked.
    pub fn has_unapplied(&mut self, owner: VenueOrderId, order: &context::WorkerOrder) -> bool {
        self.notifications.has_pending(owner)
            || self.by_order.get_mut(&owner).is_some_and(|facts| {
                if facts.ids.len() == order.fill_count()
                    && facts
                        .prefix
                        .as_ref()
                        .is_some_and(|prefix| prefix.cursor.is_current(order))
                {
                    return false;
                }
                let Some(cursor) = order.applied_fill_cursor(facts.ids.iter().copied()) else {
                    return true;
                };
                if facts.ids.len() == order.fill_count() {
                    facts.prefix = Some(VerifiedPrefix {
                        cursor,
                        raw: None,
                        collateral_fee: U256::ZERO,
                    });
                }
                false
            })
    }

    pub fn retirement_work(&self, owner: VenueOrderId) -> usize {
        self.by_order
            .get(&owner)
            .map_or(0, |facts| facts.ids.len())
            .saturating_add(self.notifications.retirement_work(owner))
    }

    /// Prepares allocations without advancing evidence. Call commit only after the whole
    /// REST batch validates, or after a wallet report was successfully queued.
    #[cfg(test)]
    pub fn prepare<O: context::OrderView>(
        &self,
        reports: &mut [BufferedFill],
        orders: &HashMap<VenueOrderId, O>,
        queued: &[BufferedFill],
        precision: impl Fn(VenueOrderId) -> u8,
    ) -> anyhow::Result<()> {
        self.prepare_with_orders(
            reports,
            |id| orders.get(&id),
            orders.iter().map(|(id, order)| (*id, order)),
            queued,
            precision,
        )
        .map(|_| ())
    }

    /// Own wallet messages borrow Native's immutable snapshot. Return true for an
    /// already applied/queued settlement; never reallocate an existing raw fact.
    pub fn prepare_wallet(
        &mut self,
        fill: &mut BufferedFill,
        order: &impl context::OrderView,
        precision: u8,
    ) -> anyhow::Result<bool> {
        if let Some(previous) = self.entries.get(&fill.report.trade_id) {
            anyhow::ensure!(
                same_facts(previous, fill),
                "Predict settlement replay contains conflicting raw fill facts"
            );
            fill.report.last_qty = previous.report.last_qty;
            fill.report.commission = previous.report.commission;
            if parse::validate_applied_fill(&fill.report, order)?
                || self
                    .queued
                    .get(&fill.report.venue_order_id)
                    .is_some_and(|pending| pending.contains_key(&fill.report.trade_id))
            {
                return Ok(true);
            }
        }
        let id = fill.report.venue_order_id;
        let raw = |trade| {
            self.entries
                .get(&trade)
                .map(|fill| &fill.raw)
                .or_else(|| (trade == fill.report.trade_id).then_some(&fill.raw))
        };
        let mut prefix = FillPrefix::capture_committed(
            order,
            &self.entries,
            self.by_order.get_mut(&id).map(|facts| &mut facts.prefix),
        )?;
        if !prefix.complete {
            // The incoming replay may fill a raw hole, but uncommitted facts never
            // enter the reusable memo even when the operation later fails.
            prefix = FillPrefix::capture(Some(order), &raw)?;
        }
        // queued is already unique by trade ID and pending_for excludes Native
        // applied IDs. Do not rebuild that membership set in the local prefix.
        for previous in self.pending_for(order, id) {
            prefix
                .include_allocation(previous.report.last_qty.as_decimal(), Some(&previous.raw))?;
        }
        let applied = self.prepare_fill(fill, Some(order), &mut prefix, precision)?;
        prefix.validate(order)?;
        Ok(applied)
    }

    pub(super) fn prepare_with_orders<'a, O: context::OrderView + 'a>(
        &self,
        reports: &mut [BufferedFill],
        lookup: impl Fn(VenueOrderId) -> Option<&'a O>,
        orders: impl Iterator<Item = (VenueOrderId, &'a O)>,
        queued: &[BufferedFill],
        precision: impl Fn(VenueOrderId) -> u8,
    ) -> anyhow::Result<bool> {
        // Borrow raw inputs once. Prefixes live only for this operation; Native still
        // owns allocations and no derived prefix survives an epoch/cache change.
        let incoming = reports
            .iter()
            .map(|fill| (fill.report.trade_id, &fill.raw))
            .collect::<HashMap<_, _>>();
        let queued_raw = queued
            .iter()
            .map(|fill| (fill.report.trade_id, &fill.raw))
            .collect::<HashMap<_, _>>();
        let raw = |id| {
            incoming
                .get(&id)
                .copied()
                .or_else(|| self.entries.get(&id).map(|fill| &fill.raw))
                .or_else(|| queued_raw.get(&id).copied())
        };
        let mut prefixes = HashMap::new();
        for (id, order) in orders {
            prefixes.insert(id, FillPrefix::capture(Some(order), &raw)?);
        }
        for report in reports.iter() {
            let id = report.report.venue_order_id;
            if let std::collections::hash_map::Entry::Vacant(entry) = prefixes.entry(id) {
                entry.insert(FillPrefix::capture(lookup(id), &raw)?);
            }
        }
        for fill in queued {
            if let Some(prefix) = prefixes.get_mut(&fill.report.venue_order_id)
                && !lookup(fill.report.venue_order_id)
                    .is_some_and(|order| order.has_applied_fill(fill.report.trade_id))
            {
                prefix.include(
                    fill.report.trade_id,
                    fill.report.last_qty.as_decimal(),
                    raw(fill.report.trade_id),
                )?;
            }
        }
        // The prefixes own sums and IDs, so input borrows end before normalization.
        let mut all_applied = true;
        for report in reports.iter_mut() {
            let id = report.report.venue_order_id;
            let prefix = prefixes.get_mut(&id).expect("prefix installed");
            all_applied &= self.prepare_fill(report, lookup(id), prefix, precision(id))?;
        }
        for (id, prefix) in &prefixes {
            if let Some(order) = lookup(*id) {
                prefix.validate(order)?;
            }
        }
        Ok(all_applied)
    }

    fn prepare_fill(
        &self,
        fill: &mut BufferedFill,
        order: Option<&impl context::OrderView>,
        prefix: &mut FillPrefix,
        dp: u8,
    ) -> anyhow::Result<bool> {
        let id = fill.report.trade_id;
        if let Some(order) = order {
            order.with_applied_fill(id, |applied| self.normalize_fill(fill, applied, prefix, dp))
        } else {
            self.normalize_fill(fill, None, prefix, dp)
        }
    }

    fn normalize_fill(
        &self,
        fill: &mut BufferedFill,
        applied: Option<&nautilus_model::events::OrderFilled>,
        prefix: &mut FillPrefix,
        dp: u8,
    ) -> anyhow::Result<bool> {
        anyhow::ensure!(
            dp <= 18,
            "Predict quantity precision exceeds atomic precision"
        );
        let amount = fill.raw.gross_shares;
        let atomic = crate::common::parse::exact_wei_quantity(amount)?;
        let retained_quantity = self
            .entries
            .get(&fill.report.trade_id)
            .map(|previous| previous.report.last_qty)
            .or_else(|| applied.map(|previous| previous.last_qty));
        // Only existing projected allocations select the legacy carry path. New
        // orders retain actual quantities independently of their fee denomination.
        let projected_prefix = !prefix.complete && !prefix.allocated.is_zero()
            || prefix.complete && crate::common::parse::exact_wei(prefix.raw)? != prefix.allocated;
        let projected_fill = retained_quantity.is_some_and(|quantity| quantity != atomic);
        if fill.report.quantity_commission.is_some() || !projected_prefix && !projected_fill {
            let unit = U256::from(10u64.pow(18 - u32::from(dp)));
            let quantity = if amount % unit == U256::ZERO {
                floor_wei_quantity(amount, dp)?
            } else {
                crate::common::parse::exact_wei_quantity(amount)?
            };
            anyhow::ensure!(!quantity.is_zero(), "Predict confirmed fill has zero size");
            if let Some(previous) = self.entries.get(&fill.report.trade_id) {
                anyhow::ensure!(
                    same_facts(previous, fill),
                    "Predict settlement replay contains conflicting raw fill facts"
                );
                anyhow::ensure!(
                    previous.report.last_qty == quantity,
                    "Predict retained gross quantity differs from atomic settlement"
                );
            }
            fill.report.last_qty = quantity;
            if fill.raw.fee_type == PredictFeeType::Collateral {
                self.normalize_collateral_commission(fill, applied, prefix)?;
            }
            if let Some(applied) = applied {
                parse::validate_native_fill(&fill.report, applied)?;
            } else {
                prefix.include(fill.report.trade_id, quantity.as_decimal(), Some(&fill.raw))?;
            }
            return Ok(applied.is_some());
        }
        let unit = U256::from(10u64.pow(18 - u32::from(dp)));
        let floor = floor_wei_quantity(amount, dp)?.as_decimal();
        let ceiling = floor
            + if amount % unit == U256::ZERO {
                Decimal::ZERO
            } else {
                Decimal::new(1, u32::from(dp))
            };
        if let Some(previous) = self.entries.get(&fill.report.trade_id) {
            anyhow::ensure!(
                same_facts(previous, fill),
                "Predict settlement replay contains conflicting raw fill facts"
            );
            fill.report.last_qty = previous.report.last_qty;
        } else if let Some(applied) = applied {
            anyhow::ensure!(
                applied.last_qty.as_decimal() >= floor && applied.last_qty.as_decimal() <= ceiling,
                "Predict applied quantity is outside the atomic normalization interval"
            );
            fill.report.last_qty = applied.last_qty;
        } else {
            let delta = if prefix.complete {
                let total = prefix
                    .raw
                    .checked_add(amount)
                    .ok_or_else(|| anyhow::anyhow!("Predict raw cumulative quantity overflow"))?;
                floor_wei_quantity(total, dp)?
                    .as_decimal()
                    .checked_sub(prefix.allocated)
                    .ok_or_else(|| anyhow::anyhow!("Predict cumulative normalization overflow"))?
            } else {
                anyhow::ensure!(
                    amount % unit == U256::ZERO,
                    "Predict subprecision fill requires complete raw settlement prefix; reconcile REST history"
                );
                floor
            };
            anyhow::ensure!(
                delta > Decimal::ZERO,
                "Predict fill is below native quantity quantum or has inconsistent prefix"
            );
            anyhow::ensure!(
                delta >= floor && delta <= ceiling,
                "Predict cumulative prefix is inconsistent with native allocation"
            );
            fill.report.last_qty = Quantity::from_decimal_dp(delta, dp)?;
        }
        if fill.raw.fee_type == PredictFeeType::Collateral {
            self.normalize_collateral_commission(fill, applied, prefix)?;
        }
        if let Some(applied) = applied {
            parse::validate_native_fill(&fill.report, applied)?;
        }
        anyhow::ensure!(
            fill.report.last_qty.as_decimal().round_dp(u32::from(dp))
                == fill.report.last_qty.as_decimal(),
            "Predict instrument precision differs from retained settlement allocation"
        );
        anyhow::ensure!(
            !fill.report.last_qty.is_zero(),
            "Predict fill is below native quantity quantum"
        );
        if applied.is_none() {
            prefix.include(
                fill.report.trade_id,
                fill.report.last_qty.as_decimal(),
                Some(&fill.raw),
            )?;
        }
        Ok(applied.is_some())
    }

    fn normalize_collateral_commission(
        &self,
        fill: &mut BufferedFill,
        applied: Option<&nautilus_model::events::OrderFilled>,
        prefix: &FillPrefix,
    ) -> anyhow::Result<()> {
        let currency = Currency::USDT();
        anyhow::ensure!(
            fill.report.commission.currency == currency,
            "Predict collateral fee currency differs from USDT"
        );
        if let Some(previous) = self.entries.get(&fill.report.trade_id) {
            fill.report.commission = previous.report.commission;
            return Ok(());
        }
        let amount = crate::common::parse::exact_wei(fill.raw.fee)?;
        if let Some(previous) = applied {
            let floor = amount.round_dp_with_strategy(
                u32::from(currency.precision),
                rust_decimal::RoundingStrategy::ToZero,
            );
            let ceiling = amount.round_dp_with_strategy(
                u32::from(currency.precision),
                rust_decimal::RoundingStrategy::ToPositiveInfinity,
            );
            let allocated = previous
                .commission
                .ok_or_else(|| anyhow::anyhow!("Predict applied fill lacks currency fee"))?;
            anyhow::ensure!(
                allocated.currency == currency
                    && allocated.as_decimal() >= floor
                    && allocated.as_decimal() <= ceiling,
                "Predict applied currency fee lies outside its atomic normalization interval"
            );
            fill.report.commission = allocated;
            return Ok(());
        }
        if let Ok(exact) = Money::from_decimal_exact(amount, currency) {
            fill.report.commission = exact;
            return Ok(());
        }
        anyhow::ensure!(
            prefix.complete,
            "Predict subprecision collateral fee requires complete raw settlement prefix; reconcile REST history"
        );
        let native_money = |amount| {
            Money::from_decimal_exact(
                crate::common::parse::exact_wei(amount)?
                    .round_dp(u32::from(nautilus_model::types::fixed::FIXED_PRECISION)),
                currency,
            )
            .map_err(anyhow::Error::from)
        };
        let total = prefix
            .collateral_fee
            .checked_add(fill.raw.fee)
            .ok_or_else(|| anyhow::anyhow!("Predict raw currency fee overflow"))?;
        fill.report.commission = native_money(total)? - native_money(prefix.collateral_fee)?;
        anyhow::ensure!(
            fill.report.commission.as_decimal() >= Decimal::ZERO,
            "Predict normalized fee is negative"
        );
        Ok(())
    }

    #[cfg(test)]
    pub fn confirmed_raw(
        &mut self,
        order: &impl context::OrderView,
        owner: VenueOrderId,
    ) -> anyhow::Result<Option<U256>> {
        let prefix = FillPrefix::capture_committed(
            order,
            &self.entries,
            order
                .venue_order_id()
                .and_then(|id| self.by_order.get_mut(&id))
                .map(|facts| &mut facts.prefix),
        )?;
        if !prefix.complete {
            return Ok(None);
        }
        let mut total = prefix.raw;
        for fill in self.pending_for(order, owner) {
            total = total
                .checked_add(fill.raw.gross_shares)
                .ok_or_else(|| anyhow::anyhow!("Predict raw cumulative quantity overflow"))?;
        }
        Ok(Some(total))
    }

    pub fn validate_commit(&self, reports: &[BufferedFill]) -> anyhow::Result<()> {
        let mut seen = HashMap::new();
        for report in reports {
            let committed = self.entries.get(&report.report.trade_id);
            if let Some(old) = committed.or_else(|| seen.get(&report.report.trade_id).copied()) {
                anyhow::ensure!(
                    same_facts(old, report)
                        && old.report.last_qty == report.report.last_qty
                        && old.report.commission == report.report.commission,
                    "Predict settlement allocation changed during replay"
                );
            }
            // Committed IDs already have an immutable reference to validate against.
            // Only unseen IDs in a multi-fill batch need a temporary duplicate index.
            if reports.len() > 1 && committed.is_none() {
                seen.insert(report.report.trade_id, report);
                #[cfg(test)]
                COMMIT_BATCH_IDS.with(|n| n.set(n.get() + 1));
            }
        }
        Ok(())
    }

    pub fn commit(&mut self, reports: &[BufferedFill]) -> anyhow::Result<()> {
        self.validate_commit(reports)?;
        for report in reports {
            if let std::collections::hash_map::Entry::Vacant(entry) =
                self.entries.entry(report.report.trade_id)
            {
                self.by_order
                    .entry(report.report.venue_order_id)
                    .or_default()
                    .ids
                    .insert(report.report.trade_id);
                entry.insert(report.clone());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct Notice {
    order: VenueOrderId,
    settlement_id: String,
    fill: PredictWalletFill,
    submitted: bool,
    terminal: Option<PredictSettlementStatus>,
    applied: bool,
}
#[derive(Debug, Default, Clone)]
pub(super) struct SettlementNotifications {
    entries: HashMap<TradeId, Notice>,
    by_order: HashMap<VenueOrderId, HashSet<TradeId>>,
}
impl SettlementNotifications {
    pub fn validate_success(
        &self,
        event: &PredictWalletEvent,
        trade_id: TradeId,
        venue_order_id: VenueOrderId,
    ) -> anyhow::Result<()> {
        if let PredictWalletEventKind::OrderTransactionSuccess {
            settlement_id,
            fill,
            ..
        } = &event.kind
            && let Some(previous) = self.entries.get(&trade_id)
        {
            anyhow::ensure!(
                previous.order == venue_order_id
                    && previous.settlement_id == *settlement_id
                    && previous.fill == *fill
                    && previous.terminal != Some(PredictSettlementStatus::Failed),
                "Predict confirmed settlement contradicts prior matching facts"
            );
        }
        Ok(())
    }
    pub fn retire_orders(&mut self, ids: &HashSet<VenueOrderId>) {
        for owner in ids {
            if let Some(trades) = self.by_order.remove(owner) {
                for id in trades {
                    self.entries.remove(&id);
                }
            }
        }
    }
    pub fn retirement_work(&self, owner: VenueOrderId) -> usize {
        self.by_order.get(&owner).map_or(0, HashSet::len)
    }
    pub fn has_pending(&self, owner: VenueOrderId) -> bool {
        self.by_order.get(&owner).is_some_and(|ids| {
            ids.iter().any(|id| {
                self.entries
                    .get(id)
                    .is_some_and(|entry| !entry.applied && entry.terminal.is_none())
            })
        })
    }
    pub fn applied(&mut self, id: TradeId) {
        if let Some(entry) = self.entries.get_mut(&id) {
            entry.applied = true;
        }
    }
    #[cfg(test)]
    pub fn pending_orders(&self) -> impl Iterator<Item = VenueOrderId> + '_ {
        self.entries
            .values()
            .filter(|entry| !entry.applied && entry.terminal.is_none())
            .map(|entry| entry.order)
    }
    pub fn update(
        &mut self,
        event: &PredictWalletEvent,
        order: &WorkerOrder,
        account: AccountId,
        epoch: u64,
        observed: UnixNanos,
    ) -> anyhow::Result<Option<PredictSettlementUpdate>> {
        let (status, settlement_id, fill) = match &event.kind {
            PredictWalletEventKind::OrderTransactionSubmitted {
                kind,
                settlement_id,
                fill,
            } => {
                anyhow::ensure!(
                    matches!(
                        (kind, order.order_side()),
                        (
                            PredictWalletTransactionKind::Purchase,
                            nautilus_model::enums::OrderSide::Buy
                        ) | (
                            PredictWalletTransactionKind::Sale,
                            nautilus_model::enums::OrderSide::Sell
                        )
                    ),
                    "Predict submitted settlement direction differs from order"
                );
                (PredictSettlementStatus::Submitted, settlement_id, fill)
            }
            PredictWalletEventKind::OrderTransactionSuccess {
                settlement_id,
                fill,
                ..
            } => (PredictSettlementStatus::Confirmed, settlement_id, fill),
            PredictWalletEventKind::OrderTransactionFailed {
                settlement_id,
                fill,
            } => (PredictSettlementStatus::Failed, settlement_id, fill),
            _ => return Ok(None),
        };
        let hash = VenueOrderId::new(format!("{:#x}", event.order.order_hash));
        let id = parse::settlement_trade_id(settlement_id, event.order.order_hash);
        anyhow::ensure!(
            !settlement_id.is_empty()
                && !fill.executed_size_wei.is_zero()
                && fill.executed_size_wei
                    <= crate::common::parse::decimal_to_wei(order.quantity().as_decimal())?
                && fill.executed_price_wei <= U256::from(1_000_000_000_000_000_000u64)
                && fill
                    .executed_price_wei
                    .checked_mul(fill.executed_size_wei)
                    .is_some_and(|product| product / U256::from(1_000_000_000_000_000_000u64)
                        == fill.executed_value_wei),
            "Predict settlement notification has inconsistent atomic amounts"
        );
        let applied = order.has_applied_fill(id);
        anyhow::ensure!(
            status != PredictSettlementStatus::Failed || !applied,
            "Predict failed settlement contradicts Native applied fill"
        );
        let timestamp = event.order.timestamp_ns()?;
        let entry = self.entries.entry(id).or_insert_with(|| Notice {
            order: hash,
            settlement_id: settlement_id.clone(),
            fill: fill.clone(),
            submitted: false,
            terminal: None,
            applied,
        });
        anyhow::ensure!(
            entry.order == hash && entry.settlement_id == *settlement_id && entry.fill == *fill,
            "Predict settlement notification raw facts changed"
        );
        self.by_order.entry(hash).or_default().insert(id);
        let publish = match status {
            PredictSettlementStatus::Submitted => {
                let publish = !entry.submitted && entry.terminal.is_none() && !entry.applied;
                entry.submitted = true;
                publish
            }
            terminal => {
                anyhow::ensure!(
                    entry.terminal.is_none_or(|previous| previous == terminal),
                    "Predict settlement terminal status changed"
                );
                let publish = entry.terminal.is_none();
                entry.terminal = Some(terminal);
                publish
            }
        };
        if !publish {
            return Ok(None);
        }
        Ok(Some(PredictSettlementUpdate {
            account_id: account,
            strategy_id: order.strategy_id(),
            instrument_id: order.instrument_id(),
            client_order_id: order.client_order_id(),
            venue_order_id: hash,
            trade_id: id,
            settlement_id: settlement_id.clone(),
            status,
            executed_price_wei: fill.executed_price_wei.to_string(),
            executed_size_wei: fill.executed_size_wei.to_string(),
            executed_value_wei: fill.executed_value_wei.to_string(),
            connection_epoch: epoch,
            ts_event: timestamp,
            ts_init: observed,
        }))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/settlement.rs"]
mod notification_tests;

#[cfg(test)]
#[path = "../../tests/unit/precision.rs"]
mod tests;
