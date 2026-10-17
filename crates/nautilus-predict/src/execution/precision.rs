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

//! Bounded settlement evidence, not an order/position ledger. Native events own applied quantities.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use alloy::primitives::{B256, U256};
use nautilus_model::{
    events::OrderEventAny,
    identifiers::{TradeId, VenueOrderId},
    orders::{Order, OrderAny},
    reports::FillReport,
    types::{Currency, Money, Quantity},
};
use rust_decimal::Decimal;

use crate::{
    common::parse::{decimal_to_wei, floor_wei_quantity},
    http::account_models::PredictFeeType,
};

// Position's signed_qty is f64 in the pinned native model. This small share tolerance
// avoids repeated synthetic corrections for sub-picounit projection residuals only;
// raw wei and per-order fill quantities are never compared with this tolerance.
pub(super) const POSITION_RECONCILIATION_TOLERANCE: Decimal =
    Decimal::from_parts(1, 0, 0, false, 12);

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
    // No balances/statuses: native orders own applied allocations; this bounded map retains facts.
    entries: HashMap<TradeId, BufferedFill>,
}

fn same_facts(left: &BufferedFill, right: &BufferedFill) -> bool {
    let (a, b) = (&left.report, &right.report);
    a.account_id == b.account_id
        && a.instrument_id == b.instrument_id
        && a.venue_order_id == b.venue_order_id
        && a.order_side == b.order_side
        && a.last_px == b.last_px
        && (a.commission == b.commission
            || (cfg!(feature = "native-quantity-commission")
                && left.raw.fee_type == PredictFeeType::Collateral
                && right.raw.fee_type == PredictFeeType::Collateral))
        && a.quantity_commission == b.quantity_commission
        && a.liquidity_side == b.liquidity_side
        && left.raw.gross_shares == right.raw.gross_shares
        && left.raw.fee == right.raw.fee
        && (left.raw.fee.is_zero() || left.raw.fee_type == right.raw.fee_type)
        && left.raw.price == right.raw.price
}

impl FillEvidence {
    /// Identify retained settlements not covered by the supplied Native trade IDs.
    /// Used only for a Cache-retirement sweep; this is not another applied-ID registry.
    pub fn unapplied_orders(&self, applied: &HashSet<TradeId>) -> HashSet<VenueOrderId> {
        self.entries
            .values()
            .filter(|fill| !applied.contains(&fill.report.trade_id))
            .map(|fill| fill.report.venue_order_id)
            .collect()
    }

    /// Prepares allocations without advancing evidence. Call commit only after the whole
    /// REST batch validates, or after a wallet report was successfully queued.
    pub fn prepare(
        &self,
        reports: &mut [BufferedFill],
        orders: &HashMap<VenueOrderId, OrderAny>,
        queued: &[BufferedFill],
        precision: impl Fn(VenueOrderId) -> u8,
    ) -> anyhow::Result<()> {
        let incoming = reports
            .iter()
            .map(|r| (r.report.trade_id, r.raw.gross_shares))
            .collect::<HashMap<_, _>>();
        let incoming_facts = if cfg!(feature = "native-quantity-commission") {
            reports
                .iter()
                .map(|fill| (fill.report.trade_id, fill.raw.clone()))
                .collect::<HashMap<_, _>>()
        } else {
            HashMap::new()
        };
        let mut planned: Vec<BufferedFill> = Vec::new();
        for report in reports.iter_mut() {
            let dp = precision(report.report.venue_order_id);
            anyhow::ensure!(
                dp <= 18,
                "Predict quantity precision exceeds atomic precision"
            );
            let amount = report.raw.gross_shares;
            let order = orders.get(&report.report.venue_order_id);
            if let Some(previous) = self.entries.get(&report.report.trade_id) {
                anyhow::ensure!(
                    same_facts(previous, report),
                    "Predict settlement replay contains conflicting raw fill facts"
                );
                report.report.last_qty = previous.report.last_qty;
            } else if let Some(fill) = order.and_then(|order| {
                order.events().into_iter().find_map(|event| match event {
                    OrderEventAny::Filled(fill) if fill.trade_id == report.report.trade_id => {
                        Some(fill)
                    }
                    _ => None,
                })
            }) {
                // Reconstruct from complete REST history after a generation change; native
                // applied allocations are immutable. At most one quantum can be carried.
                let floor = floor_wei_quantity(amount, dp)?.as_decimal();
                let quantum = Decimal::new(1, u32::from(dp));
                anyhow::ensure!(
                    fill.last_qty.as_decimal() >= floor
                        && fill.last_qty.as_decimal()
                            <= floor
                                + if amount % U256::from(10u64.pow(18 - u32::from(dp)))
                                    == U256::ZERO
                                {
                                    Decimal::ZERO
                                } else {
                                    quantum
                                },
                    "Predict applied quantity is outside the atomic normalization interval"
                );
                report.report.last_qty = fill.last_qty;
            } else {
                let mut prefix_qty =
                    order.map_or(Decimal::ZERO, |order| order.filled_qty().as_decimal());
                let mut ids: HashSet<TradeId> = order.map_or_else(HashSet::new, |order| {
                    order.trade_ids().into_iter().copied().collect()
                });
                for previous in queued
                    .iter()
                    .chain(planned.iter())
                    .filter(|r| r.report.venue_order_id == report.report.venue_order_id)
                {
                    if ids.insert(previous.report.trade_id) {
                        prefix_qty = prefix_qty
                            .checked_add(previous.report.last_qty.as_decimal())
                            .ok_or_else(|| {
                                anyhow::anyhow!("Predict native cumulative quantity overflow")
                            })?;
                    }
                }
                let mut prefix_raw = U256::ZERO;
                let mut complete = true;
                for id in ids {
                    let raw = incoming
                        .get(&id)
                        .copied()
                        .or_else(|| self.entries.get(&id).map(|r| r.raw.gross_shares))
                        .or_else(|| {
                            queued
                                .iter()
                                .find(|r| r.report.trade_id == id)
                                .map(|r| r.raw.gross_shares)
                        });
                    if let Some(raw) = raw {
                        prefix_raw = prefix_raw.checked_add(raw).ok_or_else(|| {
                            anyhow::anyhow!("Predict raw cumulative quantity overflow")
                        })?;
                    } else {
                        complete = false;
                    }
                }
                let unit = U256::from(10u64.pow(18 - u32::from(dp)));
                let delta = if !complete {
                    anyhow::ensure!(
                        amount % unit == U256::ZERO,
                        "Predict subprecision fill requires complete raw settlement prefix; reconcile REST history"
                    );
                    floor_wei_quantity(amount, dp)?.as_decimal()
                } else {
                    let total = prefix_raw.checked_add(amount).ok_or_else(|| {
                        anyhow::anyhow!("Predict raw cumulative quantity overflow")
                    })?;
                    if let Some(order) = order {
                        anyhow::ensure!(
                            total <= decimal_to_wei(order.quantity().as_decimal())?,
                            "Predict raw fills exceed cached native order quantity"
                        );
                    }
                    floor_wei_quantity(total, dp)?
                        .as_decimal()
                        .checked_sub(prefix_qty)
                        .ok_or_else(|| {
                            anyhow::anyhow!("Predict cumulative normalization overflow")
                        })?
                };
                anyhow::ensure!(
                    delta > Decimal::ZERO,
                    "Predict fill is below native quantity quantum or has inconsistent prefix"
                );
                let floor = floor_wei_quantity(amount, dp)?.as_decimal();
                let ceiling = floor
                    + if amount % unit == U256::ZERO {
                        Decimal::ZERO
                    } else {
                        Decimal::new(1, u32::from(dp))
                    };
                anyhow::ensure!(
                    delta >= floor && delta <= ceiling,
                    "Predict cumulative prefix is inconsistent with native allocation"
                );
                report.report.last_qty = Quantity::from_decimal_dp(delta, dp)?;
            }
            if cfg!(feature = "native-quantity-commission")
                && report.raw.fee_type == PredictFeeType::Collateral
            {
                self.normalize_collateral_commission(
                    report,
                    order,
                    queued,
                    &planned,
                    &incoming_facts,
                )?;
            }
            if let Some(order) = order {
                super::parse::validate_applied_fill(&report.report, order)?;
            }
            anyhow::ensure!(
                report.report.last_qty.as_decimal().round_dp(u32::from(dp))
                    == report.report.last_qty.as_decimal(),
                "Predict instrument precision differs from retained settlement allocation"
            );
            anyhow::ensure!(
                !report.report.last_qty.is_zero(),
                "Predict fill is below native quantity quantum"
            );
            planned.push(report.clone());
        }
        for (venue_id, order) in orders {
            let mut ids: HashSet<_> = order.trade_ids().into_iter().copied().collect();
            ids.extend(
                reports
                    .iter()
                    .chain(queued.iter())
                    .filter(|r| r.report.venue_order_id == *venue_id)
                    .map(|r| r.report.trade_id),
            );
            let mut total = U256::ZERO;
            let mut complete = true;
            for id in ids {
                let raw = incoming
                    .get(&id)
                    .copied()
                    .or_else(|| self.entries.get(&id).map(|r| r.raw.gross_shares))
                    .or_else(|| {
                        queued
                            .iter()
                            .find(|r| r.report.trade_id == id)
                            .map(|r| r.raw.gross_shares)
                    });
                if let Some(raw) = raw {
                    total = total.checked_add(raw).ok_or_else(|| {
                        anyhow::anyhow!("Predict raw cumulative quantity overflow")
                    })?;
                } else {
                    complete = false;
                }
            }
            anyhow::ensure!(
                total <= decimal_to_wei(order.quantity().as_decimal())?,
                "Predict raw fills exceed cached native order quantity"
            );
            let mut known: HashSet<_> = order.trade_ids().into_iter().copied().collect();
            let mut allocated = order.filled_qty().as_decimal();
            for report in queued
                .iter()
                .chain(reports.iter())
                .filter(|r| r.report.venue_order_id == *venue_id)
            {
                if known.insert(report.report.trade_id) {
                    allocated = allocated
                        .checked_add(report.report.last_qty.as_decimal())
                        .ok_or_else(|| anyhow::anyhow!("Predict allocated quantity overflow"))?;
                }
            }
            anyhow::ensure!(
                allocated <= order.quantity().as_decimal(),
                "Predict normalized fills exceed cached native order quantity"
            );
            if allocated == order.quantity().as_decimal() {
                anyhow::ensure!(
                    complete && total == decimal_to_wei(order.quantity().as_decimal())?,
                    "Predict full fill requires complete exact raw settlement quantity"
                );
            }
        }
        self.check_capacity(reports)?;
        Ok(())
    }

    // Protocol normalization only: Native Order retains applied Money allocations, while
    // this generation's existing evidence retains the original atomic fees.
    fn normalize_collateral_commission(
        &self,
        fill: &mut BufferedFill,
        order: Option<&OrderAny>,
        queued: &[BufferedFill],
        planned: &[BufferedFill],
        incoming: &HashMap<TradeId, RawFillFacts>,
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
        if let Some(previous) = order.and_then(|order| {
            order.events().into_iter().find_map(|event| match event {
                OrderEventAny::Filled(previous) if previous.trade_id == fill.report.trade_id => {
                    Some(previous)
                }
                _ => None,
            })
        }) {
            let amount = crate::common::parse::exact_wei(fill.raw.fee)?;
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
        let mut ids: HashSet<_> = order.map_or_else(HashSet::new, |order| {
            order.trade_ids().into_iter().copied().collect()
        });
        let mut allocated = order
            .and_then(|order| order.commissions().get(&currency).copied())
            .unwrap_or_else(|| Money::zero(currency));
        for previous in queued
            .iter()
            .chain(planned)
            .filter(|previous| previous.report.venue_order_id == fill.report.venue_order_id)
        {
            if ids.insert(previous.report.trade_id) {
                allocated = allocated + previous.report.commission;
            }
        }
        let mut raw = U256::ZERO;
        let mut complete = true;
        for id in ids {
            let facts = incoming
                .get(&id)
                .or_else(|| self.entries.get(&id).map(|entry| &entry.raw))
                .or_else(|| {
                    queued
                        .iter()
                        .find(|entry| entry.report.trade_id == id)
                        .map(|entry| &entry.raw)
                });
            if let Some(facts) = facts {
                if facts.fee_type == PredictFeeType::Collateral {
                    raw = raw
                        .checked_add(facts.fee)
                        .ok_or_else(|| anyhow::anyhow!("Predict raw currency fee overflow"))?;
                }
            } else {
                complete = false;
            }
        }
        if !complete {
            let amount = crate::common::parse::exact_wei(fill.raw.fee)?;
            anyhow::ensure!(
                amount.round_dp(u32::from(currency.precision)) == amount,
                "Predict subprecision collateral fee requires complete raw settlement prefix; reconcile REST history"
            );
            fill.report.commission = Money::from_decimal(amount, currency)?;
            return Ok(());
        }
        let prefix = Money::from_decimal(crate::common::parse::exact_wei(raw)?, currency)?;
        anyhow::ensure!(
            prefix == allocated,
            "Predict applied currency fees differ from complete atomic prefix"
        );
        let total = raw
            .checked_add(fill.raw.fee)
            .ok_or_else(|| anyhow::anyhow!("Predict raw currency fee overflow"))?;
        fill.report.commission =
            Money::from_decimal(crate::common::parse::exact_wei(total)?, currency)? - allocated;
        anyhow::ensure!(
            fill.report.commission.as_decimal() >= Decimal::ZERO,
            "Predict normalized fee is negative"
        );
        Ok(())
    }

    pub fn confirmed_raw(
        &self,
        order: &OrderAny,
        queued: &[BufferedFill],
    ) -> anyhow::Result<Option<U256>> {
        let mut ids: HashSet<_> = order.trade_ids().into_iter().copied().collect();
        ids.extend(
            queued
                .iter()
                .filter(|r| {
                    r.report.venue_order_id
                        == order.venue_order_id().unwrap_or(r.report.venue_order_id)
                })
                .map(|r| r.report.trade_id),
        );
        let mut total = U256::ZERO;
        for id in ids {
            let raw = self
                .entries
                .get(&id)
                .or_else(|| queued.iter().find(|r| r.report.trade_id == id));
            let Some(report) = raw else {
                return Ok(None);
            };
            total = total
                .checked_add(report.raw.gross_shares)
                .ok_or_else(|| anyhow::anyhow!("Predict raw cumulative quantity overflow"))?;
        }
        Ok(Some(total))
    }

    fn check_capacity(&self, reports: &[BufferedFill]) -> anyhow::Result<()> {
        let new = reports
            .iter()
            .filter(|r| !self.entries.contains_key(&r.report.trade_id))
            .map(|r| r.report.trade_id)
            .collect::<HashSet<_>>()
            .len();
        anyhow::ensure!(
            self.entries.len() + new <= 10000,
            "Predict raw settlement evidence capacity reached; reconcile in a new generation"
        );
        Ok(())
    }

    pub fn commit(&mut self, reports: &[BufferedFill]) -> anyhow::Result<()> {
        self.check_capacity(reports)?;
        let mut seen = HashMap::new();
        for report in reports {
            if let Some(old) = self
                .entries
                .get(&report.report.trade_id)
                .or_else(|| seen.get(&report.report.trade_id).copied())
            {
                anyhow::ensure!(
                    same_facts(old, report)
                        && old.report.last_qty == report.report.last_qty
                        && old.report.commission == report.report.commission,
                    "Predict settlement allocation changed during replay"
                );
            }
            seen.insert(report.report.trade_id, report);
        }
        for report in reports {
            self.entries
                .entry(report.report.trade_id)
                .or_insert_with(|| report.clone());
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/precision.rs"]
mod tests;
