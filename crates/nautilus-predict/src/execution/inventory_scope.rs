// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Per-query venue evidence selection. No retained position state or strategy ownership.

use std::collections::{BTreeMap, HashMap, HashSet};

use alloy::primitives::{Address, B256};
use rust_decimal::Decimal;

use crate::{
    common::parse::exact_wei,
    http::account_models::{
        PredictFeeType, PredictMatchData, PredictPositionData, PredictQuoteType,
    },
};

/// Keep complete known orders and the nearest confirmed inventory prefix explaining
/// each current holding. Older flat cycles without an order owner are not new activity.
/// Group equal timestamps: venue history does not establish an order within that group.
pub(super) fn required_orders(
    matches: &[PredictMatchData],
    positions: &[PredictPositionData],
    wallet: Address,
    known: &HashSet<B256>,
    require_prefix: bool,
) -> anyhow::Result<HashSet<B256>> {
    type Groups = BTreeMap<nautilus_core::UnixNanos, (Decimal, HashSet<B256>, u8)>;
    let mut groups: HashMap<(u64, u32), Groups> = HashMap::new();
    let mut holdings = HashMap::new();
    for position in positions {
        anyhow::ensure!(
            holdings
                .insert(
                    (position.market.id, position.outcome.index_set),
                    exact_wei(position.amount)?
                )
                .is_none(),
            "Predict inventory scope has duplicate outcome holdings"
        );
    }
    let mut seen = HashMap::new();
    for row in matches {
        let timestamp = super::matches::match_timestamp(&row.executed_at)?;
        let mut own = false;
        for leg in std::iter::once(&row.taker)
            .chain(row.makers.iter())
            .filter(|leg| leg.signer == wallet)
        {
            own = true;
            let hash = leg
                .hash
                .ok_or_else(|| anyhow::anyhow!("Predict inventory history lacks order hash"))?;
            let gross = leg.amount.parse::<alloy::primitives::U256>()?;
            anyhow::ensure!(
                !gross.is_zero()
                    && !hash.is_zero()
                    && leg.amount.bytes().all(|b| b.is_ascii_digit()),
                "Predict inventory history has invalid gross quantity or hash"
            );
            let net = if leg.fee.fee_type == PredictFeeType::Shares && !leg.fee.amount.is_zero() {
                anyhow::ensure!(
                    matches!(leg.quote_type, PredictQuoteType::Bid) && leg.fee.amount < gross,
                    "Predict inventory history has invalid share fee"
                );
                gross - leg.fee.amount
            } else {
                gross
            };
            let key = (row.market.id, leg.outcome.index_set);
            let signed = exact_wei(net)?
                * if matches!(leg.quote_type, PredictQuoteType::Bid) {
                    Decimal::ONE
                } else {
                    -Decimal::ONE
                };
            let identity = (
                row.settlement_id
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        anyhow::anyhow!("Predict inventory history lacks settlement identity")
                    })?,
                hash,
            );
            let facts = (
                key,
                signed,
                &leg.amount,
                &leg.price,
                leg.fee.amount,
                leg.fee.fee_type,
                timestamp,
                row.transaction_hash,
            );
            if let Some(previous) = seen.insert(identity, facts) {
                anyhow::ensure!(
                    previous == facts,
                    "Predict inventory history has contradictory duplicate facts"
                );
                continue;
            }
            let group = groups.entry(key).or_default().entry(timestamp).or_default();
            group.0 = group
                .0
                .checked_add(signed)
                .ok_or_else(|| anyhow::anyhow!("Predict inventory sum overflow"))?;
            group.1.insert(hash);
            group.2 |= if matches!(leg.quote_type, PredictQuoteType::Bid) {
                1
            } else {
                2
            };
        }
        anyhow::ensure!(
            own,
            "Predict inventory history is outside authenticated account"
        );
    }
    let mut selected = known.clone();
    for (key, target) in &holdings {
        anyhow::ensure!(
            !require_prefix || target.is_zero() || groups.contains_key(key),
            "Predict current holding lacks confirmed inventory prefix"
        );
    }
    for (key, groups) in groups {
        let mut target = holdings.get(&key).copied().unwrap_or_default();
        let mut current = !target.is_zero();
        let mut quantity = Decimal::ZERO;
        let mut segment = HashSet::new();
        let mut ambiguous = false;
        for (_, (delta, hashes, directions)) in groups.into_iter().rev() {
            quantity = quantity
                .checked_add(delta)
                .ok_or_else(|| anyhow::anyhow!("Predict inventory sum overflow"))?;
            segment.extend(hashes);
            ambiguous |= directions == 3;
            if quantity == target {
                if current || !segment.is_disjoint(known) {
                    anyhow::ensure!(
                        !ambiguous || segment.is_subset(known),
                        "Predict inventory history has ambiguous equal-time buy/sell ownership"
                    );
                    selected.extend(segment.drain());
                }
                segment.clear();
                current = false;
                target = Decimal::ZERO;
                quantity = Decimal::ZERO;
                ambiguous = false;
            }
        }
        if current {
            anyhow::ensure!(
                !require_prefix,
                "Predict current holding lacks complete confirmed inventory prefix"
            );
            // Runtime may already hold wallet fills not yet visible in REST. Keep
            // known orders; do not adopt unknown history without a proven prefix.
            continue;
        }
        // A known partially recovered order must retain its full available history;
        // cumulative/status validation below decides whether coverage is sufficient.
        if !segment.is_disjoint(known) {
            selected.extend(segment);
        }
    }
    // A settlement may contain multiple owned legs. Preserve its participant context
    // rather than silently rewriting an authenticated match into a different event.
    loop {
        let before = selected.len();
        for row in matches {
            let hashes = std::iter::once(&row.taker)
                .chain(row.makers.iter())
                .filter(|leg| leg.signer == wallet)
                .filter_map(|leg| leg.hash)
                .collect::<Vec<_>>();
            if hashes.iter().any(|hash| selected.contains(hash)) {
                selected.extend(hashes);
            }
        }
        if before == selected.len() {
            break;
        }
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn actual_closed_cycles_do_not_become_current_inventory_or_new_strategy_orders() {
        let source: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/private_api/live_fill_20261016T123240Z_independent/calibration.json"
        )).unwrap();
        let rows = source["after"]["matches"].as_array().unwrap();
        let current_buy = source["wallet"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["type"] == "orderTransactionSuccess" && e["details"]["quoteType"] == "BID")
            .unwrap()["orderHash"]
            .as_str()
            .unwrap();
        let current_sell = source["wallet"]["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["type"] == "orderTransactionSuccess" && e["details"]["quoteType"] == "ASK")
            .unwrap()["orderHash"]
            .as_str()
            .unwrap();
        let wallet = rows
            .iter()
            .find(|r| r["taker"]["hash"] == current_buy)
            .unwrap()["taker"]["signer"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let all: Vec<PredictMatchData> = rows
            .iter()
            .cloned()
            .map(|r| serde_json::from_value(r).unwrap())
            .collect();
        assert_eq!(all.len(), 6);
        assert!(
            required_orders(&all, &[], wallet, &HashSet::new(), true)
                .unwrap()
                .is_empty()
        );
        let known = HashSet::from([current_buy.parse().unwrap()]);
        let recovered = required_orders(&all, &[], wallet, &known, true).unwrap();
        assert_eq!(
            recovered,
            HashSet::from([current_buy.parse().unwrap(), current_sell.parse().unwrap()])
        );
        let before_exit = rows
            .iter()
            .filter(|r| r["taker"]["hash"] != current_sell)
            .cloned()
            .collect::<Vec<_>>();
        let current = before_exit
            .iter()
            .find(|r| r["taker"]["hash"] == current_buy)
            .unwrap();
        let mut holding = json!({"id":"derived-current-net","market":current["market"],"outcome":current["taker"]["outcome"],
            "amount":"31360000000000000000","valueUsd":"0","averageBuyPriceUsd":"0","pnlUsd":"0"});
        let mut duplicate = before_exit.clone();
        duplicate.extend(before_exit);
        duplicate.reverse();
        let partial: Vec<_> = duplicate
            .into_iter()
            .map(|r| serde_json::from_value(r).unwrap())
            .collect();
        let positions = [serde_json::from_value(holding.clone()).unwrap()];
        assert_eq!(
            required_orders(&partial, &positions, wallet, &HashSet::new(), true).unwrap(),
            known
        );
        // Runtime REST may lag a known WS fill. Preserve Native ownership without
        // inventing unknown orders; cold-start still requires positive evidence.
        assert_eq!(
            required_orders(&[], &positions, wallet, &known, false).unwrap(),
            known
        );
        assert!(required_orders(&[], &positions, wallet, &known, true).is_err());
        let tied: Vec<PredictMatchData> = rows
            .iter()
            .filter(|r| r["taker"]["hash"] != current_sell)
            .cloned()
            .map(|mut r| {
                r["executedAt"] = json!("2026-10-16T12:00:00Z");
                serde_json::from_value(r).unwrap()
            })
            .collect();
        assert!(required_orders(&tied, &positions, wallet, &HashSet::new(), true).is_err());
        holding["amount"] = json!("31350000000000000000");
        assert!(
            required_orders(
                &partial,
                &[serde_json::from_value(holding).unwrap()],
                wallet,
                &HashSet::new(),
                true
            )
            .is_err()
        );
    }
}
