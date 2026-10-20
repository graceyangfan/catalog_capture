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

//! Private precision conformance and explicit synthetic evidence export.
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{LiquiditySide, OrderSide},
    reports::FillReport,
    types::{Currency, Money, Price},
};

use super::*;
use crate::http::account_models::PredictFeeType;

fn report(raw: U256, id: u32, dp: u8) -> BufferedFill {
    let report = FillReport::new(
        "PREDICT-fixture".into(),
        "42-1.PREDICT".into(),
        "ORDER-1".into(),
        format!("T-{id}").as_str().into(),
        OrderSide::Buy,
        floor_wei_quantity(raw, dp).unwrap(),
        Price::from("0.130"),
        Money::zero(Currency::USDT()),
        LiquiditySide::Taker,
        None,
        None,
        UnixNanos::default(),
        UnixNanos::default(),
        None,
    );
    BufferedFill {
        report,
        raw: RawFillFacts {
            settlement_id: "precision".into(),
            gross_shares: raw,
            price: U256::from(130_000_000_000_000_000u64),
            fee: U256::ZERO,
            fee_type: PredictFeeType::Collateral,
            transaction_hash: None,
        },
    }
}

#[test]
fn cumulative_floor_carries_for_native_precisions_and_freezes_reordered_allocations() {
    let scale = U256::from(1_000_000_000_000_000_000u64);
    for dp in [3, 9, 16] {
        let unit = U256::from(10u64.pow(18 - u32::from(dp)));
        let first = scale * U256::from(17) + unit - U256::from(1);
        let second = scale * U256::from(100) - first;
        let mut reports = vec![report(first, 1, dp), report(second, 2, dp)];
        let mut evidence = FillEvidence::default();
        evidence
            .prepare(&mut reports, &HashMap::new(), &[], |_| dp)
            .unwrap();
        assert_eq!(reports[0].report.last_qty, Quantity::from(17));
        assert_eq!(reports[1].report.last_qty, Quantity::from(83));
        evidence.commit(&reports).unwrap();
        reports.reverse();
        evidence
            .prepare(&mut reports, &HashMap::new(), &[], |_| dp)
            .unwrap();
        assert_eq!(reports[0].report.last_qty, Quantity::from(83));
        assert_eq!(reports[1].report.last_qty, Quantity::from(17));
        assert_eq!(evidence.entries.len(), 2);
    }
}

#[cfg(feature = "native-quantity-commission")]
#[test]
fn collateral_atomic_fees_accumulate_and_duplicate_replay_keeps_allocations() {
    let quantity = U256::from(1_225_000_000_250u64);
    let mut reports = (0..100)
        .map(|id| {
            let mut fill = report(quantity, id, 16);
            fill.report.order_side = OrderSide::Sell;
            fill.report.last_px = Price::from("0.200");
            fill.raw.price = U256::from(200_000_000_000_000_000u64);
            fill.raw.fee_type = PredictFeeType::Collateral;
            fill.raw.fee = U256::from(4_900_000_001u64);
            fill
        })
        .collect::<Vec<_>>();
    let mut evidence = FillEvidence::default();
    evidence
        .prepare(&mut reports, &HashMap::new(), &[], |_| 16)
        .unwrap();
    assert_eq!(
        reports
            .iter()
            .map(|fill| fill.report.commission.as_decimal())
            .sum::<Decimal>(),
        Decimal::from_str_exact("0.0000004900000001").unwrap()
    );
    assert_eq!(
        reports.iter().map(|fill| fill.raw.fee).sum::<U256>(),
        U256::from(490_000_000_100u64)
    );
    let expected = reports.clone();
    evidence.commit(&reports).unwrap();
    for fill in &mut reports {
        fill.report.commission = Money::zero(Currency::USDT());
    }
    reports.reverse();
    evidence
        .prepare(&mut reports, &HashMap::new(), &[], |_| 16)
        .unwrap();
    reports.reverse();
    assert_eq!(reports, expected);
    evidence.commit(&reports).unwrap();
    let mut conflict = reports[0].clone();
    conflict.report.commission = Money::from("0.00000001 USDT");
    assert!(evidence.commit(&[conflict]).is_err());
}

#[test]
fn one_wei_terminal_settlement_can_release_a_carried_native_quantum() {
    let mut reports = vec![
        report(U256::from(17_000_000_000_000_000_099u128), 1, 16),
        report(U256::from(1), 2, 16),
    ];
    let evidence = FillEvidence::default();
    evidence
        .prepare(&mut reports, &HashMap::new(), &[], |_| 16)
        .unwrap();
    assert_eq!(reports[0].report.last_qty, Quantity::from(17));
    assert_eq!(
        reports[1].report.last_qty,
        Quantity::from("0.0000000000000001")
    );
    assert_eq!(
        reports
            .iter()
            .map(|r| r.report.last_qty.as_decimal())
            .sum::<Decimal>(),
        Decimal::from_str_exact("17.0000000000000001").unwrap()
    );
}

#[test]
fn different_orders_do_not_share_rounding_carry_and_dust_never_becomes_a_zero_fill() {
    let mut reports = vec![
        report(U256::from(17_000_000_000_000_000_099u128), 1, 16),
        report(U256::from(5_000_000_000_000_000_099u128), 2, 16),
    ];
    reports[1].report.venue_order_id = "ORDER-2".into();
    let evidence = FillEvidence::default();
    evidence
        .prepare(&mut reports, &HashMap::new(), &[], |_| 16)
        .unwrap();
    assert_eq!(reports[0].report.last_qty, Quantity::from(17));
    assert_eq!(reports[1].report.last_qty, Quantity::from(5));
    let mut dust = vec![report(U256::from(1), 3, 16)];
    assert!(
        evidence
            .prepare(&mut dust, &HashMap::new(), &[], |_| 16)
            .unwrap_err()
            .to_string()
            .contains("below native quantity quantum")
    );
    assert!(evidence.entries.is_empty());
}

#[test]
fn conflicting_commit_is_atomic_and_capacity_never_silently_evicts_raw_prefixes() {
    let base = report(U256::from(1_000_000_000_000_000_000u64), 1, 16);
    let mut evidence = FillEvidence::default();
    let mut conflict = base.clone();
    conflict.raw.fee = U256::from(1);
    assert!(evidence.commit(&[base.clone(), conflict]).is_err());
    assert!(evidence.entries.is_empty());
    let reports = (0..10000)
        .map(|i| {
            let mut report = base.clone();
            report.report.trade_id = format!("T-{i}").as_str().into();
            report
        })
        .collect::<Vec<_>>();
    evidence.commit(&reports).unwrap();
    let mut next = base.clone();
    next.report.trade_id = "T-over-cap".into();
    assert!(evidence.commit(&[next]).is_err());
    assert_eq!(evidence.entries.len(), 10000);
    evidence.commit(&[base]).unwrap(); // An existing replay remains valid at the cap.
    assert_eq!(evidence.entries.len(), 10000);
}
#[test]
fn private_raw_conflicts_cannot_hide_behind_identical_native_projections() {
    let base = report(U256::from(17_000_000_000_000_000_099u128), 1, 16);
    let mut evidence = FillEvidence::default();
    evidence.commit(std::slice::from_ref(&base)).unwrap();
    for field in ["quantity", "price", "fee"] {
        let mut conflict = base.clone();
        match field {
            "quantity" => conflict.raw.gross_shares += U256::from(1),
            "price" => conflict.raw.price += U256::from(1),
            "fee" => conflict.raw.fee += U256::from(1),
            _ => unreachable!(),
        }
        assert_eq!(conflict.report, base.report);
        assert!(
            evidence.commit(&[conflict]).is_err(),
            "raw {field} conflict must reject"
        );
        assert_eq!(evidence.entries.len(), 1);
    }
    let mut fee = base.clone();
    fee.report.trade_id = "T-fee".into();
    fee.raw.fee = U256::from(1);
    evidence.commit(std::slice::from_ref(&fee)).unwrap();
    let mut conflict = fee.clone();
    conflict.raw.fee_type = PredictFeeType::Shares;
    assert_eq!(conflict.report, fee.report);
    assert!(evidence.commit(&[conflict]).is_err());
    assert_eq!(evidence.entries.len(), 2);
}

fn private_replay() -> serde_json::Value {
    use serde_json::json;
    let scale = U256::from(1_000_000_000_000_000_000u64);
    let first = U256::from(17_000_000_000_000_000_099u128);
    let mut reports = vec![
        report(first, 1, 16),
        report(scale * U256::from(100) - first, 2, 16),
    ];
    for fill in &mut reports {
        fill.raw.fee = fill.raw.gross_shares / U256::from(50);
        fill.raw.fee_type = PredictFeeType::Shares;
        fill.report.commission = super::super::parse::fill_commission(
            fill.raw.fee,
            fill.raw.fee_type,
            fill.raw.gross_shares,
            fill.raw.price,
            nautilus_model::enums::OrderSide::Buy,
            nautilus_model::enums::LiquiditySide::Taker,
            true,
        )
        .unwrap();
        if cfg!(feature = "native-quantity-commission") {
            fill.report.quantity_commission =
                Some(crate::common::parse::exact_wei(fill.raw.fee).unwrap());
        }
    }
    let mut evidence = FillEvidence::default();
    evidence
        .prepare(&mut reports, &HashMap::new(), &[], |_| 16)
        .unwrap();
    let allocations = reports
        .iter()
        .map(|r| r.report.last_qty)
        .collect::<Vec<_>>();
    assert_eq!(
        allocations.iter().map(|q| q.as_decimal()).sum::<Decimal>(),
        Decimal::from(100)
    );
    evidence.commit(&reports).unwrap();
    reports.reverse();
    evidence
        .prepare(&mut reports, &HashMap::new(), &[], |_| 16)
        .unwrap();
    reports.reverse();
    assert_eq!(
        allocations,
        reports
            .iter()
            .map(|r| r.report.last_qty)
            .collect::<Vec<_>>()
    );
    let native = reports
        .iter()
        .map(|fill| {
            let mut projection = serde_json::to_value(&fill.report).unwrap();
            assert!(projection.get("info").is_none());
            projection.as_object_mut().unwrap().remove("report_id");
            projection
        })
        .collect::<Vec<_>>();
    let raw = reports.iter().map(|fill| json!({"tradeId":fill.report.trade_id.to_string(),
        "settlementId":fill.raw.settlement_id,"grossSharesWei":fill.raw.gross_shares.to_string(),
        "priceWei":fill.raw.price.to_string(),"feeWei":fill.raw.fee.to_string(),"feeAsset":"SHARES"})).collect::<Vec<_>>();
    json!({"source":"synthetic private adapter fill evidence replay; not mainnet",
        "nativeReports":native,"privateEvidence":raw,"duplicateReplayAllocationsUnchanged":true,
        "rawGrossSumWei":(scale*U256::from(100)).to_string(),"nativeGrossSum":"100","evidenceEntries":evidence.entries.len()})
}
fn private_replay_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/private_api/private_fill_evidence_synthetic_20261016.json")
}
#[test]
fn private_share_fee_evidence_replays_without_native_info() {
    let mut saved: serde_json::Value =
        serde_json::from_slice(&std::fs::read(private_replay_path()).unwrap()).unwrap();
    if cfg!(feature = "native-quantity-commission") {
        for index in 0..2 {
            let raw: U256 = saved["privateEvidence"][index]["feeWei"]
                .as_str()
                .unwrap()
                .parse()
                .unwrap();
            saved["nativeReports"][index]["commission"] =
                serde_json::json!(Money::zero(Currency::USDT()).to_string());
            saved["nativeReports"][index]["quantity_commission"] =
                serde_json::json!(crate::common::parse::exact_wei(raw).unwrap().to_string());
        }
    }
    assert_eq!(private_replay(), saved);
}
#[test]
#[ignore = "explicit synthetic raw evidence capture; create_new only"]
fn record_private_fill_evidence_json() {
    let result = private_replay();
    serde_json::to_writer_pretty(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(private_replay_path())
            .unwrap(),
        &result,
    )
    .unwrap();
}

#[test]
fn retired_order_evidence_releases_capacity_without_eviction_of_other_orders() {
    let mut evidence = FillEvidence::default();
    let scale = U256::from(1_000_000_000_000_000_000u64);
    let batch = (0..10000)
        .map(|id| report(scale, id, 16))
        .collect::<Vec<_>>();
    evidence.commit(&batch).unwrap();
    let mut other = report(scale, 10000, 16);
    other.report.venue_order_id = "ORDER-2".into();
    assert!(evidence.commit(std::slice::from_ref(&other)).is_err());
    evidence.retire_orders(&HashSet::from(["unrelated".into()]));
    assert_eq!(evidence.entries.len(), 10000);
    evidence.retire_orders(&HashSet::from(["ORDER-1".into()]));
    evidence.commit(std::slice::from_ref(&other)).unwrap();
    evidence.retire_orders(&HashSet::from(["ORDER-1".into()]));
    assert_eq!(evidence.entries.len(), 1);
    assert!(evidence.entries.contains_key(&other.report.trade_id));
}

#[cfg(feature = "native-quantity-commission")]
#[test]
fn representable_subcurrency_fees_use_native_exact_money_without_currency_rounding() {
    let scale = U256::from(1_000_000_000_000_000_000u64);
    let mut fill = report(scale, 1, 16);
    fill.raw.fee = U256::from(4_900_000_000u64);
    let mut evidence = FillEvidence::default();
    evidence
        .prepare(
            std::slice::from_mut(&mut fill),
            &HashMap::new(),
            &[],
            |_| 16,
        )
        .unwrap();
    assert_eq!(
        fill.report.commission.as_decimal(),
        Decimal::from_str_exact("0.0000000049").unwrap()
    );
    evidence.commit(std::slice::from_ref(&fill)).unwrap();
    assert_eq!(evidence.by_order.len(), 1);
    evidence.retire_orders(&HashSet::from([fill.report.venue_order_id]));
    assert!(evidence.by_order.is_empty() && evidence.entries.is_empty());
}

#[test]
fn collecting_raw_evidence_does_not_mark_a_fill_as_queued_to_native() {
    use nautilus_model::{enums::OrderType, orders::builder::OrderTestBuilder};
    let fill = report(U256::from(1_000_000_000_000_000_000u64), 1, 16);
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(fill.report.instrument_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from(2))
        .price(fill.report.last_px)
        .build();
    let mut evidence = FillEvidence::default();
    evidence.commit(std::slice::from_ref(&fill)).unwrap();
    assert!(
        evidence
            .pending_for(&order, fill.report.venue_order_id)
            .is_empty()
    );
    evidence.queue(std::slice::from_ref(&fill)).unwrap();
    assert_eq!(
        evidence
            .pending_for(&order, fill.report.venue_order_id)
            .as_slice(),
        std::slice::from_ref(&fill)
    );
    evidence.queue_wallet(&fill).unwrap();
    evidence.validate_wallet_timestamp(&fill).unwrap();
    let mut changed = fill.clone();
    changed.report.ts_event = UnixNanos::from(1);
    assert!(evidence.validate_wallet_timestamp(&changed).is_err());
    evidence.applied(fill.report.trade_id);
    assert!(
        evidence
            .pending_for(&order, fill.report.venue_order_id)
            .is_empty()
    );
    assert_eq!(
        evidence.entries.len(),
        1,
        "retain raw prefix until Native retirement"
    );
}
