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
            .prepare(
                &mut reports,
                &HashMap::<VenueOrderId, OrderAny>::new(),
                &[],
                |_| dp,
            )
            .unwrap();
        assert_eq!(reports[0].report.last_qty, Quantity::from(17));
        assert_eq!(reports[1].report.last_qty, Quantity::from(83));
        evidence.commit(&reports).unwrap();
        reports.reverse();
        evidence
            .prepare(
                &mut reports,
                &HashMap::<VenueOrderId, OrderAny>::new(),
                &[],
                |_| dp,
            )
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
        .prepare(
            &mut reports,
            &HashMap::<VenueOrderId, OrderAny>::new(),
            &[],
            |_| 16,
        )
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
        .prepare(
            &mut reports,
            &HashMap::<VenueOrderId, OrderAny>::new(),
            &[],
            |_| 16,
        )
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
        .prepare(
            &mut reports,
            &HashMap::<VenueOrderId, OrderAny>::new(),
            &[],
            |_| 16,
        )
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
        .prepare(
            &mut reports,
            &HashMap::<VenueOrderId, OrderAny>::new(),
            &[],
            |_| 16,
        )
        .unwrap();
    assert_eq!(reports[0].report.last_qty, Quantity::from(17));
    assert_eq!(reports[1].report.last_qty, Quantity::from(5));
    let mut dust = vec![report(U256::from(1), 3, 16)];
    assert!(
        evidence
            .prepare(
                &mut dust,
                &HashMap::<VenueOrderId, OrderAny>::new(),
                &[],
                |_| 16
            )
            .unwrap_err()
            .to_string()
            .contains("below native quantity quantum")
    );
    assert!(evidence.entries.is_empty());
}

#[test]
fn conflicting_commit_is_atomic_and_native_retention_does_not_evict_raw_prefixes() {
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
    evidence.commit(&[next]).unwrap();
    assert_eq!(evidence.entries.len(), 10001);
    evidence.commit(&[base]).unwrap(); // Existing replay does not consume capacity.
    assert_eq!(evidence.entries.len(), 10001);
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
        .prepare(
            &mut reports,
            &HashMap::<VenueOrderId, OrderAny>::new(),
            &[],
            |_| 16,
        )
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
        .prepare(
            &mut reports,
            &HashMap::<VenueOrderId, OrderAny>::new(),
            &[],
            |_| 16,
        )
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
    evidence.commit(std::slice::from_ref(&other)).unwrap();
    evidence.retire_orders(&HashSet::from(["unrelated".into()]));
    assert_eq!(evidence.entries.len(), 10001);
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
            &HashMap::<VenueOrderId, OrderAny>::new(),
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
fn queued_raw_uses_transmitted_owner_before_native_acceptance_and_borrows_facts() {
    use nautilus_common::factories::OrderEventFactory;
    use nautilus_model::{
        enums::{AccountType, OrderType},
        orders::builder::OrderTestBuilder,
    };
    let mut first = report(U256::from(1_000_000_000_000_000_000u64), 1, 16);
    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(first.report.instrument_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from(10))
        .price(first.report.last_px)
        .build();
    let factory = OrderEventFactory::new(
        order.trader_id(),
        first.report.account_id,
        AccountType::Cash,
        None,
    );
    order
        .apply(factory.generate_order_submitted(&order, 1.into()))
        .unwrap();
    let owner = first.report.venue_order_id;
    first.report.client_order_id = Some(order.client_order_id());
    let mut evidence = FillEvidence::default();
    evidence.prepare_wallet(&mut first, &order, 16).unwrap();
    evidence.queue_wallet(&first).unwrap();
    evidence.queue_wallet(&first).unwrap();
    let mut second = first.clone();
    second.report.trade_id = "SECOND".into();
    assert!(!evidence.prepare_wallet(&mut second, &order, 16).unwrap());
    evidence.queue_wallet(&second).unwrap();
    let mut other = first.clone();
    other.report.trade_id = "OTHER".into();
    other.report.venue_order_id = "OTHER-ORDER".into();
    evidence.queue_wallet(&other).unwrap();
    assert!(order.venue_order_id().is_none());
    assert_eq!(
        evidence.confirmed_raw(&order, owner).unwrap(),
        Some(first.raw.gross_shares * U256::from(2))
    );
    let pending = evidence.pending_for(&order, owner).collect::<Vec<_>>();
    assert_eq!(pending.len(), 2);
    for fill in pending {
        assert!(
            std::ptr::eq(fill, evidence.entries.get(&fill.report.trade_id).unwrap()),
            "pending must borrow the retained allocation rather than copy an economic fact"
        );
    }
    assert_eq!(
        evidence
            .pending_for(&order, other.report.venue_order_id)
            .count(),
        1
    );
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
            .next()
            .is_none()
    );
    evidence.queue(std::slice::from_ref(&fill)).unwrap();
    assert_eq!(
        evidence
            .pending_for(&order, fill.report.venue_order_id)
            .collect::<Vec<_>>()
            .as_slice(),
        &[&fill]
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
            .next()
            .is_none()
    );
    assert_eq!(
        evidence.entries.len(),
        1,
        "retain raw prefix until Native retirement"
    );
}

fn wallet_history(retained: u32) -> (FillEvidence, OrderAny, BufferedFill) {
    use nautilus_common::factories::OrderEventFactory;
    use nautilus_model::{
        enums::{AccountType, OrderType},
        orders::builder::OrderTestBuilder,
    };
    let mut target = report(U256::from(1_000_000_000_000_000_000u64), retained, 16);
    let factory = OrderEventFactory::new(
        "TESTER-001".into(),
        target.report.account_id,
        AccountType::Cash,
        None,
    );
    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id("TESTER-001".into())
        .instrument_id(target.report.instrument_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from(20000))
        .price(target.report.last_px)
        .build();
    order
        .apply(factory.generate_order_submitted(&order, 1.into()))
        .unwrap();
    order
        .apply(factory.generate_order_accepted(
            &order,
            target.report.venue_order_id,
            2.into(),
            2.into(),
        ))
        .unwrap();
    let mut history = Vec::new();
    for n in 0..retained {
        let mut fill = report(target.raw.gross_shares, n, 16);
        fill.report.client_order_id = Some(order.client_order_id());
        order
            .apply(factory.generate_order_filled(
                &order,
                fill.report.venue_order_id,
                None,
                fill.report.trade_id,
                fill.report.last_qty,
                fill.report.last_px,
                Currency::USDT(),
                Some(fill.report.commission),
                fill.report.liquidity_side,
                (3 + u64::from(n)).into(),
                (3 + u64::from(n)).into(),
            ))
            .unwrap();
        history.push(fill);
    }
    target.report.client_order_id = Some(order.client_order_id());
    let mut evidence = FillEvidence::default();
    evidence.commit(&history).unwrap();
    (evidence, order, target)
}

fn bench_wallet_prepare(
    evidence: &mut FillEvidence,
    order: &impl crate::execution::context::OrderView,
    fill: &mut BufferedFill,
) {
    std::hint::black_box(evidence.prepare_wallet(fill, order, 16).unwrap());
}

#[test]
fn borrowed_wallet_allocations_match_rest_and_keep_scoped_pending_and_raw_conflicts() {
    let (mut evidence, order, mut incoming) = wallet_history(64);
    let mut batch = vec![incoming.clone()];
    evidence
        .prepare(
            &mut batch,
            &HashMap::from([(incoming.report.venue_order_id, order.clone())]),
            &[],
            |_| 16,
        )
        .unwrap();
    assert!(!evidence.prepare_wallet(&mut incoming, &order, 16).unwrap());
    assert_eq!(incoming, batch[0]);
    evidence.commit(std::slice::from_ref(&incoming)).unwrap();
    evidence.queue_wallet(&incoming).unwrap();
    assert_eq!(
        evidence
            .pending_for(&order, incoming.report.venue_order_id)
            .collect::<Vec<_>>(),
        vec![&incoming]
    );
    let other: VenueOrderId = "UNRELATED".into();
    assert!(evidence.pending_for(&order, other).next().is_none());
    assert!(
        evidence
            .prepare_wallet(&mut incoming.clone(), &order, 16)
            .unwrap()
    );
    let mut conflict = incoming.clone();
    conflict.raw.gross_shares += U256::from(1);
    assert!(evidence.prepare_wallet(&mut conflict, &order, 16).is_err());
    evidence.applied(incoming.report.trade_id);
    assert!(
        evidence
            .pending_for(&order, incoming.report.venue_order_id)
            .next()
            .is_none()
    );
    evidence.retire_orders(&HashSet::from([incoming.report.venue_order_id]));
    assert!(
        evidence.entries.is_empty() && evidence.queued.is_empty() && evidence.by_order.is_empty()
    );
}

#[test]
fn new_wallet_fill_reads_native_history_once_and_pending_membership_does_not_clone_fills() {
    use crate::execution::context::{
        APPLIED_FILL_CLONES, TRADE_ID_COPIES, TRADE_ID_READS, WorkerOrder,
    };
    let (mut evidence, native, mut fill) = wallet_history(1000);
    let order = WorkerOrder::capture(&native);
    TRADE_ID_READS.with(|reads| reads.set(0));
    TRADE_ID_COPIES.with(|copies| copies.set(0));
    assert!(!evidence.prepare_wallet(&mut fill, &order, 16).unwrap());
    assert_eq!(
        TRADE_ID_READS.with(|reads| reads.get()),
        1,
        "one new fill must not rebuild its applied prefix for each check"
    );
    assert_eq!(
        TRADE_ID_COPIES.with(|copies| copies.get()),
        0,
        "wallet preparation borrows the Native index"
    );
    assert_eq!(
        evidence
            .confirmed_raw(&order, order.venue_order_id().unwrap())
            .unwrap(),
        Some(U256::from(1000) * U256::from(1_000_000_000_000_000_000u64))
    );
    assert_eq!(
        TRADE_ID_COPIES.with(|copies| copies.get()),
        0,
        "lifecycle validation borrows the same Native index"
    );
    let old = evidence.entries.get(&"T-0".into()).unwrap().clone();
    evidence.queue_wallet(&old).unwrap();
    APPLIED_FILL_CLONES.with(|clones| clones.set(0));
    assert!(
        evidence
            .pending_for(&order, old.report.venue_order_id)
            .next()
            .is_none()
    );
    assert_eq!(
        APPLIED_FILL_CLONES.with(|clones| clones.get()),
        0,
        "membership must not copy an economic fill"
    );
}

#[test]
fn recovery_snapshot_is_scoped_and_validation_does_not_commit_other_orders() {
    let (mut evidence, _, _) = wallet_history(4);
    let own = VenueOrderId::from("ORDER-1");
    let mut other = report(U256::from(1_000_000_000_000_000_000u64), 99, 16);
    other.report.venue_order_id = VenueOrderId::from("OTHER");
    evidence.queue_wallet(&other).unwrap();
    let scoped = evidence.snapshot_for_orders(std::iter::once(own));
    assert_eq!(scoped.entries.len(), 4);
    assert_eq!(scoped.by_order.len(), 1);
    assert!(scoped.queued.is_empty());
    assert!(!scoped.entries.contains_key(&other.report.trade_id));
    assert_eq!(evidence.queued.len(), 1);
    let mut conflict = other.clone();
    conflict.raw.gross_shares += U256::from(1);
    assert!(
        evidence
            .validate_commit(&[other.clone(), conflict])
            .is_err()
    );
    assert_eq!(evidence.entries.len(), 5);
    assert_eq!(evidence.entries[&other.report.trade_id], other);
}

#[test]
fn large_partial_fill_batch_reads_each_native_prefix_once() {
    use crate::execution::context::{TRADE_ID_READS, WorkerOrder};
    let (evidence, native, incoming) = wallet_history(64);
    let order = WorkerOrder::capture(&native);
    let mut batch = (100..200)
        .map(|id| {
            let mut fill = incoming.clone();
            fill.report.trade_id = format!("B-{id}").as_str().into();
            fill
        })
        .collect::<Vec<_>>();
    TRADE_ID_READS.with(|reads| reads.set(0));
    evidence
        .prepare(
            &mut batch,
            &HashMap::from([(incoming.report.venue_order_id, order)]),
            &[],
            |_| 16,
        )
        .unwrap();
    assert_eq!(TRADE_ID_READS.with(|reads| reads.get()), 1);
    assert_eq!(
        batch
            .iter()
            .map(|fill| fill.report.last_qty.as_decimal())
            .sum::<Decimal>(),
        Decimal::from(100)
    );
}

#[test]
#[ignore = "explicit release microbench; PREDICT_FILL_BENCH_PATH writes create_new JSON"]
fn benchmark_wallet_fill_history() {
    use std::{hint::black_box, time::Instant};
    let mut rows = Vec::new();
    for retained in [0, 1000, 9999] {
        let (mut evidence, native, target) = wallet_history(retained);
        let order = crate::execution::context::WorkerOrder::capture(&native);
        for duplicate in [false, true] {
            if duplicate && retained == 0 {
                continue;
            }
            let input = if duplicate {
                evidence.entries.get(&"T-0".into()).unwrap().clone()
            } else {
                target.clone()
            };
            let mut samples = Vec::new();
            for n in 0..110 {
                let mut fill = input.clone();
                let started = Instant::now();
                bench_wallet_prepare(
                    black_box(&mut evidence),
                    black_box(&order),
                    black_box(&mut fill),
                );
                let elapsed = started.elapsed().as_nanos() as u64;
                if n >= 10 {
                    samples.push(elapsed);
                }
            }
            samples.sort_unstable();
            rows.push(serde_json::json!({"retained_fills":retained,"duplicate":duplicate,"samples_ns":samples,
                "p50_ns":samples[49],"p95_ns":samples[94],"p99_ns":samples[98]}));
        }
    }
    let path = std::env::var("PREDICT_FILL_BENCH_PATH").expect("explicit output path required");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .unwrap();
    serde_json::to_writer_pretty(file,&serde_json::json!({"profile":"release production wallet Success pending lookup and precision preparation, 10 warmups + 100 samples; excludes fixture seeding, transport, Native application and lifecycle-only confirmed_raw", "rows":rows})).unwrap();
}

#[test]
#[ignore = "explicit Native snapshot copy audit; PREDICT_SNAPSHOT_COPY_PATH writes create_new JSON"]
fn benchmark_native_order_snapshot_copy() {
    use nautilus_live::execution::context::OrderContext;
    use std::{hint::black_box, time::Instant};
    let mut rows = Vec::new();
    for retained in [0, 1000, 9999] {
        let (_, order, _) = wallet_history(retained);
        for full_history in [true, false] {
            let mut samples = Vec::new();
            for index in 0..110 {
                let started = Instant::now();
                if full_history {
                    let copy: OrderAny = black_box(&order).clone();
                    black_box(copy);
                } else {
                    black_box(OrderContext::from(black_box(&order)));
                }
                if index >= 10 {
                    samples.push(started.elapsed().as_nanos() as u64);
                }
            }
            samples.sort_unstable();
            rows.push(serde_json::json!({"retained_fills":retained,"full_history_copy_and_drop":full_history,
                "p50_ns":samples[49],"p95_ns":samples[94],"p99_ns":samples[98],"samples_ns":samples}));
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_SNAPSHOT_COPY_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file, &serde_json::json!({
        "source":"isolated release copy/drop microbench; not full observer or strategy callback; compact context is not a precision replacement",
        "rows":rows,
    })).unwrap();
}

fn native_fill(order: &OrderAny, fill: &BufferedFill) -> nautilus_model::events::OrderEventAny {
    nautilus_common::factories::OrderEventFactory::new(
        order.trader_id(),
        fill.report.account_id,
        nautilus_model::enums::AccountType::Cash,
        None,
    )
    .generate_order_filled(
        order,
        fill.report.venue_order_id,
        None,
        fill.report.trade_id,
        fill.report.last_qty,
        fill.report.last_px,
        Currency::USDT(),
        Some(fill.report.commission),
        fill.report.liquidity_side,
        fill.report.ts_event,
        fill.report.ts_init,
    )
}

#[test]
fn compact_native_views_keep_snapshot_cutoffs_and_identical_atomic_allocations() {
    use crate::execution::context::WorkerOrder;
    let (mut evidence, mut native, mut target) = wallet_history(64);
    let previous = WorkerOrder::capture(&native);
    assert!(!evidence.prepare_wallet(&mut target, &previous, 16).unwrap());
    let mut native_target = target.clone();
    assert!(
        !evidence
            .prepare_wallet(&mut native_target, &native, 16)
            .unwrap()
    );
    assert_eq!(native_target, target);
    let event = native_fill(&native, &target);
    native.apply(event.clone()).unwrap();
    let current = WorkerOrder::observe(&native, &event, Some(&previous));
    assert_eq!(previous.trade_ids().len(), 64);
    assert_eq!(current.trade_ids().len(), 65);
    assert!(previous.applied_fill(target.report.trade_id).is_none());
    assert!(current.applied_fill(target.report.trade_id).is_some());
    assert_eq!(current.filled_qty(), native.filled_qty());
    assert_eq!(current.status(), native.status());
    assert!(!crate::execution::parse::validate_applied_fill(&target.report, &previous).unwrap());
    assert!(crate::execution::parse::validate_applied_fill(&target.report, &current).unwrap());
    evidence.commit(std::slice::from_ref(&target)).unwrap();
    assert!(evidence.prepare_wallet(&mut target, &current, 16).unwrap());
}

#[test]
#[ignore = "explicit full-copy versus production worker observation; PREDICT_OBSERVER_BENCH_PATH create_new JSON"]
fn benchmark_native_worker_observation() {
    use crate::execution::context::WorkerOrder;
    use std::{hint::black_box, time::Instant};
    let mut rows = Vec::new();
    for retained in [0, 1000, 9999] {
        let (mut evidence, old, mut target) = wallet_history(retained);
        evidence.prepare_wallet(&mut target, &old, 16).unwrap();
        let event = native_fill(&old, &target);
        let mut native = old.clone();
        native.apply(event.clone()).unwrap();
        let mut old_samples = Vec::new();
        let mut new_samples = Vec::new();
        for index in 0..110 {
            for full in if index % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            } {
                // Seeding/recovery is explicitly outside the measured observer boundary.
                let previous = WorkerOrder::capture(&old);
                let started = Instant::now();
                if full {
                    black_box(black_box(&native).clone());
                } else {
                    black_box(WorkerOrder::observe(
                        black_box(&native),
                        black_box(&event),
                        Some(black_box(&previous)),
                    ));
                }
                let elapsed = started.elapsed().as_nanos() as u64;
                if index >= 10 {
                    if full {
                        old_samples.push(elapsed);
                    } else {
                        new_samples.push(elapsed);
                    }
                }
            }
        }
        for (path, mut samples) in [
            ("full_native_clone_drop", old_samples),
            ("compact_native_observe_drop", new_samples),
        ] {
            samples.sort_unstable();
            rows.push(serde_json::json!({"retained_fills":retained,"path":path,"p50_ns":samples[49],"p95_ns":samples[94],"p99_ns":samples[98],"samples_ns":samples}));
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_OBSERVER_BENCH_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file,&serde_json::json!({"source":"release isolated capture/drop versus production Native projection; AB/BA order, 10 warmups + 100 samples each; excludes Native engine fill application and transport; not strategy end-to-end", "rows":rows})).unwrap();
}

#[test]
fn native_retirement_reclaims_full_raw_capacity_without_forgetting_other_orders() {
    let mut evidence = FillEvidence::default();
    let fills = (0..10000)
        .map(|id| report(U256::from(100_000_000_000_000u64), id, 16))
        .collect::<Vec<_>>();
    evidence.commit(&fills).unwrap();
    let mut excess = report(U256::from(100_000_000_000_000u64), 10000, 16);
    excess.report.venue_order_id = VenueOrderId::from("other-order");
    evidence.commit(std::slice::from_ref(&excess)).unwrap();
    evidence.retire_orders(&HashSet::from([fills[0].report.venue_order_id]));
    assert_eq!(evidence.entries.len(), 1);
    assert_eq!(evidence.by_order.len(), 1);
    evidence.commit(std::slice::from_ref(&excess)).unwrap();
    evidence.retire_orders(&HashSet::from([fills[0].report.venue_order_id]));
    assert!(evidence.entries.contains_key(&excess.report.trade_id));
}

#[test]
fn lifecycle_raw_borrow_keeps_old_cutoff_and_deduplicates_queued_applied_ids() {
    use crate::execution::context::{TRADE_ID_COPIES, WorkerOrder};
    let (mut evidence, mut native, mut next) = wallet_history(64);
    let before = WorkerOrder::capture(&native);
    evidence.prepare_wallet(&mut next, &before, 16).unwrap();
    let event = native_fill(&native, &next);
    native.apply(event.clone()).unwrap();
    let after = WorkerOrder::observe(&native, &event, Some(&before));
    evidence.commit(std::slice::from_ref(&next)).unwrap();
    let old_raw = U256::from(64) * U256::from(1_000_000_000_000_000_000u64);
    TRADE_ID_COPIES.with(|copies| copies.set(0));
    assert_eq!(
        evidence
            .confirmed_raw(&before, before.venue_order_id().unwrap())
            .unwrap(),
        Some(old_raw)
    );
    evidence.queue_wallet(&next).unwrap();
    evidence.queue_wallet(&next).unwrap();
    assert_eq!(
        evidence
            .confirmed_raw(&before, before.venue_order_id().unwrap())
            .unwrap(),
        Some(old_raw + next.raw.gross_shares)
    );
    assert_eq!(
        evidence
            .confirmed_raw(&after, after.venue_order_id().unwrap())
            .unwrap(),
        Some(old_raw + next.raw.gross_shares)
    );
    assert_eq!(TRADE_ID_COPIES.with(|copies| copies.get()), 0);
    let mut missing = evidence.clone();
    missing.entries.remove(&"T-0".into());
    assert_eq!(
        missing
            .confirmed_raw(&before, before.venue_order_id().unwrap())
            .unwrap(),
        None
    );
    assert!(
        missing
            .prepare_wallet(&mut next.clone(), &before, 16)
            .is_ok()
    );
}

#[test]
#[ignore = "explicit lifecycle raw history benchmark; PREDICT_RAW_BENCH_PATH is create_new JSON"]
fn benchmark_lifecycle_raw_history() {
    use std::{hint::black_box, time::Instant};
    let mut rows = Vec::new();
    for retained in [0, 1000, 9999] {
        let (mut evidence, native, target) = wallet_history(retained);
        let order = crate::execution::context::WorkerOrder::capture(&native);
        for count in [0, 1, 128] {
            evidence.queued.clear();
            let queued = (0..count)
                .map(|index| {
                    let mut fill = target.clone();
                    fill.report.trade_id = format!("PENDING-{index}").as_str().into();
                    fill
                })
                .collect::<Vec<_>>();
            for fill in &queued {
                evidence.queue_wallet(fill).unwrap();
            }
            let expected = U256::from(retained as u64 + count as u64)
                * U256::from(1_000_000_000_000_000_000u64);
            let mut samples = Vec::new();
            for iteration in 0..110 {
                let started = Instant::now();
                let raw = black_box(&mut evidence)
                    .confirmed_raw(black_box(&order), order.venue_order_id().unwrap())
                    .unwrap();
                let elapsed = started.elapsed().as_nanos() as u64;
                assert_eq!(raw, Some(expected));
                if iteration >= 10 {
                    samples.push(elapsed);
                }
            }
            samples.sort_unstable();
            rows.push(serde_json::json!({"retained_fills":retained,"queued":count,"p50_ns":samples[49],"p95_ns":samples[94],"p99_ns":samples[98],"samples_ns":samples}));
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_RAW_BENCH_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file, &serde_json::json!({"source":"release lifecycle confirmed_raw only; 10 warmups and 100 samples; setup/Native application/transport excluded", "rows":rows})).unwrap();
}

#[test]
fn live_native_fill_crosses_ten_thousand_retained_facts_without_restart_or_eviction() {
    use crate::execution::context::WorkerOrder;
    let (mut evidence, mut native, mut fill) = wallet_history(10000);
    let before = WorkerOrder::capture(&native);
    assert!(!evidence.prepare_wallet(&mut fill, &before, 16).unwrap());
    evidence.queue_wallet(&fill).unwrap();
    let event = native_fill(&native, &fill);
    native.apply(event.clone()).unwrap();
    let after = WorkerOrder::observe(&native, &event, Some(&before));
    evidence.applied(fill.report.trade_id);
    assert_eq!(after.filled_qty().as_decimal(), Decimal::from(10001));
    assert_eq!(evidence.entries.len(), 10001);
    assert!(
        evidence.entries.contains_key(&"T-0".into()),
        "the first raw fact must not be evicted to admit a later fill"
    );
    assert!(evidence.prepare_wallet(&mut fill, &after, 16).unwrap());
    let mut conflict = fill.clone();
    conflict.raw.gross_shares += U256::from(1);
    assert!(evidence.prepare_wallet(&mut conflict, &after, 16).is_err());
    evidence.retire_orders(&HashSet::from([fill.report.venue_order_id]));
    assert!(
        evidence.entries.is_empty() && evidence.by_order.is_empty() && evidence.queued.is_empty()
    );
}

#[test]
fn verified_prefix_visits_only_appended_native_ids_and_preserves_atomic_carries() {
    use crate::execution::context::{TRADE_IDS_VISITED, WorkerOrder};
    let (mut evidence, mut native, target) = wallet_history(1000);
    let old = WorkerOrder::capture(&native);
    TRADE_IDS_VISITED.with(|n| n.set(0));
    evidence
        .confirmed_raw(&old, old.venue_order_id().unwrap())
        .unwrap();
    assert_eq!(TRADE_IDS_VISITED.with(|n| n.get()), 1000);
    let mut queued = Vec::new();
    for index in 0..3 {
        let mut fill = target.clone();
        fill.report.trade_id = format!("APPEND-{index}").as_str().into();
        fill.raw.gross_shares += U256::from(99);
        fill.raw.fee = U256::from(4_900_000_001u64);
        let mut oracle = fill.clone();
        // An independent full Native view must make identical quantity/fee allocations.
        evidence
            .clone()
            .prepare_wallet(&mut oracle, &native, 16)
            .unwrap();
        TRADE_IDS_VISITED.with(|n| n.set(0));
        assert!(!evidence.prepare_wallet(&mut fill, &old, 16).unwrap());
        assert_eq!(TRADE_IDS_VISITED.with(|n| n.get()), 0);
        assert_eq!(fill, oracle);
        evidence.queue_wallet(&fill).unwrap();
        queued.push(fill);
    }
    let mut current = old.clone();
    for fill in &queued {
        let event = native_fill(&native, fill);
        native.apply(event.clone()).unwrap();
        current = WorkerOrder::observe(&native, &event, Some(&current));
        evidence.applied(fill.report.trade_id);
    }
    TRADE_IDS_VISITED.with(|n| n.set(0));
    let expected = U256::from(1000) * U256::from(1_000_000_000_000_000_000u64)
        + queued.iter().map(|f| f.raw.gross_shares).sum::<U256>();
    assert_eq!(
        evidence
            .confirmed_raw(&current, current.venue_order_id().unwrap())
            .unwrap(),
        Some(expected)
    );
    assert_eq!(
        TRADE_IDS_VISITED.with(|n| n.get()),
        3,
        "skip only the verified prefix, not appended Native facts"
    );
    TRADE_IDS_VISITED.with(|n| n.set(0));
    assert_eq!(
        evidence
            .confirmed_raw(&current, current.venue_order_id().unwrap())
            .unwrap(),
        Some(expected)
    );
    assert_eq!(TRADE_IDS_VISITED.with(|n| n.get()), 0);
    // An older snapshot is still immutable after later facts were memoized.
    TRADE_IDS_VISITED.with(|n| n.set(0));
    assert_eq!(
        evidence
            .confirmed_raw(&old, old.venue_order_id().unwrap())
            .unwrap(),
        Some(expected - queued.iter().map(|f| f.raw.gross_shares).sum::<U256>())
    );
    assert_eq!(TRADE_IDS_VISITED.with(|n| n.get()), 1000);
    let mut next = target.clone();
    next.report.trade_id = "AFTER-CARRIES".into();
    next.raw.gross_shares += U256::from(99);
    next.raw.fee = U256::from(4_900_000_001u64);
    let mut oracle = next.clone();
    evidence
        .clone()
        .prepare_wallet(&mut oracle, &native, 16)
        .unwrap();
    evidence.prepare_wallet(&mut next, &current, 16).unwrap();
    assert_eq!(next, oracle);
    let mut conflict = next.clone();
    evidence.queue_wallet(&next).unwrap();
    conflict.raw.fee += U256::from(1);
    assert!(
        evidence
            .prepare_wallet(&mut conflict, &current, 16)
            .is_err()
    );
}

#[test]
fn verified_prefix_rejects_same_count_cursor_from_another_native_capture() {
    use crate::execution::context::{TRADE_IDS_VISITED, WorkerOrder};
    let (mut evidence, native, _) = wallet_history(64);
    let first = WorkerOrder::capture(&native);
    evidence
        .confirmed_raw(&first, first.venue_order_id().unwrap())
        .unwrap();
    let (_, mut restored, mut different) = wallet_history(63);
    different.report.trade_id = "RESTORED-DIFFERENT".into();
    different.raw.gross_shares *= U256::from(2);
    different.report.last_qty = Quantity::from(2);
    let event = native_fill(&restored, &different);
    restored.apply(event).unwrap();
    evidence.commit(&[different]).unwrap();
    let other = WorkerOrder::capture(&restored);
    assert_eq!(other.trade_ids().len(), first.trade_ids().len());
    TRADE_IDS_VISITED.with(|n| n.set(0));
    assert_eq!(
        evidence
            .confirmed_raw(&other, other.venue_order_id().unwrap())
            .unwrap(),
        Some(U256::from(65) * U256::from(1_000_000_000_000_000_000u64))
    );
    assert_eq!(
        TRADE_IDS_VISITED.with(|n| n.get()),
        64,
        "equal counts do not establish Native history identity"
    );
}

#[test]
fn verified_prefix_never_caches_missing_or_uncommitted_facts_and_clone_rebuilds() {
    use crate::execution::context::{TRADE_IDS_VISITED, WorkerOrder};
    let (mut evidence, native, _) = wallet_history(64);
    let owner = native.venue_order_id().unwrap();
    let order = WorkerOrder::capture(&native);
    evidence
        .confirmed_raw(&order, order.venue_order_id().unwrap())
        .unwrap();
    let mut missing = evidence.clone();
    assert!(missing.by_order[&owner].prefix.is_none());
    let hole = missing.entries.remove(&"T-0".into()).unwrap();
    assert_eq!(
        missing
            .confirmed_raw(&order, order.venue_order_id().unwrap())
            .unwrap(),
        None
    );
    assert!(missing.by_order[&owner].prefix.is_none());
    assert!(
        missing
            .prepare_wallet(&mut hole.clone(), &order, 16)
            .unwrap()
    );
    assert!(
        missing.by_order[&owner].prefix.is_none(),
        "a read-only supplied raw fact must not become a committed memo"
    );
    assert_eq!(
        missing
            .confirmed_raw(&order, order.venue_order_id().unwrap())
            .unwrap(),
        None
    );
    missing.commit(&[hole]).unwrap();
    TRADE_IDS_VISITED.with(|n| n.set(0));
    assert_eq!(
        missing
            .confirmed_raw(&order, order.venue_order_id().unwrap())
            .unwrap(),
        evidence
            .confirmed_raw(&order, order.venue_order_id().unwrap())
            .unwrap()
    );
    assert_eq!(TRADE_IDS_VISITED.with(|n| n.get()), 64);
    let mut scoped = missing.snapshot_for_orders(std::iter::once(owner));
    assert!(scoped.by_order[&owner].prefix.is_none());
    TRADE_IDS_VISITED.with(|n| n.set(0));
    scoped
        .confirmed_raw(&order, order.venue_order_id().unwrap())
        .unwrap();
    assert_eq!(TRADE_IDS_VISITED.with(|n| n.get()), 64);
    let mut external = report(U256::from(10), 100, 16);
    external.report.venue_order_id = "OTHER-OWNER".into();
    missing.commit(&[external.clone()]).unwrap();
    missing.retire_orders(&HashSet::from([owner]));
    assert_eq!(missing.by_order.len(), 1);
    assert_eq!(missing.entries.len(), 1);
    assert!(missing.entries.contains_key(&external.report.trade_id));
}

#[test]
#[ignore = "explicit advancing-prefix capture; PREDICT_ADVANCING_PREFIX_PATH is create_new JSON"]
fn benchmark_advancing_wallet_prefix() {
    use crate::execution::context::WorkerOrder;
    use std::{hint::black_box, time::Instant};
    let mut rows = Vec::new();
    for retained in [0, 1000, 9999] {
        for operation in ["wallet", "lifecycle", "wallet_fresh_capture"] {
            let (mut evidence, mut native, target) = wallet_history(retained);
            let mut order = WorkerOrder::capture(&native);
            evidence
                .confirmed_raw(&order, order.venue_order_id().unwrap())
                .unwrap();
            let mut timings = Vec::new();
            for index in 0..110 {
                let mut fill = target.clone();
                fill.report.trade_id = format!("ADV-{index}").as_str().into();
                if operation == "wallet_fresh_capture" {
                    order = WorkerOrder::capture(&native);
                }
                if operation == "lifecycle" {
                    evidence.commit(std::slice::from_ref(&fill)).unwrap();
                    let event = native_fill(&native, &fill);
                    native.apply(event.clone()).unwrap();
                    order = WorkerOrder::observe(&native, &event, Some(&order));
                }
                let started = Instant::now();
                if operation == "lifecycle" {
                    assert_eq!(
                        black_box(&mut evidence)
                            .confirmed_raw(black_box(&order), order.venue_order_id().unwrap())
                            .unwrap(),
                        Some(
                            U256::from(retained + index + 1)
                                * U256::from(1_000_000_000_000_000_000u64)
                        )
                    );
                } else {
                    bench_wallet_prepare(
                        black_box(&mut evidence),
                        black_box(&order),
                        black_box(&mut fill),
                    );
                }
                let ns = started.elapsed().as_nanos() as u64;
                if index >= 10 {
                    timings.push(ns);
                }
                if operation != "lifecycle" {
                    evidence.queue_wallet(&fill).unwrap();
                    let event = native_fill(&native, &fill);
                    native.apply(event.clone()).unwrap();
                    order = WorkerOrder::observe(&native, &event, Some(&order));
                    evidence.applied(fill.report.trade_id);
                }
            }
            timings.sort_unstable();
            rows.push(serde_json::json!({"retained_fills":retained,"operation":operation,"samples_ns":timings,"p50_ns":timings[49],"p95_ns":timings[94],"p99_ns":timings[98]}));
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_ADVANCING_PREFIX_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file,&serde_json::json!({"source":"production wallet pending/prepare or lifecycle raw with a real additional Native-applied fill per iteration; 10 warmups + 100 samples; seeding, Native apply, capture and commit excluded; fresh capture intentionally invalidates memo every iteration", "rows":rows})).unwrap();
}

#[test]
fn verified_prefix_checks_appended_overflow_without_advancing_verified_facts() {
    use crate::execution::context::WorkerOrder;
    let (mut evidence, mut native, mut next) = wallet_history(1);
    evidence
        .entries
        .get_mut(&"T-0".into())
        .unwrap()
        .raw
        .gross_shares = U256::MAX - U256::from(1);
    let old = WorkerOrder::capture(&native);
    assert_eq!(
        evidence
            .confirmed_raw(&old, old.venue_order_id().unwrap())
            .unwrap(),
        Some(U256::MAX - U256::from(1))
    );
    next.raw.gross_shares = U256::from(2);
    evidence.commit(std::slice::from_ref(&next)).unwrap();
    let event = native_fill(&native, &next);
    native.apply(event.clone()).unwrap();
    let current = WorkerOrder::observe(&native, &event, Some(&old));
    assert!(
        evidence
            .confirmed_raw(&current, current.venue_order_id().unwrap())
            .is_err()
    );
    assert_eq!(
        evidence.by_order[&next.report.venue_order_id]
            .prefix
            .as_ref()
            .unwrap()
            .raw,
        Some(U256::MAX - U256::from(1))
    );
    assert_eq!(
        evidence
            .confirmed_raw(&old, old.venue_order_id().unwrap())
            .unwrap(),
        Some(U256::MAX - U256::from(1))
    );
}

#[test]
fn recovery_fence_matches_reordered_facts_and_rejects_missing_duplicate_or_changed_facts() {
    use crate::execution::{context::WorkerOrder, readiness::verify_match_fence};
    let (evidence, native, _) = wallet_history(64);
    let orders = HashMap::from([(
        native.venue_order_id().unwrap(),
        WorkerOrder::capture(&native),
    )]);
    let fills = evidence.entries.values().cloned().collect::<Vec<_>>();
    let mut fence = fills.clone();
    fence.reverse();
    verify_match_fence(&fills, &fence, &orders).unwrap();
    assert!(verify_match_fence(&fills, &fence[..63], &orders).is_err());
    let mut duplicate = fence.clone();
    duplicate[0] = duplicate[1].clone();
    assert!(verify_match_fence(&fills, &duplicate, &orders).is_err());
    let mut changed = fence.clone();
    changed[0].report.last_qty = Quantity::from(2);
    assert!(verify_match_fence(&fills, &changed, &orders).is_err());
    let mut foreign = fence;
    foreign[0].report.venue_order_id = "FOREIGN".into();
    assert!(verify_match_fence(&fills, &foreign, &orders).is_err());
}

#[test]
#[ignore = "explicit recovery fence CPU benchmark; PREDICT_RECOVERY_FENCE_PATH create_new JSON"]
fn benchmark_recovery_match_fence() {
    use crate::execution::{context::WorkerOrder, readiness::verify_match_fence};
    use std::{hint::black_box, time::Instant};
    let mut rows = Vec::new();
    for retained in [1, 1000, 10000] {
        let (evidence, native, _) = wallet_history(retained);
        let orders = HashMap::from([(
            native.venue_order_id().unwrap(),
            WorkerOrder::capture(&native),
        )]);
        let fills = evidence.entries.values().cloned().collect::<Vec<_>>();
        let mut fence = fills.clone();
        fence.reverse();
        let mut samples = Vec::new();
        for iteration in 0..23 {
            let started = Instant::now();
            verify_match_fence(black_box(&fills), black_box(&fence), black_box(&orders)).unwrap();
            let ns = started.elapsed().as_nanos() as u64;
            if iteration >= 3 {
                samples.push(ns);
            }
        }
        samples.sort_unstable();
        rows.push(serde_json::json!({"fills":retained,"samples_ns":samples,"p50_ns":samples[9],"p95_ns":samples[18],"p99_ns":samples[19]}));
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_RECOVERY_FENCE_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file,&serde_json::json!({"source":"production parsed recovery fill fence plus Native validation; 3 warmups and 20 samples; parsing/transport/Native application excluded; p99 maximum","rows":rows})).unwrap();
}

#[test]
fn applied_fill_inspection_borrows_native_events_and_preserves_cutoff_and_conflicts() {
    use crate::execution::context::{APPLIED_FILL_CLONES, WorkerOrder};
    let (mut evidence, native, _) = wallet_history(64);
    let old = WorkerOrder::capture(&native);
    let applied = evidence.entries[&"T-63".into()].clone();
    APPLIED_FILL_CLONES.with(|n| n.set(0));
    assert!(crate::execution::parse::validate_applied_fill(&applied.report, &old).unwrap());
    let shared = Arc::new(old.clone());
    assert!(crate::execution::parse::validate_applied_fill(&applied.report, &shared).unwrap());
    assert!(crate::execution::parse::validate_applied_fill(&applied.report, &native).unwrap());
    let mut replay = vec![applied.clone()];
    evidence
        .prepare(
            &mut replay,
            &HashMap::from([(applied.report.venue_order_id, old.clone())]),
            &[],
            |_| 16,
        )
        .unwrap();
    assert_eq!(replay[0], applied);
    let missing = FillEvidence::default();
    missing
        .prepare(
            &mut replay,
            &HashMap::from([(applied.report.venue_order_id, old.clone())]),
            &[],
            |_| 16,
        )
        .unwrap();
    assert_eq!(replay[0], applied);
    assert!(evidence.prepare_wallet(&mut replay[0], &old, 16).unwrap());
    assert_eq!(
        APPLIED_FILL_CLONES.with(|n| n.get()),
        0,
        "validation and replay must borrow the Native fill"
    );

    let mut changed = applied.report.clone();
    changed.last_px = Price::from("0.140");
    assert!(crate::execution::parse::validate_applied_fill(&changed, &old).is_err());
    changed = applied.report.clone();
    changed.quantity_commission = Some(Decimal::ONE);
    assert!(crate::execution::parse::validate_applied_fill(&changed, &old).is_err());
    changed = applied.report.clone();
    changed.account_id = "PREDICT-other".into();
    assert!(crate::execution::parse::validate_applied_fill(&changed, &old).is_err());
    changed = applied.report.clone();
    changed.trade_id = "MISSING".into();
    assert!(!crate::execution::parse::validate_applied_fill(&changed, &old).unwrap());

    let mut native = native;
    let mut next = applied;
    next.report.trade_id = "AFTER-SNAPSHOT".into();
    let event = native_fill(&native, &next);
    native.apply(event.clone()).unwrap();
    let current = WorkerOrder::observe(&native, &event, Some(&old));
    assert!(!old.with_applied_fill(next.report.trade_id, |fill| fill.is_some()));
    assert!(current.with_applied_fill(next.report.trade_id, |fill| fill.is_some()));
    assert!(crate::execution::parse::validate_applied_fill(&next.report, &current).unwrap());
    assert!(!crate::execution::parse::validate_applied_fill(&next.report, &old).unwrap());
    assert_eq!(APPLIED_FILL_CLONES.with(|n| n.get()), 0);
}

#[test]
#[ignore = "explicit applied-fill inspection capture; PREDICT_APPLIED_INSPECTION_PATH is create_new JSON"]
fn benchmark_applied_fill_inspection() {
    use crate::execution::context::{APPLIED_FILL_CLONES, WorkerOrder};
    use std::{hint::black_box, time::Instant};
    let mut rows = Vec::new();
    for retained in [1, 1000, 9999] {
        for operation in [
            "validate",
            "wallet_duplicate",
            "rest_applied",
            "wallet_queue_new",
        ] {
            let (mut evidence, native, _) = wallet_history(retained);
            let order = WorkerOrder::capture(&native);
            let last = evidence.entries[&format!("T-{}", retained - 1).as_str().into()].clone();
            let mut samples = Vec::new();
            let mut clones = Vec::new();
            for iteration in 0..110 {
                let mut incoming = last.clone();
                if operation == "wallet_queue_new" {
                    incoming.report.trade_id = format!("QUEUE-{iteration}").as_str().into();
                }
                let expected = incoming.clone();
                // Cold recovery inputs do not carry raw evidence. Prefix capture
                // stays outside this narrow existing-fill inspection benchmark.
                let mut prefix = FillPrefix {
                    complete: false,
                    ..Default::default()
                };
                APPLIED_FILL_CLONES.with(|n| n.set(0));
                let started = Instant::now();
                let applied = match operation {
                    "validate" => crate::execution::parse::validate_applied_fill(
                        black_box(&incoming.report),
                        black_box(&order),
                    )
                    .unwrap(),
                    "wallet_duplicate" => evidence
                        .prepare_wallet(black_box(&mut incoming), black_box(&order), 16)
                        .unwrap(),
                    "wallet_queue_new" => {
                        evidence.queue_wallet(black_box(&incoming)).unwrap();
                        true
                    }
                    _ => FillEvidence::default()
                        .prepare_fill(
                            black_box(&mut incoming),
                            Some(black_box(&order)),
                            black_box(&mut prefix),
                            16,
                        )
                        .unwrap(),
                };
                let elapsed = started.elapsed().as_nanos() as u64;
                assert!(applied);
                assert_eq!(incoming, expected);
                if operation == "wallet_queue_new" {
                    assert_eq!(evidence.entries[&incoming.report.trade_id], incoming);
                    assert_eq!(
                        evidence.queued[&incoming.report.venue_order_id][&incoming.report.trade_id],
                        Some(incoming.report.ts_event)
                    );
                }
                if iteration >= 10 {
                    samples.push(elapsed);
                    clones.push(APPLIED_FILL_CLONES.with(|n| n.get()));
                }
            }
            samples.sort_unstable();
            rows.push(serde_json::json!({"retained_fills":retained,"operation":operation,"samples_ns":samples,"native_fill_clones":clones,"p50_ns":samples[49],"p95_ns":samples[94],"p99_ns":samples[98]}));
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_APPLIED_INSPECTION_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file, &serde_json::json!({"source":"production existing Native fill validation / duplicate wallet / applied recovery normalization; 10 warmups + 100 samples; setup, raw prefix, transport and Native apply excluded; test-only clone counter included", "rows":rows})).unwrap();
}

#[test]
fn wallet_commit_avoids_batch_index_and_preserves_atomic_replay_validation() {
    let mut evidence = FillEvidence::default();
    let first = report(U256::from(1_000_000_000_000_000_000u64), 1, 16);
    COMMIT_BATCH_IDS.with(|n| n.set(0));
    evidence.queue_wallet(&first).unwrap();
    evidence.queue_wallet(&first).unwrap();
    assert_eq!(
        COMMIT_BATCH_IDS.with(|n| n.get()),
        0,
        "single wallet fill must not build a batch ID map"
    );
    assert_eq!(evidence.entries.len(), 1);
    assert_eq!(evidence.by_order[&first.report.venue_order_id].ids.len(), 1);
    assert_eq!(evidence.queued[&first.report.venue_order_id].len(), 1);
    assert_eq!(
        evidence.queued[&first.report.venue_order_id][&first.report.trade_id],
        Some(first.report.ts_event)
    );
    let mut conflict = first.clone();
    conflict.raw.gross_shares += U256::from(1);
    assert!(evidence.queue_wallet(&conflict).is_err());
    assert_eq!(evidence.entries[&first.report.trade_id], first);
    assert_eq!(
        evidence.queued[&first.report.venue_order_id][&first.report.trade_id],
        Some(first.report.ts_event)
    );

    let second = report(first.raw.gross_shares, 2, 16);
    evidence.queue(&[first.clone(), second.clone()]).unwrap();
    assert_eq!(COMMIT_BATCH_IDS.with(|n| n.get()), 2);
    assert_eq!(
        evidence.queued[&second.report.venue_order_id][&second.report.trade_id],
        None
    );
    assert_eq!(
        evidence.queued[&first.report.venue_order_id][&first.report.trade_id],
        Some(first.report.ts_event)
    );
    let third = report(first.raw.gross_shares, 3, 16);
    let mut changed = third.clone();
    changed.report.commission = Money::from("0.01 USDT");
    assert!(evidence.queue(&[third.clone(), changed]).is_err());
    assert!(!evidence.entries.contains_key(&third.report.trade_id));
    assert_eq!(evidence.entries.len(), 2);
    assert_eq!(evidence.by_order[&first.report.venue_order_id].ids.len(), 2);
    evidence.applied(first.report.trade_id);
    evidence.applied(second.report.trade_id);
    assert!(evidence.queued.is_empty());
    evidence.retire_orders(&HashSet::from([first.report.venue_order_id]));
    assert!(evidence.entries.is_empty() && evidence.by_order.is_empty());
}

#[test]
fn owner_retirement_checks_membership_once_and_respects_old_native_cutoffs() {
    use crate::execution::context::{APPLIED_MEMBERSHIP_READS, WorkerOrder};
    let (mut evidence, mut native, next) = wallet_history(1000);
    let old = WorkerOrder::capture(&native);
    APPLIED_MEMBERSHIP_READS.with(|n| n.set(0));
    assert!(!evidence.has_unapplied(next.report.venue_order_id, &old));
    assert_eq!(
        APPLIED_MEMBERSHIP_READS.with(|n| n.get()),
        1,
        "one owner must inspect Native membership under one lock"
    );
    evidence.commit(std::slice::from_ref(&next)).unwrap();
    assert!(evidence.has_unapplied(next.report.venue_order_id, &old));
    let event = native_fill(&native, &next);
    native.apply(event.clone()).unwrap();
    let current = WorkerOrder::observe(&native, &event, Some(&old));
    assert!(evidence.has_unapplied(next.report.venue_order_id, &old));
    assert!(!evidence.has_unapplied(next.report.venue_order_id, &current));
    let mut foreign = next;
    foreign.report.trade_id = "NOT-APPLIED".into();
    evidence.commit(&[foreign.clone()]).unwrap();
    assert!(evidence.has_unapplied(foreign.report.venue_order_id, &current));
}

#[test]
fn recovery_snapshot_reuses_observed_fills_and_preserves_native_cutoffs() {
    use crate::execution::context::{CAPTURED_FILLS, WorkerOrder};
    use nautilus_model::{events::OrderEventAny, orders::Order};
    use std::sync::Arc;
    let (_, mut native, incoming) = wallet_history(1000);
    let initial = WorkerOrder::capture(&native);
    let observed: crate::execution::types::OrderSnapshots<
        nautilus_model::identifiers::ClientOrderId,
    > = Default::default();
    observed
        .lock()
        .unwrap()
        .insert(native.client_order_id(), Arc::new(initial));
    CAPTURED_FILLS.with(|n| n.set(0));
    let old = WorkerOrder::snapshot(
        &native,
        observed.lock().unwrap().get(&native.client_order_id()),
    );
    assert_eq!(CAPTURED_FILLS.with(|n| n.get()), 0);
    assert_eq!(old.fill_count(), 1000);
    assert!(Arc::ptr_eq(
        &old,
        &observed.lock().unwrap()[&native.client_order_id()]
    ));
    assert!(old.has_applied_fill("T-0".into()));
    let mut next = (*native.events().last().unwrap()).clone();
    let OrderEventAny::Filled(fill) = &mut next else {
        panic!("nonempty fill history");
    };
    fill.trade_id = incoming.report.trade_id;
    fill.event_id = nautilus_core::UUID4::new();
    fill.ts_event = 2000.into();
    fill.ts_init = 2000.into();
    native.apply(next.clone()).unwrap();
    // Native has advanced before the observer: recover from Native, never reuse a stale prefix.
    CAPTURED_FILLS.with(|n| n.set(0));
    let cold = WorkerOrder::snapshot(
        &native,
        observed.lock().unwrap().get(&native.client_order_id()),
    );
    assert_eq!(CAPTURED_FILLS.with(|n| n.get()), 1001);
    assert!(cold.has_applied_fill(incoming.report.trade_id));
    let current = WorkerOrder::observe(&native, &next, Some(&old));
    observed
        .lock()
        .unwrap()
        .insert(native.client_order_id(), Arc::new(current));
    CAPTURED_FILLS.with(|n| n.set(0));
    let new = WorkerOrder::snapshot(
        &native,
        observed.lock().unwrap().get(&native.client_order_id()),
    );
    assert_eq!(CAPTURED_FILLS.with(|n| n.get()), 0);
    assert_eq!(new.fill_count(), 1001);
    assert!(new.has_applied_fill(incoming.report.trade_id));
    assert!(!old.has_applied_fill(incoming.report.trade_id));
    assert_eq!(old.fill_count(), 1000);
    assert!(Arc::ptr_eq(
        &new,
        &observed.lock().unwrap()[&native.client_order_id()]
    ));
    let factory = nautilus_common::factories::OrderEventFactory::new(
        native.trader_id(),
        incoming.report.account_id,
        nautilus_model::enums::AccountType::Cash,
        None,
    );
    let cancel = factory.generate_order_canceled(
        &native,
        native.venue_order_id(),
        Order::ts_last(&native),
        Order::ts_last(&native),
    );
    native.apply(cancel.clone()).unwrap();
    CAPTURED_FILLS.with(|n| n.set(0));
    let canceled = WorkerOrder::snapshot(
        &native,
        observed.lock().unwrap().get(&native.client_order_id()),
    );
    assert!(
        canceled.is_closed(),
        "equal timestamps must not reuse an older Native event"
    );
    assert!(!new.is_closed());
    assert_eq!(CAPTURED_FILLS.with(|n| n.get()), 1001);
    let current = WorkerOrder::observe(&native, &cancel, Some(&new));
    observed
        .lock()
        .unwrap()
        .insert(native.client_order_id(), Arc::new(current));
    CAPTURED_FILLS.with(|n| n.set(0));
    let canceled = WorkerOrder::snapshot(
        &native,
        observed.lock().unwrap().get(&native.client_order_id()),
    );
    assert!(Arc::ptr_eq(
        &canceled,
        &observed.lock().unwrap()[&native.client_order_id()]
    ));
    assert_eq!(CAPTURED_FILLS.with(|n| n.get()), 0);
    observed.lock().unwrap().clear();
    CAPTURED_FILLS.with(|n| n.set(0));
    assert_eq!(
        WorkerOrder::snapshot(
            &native,
            observed.lock().unwrap().get(&native.client_order_id())
        )
        .fill_count(),
        1001
    );
    assert_eq!(CAPTURED_FILLS.with(|n| n.get()), 1001);
}

#[test]
fn verified_retirement_reuses_native_prefix_and_checks_changed_owners() {
    use crate::execution::context::{APPLIED_MEMBERSHIP_READS, WorkerOrder};
    let (mut evidence, mut native, next) = wallet_history(1000);
    let old = WorkerOrder::capture(&native);
    let owner = next.report.venue_order_id;
    assert!(
        evidence
            .confirmed_raw(&old, old.venue_order_id().unwrap())
            .unwrap()
            .is_some()
    );
    APPLIED_MEMBERSHIP_READS.with(|n| n.set(0));
    assert!(!evidence.has_unapplied(owner, &old));
    assert_eq!(APPLIED_MEMBERSHIP_READS.with(|n| n.get()), 0);
    evidence
        .commit(&[evidence.entries.values().next().unwrap().clone()])
        .unwrap();
    assert!(!evidence.has_unapplied(owner, &old));
    assert_eq!(APPLIED_MEMBERSHIP_READS.with(|n| n.get()), 0);

    let rebuilt = WorkerOrder::capture(&native);
    assert!(!evidence.has_unapplied(owner, &rebuilt));
    assert_eq!(
        APPLIED_MEMBERSHIP_READS.with(|n| n.get()),
        1,
        "equal lengths in a replaced Native history must not reuse the cursor"
    );
    let restored_events = native
        .events()
        .into_iter()
        .cloned()
        .map(|mut event| {
            if let nautilus_model::events::OrderEventAny::Filled(fill) = &mut event {
                fill.trade_id = format!("RESTORED-{}", fill.trade_id).as_str().into();
            }
            event
        })
        .collect();
    let restored_native = OrderAny::from_events(restored_events).unwrap();
    let replaced = WorkerOrder::capture(&restored_native);
    assert!(
        evidence.has_unapplied(owner, &replaced),
        "same-sized replacement histories must validate their actual IDs"
    );
    evidence.commit(std::slice::from_ref(&next)).unwrap();
    assert!(evidence.has_unapplied(owner, &old));
    let event = native_fill(&native, &next);
    native.apply(event.clone()).unwrap();
    let current = WorkerOrder::observe(&native, &event, Some(&old));
    assert!(!evidence.has_unapplied(owner, &current));
    assert!(evidence.has_unapplied(owner, &old));
    assert!(
        evidence
            .confirmed_raw(&current, current.venue_order_id().unwrap())
            .unwrap()
            .is_some()
    );
    APPLIED_MEMBERSHIP_READS.with(|n| n.set(0));
    assert!(!evidence.has_unapplied(owner, &current));
    assert_eq!(APPLIED_MEMBERSHIP_READS.with(|n| n.get()), 0);
    let mut extra = next;
    extra.report.trade_id = "NOT-APPLIED".into();
    evidence.commit(&[extra]).unwrap();
    assert!(evidence.has_unapplied(owner, &current));
}

#[test]
fn retirement_membership_cursor_does_not_substitute_zero_raw_economics() {
    use crate::execution::context::{APPLIED_MEMBERSHIP_READS, WorkerOrder};
    let (mut evidence, native, next) = wallet_history(1000);
    let order = WorkerOrder::capture(&native);
    let owner = next.report.venue_order_id;
    let expected = evidence
        .clone()
        .confirmed_raw(&order, order.venue_order_id().unwrap())
        .unwrap()
        .unwrap();
    assert!(!expected.is_zero());
    assert!(!evidence.has_unapplied(owner, &order));
    assert!(
        evidence.by_order[&owner]
            .prefix
            .as_ref()
            .unwrap()
            .raw
            .is_none()
    );
    APPLIED_MEMBERSHIP_READS.with(|n| n.set(0));
    assert!(!evidence.has_unapplied(owner, &order));
    assert_eq!(APPLIED_MEMBERSHIP_READS.with(|n| n.get()), 0);
    assert_eq!(
        evidence
            .confirmed_raw(&order, order.venue_order_id().unwrap())
            .unwrap(),
        Some(expected)
    );
    assert_eq!(
        evidence.by_order[&owner].prefix.as_ref().unwrap().raw,
        Some(expected)
    );
}

#[test]
#[ignore = "explicit pending wallet benchmark; PREDICT_PENDING_PATH is create_new JSON"]
fn benchmark_wallet_pending_work() {
    use std::{hint::black_box, time::Instant};
    let mut rows = Vec::new();
    for retained in [0, 1000, 9999] {
        for count in [0, 1, 32, 256] {
            let (mut evidence, native, mut incoming) = wallet_history(retained);
            let order = crate::execution::context::WorkerOrder::capture(&native);
            for index in 0..count {
                let mut fill = incoming.clone();
                fill.report.trade_id = format!("PENDING-{index}").as_str().into();
                evidence.prepare_wallet(&mut fill, &order, 16).unwrap();
                evidence.queue_wallet(&fill).unwrap();
            }
            let expected = U256::from(retained as u64 + count as u64) * incoming.raw.gross_shares;
            let mut wallet = Vec::new();
            let mut lifecycle = Vec::new();
            for iteration in 0..110 {
                let started = Instant::now();
                bench_wallet_prepare(
                    black_box(&mut evidence),
                    black_box(&order),
                    black_box(&mut incoming),
                );
                let elapsed = started.elapsed().as_nanos() as u64;
                if iteration >= 10 {
                    wallet.push(elapsed);
                }
                let started = Instant::now();
                assert_eq!(
                    evidence
                        .confirmed_raw(&order, order.venue_order_id().unwrap())
                        .unwrap(),
                    Some(expected)
                );
                let elapsed = started.elapsed().as_nanos() as u64;
                if iteration >= 10 {
                    lifecycle.push(elapsed);
                }
            }
            wallet.sort_unstable();
            lifecycle.sort_unstable();
            rows.push(serde_json::json!({"retained":retained,"pending":count,"wallet_ns":wallet,"lifecycle_ns":lifecycle}));
        }
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var("PREDICT_PENDING_PATH").unwrap())
        .unwrap();
    serde_json::to_writer_pretty(file, &serde_json::json!({"source":"release production wallet pending lookup + preparation and lifecycle pending lookup + raw sum; 10 warmups + 100 samples; excludes seeding, transport, Native application and callbacks", "rows":rows})).unwrap();
}
