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

//! Account-scoped confirmed match history, collected off-thread and finalized on the core thread.

use std::collections::{BTreeSet, HashMap, HashSet};

use alloy::primitives::{B256, U256};
use anyhow::Context;
use nautilus_common::{clients::ExecutionReportTask, messages::execution::GenerateFillReports};
use nautilus_core::{UnixNanos, time::AtomicTime};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    accounts::AccountAny,
    enums::{LiquiditySide, OrderSide},
    identifiers::{InstrumentId, TradeId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    reports::FillReport,
    types::Currency,
};

use crate::execution::{context, fills, parse, reports};
use crate::{
    common::{instrument_id_from_outcome, parse::exact_wei, parse_market_id},
    config::{PredictAccountStateMode, PredictExecutionClientConfig},
    execution::{
        context::ReportContext,
        fills::{BufferedFill, RawFillFacts, SharedFillEvidence},
        parse::{execution_price, settlement_trade_id},
    },
    http::account_models::{PredictFeeType, PredictMatchData},
};

pub(in crate::execution) fn fill_report_task(
    config: PredictExecutionClientConfig,
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    evidence: SharedFillEvidence,
    observed_orders: context::OrderSnapshots<nautilus_model::identifiers::ClientOrderId>,
    cmd: &GenerateFillReports,
) -> ExecutionReportTask<Vec<FillReport>> {
    let cmd = cmd.clone();
    let scope = cmd.clone();
    let wallet = config.session.account_address();
    let reported = config.account_state_mode == PredictAccountStateMode::Reported;
    // Native position recovery uses a bounded instrument-wide query. Explicit
    // order queries and unbounded history queries retain their original scope.
    let inventory_recovery =
        reported && cmd.start.is_some() && cmd.end.is_some() && cmd.venue_order_id.is_none();
    let (known, recovery_instruments): (HashSet<B256>, HashSet<InstrumentId>) =
        if inventory_recovery {
            core.cache()
                .orders_refs(Some(&core.venue), None, None, Some(&core.account_id), None)
                .into_iter()
                .filter(|order| {
                    cmd.instrument_id
                        .is_none_or(|id| id == order.instrument_id())
                })
                .filter_map(|order| {
                    Some((
                        order.venue_order_id()?.as_str().parse::<B256>().ok()?,
                        order.instrument_id(),
                    ))
                })
                .unzip()
        } else {
            Default::default()
        };
    let expirations = if inventory_recovery {
        core.cache()
            .instruments(&core.venue, None)
            .into_iter()
            .filter_map(|instrument| {
                if let InstrumentAny::BinaryOption(binary) = instrument {
                    Some((binary.id(), binary.expiration_ns.as_u64()))
                } else {
                    None
                }
            })
            .collect()
    } else {
        HashMap::new()
    };
    ExecutionReportTask::new(
        async move {
            anyhow::ensure!(
                cmd.start
                    .zip(cmd.end)
                    .is_none_or(|(start, end)| start <= end),
                "Predict fill query has an inverted time window"
            );
            if let Some(id) = cmd.venue_order_id {
                let hash: B256 = id.as_str().parse().map_err(|_| {
                    anyhow::anyhow!("Predict fill query order ID is not an exchange hash")
                })?;
                anyhow::ensure!(
                    !hash.is_zero() && VenueOrderId::new(format!("{hash:#x}")) == id,
                    "Predict fill query order hash is not canonical"
                );
            }
            let market_id = cmd
                .instrument_id
                .as_ref()
                .map(parse_market_id)
                .transpose()?;
            let rows = tokio::time::timeout(
                std::time::Duration::from_secs(config.request_timeout_secs),
                async {
                    config.session.ensure_account_identity().await?;
                    if !inventory_recovery {
                        return config
                            .session
                            .collect_matches(
                                market_id,
                                config.report_page_size,
                                config.max_report_pages,
                            )
                            .await;
                    }
                    let holdings = config
                        .session
                        .collect_positions(
                            market_id,
                            config.report_page_size,
                            config.max_report_pages,
                        )
                        .await?;
                    let holdings = reports::scope::active_holdings(
                        &config,
                        holdings,
                        &expirations,
                        clock.get_time_ns(),
                    )
                    .await?;
                    // Known orders retain late fills even after market expiry. Static retired
                    // wallet tokens alone do not widen a Native position-recovery query.
                    let mut markets = recovery_instruments
                        .iter()
                        .map(parse_market_id)
                        .collect::<anyhow::Result<BTreeSet<_>>>()?;
                    markets.extend(holdings.iter().map(|holding| holding.market.id));
                    let mut rows = Vec::new();
                    for market in markets {
                        rows.extend(
                            config
                                .session
                                .collect_matches(
                                    Some(market),
                                    config.report_page_size,
                                    config.max_report_pages,
                                )
                                .await?,
                        );
                    }
                    let selected =
                        reports::scope::required_orders(&rows, &holdings, wallet, &known, false)?;
                    rows.retain(|row| {
                        std::iter::once(&row.taker)
                            .chain(row.makers.iter())
                            .any(|leg| {
                                leg.signer == wallet
                                    && leg.hash.is_some_and(|hash| selected.contains(&hash))
                            })
                    });
                    Ok::<_, anyhow::Error>(rows)
                },
            )
            .await
            .context("Predict match history collection timed out")??;
            Ok((rows, clock.get_time_ns()))
        },
        move |(rows, ts_init)| {
            let cache = core.cache();
            let mut orders = HashMap::new();
            let mut instruments = HashMap::new();
            let observed = observed_orders.lock().expect("order snapshots poisoned");
            for row in &rows {
                for leg in std::iter::once(&row.taker)
                    .chain(&row.makers)
                    .filter(|leg| leg.signer == wallet)
                {
                    let id = crate::common::instrument_id_from_outcome(
                        row.market.id,
                        u8::try_from(leg.outcome.index_set)?,
                    )?;
                    if scope.instrument_id.is_some_and(|requested| requested != id) {
                        continue;
                    }
                    let hash = leg
                        .hash
                        .ok_or_else(|| anyhow::anyhow!("Predict match leg lacks order hash"))?;
                    let venue_id = VenueOrderId::new(format!("{hash:#x}"));
                    if scope
                        .venue_order_id
                        .is_some_and(|requested| requested != venue_id)
                    {
                        continue;
                    }
                    if let std::collections::hash_map::Entry::Vacant(entry) = instruments.entry(id)
                        && let Some(instrument) = cache.instrument(&id)
                    {
                        entry.insert(instrument.clone());
                    }
                    if let std::collections::hash_map::Entry::Vacant(entry) = orders.entry(venue_id)
                        && let Some(client_id) = cache.client_order_id(&venue_id)
                        && let Some(order) = cache.order(client_id)
                        && order.account_id() == Some(core.account_id)
                    {
                        entry.insert(context::WorkerOrder::snapshot(
                            &order,
                            observed.get(client_id),
                        ));
                    }
                }
            }
            drop(observed);
            let context = ReportContext {
                account_id: core.account_id,
                wallet,
                shares_allowed: reported && cache.account(&core.account_id).is_some_and(|account| {
                    matches!(&*account, AccountAny::Cash(cash) if !cash.base.calculate_account_state)
                }),
                instruments,
                orders,
            };
            Ok(
                commit_match_reports(rows, ts_init, &context, &scope, &evidence)?
                    .into_iter()
                    .map(|f| f.report)
                    .collect(),
            )
        },
    )
}

fn integer_wei(value: &str) -> anyhow::Result<U256> {
    anyhow::ensure!(
        !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
        "Predict match amount/price is not integer wei"
    );
    value
        .parse()
        .map_err(|_| anyhow::anyhow!("Predict match amount/price exceeds U256"))
}

pub(in crate::execution) fn match_timestamp(value: &str) -> anyhow::Result<UnixNanos> {
    let nanos = chrono::DateTime::parse_from_rfc3339(value)?
        .timestamp_nanos_opt()
        .filter(|value| *value >= 0)
        .ok_or_else(|| anyhow::anyhow!("Predict match timestamp exceeds UnixNanos range"))?;
    Ok(UnixNanos::from(nanos as u64))
}

pub(in crate::execution) fn commit_match_reports<O: context::OrderView>(
    rows: Vec<PredictMatchData>,
    ts_init: UnixNanos,
    context: &ReportContext<O>,
    scope: &GenerateFillReports,
    evidence: &SharedFillEvidence,
) -> anyhow::Result<Vec<BufferedFill>> {
    let mut facts = evidence
        .lock()
        .map_err(|_| anyhow::anyhow!("Predict fill evidence poisoned"))?;
    let mut reports = prepare_match_reports(rows, ts_init, context, scope, &facts, false)?;
    // Retain complete raw prefixes even when the caller requested a time window.
    facts.commit(&reports)?;
    filter_report_window(&mut reports, scope);
    Ok(reports)
}

/// Validate and normalize without committing. Target/bulk report finalizers first
/// validate status, then publish/commit; no clone of the account's evidence is needed.
/// Time filtering belongs after commit so raw cumulative prefixes remain complete.
pub(in crate::execution) fn prepare_match_reports<O: context::OrderView>(
    rows: Vec<PredictMatchData>,
    ts_init: UnixNanos,
    context: &ReportContext<O>,
    scope: &GenerateFillReports,
    evidence: &fills::FillEvidence,
    cached_only: bool,
) -> anyhow::Result<Vec<BufferedFill>> {
    let accept = |id| !cached_only || context.orders.contains_key(&id);
    let account_id = context.account_id;
    let wallet = context.wallet;
    let mut reports: HashMap<TradeId, BufferedFill> = HashMap::new();
    let mut identities = HashMap::new();
    for row in rows {
        if let Some(id) = scope.instrument_id {
            anyhow::ensure!(
                parse_market_id(&id)? == row.market.id,
                "Predict match response differs from requested market"
            );
        }
        let ts_event = match_timestamp(&row.executed_at)?;
        anyhow::ensure!(
            row.taker.signer == wallet || row.makers.iter().any(|leg| leg.signer == wallet),
            "Predict match response is outside the authenticated account"
        );
        for (leg, liquidity) in std::iter::once((&row.taker, LiquiditySide::Taker))
            .chain(row.makers.iter().map(|leg| (leg, LiquiditySide::Maker)))
        {
            if leg.signer != wallet {
                continue;
            }
            let index = u8::try_from(leg.outcome.index_set)?;
            let id = instrument_id_from_outcome(row.market.id, index)?;
            if scope.instrument_id.is_some_and(|requested| requested != id) {
                continue;
            }
            let hash = leg
                .hash
                .ok_or_else(|| anyhow::anyhow!("Predict match leg lacks order hash"))?;
            let venue_id = VenueOrderId::new(format!("{hash:#x}"));
            if !accept(venue_id) {
                continue;
            }
            if scope
                .venue_order_id
                .is_some_and(|requested| requested != venue_id)
            {
                continue;
            }
            let settlement = row
                .settlement_id
                .as_deref()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!("Predict match lacks cross-stream settlement identity")
                })?;
            anyhow::ensure!(
                !row.transaction_hash.is_zero() && !hash.is_zero(),
                "Predict match has an empty transaction or order hash"
            );
            let Some(InstrumentAny::BinaryOption(instrument)) = context.instruments.get(&id) else {
                anyhow::bail!("Predict match instrument is absent from Nautilus cache");
            };
            anyhow::ensure!(
                instrument.quote_currency() == Currency::USDT()
                    && instrument.raw_symbol().as_str().parse::<U256>().ok()
                        == Some(leg.outcome.on_chain_id)
                    && row
                        .market
                        .outcomes
                        .iter()
                        .any(|outcome| outcome.index_set == leg.outcome.index_set
                            && outcome.on_chain_id == leg.outcome.on_chain_id),
                "Predict match outcome/token differs from cached instrument"
            );
            let amount = integer_wei(&leg.amount)?;
            let price_wei = integer_wei(&leg.price)?;
            let last_qty = crate::common::parse::exact_wei_quantity(amount)?;
            let last_px = execution_price(exact_wei(price_wei)?, instrument.price_precision())?;
            anyhow::ensure!(
                !amount.is_zero() && last_px.as_decimal() > rust_decimal::Decimal::ZERO,
                "Predict match size or price is zero"
            );
            let side = OrderSide::from(leg.quote_type);
            if let Some(previous) = identities.insert(venue_id, (id, side)) {
                anyhow::ensure!(
                    previous == (id, side),
                    "Predict order hash has contradictory instrument or side identities"
                );
            }
            let commission = parse::fill_commission(
                leg.fee.amount,
                leg.fee.fee_type,
                amount,
                side,
                liquidity,
                context.shares_allowed,
            )?;
            let order = context.orders.get(&venue_id).map(|order| order.as_ref());
            let client_id = order.map(|o| o.client_order_id());
            if let Some(order) = order {
                // The hash and immutable terms identify this order. Venue timestamps
                // can be millisecond-rounded or precede the local submission clock;
                // they are not an ownership check (as in Binance/Polymarket dispatch).
                anyhow::ensure!(
                    order.account_id() == Some(account_id)
                        && (order.venue_order_id() == Some(venue_id)
                            || order.venue_order_id().is_none()
                                && matches!(
                                    order.status(),
                                    nautilus_model::enums::OrderStatus::Submitted
                                        | nautilus_model::enums::OrderStatus::PendingCancel
                                )
                                && order.ts_submitted().is_some()
                                && order.ts_accepted().is_none())
                        && order.instrument_id() == id
                        && order.order_side() == side
                        && !order.is_quote_quantity()
                        && last_qty <= order.submitted_quantity(),
                    "Predict match differs from cached native order context"
                );
            }
            let mut report = FillReport::new(
                account_id,
                id,
                venue_id,
                settlement_trade_id(settlement, hash),
                side,
                last_qty,
                last_px,
                commission,
                liquidity,
                client_id,
                None,
                ts_event,
                ts_init,
                None,
            );
            if leg.fee.fee_type == PredictFeeType::Shares && !leg.fee.amount.is_zero() {
                report.quantity_commission =
                    Some(crate::common::parse::exact_wei_quantity(leg.fee.amount)?);
            }
            let mut report = BufferedFill {
                report,
                raw: RawFillFacts {
                    settlement_id: settlement.to_owned(),
                    gross_shares: amount,
                    price: price_wei,
                    fee: leg.fee.amount,
                    fee_type: leg.fee.fee_type,
                    transaction_hash: Some(row.transaction_hash),
                },
            };
            if let Some(previous) = reports.get(&report.report.trade_id) {
                report.report.report_id = previous.report.report_id;
                anyhow::ensure!(
                    previous == &report,
                    "Predict duplicate match contains contradictory fill facts"
                );
            } else {
                reports.insert(report.report.trade_id, report);
            }
        }
    }
    let mut reports = reports.into_values().collect::<Vec<_>>();
    reports.sort_by_key(|report| (report.report.ts_event, report.report.trade_id));
    evidence.prepare_with_orders(
        &mut reports,
        |id| context.orders.get(&id).map(|order| order.as_ref()),
        identities
            .keys()
            .filter_map(|id| context.orders.get(id).map(|order| (*id, order.as_ref()))),
        &[],
        |id| context.instruments[&identities[&id].0].size_precision(),
    )?;
    Ok(reports)
}

fn filter_report_window(reports: &mut Vec<BufferedFill>, scope: &GenerateFillReports) {
    reports.retain(|report| {
        scope
            .start
            .is_none_or(|start| report.report.ts_event >= start)
            && scope.end.is_none_or(|end| report.report.ts_event <= end)
    });
}
