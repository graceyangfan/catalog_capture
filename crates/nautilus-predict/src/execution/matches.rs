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

use std::collections::{HashMap, HashSet};

use alloy::primitives::{Address, B256, U256};
use nautilus_common::{clients::ExecutionReportTask, messages::execution::GenerateFillReports};
use nautilus_core::{UnixNanos, time::AtomicTime};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    accounts::AccountAny,
    enums::{LiquiditySide, OrderSide},
    identifiers::{AccountId, InstrumentId, TradeId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    reports::FillReport,
    types::Currency,
};

use super::{
    parse::{market_id_from_instrument, price, settlement_trade_id},
    precision::{BufferedFill, RawFillFacts, SharedFillEvidence},
};
use crate::{
    common::{instrument_id_from_outcome, parse::exact_wei},
    config::{PredictAccountStateMode, PredictExecutionClientConfig},
    http::{
        account_models::{PredictFeeType, PredictMatchData, PredictQuoteType},
        query::PredictMatchesQuery,
    },
};

pub(super) fn fill_report_task(
    config: PredictExecutionClientConfig,
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    evidence: SharedFillEvidence,
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
    let known = core
        .cache()
        .orders_refs(Some(&core.venue), None, None, Some(&core.account_id), None)
        .into_iter()
        .filter_map(|o| o.venue_order_id())
        .filter_map(|id| id.as_str().parse().ok())
        .collect::<HashSet<B256>>();
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
                .map(market_id_from_instrument)
                .transpose()?;
            let rows = tokio::time::timeout(
                std::time::Duration::from_secs(config.report_timeout_secs),
                async {
                    let mut rows = collect_matches(&config, market_id).await?;
                    if inventory_recovery {
                        let holdings =
                            super::reports::collect_positions(&config, market_id).await?;
                        let selected = super::inventory_scope::required_orders(
                            &rows, &holdings, wallet, &known, false,
                        )?;
                        rows.retain(|row| {
                            std::iter::once(&row.taker)
                                .chain(row.makers.iter())
                                .any(|leg| {
                                    leg.signer == wallet
                                        && leg.hash.is_some_and(|h| selected.contains(&h))
                                })
                        });
                    }
                    Ok::<_, anyhow::Error>(rows)
                },
            )
            .await
            .map_err(|_| anyhow::anyhow!("Predict match history collection timed out"))??;
            Ok((rows, clock.get_time_ns()))
        },
        move |(rows, ts_init)| {
            let cache = core.cache();
            let context = MatchContext {
                account_id: core.account_id,
                wallet,
                shares_allowed: reported && cache.account(&core.account_id).is_some_and(|account| {
                    matches!(&*account, AccountAny::Cash(cash) if !cash.base.calculate_account_state)
                }),
                instruments: cache.instruments(&core.venue, None).into_iter().map(|i| (i.id(), i.clone())).collect(),
                orders: cache.orders_refs(Some(&core.venue), None, None, Some(&core.account_id), None)
                    .into_iter().filter_map(|o| o.venue_order_id().map(|id| (id, (*o).clone()))).collect(),
            };
            Ok(
                parse_match_reports(rows, ts_init, &context, &scope, &evidence)?
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

pub(super) fn match_timestamp(value: &str) -> anyhow::Result<UnixNanos> {
    let nanos = chrono::DateTime::parse_from_rfc3339(value)?
        .timestamp_nanos_opt()
        .filter(|value| *value >= 0)
        .ok_or_else(|| anyhow::anyhow!("Predict match timestamp exceeds UnixNanos range"))?;
    Ok(UnixNanos::from(nanos as u64))
}

pub(super) async fn collect_matches(
    config: &PredictExecutionClientConfig,
    market_id: Option<u64>,
) -> anyhow::Result<Vec<PredictMatchData>> {
    config.session.request_account().await?;
    let mut rows = Vec::new();
    let mut after = None;
    let mut seen = HashSet::new();
    for _ in 0..config.max_report_pages {
        let page = config
            .session
            .request_matches(&PredictMatchesQuery {
                first: Some(config.report_page_size),
                after: after.as_deref(),
                market_id,
                ..Default::default()
            })
            .await?;
        rows.extend(page.data);
        match page.cursor {
            None => return Ok(rows),
            Some(cursor) => {
                anyhow::ensure!(
                    seen.insert(cursor.clone()),
                    "Predict match pagination repeated a cursor"
                );
                after = Some(cursor);
            }
        }
    }
    anyhow::bail!("Predict match pagination exceeded its configured page bound")
}

/// Immutable native context; no worker may mutate Cache or adopt an arbitrary venue order.
pub(super) struct MatchContext {
    pub account_id: AccountId,
    pub wallet: Address,
    pub shares_allowed: bool,
    pub instruments: HashMap<InstrumentId, InstrumentAny>,
    pub orders: HashMap<VenueOrderId, OrderAny>,
}

pub(super) fn parse_match_reports(
    rows: Vec<PredictMatchData>,
    ts_init: UnixNanos,
    context: &MatchContext,
    scope: &GenerateFillReports,
    evidence: &SharedFillEvidence,
) -> anyhow::Result<Vec<BufferedFill>> {
    parse_match_reports_with_filter(rows, ts_init, context, scope, evidence, |_| true)
}

pub(super) fn parse_cached_match_reports(
    rows: Vec<PredictMatchData>,
    ts_init: UnixNanos,
    context: &MatchContext,
    scope: &GenerateFillReports,
    evidence: &SharedFillEvidence,
) -> anyhow::Result<Vec<BufferedFill>> {
    parse_match_reports_with_filter(rows, ts_init, context, scope, evidence, |id| {
        context.orders.contains_key(&id)
    })
}

fn parse_match_reports_with_filter(
    rows: Vec<PredictMatchData>,
    ts_init: UnixNanos,
    context: &MatchContext,
    scope: &GenerateFillReports,
    evidence: &SharedFillEvidence,
    accept: impl Fn(VenueOrderId) -> bool,
) -> anyhow::Result<Vec<BufferedFill>> {
    let account_id = context.account_id;
    let wallet = context.wallet;
    let mut reports: HashMap<TradeId, BufferedFill> = HashMap::new();
    let mut identities = HashMap::new();
    for row in rows {
        if let Some(id) = scope.instrument_id {
            anyhow::ensure!(
                market_id_from_instrument(&id)? == row.market.id,
                "Predict match response differs from requested market"
            );
        }
        let ts_event = match_timestamp(&row.executed_at)?;
        anyhow::ensure!(
            ts_event <= ts_init,
            "Predict match timestamp is in the future"
        );
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
            let last_qty =
                crate::common::parse::floor_wei_quantity(amount, instrument.size_precision())?;
            let last_px = price(exact_wei(price_wei)?, instrument.price_precision())?;
            anyhow::ensure!(
                !amount.is_zero() && last_px.as_decimal() > rust_decimal::Decimal::ZERO,
                "Predict match size or price is zero"
            );
            let side = match leg.quote_type {
                PredictQuoteType::Bid => OrderSide::Buy,
                PredictQuoteType::Ask => OrderSide::Sell,
            };
            if let Some(previous) = identities.insert(venue_id, (id, side)) {
                anyhow::ensure!(
                    previous == (id, side),
                    "Predict order hash has contradictory instrument or side identities"
                );
            }
            let commission = super::parse::fill_commission(
                leg.fee.amount,
                leg.fee.fee_type,
                amount,
                price_wei,
                side,
                liquidity,
                context.shares_allowed,
            )?;
            let order = context.orders.get(&venue_id);
            let client_id = order.map(|o| o.client_order_id());
            if let Some(order) = order {
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
                        && last_qty <= order.quantity()
                        && (!order.is_post_only() || liquidity == LiquiditySide::Maker)
                        && order
                            .ts_submitted()
                            .is_none_or(|submitted| ts_event >= submitted)
                        && order.price().is_none_or(|limit| match side {
                            OrderSide::Buy => last_px <= limit,
                            _ => last_px >= limit,
                        }),
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
            if cfg!(feature = "native-quantity-commission")
                && leg.fee.fee_type == PredictFeeType::Shares
                && !leg.fee.amount.is_zero()
            {
                report.quantity_commission = Some(crate::common::parse::exact_wei(leg.fee.amount)?);
            }
            let report = BufferedFill {
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
                let mut comparison = report.clone();
                comparison.report.report_id = previous.report.report_id;
                anyhow::ensure!(
                    previous == &comparison,
                    "Predict duplicate match contains contradictory fill facts"
                );
            } else {
                reports.insert(report.report.trade_id, report);
            }
        }
    }
    let mut reports = reports.into_values().collect::<Vec<_>>();
    reports.sort_by_key(|report| (report.report.ts_event, report.report.trade_id));
    let orders = reports
        .iter()
        .filter_map(|report| {
            context
                .orders
                .get(&report.report.venue_order_id)
                .cloned()
                .map(|order| (report.report.venue_order_id, order))
        })
        .collect();
    let precisions: HashMap<_, _> = reports
        .iter()
        .map(|report| {
            let instrument = context
                .instruments
                .get(&report.report.instrument_id)
                .expect("validated instrument");
            (report.report.venue_order_id, instrument.size_precision())
        })
        .collect();
    let mut facts = evidence
        .lock()
        .map_err(|_| anyhow::anyhow!("Predict fill evidence poisoned"))?;
    facts.prepare(&mut reports, &orders, &[], |id| precisions[&id])?;
    facts.commit(&reports)?;
    reports.retain(|report| {
        scope
            .start
            .is_none_or(|start| report.report.ts_event >= start)
            && scope.end.is_none_or(|end| report.report.ts_event <= end)
    });
    Ok(reports)
}
