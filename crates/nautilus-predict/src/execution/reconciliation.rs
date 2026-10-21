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

//! Venue report collection for Nautilus bulk and startup reconciliation. No cache mutation.
use std::{
    collections::{HashMap, HashSet},
    sync::atomic::Ordering,
};

use nautilus_common::{
    clients::ExecutionReportTask,
    messages::execution::{GenerateFillReports, GenerateOrderStatusReports},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    accounts::Account,
    identifiers::VenueOrderId,
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    reports::{FillReport, OrderStatusReport},
};

use super::{
    PredictExecutionClient,
    matches::MatchContext,
    precision::{BufferedFill, SharedFillEvidence},
};
use crate::{
    common::contracts::PredictExchangeType,
    config::PredictExecutionClientConfig,
    http::{
        account_models::{PredictMatchData, PredictOrderData},
        query::PredictOrdersQuery,
    },
    signing::PredictOrderHasher,
};

pub(super) fn fills_by_order(fills: &[BufferedFill]) -> HashMap<VenueOrderId, Vec<&BufferedFill>> {
    let mut grouped = HashMap::<_, Vec<_>>::new();
    for fill in fills {
        grouped
            .entry(fill.report.venue_order_id)
            .or_default()
            .push(fill);
    }
    grouped
}

pub(super) fn status_report<O: super::context::OrderView, C>(
    row: &PredictOrderData,
    order: &O,
    instrument: &nautilus_model::instruments::BinaryOption,
    context: &MatchContext<C>,
    hasher: &PredictOrderHasher,
    fills: &[&BufferedFill],
    now: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let id = VenueOrderId::new(format!(
        "{:#x}",
        row.order
            .hash
            .ok_or_else(|| anyhow::anyhow!("Predict report lacks signed hash"))?
    ));
    let amount = rust_decimal::Decimal::from_str_exact(&row.amount_filled)?;
    anyhow::ensure!(
        amount >= rust_decimal::Decimal::ZERO,
        "Predict negative REST filled amount"
    );
    anyhow::ensure!(
        VenueOrderId::new(format!(
            "{:#x}",
            hasher.order_hash(
                &row.order.fields,
                PredictExchangeType::from_market_flags(row.is_neg_risk, row.is_yield_bearing)
            )
        )) == id,
        "Predict report fields do not reproduce signed hash"
    );
    anyhow::ensure!(
        fills.iter().all(|fill| fill.report.venue_order_id == id),
        "Predict report received a different order's match group"
    );
    if !amount.is_zero() {
        return super::recovery::OrderRecovery {
            order,
            instrument,
            venue_id: id,
            account_id: context.account_id,
            wallet: context.wallet,
            hasher,
        }
        .report(row, fills, now);
    }
    anyhow::ensure!(
        fills.is_empty(),
        "Predict zero REST fill contradicts confirmed matches"
    );
    if order.venue_order_id().is_none() {
        super::parse::parse_submitted_order_report(row, order, instrument, hasher, id, now)
    } else {
        super::parse::rest_order_report_from_view(
            row,
            order,
            instrument,
            context.account_id,
            context.wallet,
            now,
        )
    }
}

pub(super) async fn collect_orders(
    config: &PredictExecutionClientConfig,
    open_only: bool,
) -> anyhow::Result<Vec<PredictOrderData>> {
    let mut rows = Vec::new();
    let mut after = None;
    let mut seen = HashSet::new();
    for _ in 0..config.max_report_pages {
        if open_only && let Some(probe) = &config.latency {
            probe.count(crate::latency::LatencyCounter::OpenPageRequests);
        }
        let page = config
            .session
            .request_orders(&PredictOrdersQuery {
                first: Some(config.report_page_size),
                after: after.as_deref(),
                status: open_only.then_some(crate::http::query::PredictOrderStatusFilter::Open),
            })
            .await?;
        rows.extend(page.data);
        match page.cursor {
            None => return Ok(rows),
            Some(cursor) => {
                anyhow::ensure!(
                    seen.insert(cursor.clone()),
                    "Predict order pagination repeated a cursor"
                );
                after = Some(cursor);
            }
        }
    }
    anyhow::bail!("Predict order pagination exceeded configured bound")
}

pub(super) fn same_order(a: &PredictOrderData, b: &PredictOrderData) -> bool {
    a.order.hash == b.order.hash
        && a.order.fields == b.order.fields
        && a.id == b.id
        && a.market_id == b.market_id
        && a.status == b.status
        && a.amount_filled == b.amount_filled
        && a.strategy == b.strategy
        && a.currency == b.currency
        && a.is_neg_risk == b.is_neg_risk
        && a.is_yield_bearing == b.is_yield_bearing
}

pub(super) fn bulk_task(
    client: &PredictExecutionClient,
    cmd: &GenerateOrderStatusReports,
    bundle_fills: bool,
    fill_start: Option<UnixNanos>,
) -> ExecutionReportTask<(Vec<OrderStatusReport>, Vec<FillReport>)> {
    client.prune_order_lookups_on_request();
    let config = client.config.clone();
    let core = client.core.clone();
    let observed_orders = client.account_orders.clone();
    let wallet = config.session.account_address();
    let clock = client.clock;
    let hasher = client.order_hasher.clone();
    let collection_hasher = hasher.clone();
    let emitter = client.emitter.clone();
    let pending = client.pending_submissions.clone();
    let evidence = client.fill_evidence.clone();
    let rest_ids = client.rest_order_ids.clone();
    let account_sync = client.account_sync.clone();
    let epoch = client.wallet_epoch.clone();
    let expected_epoch = epoch.as_ref().map(|e| e.load(Ordering::Acquire));
    let cancellation = client.tasks.cancellation_token();
    let targets = client
        .core
        .cache()
        .orders_refs(
            Some(&client.core.venue),
            None,
            None,
            Some(&client.core.account_id),
            None,
        )
        .into_iter()
        .filter(|o| cmd.instrument_id.is_none_or(|id| id == o.instrument_id()))
        .filter_map(|o| {
            o.venue_order_id().or_else(|| {
                pending
                    .lock()
                    .expect("submission registry poisoned")
                    .entries
                    .get(&o.client_order_id())
                    .and_then(|entry| entry.venue_id)
            })
        })
        .filter(|id| id.as_str().parse::<alloy::primitives::B256>().is_ok())
        .collect::<HashSet<_>>();
    let instrument_id = cmd.instrument_id;
    let open_only = cmd.open_only;
    let requested_token = instrument_id.and_then(|id| {
        core.cache()
            .instrument(&id)
            .map(|i| i.raw_symbol().to_string())
    });
    ExecutionReportTask::new(
        async move {
            // Like Polymarket's current-order list, Predict has no historical order
            // timestamps. Return current OPEN plus known hashes, regardless of Native's
            // lookback hint; never fabricate timestamps or adopt closed historical orders.
            anyhow::ensure!(
                instrument_id.is_none() || requested_token.is_some(),
                "Predict requested bulk instrument is absent"
            );
            let operation = async {
                config.session.ensure_account_identity().await?;
                let mut rows = collect_orders(&config, true).await?;
                if open_only {
                    anyhow::ensure!(
                        rows.iter().all(|row| row.status
                            == crate::http::account_models::PredictOrderStatus::Open),
                        "Predict OPEN query returned terminal order"
                    );
                }
                let mut hashes = HashSet::new();
                rows.retain(|row| {
                    instrument_id.is_none_or(|id| {
                        super::parse::market_id_from_instrument(&id).ok() == Some(row.market_id)
                    }) && requested_token
                        .as_ref()
                        .is_none_or(|token| *token == row.order.fields.token_id.to_string())
                });
                // A known canceled/expired order may be absent from an active-order list.
                // Ask by its original signed hash; absence is never interpreted as cancellation.
                for row in &rows {
                    let hash = row
                        .order
                        .hash
                        .ok_or_else(|| anyhow::anyhow!("Predict list lacks order hash"))?;
                    let id = VenueOrderId::new(format!("{hash:#x}"));
                    anyhow::ensure!(
                        hash == collection_hasher.order_hash(
                            &row.order.fields,
                            PredictExchangeType::from_market_flags(
                                row.is_neg_risk,
                                row.is_yield_bearing
                            )
                        ),
                        "Predict order fields do not reproduce signed hash"
                    );
                    anyhow::ensure!(
                        bundle_fills || targets.contains(&id),
                        "Predict order list contains unclaimed venue order"
                    );
                    anyhow::ensure!(
                        hashes.insert(id),
                        "Predict order pages contain duplicate hash"
                    );
                }
                if !open_only {
                    anyhow::ensure!(
                        targets.len()
                            <= usize::try_from(config.report_page_size)? * config.max_report_pages,
                        "Predict cached report scope exceeds bound"
                    );
                    for id in targets.difference(&hashes) {
                        rows.push(config.session.request_order(id.as_str().parse()?).await?);
                    }
                }
                let matched = !open_only
                    || rows.iter().any(|row| {
                        rust_decimal::Decimal::from_str_exact(&row.amount_filled)
                            .map_or(true, |qty| !qty.is_zero())
                    });
                let mut matches = if matched {
                    super::matches::collect_matches(&config, None).await?
                } else {
                    vec![]
                };
                if bundle_fills {
                    let holdings = super::reports::collect_positions(&config, None).await?;
                    let known = hashes
                        .iter()
                        .chain(targets.iter())
                        .map(|id| id.as_str().parse())
                        .collect::<Result<HashSet<_>, _>>()?;
                    let selected = super::inventory_scope::required_orders(
                        &matches, &holdings, wallet, &known, true,
                    )?;
                    matches.retain(|row| {
                        std::iter::once(&row.taker)
                            .chain(row.makers.iter())
                            .any(|leg| {
                                leg.signer == wallet
                                    && leg.hash.is_some_and(|h| selected.contains(&h))
                            })
                    });
                    let required = selected
                        .into_iter()
                        .map(|h| VenueOrderId::new(format!("{h:#x}")))
                        .collect::<HashSet<_>>();
                    anyhow::ensure!(
                        required.len()
                            <= usize::try_from(config.report_page_size)? * config.max_report_pages,
                        "Predict cold-start order scope exceeds bound"
                    );
                    let present = rows
                        .iter()
                        .filter_map(|r| r.order.hash)
                        .map(|h| VenueOrderId::new(format!("{h:#x}")))
                        .collect::<HashSet<_>>();
                    for id in required.difference(&present) {
                        rows.push(config.session.request_order(id.as_str().parse()?).await?);
                    }
                }
                // A second read protects status and raw cumulative interpretation while history is fetched.
                if matched {
                    for row in &rows {
                        let hash = row
                            .order
                            .hash
                            .ok_or_else(|| anyhow::anyhow!("Predict report lacks hash"))?;
                        let fence = config.session.request_order(hash).await?;
                        anyhow::ensure!(
                            same_order(row, &fence),
                            "Predict order changed during bulk match collection"
                        );
                    }
                }
                Ok::<_, anyhow::Error>((rows, matches, clock.get_time_ns()))
            };
            tokio::time::timeout(
                std::time::Duration::from_secs(config.report_timeout_secs),
                operation,
            )
            .await
            .map_err(|_| anyhow::anyhow!("Predict bulk report collection timed out"))?
        },
        move |(rows, matches, now)| {
            anyhow::ensure!(
                !cancellation.is_cancelled()
                    && epoch.as_ref().map(|e| e.load(Ordering::Acquire)) == expected_epoch,
                "Predict bulk report generation stopped or wallet epoch changed"
            );
            let cache = core.cache();
            let required = rows
                .iter()
                .filter_map(|row| row.order.hash)
                .chain(matches.iter().flat_map(|row| {
                    std::iter::once(&row.taker)
                        .chain(row.makers.iter())
                        .filter(|leg| leg.signer == wallet)
                        .filter_map(|leg| leg.hash)
                }))
                .map(|hash| VenueOrderId::new(format!("{hash:#x}")))
                .collect::<HashSet<_>>();
            let mut orders = HashMap::new();
            let observed = observed_orders.lock().expect("order snapshots poisoned");
            for client_id in
                cache.iter_client_order_ids(Some(&core.venue), None, None, Some(&core.account_id))
            {
                let order = cache.try_order_ref(&client_id)?;
                if let Some(id) = order.venue_order_id().or_else(|| {
                    pending
                        .lock()
                        .expect("submission registry poisoned")
                        .entries
                        .get(&order.client_order_id())
                        .and_then(|entry| entry.venue_id)
                }) {
                    if !required.contains(&id) {
                        continue;
                    }
                    anyhow::ensure!(
                        orders
                            .insert(
                                id,
                                super::context::WorkerOrder::snapshot(
                                    &order,
                                    observed.get(&client_id)
                                )
                            )
                            .is_none(),
                        "Predict duplicate native hash ownership"
                    );
                }
            }
            drop(observed);
            let context = MatchContext {
                account_id: core.account_id,
                wallet,
                shares_allowed: cache
                    .account(&core.account_id)
                    .is_some_and(|a| !a.calculated_account_state()),
                orders,
                instruments: cache
                    .instruments(&core.venue, None)
                    .into_iter()
                    .map(|i| (i.id(), i.clone()))
                    .collect(),
            };
            drop(cache);
            let routes = rows
                .iter()
                .map(|row| {
                    Ok((
                        VenueOrderId::new(format!(
                            "{:#x}",
                            row.order
                                .hash
                                .ok_or_else(|| anyhow::anyhow!("Predict route lacks hash"))?
                        )),
                        super::routing::parse_id(&row.id)?,
                    ))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let result = finish(
                rows,
                matches,
                now,
                &context,
                &hasher,
                &evidence,
                instrument_id,
                fill_start,
                bundle_fills,
            )?;
            let reported = result
                .0
                .iter()
                .map(|report| report.venue_order_id)
                .collect::<HashSet<_>>();
            let routes = routes
                .into_iter()
                .filter(|(hash, _)| reported.contains(hash))
                .collect::<Vec<_>>();
            super::routing::register_many(&rest_ids, &routes)?;
            for report in &result.0 {
                account_sync.confirm_report(report);
            }
            if !bundle_fills
                && publish_unapplied(&result.0, &result.1, &context, &evidence, &emitter)?
            {
                anyhow::bail!(
                    "Predict confirmed matches queued to native engine; defer status until applied"
                );
            }
            Ok((result.0, result.1.into_iter().map(|f| f.report).collect()))
        },
    )
}

#[expect(clippy::too_many_arguments)]
fn finish(
    rows: Vec<PredictOrderData>,
    matches: Vec<PredictMatchData>,
    now: UnixNanos,
    context: &MatchContext<impl super::context::OrderView>,
    hasher: &PredictOrderHasher,
    evidence: &SharedFillEvidence,
    instrument_id: Option<nautilus_model::identifiers::InstrumentId>,
    fill_start: Option<UnixNanos>,
    allow_external: bool,
) -> anyhow::Result<(Vec<OrderStatusReport>, Vec<BufferedFill>)> {
    let scope = GenerateFillReports::new(
        UUID4::new(),
        now,
        instrument_id,
        None,
        None,
        None,
        None,
        None,
    );
    let reported_hashes = rows
        .iter()
        .filter_map(|r| r.order.hash)
        .map(|h| VenueOrderId::new(format!("{h:#x}")))
        .collect::<HashSet<_>>();
    let mut scoped = Vec::with_capacity(matches.len());
    for row in matches {
        let own = std::iter::once(&row.taker)
            .chain(row.makers.iter())
            .filter(|leg| leg.signer == context.wallet)
            .collect::<Vec<_>>();
        anyhow::ensure!(
            !own.is_empty(),
            "Predict mass history is outside the authenticated account"
        );
        let mut known = false;
        for leg in own {
            let hash = leg
                .hash
                .ok_or_else(|| anyhow::anyhow!("Predict mass history lacks order hash"))?;
            let id = VenueOrderId::new(format!("{hash:#x}"));
            known |= context.orders.contains_key(&id) || reported_hashes.contains(&id);
        }
        let timestamp = super::matches::match_timestamp(&row.executed_at)?;
        if !known && fill_start.is_some_and(|start| timestamp < start) {
            continue;
        }
        scoped.push(row);
    }
    let mut fills = super::matches::prepare_match_reports(
        scoped,
        now,
        context,
        &scope,
        &*evidence
            .lock()
            .map_err(|_| anyhow::anyhow!("Predict fill evidence poisoned"))?,
        !allow_external,
    )?;
    anyhow::ensure!(
        fills
            .iter()
            .all(|f| context.orders.contains_key(&f.report.venue_order_id)
                || allow_external && reported_hashes.contains(&f.report.venue_order_id)),
        "Predict mass history contains unclaimed order"
    );
    let grouped = fills_by_order(&fills);
    let mut reports = Vec::new();
    for row in rows {
        if let Some(requested) = instrument_id {
            let instrument = context
                .instruments
                .get(&requested)
                .ok_or_else(|| anyhow::anyhow!("Predict requested bulk instrument is absent"))?;
            if instrument.raw_symbol().as_str() != row.order.fields.token_id.to_string() {
                continue;
            }
        }
        let id = VenueOrderId::new(format!(
            "{:#x}",
            row.order
                .hash
                .ok_or_else(|| anyhow::anyhow!("Predict report lacks hash"))?
        ));
        let Some(order) = context.orders.get(&id) else {
            anyhow::ensure!(
                allow_external,
                "Predict bulk report has unclaimed venue order"
            );
            reports.push(super::external::order_report(
                &row,
                context,
                hasher,
                grouped.get(&id).map_or(&[], Vec::as_slice),
                now,
            )?);
            continue;
        };
        if instrument_id.is_some_and(|id| id != order.instrument_id()) {
            continue;
        }
        let Some(InstrumentAny::BinaryOption(instrument)) =
            context.instruments.get(&order.instrument_id())
        else {
            anyhow::bail!("Predict bulk report instrument is absent");
        };
        let report = status_report(
            &row,
            order,
            instrument,
            context,
            hasher,
            grouped.get(&id).map_or(&[], Vec::as_slice),
            now,
        )?;
        reports.push(report);
    }
    reports.sort_by_key(|r| r.venue_order_id);
    let reported = reports
        .iter()
        .map(|r| r.venue_order_id)
        .collect::<HashSet<_>>();
    drop(grouped);
    fills.retain(|fill| reported.contains(&fill.report.venue_order_id));
    Ok((reports, fills))
}

/// A status-only reconciliation would infer a new trade ID for any unapplied cumulative
/// increment. Queue the authoritative native bundle and defer this status-only cycle.
pub(super) fn publish_unapplied(
    reports: &[OrderStatusReport],
    fills: &[BufferedFill],
    context: &MatchContext<impl super::context::OrderView>,
    evidence: &SharedFillEvidence,
    emitter: &nautilus_live::ExecutionEventEmitter,
) -> anyhow::Result<bool> {
    let mut targets = HashSet::new();
    for fill in fills {
        let order = context
            .orders
            .get(&fill.report.venue_order_id)
            .ok_or_else(|| anyhow::anyhow!("Predict report fill has no native owner"))?;
        if !super::parse::validate_applied_fill(&fill.report, order)? {
            targets.insert(fill.report.venue_order_id);
        }
    }
    if targets.is_empty() {
        return Ok(false);
    }
    let mut facts = evidence
        .lock()
        .map_err(|_| anyhow::anyhow!("Predict fill evidence poisoned"))?;
    // Wallet workers may have committed evidence since the staged report parse. Validate
    // every allocation against the current registry before publishing any native event.
    facts.validate_commit(fills)?;
    let grouped = fills_by_order(fills);
    for report in reports
        .iter()
        .filter(|r| targets.contains(&r.venue_order_id))
    {
        let own = grouped
            .get(&report.venue_order_id)
            .expect("unapplied target has fills");
        emitter.try_send_execution_report(
            nautilus_common::messages::ExecutionReport::OrderWithFills(
                Box::new(report.clone()),
                own.iter().map(|fill| fill.report.clone()).collect(),
            ),
        )?;
        for fill in own {
            facts.queue(std::slice::from_ref(*fill))?;
        }
    }
    Ok(true)
}
