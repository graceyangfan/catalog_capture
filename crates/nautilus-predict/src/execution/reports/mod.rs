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

//! Owned HTTP collection followed by Nautilus core-thread cache-dependent finalization.

pub(in crate::execution) mod bulk;
pub(in crate::execution) mod scope;
pub(in crate::execution) mod trades;

use std::{
    collections::HashSet,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use alloy::primitives::B256;
use anyhow::Context;
use nautilus_common::{
    clients::{ExecutionClient, ExecutionReportTask},
    factories::OrderEventFactory,
    messages::{
        ExecutionReport,
        execution::{
            GenerateFillReports, GenerateOrderStatusReport, GenerateOrderStatusReports,
            GeneratePositionStatusReports, QueryOrder,
        },
    },
};
use nautilus_core::{UUID4, UnixNanos, time::AtomicTime};
use nautilus_live::{ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::Account,
    enums::{AccountType, OrderStatus},
    identifiers::VenueOrderId,
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    reports::{ExecutionMassStatus, OrderStatusReport, PositionStatusReport},
};
use rust_decimal::Decimal;

use crate::execution::{PredictExecutionClient, account, context, fills, orders, parse};
use crate::{
    common::instrument_id_from_outcome,
    config::{PredictAccountStateMode, PredictExecutionClientConfig},
    execution::parse::market_id_from_instrument,
    http::{
        account_models::{PredictMatchData, PredictOrderData, PredictOrderStatus},
        parse::parse_position_status_report,
    },
    signing::PredictOrderHasher,
};

/// Explicit queries and report tasks collect independent order and trade observations. The venue's
/// amountFilled unit is unspecified: use zero/nonzero, never compare it to shares.
async fn collect_target_order(
    config: &PredictExecutionClientConfig,
    hash: B256,
    on_order: impl Fn(&PredictOrderData) -> anyhow::Result<()>,
) -> anyhow::Result<(PredictOrderData, Vec<PredictMatchData>)> {
    config.session.ensure_account_identity().await?;
    let row = config.session.request_order(hash).await?;
    on_order(&row)?;
    let matched = Decimal::from_str_exact(&row.amount_filled)?;
    anyhow::ensure!(matched >= Decimal::ZERO, "Predict negative filled amount");
    if matched.is_zero() {
        return Ok((row, Vec::new()));
    }
    let history = config
        .session
        .collect_matches(
            Some(row.market_id),
            config.report_page_size,
            config.max_report_pages,
        )
        .await?;
    Ok((row, history))
}

// The local submission registry supplies correlation before Native acceptance, never order state.
fn target_order_id(
    order: &impl context::OrderView,
    pending: &orders::PendingSubmissions,
) -> anyhow::Result<(VenueOrderId, B256)> {
    let venue_id = order
        .venue_order_id()
        .or_else(|| {
            pending
                .lock()
                .expect("submission registry poisoned")
                .entries
                .get(&order.client_order_id())
                .and_then(|entry| entry.venue_id)
        })
        .ok_or_else(|| {
            anyhow::anyhow!("Predict order query lacks a cached or locally transmitted hash")
        })?;
    let hash: B256 = venue_id
        .as_str()
        .parse()
        .map_err(|_| anyhow::anyhow!("Predict venue order ID is not an exchange hash"))?;
    Ok((venue_id, hash))
}

// A terminal lifecycle fact is not a cumulative fill snapshot. Publish events so
// a concurrent wallet fill cannot be voided by a quantity sampled before this IO.
fn publish_terminal(
    row: &PredictOrderData,
    context: &context::ReportContext<Arc<context::WorkerOrder>>,
    hasher: &PredictOrderHasher,
    emitter: &ExecutionEventEmitter,
    sync: &account::AccountSync,
    observed: UnixNanos,
) -> anyhow::Result<()> {
    if Decimal::from_str_exact(&row.amount_filled)?.is_zero() {
        return Ok(());
    }
    if !matches!(
        row.status,
        PredictOrderStatus::Cancelled | PredictOrderStatus::Expired
    ) {
        return Ok(());
    }
    let id = VenueOrderId::new(format!(
        "{:#x}",
        row.order
            .hash
            .ok_or_else(|| anyhow::anyhow!("Predict terminal row lacks hash"))?
    ));
    let order = context
        .orders
        .get(&id)
        .ok_or_else(|| anyhow::anyhow!("Predict terminal row lost its Native owner"))?;
    let Some(InstrumentAny::BinaryOption(instrument)) =
        context.instruments.get(&order.instrument_id())
    else {
        anyhow::bail!("Predict terminal instrument is missing");
    };
    let report =
        parse::parse_order_status_report(row, order, instrument, context, hasher, &[], observed)?;
    sync.confirm_report(&report);
    if order.is_closed() {
        return Ok(());
    }
    let factory = OrderEventFactory::new(
        emitter.trader_id(),
        context.account_id,
        AccountType::Cash,
        None,
    );
    if (report.order_status == OrderStatus::Expired || order.venue_order_id().is_none())
        && order.ts_accepted().is_none()
        && order.queued_acceptance().is_none()
    {
        emitter.try_send_order_event(order.accepted(&factory, id, observed, observed))?;
        order.record_acceptance(observed);
    }
    let event = if report.order_status == OrderStatus::Expired {
        order.expired(&factory, Some(id), observed, observed)
    } else {
        order.canceled(&factory, Some(id), observed, observed)
    };
    emitter.try_send_order_event(event)?;
    Ok(())
}

/// Conversion is shared; callers retain their Native delivery/commit boundaries.
fn prepare_target_report(
    venue_id: VenueOrderId,
    row: &PredictOrderData,
    matches: Vec<PredictMatchData>,
    context: &context::ReportContext<Arc<context::WorkerOrder>>,
    hasher: &PredictOrderHasher,
    evidence: &fills::FillEvidence,
    observed: UnixNanos,
) -> anyhow::Result<(OrderStatusReport, Vec<fills::BufferedFill>)> {
    let order = context
        .orders
        .get(&venue_id)
        .ok_or_else(|| anyhow::anyhow!("Predict target report lost order context"))?;
    let Some(InstrumentAny::BinaryOption(instrument)) =
        context.instruments.get(&order.instrument_id())
    else {
        anyhow::bail!("Predict target report instrument is missing");
    };
    let scope = GenerateFillReports::new(
        UUID4::new(),
        observed,
        Some(order.instrument_id()),
        Some(venue_id),
        None,
        None,
        None,
        None,
    );
    let mut fills =
        trades::prepare_match_reports(matches, observed, context, &scope, evidence, false)?;
    // A wallet fill queued before this report is also a confirmed fact. Include it
    // so the Native snapshot cannot void it merely because REST history is lagging.
    let mut ids = fills
        .iter()
        .map(|fill| fill.report.trade_id)
        .collect::<HashSet<_>>();
    for fill in evidence.pending_for(order, venue_id) {
        if ids.insert(fill.report.trade_id) {
            fills.push(fill.clone());
        }
    }
    let report = parse::parse_order_status_report(
        row,
        order,
        instrument,
        context,
        hasher,
        &fills.iter().collect::<Vec<_>>(),
        observed,
    )?;
    Ok((report, fills))
}

pub(in crate::execution) fn order_report_task(
    client: &PredictExecutionClient,
    cmd: &GenerateOrderStatusReport,
) -> ExecutionReportTask<Option<OrderStatusReport>> {
    let config = client.config.clone();
    let core = client.core.clone();
    let observed_orders = client.account_orders.clone();
    let clock = client.clock;
    let pending = client.pending_submissions.clone();
    let hasher = client.order_hasher.clone();
    let evidence = client.fill_evidence.clone();
    let rest_ids = client.rest_order_ids.clone();
    let account_sync = client.account_sync.clone();
    let emitter = client.emitter.clone();
    let cancellation = client.tasks.cancellation_token();
    let target = (|| -> anyhow::Result<_> {
        let cache = core.cache();
        let order = if let Some(id) = cmd.client_order_id {
            cache.order(&id)
        } else if let Some(id) = cmd.venue_order_id {
            cache.client_order_id(&id).and_then(|id| cache.order(id))
        } else {
            None
        }
        .ok_or_else(|| anyhow::anyhow!("Predict REST report requires a known native order"))?;
        let (venue_id, hash) = target_order_id(&*order, &pending)?;
        anyhow::ensure!(
            order.account_id() == Some(config.account_id)
                && cmd.venue_order_id.is_none_or(|id| id == venue_id)
                && cmd
                    .instrument_id
                    .is_none_or(|id| id == order.instrument_id()),
            "Predict REST command differs from cached order context"
        );
        market_id_from_instrument(&order.instrument_id())?;
        Ok((order.client_order_id(), venue_id, hash))
    })();
    let wallet = config.session.account_address();
    let account_id = config.account_id;
    let collection_orders = observed_orders.clone();
    let collection_instruments = client.metadata.instruments.clone();
    let collection_emitter = emitter.clone();
    let collection_sync = account_sync.clone();
    let collection_hasher = hasher.clone();
    let collection_cancellation = cancellation.clone();
    ExecutionReportTask::new(
        async move {
            let (client_id, venue_id, hash) = target?;
            let (row, matches) = tokio::time::timeout(
                Duration::from_secs(config.request_timeout_secs),
                collect_target_order(&config, hash, |row| {
                    anyhow::ensure!(
                        !collection_cancellation.is_cancelled(),
                        "Predict order report generation stopped"
                    );
                    if !matches!(
                        row.status,
                        PredictOrderStatus::Cancelled | PredictOrderStatus::Expired
                    ) {
                        return Ok(());
                    }
                    let Some(current) = collection_orders
                        .lock()
                        .expect("order snapshots poisoned")
                        .get(&client_id)
                        .cloned()
                    else {
                        return Ok(());
                    };
                    let Some(definition) =
                        collection_instruments.get_cloned(&current.instrument_id())
                    else {
                        return Ok(());
                    };
                    let instrument = definition.instrument;
                    let context = context::ReportContext::single_order(
                        account_id, wallet, false, venue_id, current, instrument,
                    );
                    publish_terminal(
                        row,
                        &context,
                        &collection_hasher,
                        &collection_emitter,
                        &collection_sync,
                        clock.get_time_ns(),
                    )
                }),
            )
            .await
            .context("Predict order report collection timed out")??;
            Ok((client_id, venue_id, row, matches, clock.get_time_ns()))
        },
        move |(client_id, venue_id, row, matches, observed)| {
            anyhow::ensure!(
                !cancellation.is_cancelled(),
                "Predict single report generation stopped"
            );
            let cache = core.cache();
            let order = cache
                .order(&client_id)
                .ok_or_else(|| anyhow::anyhow!("Predict order report lost native order context"))?;
            anyhow::ensure!(
                target_order_id(&*order, &pending)?.0 == venue_id,
                "Predict order venue ID changed during report collection"
            );
            let Some(InstrumentAny::BinaryOption(instrument)) =
                cache.instrument(&order.instrument_id())
            else {
                anyhow::bail!("Predict order report instrument is absent from Nautilus cache");
            };
            let current = context::WorkerOrder::snapshot(
                &order,
                observed_orders
                    .lock()
                    .expect("order snapshots poisoned")
                    .get(&client_id),
            );
            let context = context::ReportContext::single_order(
                account_id,
                wallet,
                cache
                    .account(&account_id)
                    .is_some_and(|a| !a.calculated_account_state()),
                venue_id,
                current,
                InstrumentAny::BinaryOption(instrument.clone()),
            );
            let (report, fills) = prepare_target_report(
                venue_id,
                &row,
                matches,
                &context,
                &hasher,
                &*evidence
                    .lock()
                    .map_err(|_| anyhow::anyhow!("Predict fill evidence poisoned"))?,
                observed,
            )?;
            orders::observe(&rest_ids, venue_id, &row.id);
            drop(order);
            drop(cache);
            account_sync.confirm_report(&report);
            anyhow::ensure!(
                !bulk::publish_unapplied(
                    std::slice::from_ref(&report),
                    &fills,
                    &context,
                    &evidence,
                    &emitter
                )?,
                "Predict confirmed matches queued to native engine; defer status until applied"
            );
            Ok(Some(report))
        },
    )
}

pub(in crate::execution) fn position_report_task(
    config: PredictExecutionClientConfig,
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    cmd: &GeneratePositionStatusReports,
) -> ExecutionReportTask<Vec<PositionStatusReport>> {
    let instrument_id = cmd.instrument_id;
    let historical = cmd.start.is_some() || cmd.end.is_some();
    let account_id = config.account_id;
    let expirations = core
        .cache()
        .instruments(&core.venue, None)
        .into_iter()
        .filter_map(|i| {
            if let InstrumentAny::BinaryOption(binary) = i {
                Some((binary.id(), binary.expiration_ns.as_u64()))
            } else {
                None
            }
        })
        .collect();
    ExecutionReportTask::new(
        async move {
            anyhow::ensure!(
                !historical,
                "Predict positions endpoint supplies current holdings only"
            );
            let market_id = instrument_id
                .as_ref()
                .map(market_id_from_instrument)
                .transpose()?;
            let rows =
                tokio::time::timeout(Duration::from_secs(config.request_timeout_secs), async {
                    config.session.ensure_account_identity().await?;
                    let rows = config
                        .session
                        .collect_positions(
                            market_id,
                            config.report_page_size,
                            config.max_report_pages,
                        )
                        .await?;
                    scope::active_holdings(&config, rows, &expirations, clock.get_time_ns()).await
                })
                .await
                .context("Predict position report collection timed out")??;
            Ok((rows, clock.get_time_ns()))
        },
        move |(rows, observed)| {
            let cache = core.cache();
            let mut reports = Vec::new();
            let mut seen = HashSet::new();
            for row in rows {
                let index = u8::try_from(row.outcome.index_set)
                    .map_err(|_| anyhow::anyhow!("unsupported Predict holding outcome"))?;
                let id = instrument_id_from_outcome(row.market.id, index)?;
                if instrument_id.is_some_and(|requested| requested != id) {
                    continue;
                }
                anyhow::ensure!(
                    seen.insert(id),
                    "Predict position pages contain duplicate outcome holdings"
                );
                let Some(InstrumentAny::BinaryOption(instrument)) = cache.instrument(&id) else {
                    anyhow::bail!("Predict holding instrument is missing from the Nautilus cache");
                };
                if instrument.expiration_ns.as_u64() <= observed.as_u64() {
                    continue;
                }
                reports.push(parse_position_status_report(
                    &row, instrument, account_id, observed,
                )?);
            }
            Ok(reports)
        },
    )
}

impl PredictExecutionClient {
    pub(in crate::execution) fn query_order_command(&self, cmd: &QueryOrder) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.core.is_connected() && self.tasks.is_open(),
            "Predict order query requires an active client generation"
        );
        let cache = self.core.cache();
        let order = cache
            .order(&cmd.client_order_id)
            .ok_or_else(|| anyhow::anyhow!("Predict order query requires a known native order"))?;
        // This registry owns transmission correlation only. Native state stays in Cache.
        let pending = Arc::clone(&self.pending_submissions);
        let (venue_id, hash) = target_order_id(&*order, &pending)?;
        anyhow::ensure!(
            cmd.trader_id == self.core.trader_id
                && cmd.trader_id == order.trader_id()
                && cmd.client_id.is_none_or(|id| id == self.core.client_id)
                && cmd.strategy_id == order.strategy_id()
                && cmd.instrument_id == order.instrument_id()
                && order.account_id() == Some(self.core.account_id)
                && cmd.venue_order_id.is_none_or(|id| id == venue_id),
            "Predict order query identity mismatch"
        );
        let client_id = order.client_order_id();
        drop(order);
        drop(cache);
        let orders = Arc::clone(&self.account_orders);
        let account_sync = Arc::clone(&self.account_sync);
        let rest_ids = Arc::clone(&self.rest_order_ids);
        let hasher = self.order_hasher.clone();
        let instruments = Arc::clone(&self.metadata.instruments);
        let recovery_read =
            self.account_refresh_required.load(Ordering::Acquire) || !self.is_connected();
        if recovery_read {
            self.account_refresh_required.store(true, Ordering::Release);
        }
        let evidence = Arc::clone(&self.fill_evidence);
        let shares_allowed = self.config.account_state_mode == PredictAccountStateMode::Reported
            && self
                .core
                .cache()
                .account(&self.core.account_id)
                .is_some_and(|a| !a.calculated_account_state());
        let config = self.config.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let cancellation = self.tasks.cancellation_token();
        self.tasks.spawn_named("predict-query-order", async move {
            let collection = collect_target_order(&config, hash, |row| {
                anyhow::ensure!(
                    !cancellation.is_cancelled(),
                    "Predict order query generation stopped"
                );
                if !matches!(
                    row.status,
                    PredictOrderStatus::Cancelled | PredictOrderStatus::Expired
                ) {
                    return Ok(());
                }
                let current = orders
                    .lock()
                    .expect("order snapshots poisoned")
                    .get(&client_id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("Predict terminal query lost Native context"))?;
                let instrument = instruments
                    .get_cloned(&current.instrument_id())
                    .ok_or_else(|| anyhow::anyhow!("Predict terminal instrument is missing"))?
                    .instrument;
                let context = context::ReportContext::single_order(
                    config.account_id,
                    config.session.account_address(),
                    shares_allowed,
                    venue_id,
                    current,
                    instrument,
                );
                publish_terminal(
                    row,
                    &context,
                    &hasher,
                    &emitter,
                    &account_sync,
                    clock.get_time_ns(),
                )
            });
            let result = tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                result = tokio::time::timeout(
                    Duration::from_secs(config.request_timeout_secs), collection,
                ) => result,
            };
            if cancellation.is_cancelled() {
                return;
            }
            let report = (|| -> anyhow::Result<_> {
                let (row, matches) = result.context("Predict targeted query timed out")??;
                let current = orders
                    .lock()
                    .expect("order snapshots poisoned")
                    .get(&client_id)
                    .cloned()
                    .filter(|order| {
                        order.client_order_id() == client_id
                            && order.account_id() == Some(config.account_id)
                    })
                    .ok_or_else(|| {
                        anyhow::anyhow!("Predict targeted query lost its native context")
                    })?;
                let Some(InstrumentAny::BinaryOption(instrument)) = instruments
                    .get_cloned(&current.instrument_id())
                    .map(|entry| entry.instrument)
                else {
                    anyhow::bail!("Predict targeted query instrument is missing");
                };
                anyhow::ensure!(
                    target_order_id(current.as_ref(), &pending)?.0 == venue_id,
                    "Predict order venue ID changed during query collection"
                );
                let context = context::ReportContext::single_order(
                    config.account_id,
                    config.session.account_address(),
                    shares_allowed,
                    venue_id,
                    current,
                    InstrumentAny::BinaryOption(instrument),
                );
                let mut facts = evidence
                    .lock()
                    .map_err(|_| anyhow::anyhow!("Predict evidence poisoned"))?;
                let (report, fills) = prepare_target_report(
                    venue_id,
                    &row,
                    matches,
                    &context,
                    &hasher,
                    &facts,
                    clock.get_time_ns(),
                )?;
                orders::observe(&rest_ids, venue_id, &row.id);
                if fills.is_empty() {
                    return Ok(Some(report));
                }
                facts.validate_commit(&fills)?;
                let terminal = matches!(
                    report.order_status,
                    OrderStatus::Canceled
                        | OrderStatus::Expired
                        | OrderStatus::Filled
                        | OrderStatus::Rejected
                );
                emitter.try_send_execution_report(ExecutionReport::OrderWithFills(
                    Box::new(report),
                    fills.iter().map(|fill| fill.report.clone()).collect(),
                ))?;
                facts.commit(&fills)?;
                drop(facts);
                // Native observers acquire account sync before fill evidence.
                // Never acquire the account lock while holding fill evidence.
                if terminal {
                    account_sync.confirm_terminal(venue_id, Some(client_id));
                }
                Ok(None)
            })();
            match report {
                Ok(Some(report)) => {
                    account_sync.confirm_report(&report);
                    let _ =
                        emitter.try_send_execution_report(ExecutionReport::Order(Box::new(report)));
                }
                Ok(None) => {}
                Err(error) => {
                    log::warn!("Predict targeted query did not resolve the order: {error}")
                }
            }
        })?;
        Ok(())
    }
}

impl PredictExecutionClient {
    pub(super) async fn mass_status(
        &self,
        lookback_mins: Option<u64>,
    ) -> anyhow::Result<Option<ExecutionMassStatus>> {
        anyhow::ensure!(
            self.core.is_connected(),
            "Predict mass status requires an active client generation"
        );
        // The native lookback scopes unknown fill history, never current orders/holdings.
        // Known orders retain their full settlement prefix for exact recovery.
        let now = self.clock.get_time_ns();
        let lookback_start = lookback_mins
            .map(nautilus_core::DurationNanos::try_from_mins)
            .transpose()?
            .map(|duration| now.saturating_sub(duration));
        let cancellation = self.tasks.cancellation_token();
        let epoch = self
            .wallet_epoch
            .as_ref()
            .map(|e| e.load(Ordering::Acquire));
        let cmd =
            GenerateOrderStatusReports::new(UUID4::new(), now, false, None, None, None, None, None);
        let task = bulk::bulk_task(self, &cmd, true, lookback_start);
        task.collection.await;
        let (orders, fills) = (task.finish)()?;
        let positions_cmd =
            GeneratePositionStatusReports::new(UUID4::new(), now, None, None, None, None, None);
        let positions = self
            .generate_position_status_reports(&positions_cmd)
            .await?;
        anyhow::ensure!(
            !cancellation.is_cancelled()
                && self.core.is_connected()
                && self
                    .wallet_epoch
                    .as_ref()
                    .map(|e| e.load(Ordering::Acquire))
                    == epoch,
            "Predict mass status generation changed during collection"
        );
        let mut report = ExecutionMassStatus::new(
            self.core.client_id,
            self.core.account_id,
            self.core.venue,
            now,
            None,
        );
        report.set_report_window(lookback_start, true);
        report.add_order_reports(orders);
        report.add_fill_reports(fills);
        report.add_position_reports(positions);
        Ok(Some(report))
    }
}
