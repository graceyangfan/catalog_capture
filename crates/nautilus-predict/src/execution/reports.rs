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

use std::{
    collections::HashSet,
    sync::{Arc, atomic::Ordering},
};

use alloy::primitives::B256;
use nautilus_common::{
    clients::{ExecutionClient, ExecutionReportTask},
    messages::{
        ExecutionReport,
        execution::{GenerateOrderStatusReport, GeneratePositionStatusReports, QueryOrder},
    },
};
use nautilus_core::time::AtomicTime;
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    accounts::Account,
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    reports::{OrderStatusReport, PositionStatusReport},
};

use crate::{
    common::instrument_id_from_outcome,
    config::PredictExecutionClientConfig,
    execution::parse::{market_id_from_instrument, parse_submitted_order_report},
    http::{
        account_models::PredictPositionData, parse::parse_position_status_report,
        query::PredictPositionsQuery,
    },
};

pub(super) fn order_report_task(
    client: &super::PredictExecutionClient,
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
            .ok_or_else(|| anyhow::anyhow!("Predict REST report requires a cached venue hash"))?;
        anyhow::ensure!(
            order.account_id() == Some(config.account_id)
                && cmd.venue_order_id.is_none_or(|id| id == venue_id)
                && cmd
                    .instrument_id
                    .is_none_or(|id| id == order.instrument_id()),
            "Predict REST command differs from cached order context"
        );
        let hash: B256 = venue_id
            .as_str()
            .parse()
            .map_err(|_| anyhow::anyhow!("Predict venue order ID is not an exchange hash"))?;
        market_id_from_instrument(&order.instrument_id())?;
        Ok((order.client_order_id(), venue_id, hash))
    })();
    let wallet = config.session.account_address();
    let account_id = config.account_id;
    ExecutionReportTask::new(
        async move {
            let (client_id, venue_id, hash) = target?;
            let (row, matches) = tokio::time::timeout(
                std::time::Duration::from_secs(config.report_timeout_secs),
                async {
                    config.session.ensure_account_identity().await?;
                    let row = config.session.request_order(hash).await?;
                    let matched = rust_decimal::Decimal::from_str_exact(&row.amount_filled)?;
                    let matches = if !matched.is_zero() {
                        super::matches::collect_matches(&config, Some(row.market_id)).await?
                    } else {
                        vec![]
                    };
                    if !matched.is_zero() {
                        let fence = config.session.request_order(hash).await?;
                        anyhow::ensure!(
                            super::reconciliation::same_order(&row, &fence),
                            "Predict order changed during report match collection"
                        );
                    }
                    Ok::<_, anyhow::Error>((row, matches))
                },
            )
            .await
            .map_err(|_| anyhow::anyhow!("Predict order report collection timed out"))??;
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
                order.venue_order_id() == Some(venue_id)
                    || pending
                        .lock()
                        .expect("submission registry poisoned")
                        .entries
                        .get(&client_id)
                        .and_then(|entry| entry.venue_id)
                        == Some(venue_id),
                "Predict order venue ID changed during report collection"
            );
            let Some(InstrumentAny::BinaryOption(instrument)) =
                cache.instrument(&order.instrument_id())
            else {
                anyhow::bail!("Predict order report instrument is absent from Nautilus cache");
            };
            let context = super::matches::MatchContext {
                account_id,
                wallet,
                shares_allowed: cache
                    .account(&account_id)
                    .is_some_and(|a| !a.calculated_account_state()),
                orders: std::iter::once((
                    venue_id,
                    super::context::WorkerOrder::snapshot(
                        &order,
                        observed_orders
                            .lock()
                            .expect("order snapshots poisoned")
                            .get(&client_id),
                    ),
                ))
                .collect(),
                instruments: std::iter::once((
                    instrument.id(),
                    InstrumentAny::BinaryOption(instrument.clone()),
                ))
                .collect(),
            };
            let scope = nautilus_common::messages::execution::GenerateFillReports::new(
                nautilus_core::UUID4::new(),
                observed,
                Some(order.instrument_id()),
                Some(venue_id),
                None,
                None,
                None,
                None,
            );
            let fills = super::matches::prepare_match_reports(
                matches,
                observed,
                &context,
                &scope,
                &*evidence
                    .lock()
                    .map_err(|_| anyhow::anyhow!("Predict fill evidence poisoned"))?,
                false,
            )?;
            let report = super::reconciliation::status_report(
                &row,
                &*order,
                instrument,
                &context,
                &hasher,
                &fills.iter().collect::<Vec<_>>(),
                observed,
            )?;
            super::routing::register(&rest_ids, venue_id, super::routing::parse_id(&row.id)?)?;
            drop(order);
            drop(cache);
            account_sync.confirm_report(&report);
            anyhow::ensure!(
                !super::reconciliation::publish_unapplied(
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

pub(super) fn position_report_task(
    config: PredictExecutionClientConfig,
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    cmd: &GeneratePositionStatusReports,
) -> ExecutionReportTask<Vec<PositionStatusReport>> {
    let instrument_id = cmd.instrument_id;
    let historical = cmd.start.is_some() || cmd.end.is_some();
    let account_id = config.account_id;
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
            let rows = tokio::time::timeout(
                std::time::Duration::from_secs(config.report_timeout_secs),
                async {
                    config.session.ensure_account_identity().await?;
                    collect_positions(&config, market_id).await
                },
            )
            .await
            .map_err(|_| anyhow::anyhow!("Predict position report collection timed out"))??;
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
                reports.push(parse_position_status_report(
                    &row, instrument, account_id, observed,
                )?);
            }
            Ok(reports)
        },
    )
}

// The enclosing report/recovery operation validates session identity once before collection.
pub(super) async fn collect_positions(
    config: &PredictExecutionClientConfig,
    market_id: Option<u64>,
) -> anyhow::Result<Vec<PredictPositionData>> {
    let mut rows = Vec::new();
    let mut after = None;
    let mut seen = HashSet::new();
    for _ in 0..config.max_report_pages {
        let page = config
            .session
            .request_positions(&PredictPositionsQuery {
                first: Some(config.report_page_size),
                after: after.as_deref(),
                market_id,
                include_lost: Some(true),
                ..Default::default()
            })
            .await?;
        rows.extend(page.data);
        match page.cursor {
            None => return Ok(rows),
            Some(cursor) => {
                anyhow::ensure!(
                    seen.insert(cursor.clone()),
                    "Predict position pagination repeated a cursor"
                );
                after = Some(cursor);
            }
        }
    }
    anyhow::bail!("Predict position pagination exceeded its configured page bound")
}

impl super::PredictExecutionClient {
    pub(super) fn query_order_command(&self, cmd: &QueryOrder) -> anyhow::Result<()> {
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
        let registered = pending
            .lock()
            .expect("submission registry poisoned")
            .entries
            .get(&order.client_order_id())
            .and_then(|entry| entry.venue_id);
        let venue_id = order.venue_order_id().or(registered).ok_or_else(|| {
            anyhow::anyhow!("Predict order query lacks a cached or locally transmitted hash")
        })?;
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
        let hash: B256 = venue_id
            .as_str()
            .parse()
            .map_err(|_| anyhow::anyhow!("Predict order query requires an exchange hash"))?;
        let client_id = order.client_order_id();
        drop(order);
        drop(cache);
        let orders = Arc::clone(&self.account_orders);
        let account_sync = Arc::clone(&self.account_sync);
        let hasher = self.order_hasher.clone();
        let instruments = Arc::clone(&self.instruments);
        let healthy = Arc::clone(&self.healthy);
        let recovery_read =
            self.reconciliation_required.load(Ordering::Acquire) || !self.is_connected();
        if recovery_read {
            self.reconciliation_required.store(true, Ordering::Release);
        }
        let epoch = self
            .wallet_epoch
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Predict order query lacks connection identity"))?;
        let query_epoch = epoch.load(Ordering::Acquire);
        let evidence = Arc::clone(&self.fill_evidence);
        let shares_allowed = self.config.account_state_mode
            == crate::config::PredictAccountStateMode::Reported
            && self
                .core
                .cache()
                .account(&self.core.account_id)
                .is_some_and(|a| !a.calculated_account_state());
        let config = self.config.clone();
        let emitter = self.emitter.clone();
        let clock = self.clock;
        let cancellation = self.tasks.cancellation_token();
        if !self
            .order_queries
            .lock()
            .expect("Predict query tasks poisoned")
            .insert(client_id)
        {
            return Ok(());
        }
        let guard = QueryGuard {
            tasks: self.order_queries.clone(),
            id: client_id,
        };
        self.tasks.spawn_named("predict-query-order", async move {
            let _guard = guard;
            let collection = async {
                config.session.ensure_account_identity().await?;
                let row = config.session.request_order(hash).await?;
                let matched = rust_decimal::Decimal::from_str_exact(&row.amount_filled)
                    .map_err(|_| anyhow::anyhow!("Predict queried filled amount is invalid"))?;
                if matched.is_zero() {return Ok::<_, anyhow::Error>((row, None));}
                anyhow::ensure!(matched > rust_decimal::Decimal::ZERO, "Predict negative filled amount");
                let rows = super::matches::collect_matches(&config, Some(row.market_id)).await?;
                let fence = config.session.request_order(hash).await?;
                anyhow::ensure!(super::reconciliation::same_order(&row,&fence), "Predict order changed during match collection; query again");
                Ok((fence, Some(rows)))
            };
            let result = tokio::select! { biased;
                _ = cancellation.cancelled() => return,
                result = tokio::time::timeout(std::time::Duration::from_secs(config.report_timeout_secs), collection) => result,
            };
            if (!recovery_read && !healthy.load(Ordering::Acquire)) || epoch.load(Ordering::Acquire) != query_epoch { return; }
            let report = (|| -> anyhow::Result<_> {
                let (row, matches) = result.map_err(|_| anyhow::anyhow!("Predict targeted query timed out"))??;
                let current = orders.lock().expect("order snapshots poisoned").get(&client_id).cloned()
                    .filter(|order| order.client_order_id() == client_id && order.account_id() == Some(config.account_id))
                    .ok_or_else(|| anyhow::anyhow!("Predict targeted query lost its native context"))?;
                let Some(InstrumentAny::BinaryOption(instrument)) = instruments.get_cloned(&current.instrument_id()).map(|entry| entry.instrument) else {
                    anyhow::bail!("Predict targeted query instrument is missing");
                };
                if let Some(rows) = matches {
                    let hasher = &hasher;
                    if current.venue_order_id().is_none() {
                        anyhow::ensure!(pending.lock().expect("submission registry poisoned").entries.get(&client_id).and_then(|entry| entry.venue_id)==Some(venue_id), "Predict match recovery lost transmitted hash ownership");
                    }
                    let scope = nautilus_common::messages::execution::GenerateFillReports::new(
                        nautilus_core::UUID4::new(), clock.get_time_ns(), Some(current.instrument_id()), Some(venue_id), None,None,None,None);
                    let context = super::matches::MatchContext {
                        account_id:config.account_id,wallet:config.session.account_address(),shares_allowed,
                        instruments: std::iter::once((instrument.id(), InstrumentAny::BinaryOption(instrument.clone()))).collect(),
                        orders: std::iter::once((venue_id,Arc::clone(&current))).collect(),
                    };
                    let mut facts = evidence.lock().map_err(|_|anyhow::anyhow!("Predict evidence poisoned"))?;
                    let fills = super::matches::prepare_match_reports(rows,clock.get_time_ns(),&context,&scope,&facts,false)?;
                    let report = super::reconciliation::status_report(&row,&current,&instrument,&context,hasher,&fills.iter().collect::<Vec<_>>(),clock.get_time_ns())?;
                    facts.validate_commit(&fills)?;
                    let terminal = matches!(report.order_status,
                        nautilus_model::enums::OrderStatus::Canceled | nautilus_model::enums::OrderStatus::Expired
                            | nautilus_model::enums::OrderStatus::Filled | nautilus_model::enums::OrderStatus::Rejected);
                    emitter.try_send_execution_report(ExecutionReport::OrderWithFills(Box::new(report), fills.iter().map(|f|f.report.clone()).collect()))?;
                    facts.commit(&fills)?;
                    drop(facts);
                    // Native observers acquire account sync before fill evidence.
                    // Never acquire the account lock while holding fill evidence.
                    if terminal { account_sync.confirm_terminal(venue_id, Some(client_id)); }
                    return Ok(None);
                }
                if current.venue_order_id() == Some(venue_id) {
                    super::parse::rest_order_report_from_view(&row, &current, &instrument, config.account_id, config.session.account_address(), clock.get_time_ns()).map(Some)
                } else {
                    anyhow::ensure!(
                        pending.lock().expect("submission registry poisoned").entries.get(&client_id).and_then(|entry| entry.venue_id) == Some(venue_id)
                            && row.order.fields.maker == config.session.account_address(),
                        "Predict targeted query lost transmitted hash ownership"
                    );
                    let hasher = &hasher;
                    parse_submitted_order_report(&row, &current, &instrument, hasher, venue_id, clock.get_time_ns()).map(Some)
                }
            })();
            match report {
                Ok(Some(report)) => { account_sync.confirm_report(&report); let _ = emitter.try_send_execution_report(ExecutionReport::Order(Box::new(report))); },
                Ok(None) => {},
                Err(error) => log::warn!("Predict targeted query did not resolve the order: {error}"),
            }
        })?;
        Ok(())
    }
}

struct QueryGuard {
    tasks: Arc<std::sync::Mutex<HashSet<nautilus_model::identifiers::ClientOrderId>>>,
    id: nautilus_model::identifiers::ClientOrderId,
}
impl Drop for QueryGuard {
    fn drop(&mut self) {
        self.tasks
            .lock()
            .expect("Predict query tasks poisoned")
            .remove(&self.id);
    }
}

/// Markets required to interpret the account's current orders and holdings at host startup.
///
/// This only collects identities. InstrumentProvider/DataClient load definitions and Native
/// reconciliation remains authoritative. No historical market discovery or settlement is performed.
///
/// # Errors
/// Incomplete pages, invalid market identities and collection timeout fail rather than shrink scope.
pub async fn reconciliation_market_ids(
    config: &crate::config::PredictExecutionClientConfig,
) -> anyhow::Result<Vec<u64>> {
    tokio::time::timeout(
        std::time::Duration::from_secs(config.report_timeout_secs),
        async {
            config.session.ensure_account_identity().await?;
            let orders = super::reconciliation::collect_orders(config, true).await?;
            let positions = collect_positions(config, None).await?;
            let mut ids = std::collections::BTreeSet::new();
            for order in orders {
                anyhow::ensure!(
                    order.status == crate::http::account_models::PredictOrderStatus::Open,
                    "Predict startup OPEN query returned a terminal order"
                );
                anyhow::ensure!(
                    order.market_id > 0 && order.market_id <= i64::MAX as u64,
                    "Predict startup order market identity is invalid"
                );
                ids.insert(order.market_id);
            }
            for position in positions {
                if !position.amount.is_zero() {
                    crate::common::instrument_id_from_outcome(
                        position.market.id,
                        u8::try_from(position.outcome.index_set)?,
                    )?;
                    ids.insert(position.market.id);
                }
            }
            Ok(ids.into_iter().collect())
        },
    )
    .await
    .map_err(|_| anyhow::anyhow!("Predict startup account scope timed out"))?
}
