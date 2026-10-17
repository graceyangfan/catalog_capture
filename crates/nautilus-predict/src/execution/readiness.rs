// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.
//! Read-only verification of already-applied native state before releasing a stream-gap gate.
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, atomic::Ordering},
};

use nautilus_common::messages::execution::GenerateFillReports;
use nautilus_core::UUID4;
use nautilus_model::{
    accounts::{Account, AccountAny},
    enums::PositionSide,
    identifiers::{InstrumentId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::Order,
    types::{Currency, Money},
};
use rust_decimal::Decimal;

use super::{PredictExecutionClient, account::AccountReporter, matches::MatchContext};

pub(super) struct ReadinessCheck {
    reporter: AccountReporter,
    context: MatchContext,
    positions: HashMap<InstrumentId, Decimal>,
    account: AccountAny,
    revision: u64,
    epoch: u64,
    verified_epoch: Arc<std::sync::atomic::AtomicU64>,
    healthy: Arc<std::sync::atomic::AtomicBool>,
    required: Arc<std::sync::atomic::AtomicBool>,
    pending: super::orders::PendingSubmissions,
    cancels: super::cancellations::PendingCancels,
    evidence: super::precision::SharedFillEvidence,
    hasher: Arc<crate::signing::PredictOrderHasher>,
}
impl ReadinessCheck {
    pub fn new(client: &PredictExecutionClient) -> anyhow::Result<Self> {
        let cache = client.core.cache();
        // A retirement sweep keeps only unresolved settlements without a Native owner.
        // Account/holdings can lag those facts; they cannot independently clear this gate.
        anyhow::ensure!(
            client
                .orders
                .load()
                .values()
                .all(|order| !order.is_closed() || cache.order(&order.client_order_id()).is_some()),
            "Predict readiness has unapplied fills for a purged native order"
        );
        let account = cache
            .account_owned(&client.core.account_id)
            .ok_or_else(|| anyhow::anyhow!("Predict readiness lacks native account"))?;
        let orders = cache.orders_refs(
            Some(&client.core.venue),
            None,
            None,
            Some(&client.core.account_id),
            None,
        );
        // Unknown native IDs must not disappear in a filter_map and accidentally permit trading.
        let mut by_hash = HashMap::new();
        for order in orders {
            // REST cannot restore post-only intent. Both regular and post-only LIMITs
            // can remain live; unresolved MARKET executions still require reconciliation.
            anyhow::ensure!(
                order.is_closed()
                    || (matches!(
                        order.status(),
                        nautilus_model::enums::OrderStatus::Accepted
                            | nautilus_model::enums::OrderStatus::PartiallyFilled
                    ) && order.order_type() == nautilus_model::enums::OrderType::Limit),
                "Predict readiness requires terminal orders or acknowledged LIMIT orders"
            );
            // Position reconciliation orders are local native adjustments, never venue orders.
            if order.tags().is_some_and(|tags| {
                tags.iter().any(|tag| {
                    tag.as_str() == nautilus_execution::reconciliation::RECONCILIATION_ORDER_TAG
                })
            }) && order
                .venue_order_id()
                .is_some_and(|id| id.as_str().parse::<alloy::primitives::B256>().is_err())
                && order.status() == nautilus_model::enums::OrderStatus::Filled
            {
                continue;
            }
            let Some(id) = order.venue_order_id() else {
                anyhow::ensure!(
                    matches!(
                        order.status(),
                        nautilus_model::enums::OrderStatus::Rejected
                            | nautilus_model::enums::OrderStatus::Denied
                    ) && order.filled_qty().is_zero(),
                    "Predict readiness lacks native venue hash"
                );
                continue;
            };
            anyhow::ensure!(
                by_hash.insert(id, (*order).clone()).is_none(),
                "Predict readiness has duplicate hash ownership"
            );
        }
        let mut positions = HashMap::new();
        for p in cache.positions_open(
            Some(&client.core.venue),
            None,
            None,
            Some(&client.core.account_id),
            None,
        ) {
            let qty = match p.side {
                PositionSide::Long => p.quantity.as_decimal(),
                PositionSide::Short => -p.quantity.as_decimal(),
                _ => Decimal::ZERO,
            };
            *positions.entry(p.instrument_id).or_insert(Decimal::ZERO) += qty;
        }
        let mut reserved: HashMap<InstrumentId, Decimal> = HashMap::new();
        for order in by_hash
            .values()
            .filter(|o| !o.is_closed() && o.order_side() == nautilus_model::enums::OrderSide::Sell)
        {
            *reserved.entry(order.instrument_id()).or_default() +=
                super::resources::native_inventory(order);
        }
        for (instrument, quantity) in reserved {
            anyhow::ensure!(
                positions.get(&instrument).copied().unwrap_or_default()
                    + super::precision::POSITION_RECONCILIATION_TOLERANCE
                    >= quantity,
                "Predict readiness active SELL exceeds reconciled net holdings"
            );
        }
        let revision = client
            .account_sync
            .state
            .lock()
            .expect("account sync poisoned")
            .revision;
        Ok(Self {
            reporter: client.account_reporter()?,
            context: MatchContext {
                account_id: client.core.account_id,
                wallet: client.config.session.account_address(),
                shares_allowed: !account.calculated_account_state(),
                orders: by_hash,
                instruments: cache
                    .instruments(&client.core.venue, None)
                    .into_iter()
                    .map(|i| (i.id(), i.clone()))
                    .collect(),
            },
            positions,
            account,
            revision,
            epoch: client
                .wallet_epoch
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Predict readiness lacks epoch"))?
                .load(Ordering::Acquire),
            verified_epoch: client.verified_epoch.clone(),
            healthy: client.healthy.clone(),
            required: client.reconciliation_required.clone(),
            pending: client.pending_submissions.clone(),
            cancels: client.pending_cancels.clone(),
            evidence: client.fill_evidence.clone(),
            hasher: client.order_hasher.clone(),
        })
    }
    fn pending_resolved(&self) -> bool {
        self.pending
            .lock()
            .expect("submission registry poisoned")
            .entries
            .iter()
            .all(|(client, hash)| {
                hash.and_then(|id| self.context.orders.get(&id))
                    .is_some_and(|order| {
                        order.client_order_id() == *client
                            && !order.is_closed()
                            && order.ts_accepted().is_some()
                    })
            })
    }
    fn verify_open(
        &self,
        rows: &[crate::http::account_models::PredictOrderData],
    ) -> anyhow::Result<()> {
        let expected = self
            .context
            .orders
            .iter()
            .filter(|(_, o)| !o.is_closed())
            .map(|(id, _)| *id)
            .collect::<HashSet<_>>();
        let mut seen = HashSet::new();
        for row in rows {
            anyhow::ensure!(
                row.status == crate::http::account_models::PredictOrderStatus::Open,
                "Predict readiness OPEN contains a terminal row"
            );
            let hash = row
                .order
                .hash
                .ok_or_else(|| anyhow::anyhow!("Predict readiness OPEN lacks hash"))?;
            let id = VenueOrderId::new(format!("{hash:#x}"));
            anyhow::ensure!(
                expected.contains(&id) && seen.insert(id),
                "Predict readiness found unclaimed or duplicate venue OPEN order"
            );
            let hasher = &self.hasher;
            anyhow::ensure!(
                hasher.order_hash(
                    &row.order.fields,
                    crate::common::contracts::PredictExchangeType::from_market_flags(
                        row.is_neg_risk,
                        row.is_yield_bearing
                    )
                ) == hash,
                "Predict readiness OPEN signed fields differ from hash"
            );
            let order = &self.context.orders[&id];
            let Some(InstrumentAny::BinaryOption(instrument)) =
                self.context.instruments.get(&order.instrument_id())
            else {
                anyhow::bail!("Predict readiness lacks OPEN instrument");
            };
            super::parse::validate_rest_order_terms(
                row,
                order,
                instrument,
                self.context.account_id,
                self.context.wallet,
            )?;
        }
        anyhow::ensure!(
            seen == expected,
            "Predict readiness OPEN coverage differs from native active orders"
        );
        Ok(())
    }
    pub async fn confirm(self) -> anyhow::Result<()> {
        tokio::time::timeout(
            std::time::Duration::from_secs(self.reporter.config.report_timeout_secs),
            self.verify(),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Predict readiness verification timed out"))?
    }
    async fn verify(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.healthy.load(Ordering::Acquire)
                && self.reporter.authentication.is_authenticated()
                && self.reporter.connection_epoch.load(Ordering::Acquire) == self.epoch,
            "Predict readiness requires a fresh uninterrupted wallet session"
        );
        anyhow::ensure!(
            self.pending_resolved()
                && self
                    .cancels
                    .lock()
                    .expect("pending cancels poisoned")
                    .is_empty(),
            "Predict readiness has unresolved commands"
        );
        let cfg = &self.reporter.config;
        anyhow::ensure!(
            self.context.orders.len()
                <= usize::try_from(cfg.report_page_size)? * cfg.max_report_pages,
            "Predict readiness order scope exceeds configured bound"
        );
        cfg.session.request_account().await?;
        let open = super::account::collect_open_orders(cfg).await?;
        self.verify_open(&open)?;
        let rows = super::matches::collect_matches(cfg, None).await?;
        // Verify settlements for native-owned orders. Historical flat cycles do not
        // become new strategy activity; current OPEN orders and holdings are fenced below.
        let staged = Arc::new(std::sync::Mutex::new(
            self.evidence
                .lock()
                .map_err(|_| anyhow::anyhow!("Predict evidence poisoned"))?
                .clone(),
        ));
        let now = self.reporter.clock.get_time_ns();
        let scope = GenerateFillReports::new(UUID4::new(), now, None, None, None, None, None, None);
        let fills =
            super::matches::parse_cached_match_reports(rows, now, &self.context, &scope, &staged)?;
        let mut applied_quantities: HashMap<VenueOrderId, Decimal> = HashMap::new();
        for fill in &fills {
            anyhow::ensure!(
                super::parse::validate_applied_fill(
                    &fill.report,
                    &self.context.orders[&fill.report.venue_order_id]
                )?,
                "Predict readiness has unapplied settlement"
            );
            let quantity = applied_quantities
                .entry(fill.report.venue_order_id)
                .or_default();
            *quantity = quantity
                .checked_add(fill.report.last_qty.as_decimal())
                .ok_or_else(|| anyhow::anyhow!("Predict readiness fill quantity overflow"))?;
        }
        // Native owns terminal order states. Confirm their complete applied settlement
        // prefix, including late fills, without querying every retained historical hash.
        // A missing or unapplied fill keeps the gate closed for Native reconciliation.
        for (id, order) in &self.context.orders {
            anyhow::ensure!(
                applied_quantities.get(id).copied().unwrap_or_default()
                    == order.filled_qty().as_decimal(),
                "Predict readiness lacks complete applied settlement coverage"
            );
        }
        // OPEN already contains the signed terms and cumulative fill quantity used by
        // single-order reports. Reuse the standard parser; the second OPEN read below
        // fences changes while matches, holdings and account state are checked.
        for row in &open {
            let id = VenueOrderId::new(format!(
                "{:#x}",
                row.order.hash.expect("verified OPEN hash")
            ));
            let order = &self.context.orders[&id];
            let Some(InstrumentAny::BinaryOption(instrument)) =
                self.context.instruments.get(&order.instrument_id())
            else {
                anyhow::bail!("Predict readiness lacks binary instrument");
            };
            let report = super::reconciliation::status_report(
                row,
                order,
                instrument,
                &self.context,
                &self.hasher,
                &fills,
                now,
            )?;
            anyhow::ensure!(
                report.order_status == order.status() && report.filled_qty == order.filled_qty(),
                "Predict readiness order state differs from applied native state"
            );
        }
        let holdings = super::reports::collect_positions(cfg, None).await?;
        let mut venue_positions = HashMap::new();
        for row in holdings {
            let id = crate::common::instrument_id_from_outcome(
                row.market.id,
                u8::try_from(row.outcome.index_set)?,
            )?;
            let Some(InstrumentAny::BinaryOption(i)) = self.context.instruments.get(&id) else {
                anyhow::bail!("Predict readiness has unknown holding instrument");
            };
            let report = crate::http::parse::parse_position_status_report(
                &row,
                i,
                self.context.account_id,
                now,
            )?;
            anyhow::ensure!(
                venue_positions
                    .insert(id, report.signed_decimal_qty)
                    .is_none(),
                "Predict readiness has duplicate holdings"
            );
        }
        let keys = self
            .positions
            .keys()
            .chain(venue_positions.keys())
            .copied()
            .collect::<HashSet<_>>();
        for id in keys {
            anyhow::ensure!(
                (self.positions.get(&id).copied().unwrap_or_default()
                    - venue_positions.get(&id).copied().unwrap_or_default())
                .abs()
                    <= super::precision::POSITION_RECONCILIATION_TOLERANCE,
                "Predict readiness net position mismatch"
            );
        }
        let balance = self
            .reporter
            .chain
            .request_balance_snapshot(self.context.wallet, self.reporter.clock)
            .await?;
        let AccountAny::Cash(cash) = &self.account else {
            anyhow::bail!("Predict readiness requires a native cash account");
        };
        let mut locked = Money::zero(Currency::USDT());
        for order in self.context.orders.values().filter(|o| !o.is_closed()) {
            let instrument = self
                .context
                .instruments
                .get(&order.instrument_id())
                .ok_or_else(|| anyhow::anyhow!("Predict readiness lacks reservation instrument"))?;
            locked = locked
                .checked_add(super::resources::native_reservation(
                    cash, order, instrument,
                )?)
                .ok_or_else(|| {
                    anyhow::anyhow!("Predict readiness reservation aggregate overflow")
                })?;
        }
        let expected = super::account::reported_balance(&balance, locked)?;
        anyhow::ensure!(
            self.account.balance_total(Some(Currency::USDT())) == Some(expected.total)
                && self.account.balance_locked(Some(Currency::USDT())) == Some(expected.locked),
            "Predict readiness cash differs from applied native account"
        );
        let latest_fill = self
            .reporter
            .sync
            .state
            .lock()
            .expect("account sync poisoned")
            .latest_fill;
        let newest = self
            .context
            .orders
            .values()
            // REST observations and off-chain removal acknowledgements do not advance
            // chain cash. Requiring their observation time would keep a polling client
            // gated forever; canonical cash must cover actual settled native fills.
            .flat_map(|o| o.events().into_iter())
            .filter_map(|event| match event {
                nautilus_model::events::OrderEventAny::Filled(fill) => Some(fill.ts_event),
                _ => None,
            })
            .max()
            .unwrap_or_default()
            .max(latest_fill);
        anyhow::ensure!(
            balance.block_timestamp >= newest,
            "Predict readiness balance block predates confirmed settlement"
        );
        let fence = super::account::collect_open_orders(cfg).await?;
        self.verify_open(&fence)?;
        anyhow::ensure!(
            open.len() == fence.len()
                && open.iter().all(|a| fence.iter().any(|b| a.id == b.id
                    && a.order.hash == b.order.hash
                    && a.order.fields == b.order.fields
                    && a.amount_filled == b.amount_filled
                    && a.status == b.status
                    && a.strategy == b.strategy
                    && a.currency == b.currency
                    && a.market_id == b.market_id
                    && a.is_neg_risk == b.is_neg_risk
                    && a.is_yield_bearing == b.is_yield_bearing)),
            "Predict readiness OPEN fence changed"
        );
        let fence_rows = super::matches::collect_matches(cfg, None).await?;
        let fence_fills = super::matches::parse_cached_match_reports(
            fence_rows,
            self.reporter.clock.get_time_ns(),
            &self.context,
            &scope,
            &staged,
        )?;
        anyhow::ensure!(
            fence_fills.len() == fills.len(),
            "Predict readiness match fence changed"
        );
        for fill in &fence_fills {
            let order = self
                .context
                .orders
                .get(&fill.report.venue_order_id)
                .ok_or_else(|| {
                    anyhow::anyhow!("Predict readiness match fence has new ownership")
                })?;
            anyhow::ensure!(
                super::parse::validate_applied_fill(&fill.report, order)?
                    && fills
                        .iter()
                        .any(|f| f.report.trade_id == fill.report.trade_id),
                "Predict readiness match fence changed"
            );
        }
        let sync = self
            .reporter
            .sync
            .state
            .lock()
            .expect("account sync poisoned");
        anyhow::ensure!(
            sync.revision == self.revision
                && self.healthy.load(Ordering::Acquire)
                && self.reporter.authentication.is_authenticated()
                && self.reporter.connection_epoch.load(Ordering::Acquire) == self.epoch
                && self.pending_resolved()
                && self
                    .cancels
                    .lock()
                    .expect("pending cancels poisoned")
                    .is_empty(),
            "Predict readiness changed during verification"
        );
        let mut state = self
            .account
            .last_event()
            .ok_or_else(|| anyhow::anyhow!("Predict readiness lacks account provenance"))?;
        state.event_id = UUID4::new();
        state.ts_event = self.reporter.clock.get_time_ns();
        state.ts_init = state.ts_event;
        let info = state.info.get_or_insert_with(Default::default);
        info.insert(
            "predict_connection_epoch".into(),
            serde_json::json!(self.epoch),
        );
        info.insert(
            "predict_reconciliation_ready".into(),
            serde_json::json!(true),
        );
        info.insert(
            "predict_balance_wei".into(),
            serde_json::json!(balance.balance_wei.to_string()),
        );
        info.insert(
            "predict_block_number".into(),
            serde_json::json!(balance.block_number),
        );
        info.insert(
            "predict_block_hash".into(),
            serde_json::json!(format!("{:#x}", balance.block_hash)),
        );
        self.verified_epoch.store(self.epoch, Ordering::Release);
        self.required.store(false, Ordering::Release);
        if !self.healthy.load(Ordering::Acquire)
            || !self.reporter.authentication.is_authenticated()
            || self.reporter.connection_epoch.load(Ordering::Acquire) != self.epoch
        {
            self.required.store(true, Ordering::Release);
            anyhow::bail!("Predict readiness session changed at publication");
        }
        if let Err(error) = self.reporter.emitter.try_send_account_state(state) {
            self.required.store(true, Ordering::Release);
            return Err(error);
        }
        log::info!("Predict applied native state verified; stream-gap submission gate released");
        Ok(())
    }
}
