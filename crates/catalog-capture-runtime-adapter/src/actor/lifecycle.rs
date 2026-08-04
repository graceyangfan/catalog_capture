// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::any::Any;

use super::*;
use crate::actor_runtime::{custom_data_client_id, optional_flush_all, optional_seal_all};
use crate::custom_data_requests::{parse_request_timer_index, CUSTOM_DATA_REQUEST_TIMER_PREFIX};
use catalog_capture_core::{
    append_hip4_universe_resolution_records, append_option_universe_resolution_records,
    forward_price_from_option_greeks, next_seal_boundary_ns,
};
use nautilus_common::{
    actor::DataActor, messages::data::CustomDataResponse, timer::TimeEvent,
};
use nautilus_core::UnixNanos;

impl CatalogCaptureActor {
    /// Bootstrap instrument metadata before market-data subscriptions.
    ///
    /// Adapters load definitions into cache during `connect` (HTTP bulk on Binance,
    /// Bybit, Deribit, OKX), so snapshot cache first. When cache is cold (e.g.
    /// Derive lazy_load), fall back to `request_instrument` and then subscribe for
    /// adapters that support instrument update streams.
    /// Instrument status is subscribed only when declared in `capture.instrument_statuses`.
    fn bootstrap_instruments(&mut self) -> Result<()> {
        for instrument_id in self.plan.planned_instrument_ids() {
            self.bootstrap_instrument(instrument_id)?;
        }

        Ok(())
    }

    fn bootstrap_instrument(&mut self, instrument_id: InstrumentId) -> Result<()> {
        let instrument = { self.cache().instrument(&instrument_id) };
        if let Some(instrument) = instrument {
            self.on_instrument(&instrument)
        } else {
            self.request_instrument(instrument_id, None, None, None, None)?;
            self.subscribe_instrument(instrument_id, None, None);
            Ok(())
        }
    }

    fn subscribe_plan(&mut self, plan: &CapturePlan) {
        // Subscribe path only (`subscribe_data` → live `on_data`).
        // Request-style jobs (`custom_data_requests`) must NOT be subscribed here.
        for spec in &plan.custom_data {
            self.subscribe_data(
                spec.data_type.clone(),
                custom_data_client_id(&spec.data_type).map(ClientId::from),
                None,
            );
        }

        for spec in &plan.mark_prices {
            self.subscribe_mark_prices(spec.instrument_id, None, None);
        }
        for spec in &plan.index_prices {
            self.subscribe_index_prices(spec.instrument_id, None, None);
        }
        for spec in &plan.funding_rates {
            self.subscribe_funding_rates(spec.instrument_id, None, None);
        }
        for spec in &plan.instrument_statuses {
            self.subscribe_instrument_status(spec.instrument_id, None, None);
        }
        for spec in &plan.instrument_closes {
            self.subscribe_instrument_close(spec.instrument_id, None, None);
        }
        for spec in &plan.option_greeks {
            self.subscribe_option_greeks(spec.instrument_id, None, None);
        }
        for spec in &plan.quotes {
            self.subscribe_quotes(spec.instrument_id, None, None);
        }
        for spec in &plan.trades {
            self.subscribe_trades(spec.instrument_id, None, None);
        }
        for spec in &plan.bars {
            self.subscribe_bars(spec.bar_type, None, None);
        }
        for spec in &plan.book_deltas {
            self.subscribe_book_deltas(spec.instrument_id, spec.book_type, None, None, false, None);
        }
    }

    fn unsubscribe_plan(&mut self, plan: &CapturePlan) {
        // Subscribe path only — request jobs have no subscription to tear down.
        for spec in &plan.custom_data {
            self.unsubscribe_data(
                spec.data_type.clone(),
                custom_data_client_id(&spec.data_type).map(ClientId::from),
                None,
            );
        }

        for spec in &plan.mark_prices {
            self.unsubscribe_mark_prices(spec.instrument_id, None, None);
        }
        for spec in &plan.index_prices {
            self.unsubscribe_index_prices(spec.instrument_id, None, None);
        }
        for spec in &plan.funding_rates {
            self.unsubscribe_funding_rates(spec.instrument_id, None, None);
        }
        for spec in &plan.instrument_statuses {
            self.unsubscribe_instrument_status(spec.instrument_id, None, None);
        }
        for spec in &plan.instrument_closes {
            self.unsubscribe_instrument_close(spec.instrument_id, None, None);
        }
        for spec in &plan.option_greeks {
            self.unsubscribe_option_greeks(spec.instrument_id, None, None);
        }
        for spec in &plan.quotes {
            self.unsubscribe_quotes(spec.instrument_id, None, None);
        }
        for spec in &plan.trades {
            self.unsubscribe_trades(spec.instrument_id, None, None);
        }
        for spec in &plan.bars {
            self.unsubscribe_bars(spec.bar_type, None, None);
        }
        for spec in &plan.book_deltas {
            self.unsubscribe_book_deltas(spec.instrument_id, None, None);
        }
    }

    fn apply_subscription_delta(&mut self, add: &CapturePlan, remove: &CapturePlan) -> Result<()> {
        for instrument_id in add.planned_instrument_ids() {
            self.bootstrap_instrument(instrument_id)?;
        }
        self.subscribe_plan(add);
        self.unsubscribe_plan(remove);
        self.sync_plan_state();
        Ok(())
    }

    fn log_option_universe_refresh(&self, change: &crate::DynamicOptionUniverseChange) {
        log::info!(
            "Option universe refresh venue_id={} underlying={} instruments={} -> {} add=[{}] remove=[{}]",
            change.venue_id,
            change.underlying,
            change.previous_count,
            change.next_count,
            change
                .added_instrument_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            change
                .removed_instrument_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
        );
    }

    fn log_hip4_universe_refresh(&self, change: &crate::DynamicHip4UniverseChange) {
        log::info!(
            "HIP-4 universe refresh venue_id={} underlying={} question_id={} instruments={} -> {} add=[{}] remove=[{}]",
            change.venue_id,
            change.underlying,
            change.question_id,
            change.previous_count,
            change.next_count,
            change
                .added_instrument_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
            change
                .removed_instrument_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", "),
        );
    }

    fn apply_dynamic_option_universe_refresh(&mut self) -> Result<()> {
        if self.dynamic_option_universe.is_none() {
            return Ok(());
        }

        let now = self.clock().timestamp_ns();
        let cache_rc = self.cache_rc();
        let delta = {
            let cache = cache_rc.borrow();
            self.dynamic_option_universe
                .as_mut()
                .expect("checked above")
                .refresh_from_cache(&cache, now)?
        };
        for change in &delta.changes {
            self.log_option_universe_refresh(change);
        }
        if !delta.is_empty() {
            for change in &delta.changes {
                if let Some(observer) = &mut self.online_option_metrics {
                    observer.apply_universe_change(change);
                }
            }
            if !delta.resolution_records.is_empty() {
                append_option_universe_resolution_records(
                    &self.catalog_root,
                    &delta.resolution_records,
                )?;
            }
            self.apply_subscription_delta(&delta.add, &delta.remove)?;
        }
        Ok(())
    }

    fn apply_dynamic_hip4_universe_refresh(&mut self) -> Result<()> {
        if self.dynamic_hip4_universe.is_none() {
            return Ok(());
        }

        let now = self.clock().timestamp_ns();
        let delta = self
            .dynamic_hip4_universe
            .as_mut()
            .expect("checked above")
            .refresh(now.as_u64())?;
        for change in &delta.changes {
            self.log_hip4_universe_refresh(change);
        }
        if !delta.is_empty() {
            if !delta.resolution_records.is_empty() {
                append_hip4_universe_resolution_records(
                    &self.catalog_root,
                    &delta.resolution_records,
                )?;
            }
            self.apply_subscription_delta(&delta.add, &delta.remove)?;
            self.purge_removed_hip4_instruments(&delta.changes);
        }
        self.schedule_hip4_universe_refresh()?;
        Ok(())
    }

    pub(crate) fn purge_removed_hip4_instruments(
        &mut self,
        changes: &[crate::DynamicHip4UniverseChange],
    ) {
        let Some(manager) = self.dynamic_hip4_universe.as_ref() else {
            return;
        };
        if !manager.purge_removed_instruments_enabled() {
            return;
        }

        let cache = self.cache_rc();
        let mut cache = cache.borrow_mut();
        for change in changes {
            for instrument_id in &change.removed_instrument_ids {
                if Some(*instrument_id) == change.perp_instrument_id {
                    continue;
                }
                cache.purge_instrument(*instrument_id);
            }
        }
    }

    fn schedule_hip4_universe_refresh(&mut self) -> Result<()> {
        let Some(manager) = &self.dynamic_hip4_universe else {
            return Ok(());
        };

        let now = self.clock().timestamp_ns();
        let delay_secs = manager.next_rotation_check_delay_secs(now.as_u64());
        let alert_time = now + UnixNanos::from(delay_secs.saturating_mul(1_000_000_000));
        self.clock().cancel_timer(HIP4_UNIVERSE_REFRESH_TIMER);
        self.clock()
            .set_time_alert_ns(HIP4_UNIVERSE_REFRESH_TIMER, alert_time, None, None)?;
        Ok(())
    }

    fn schedule_metrics_export(&mut self) -> Result<()> {
        let Some(interval_secs) = self.metrics_refresh_interval_secs else {
            return Ok(());
        };
        if self.metrics_snapshot.is_none() {
            return Ok(());
        }

        let interval_ns = interval_secs.saturating_mul(1_000_000_000);
        self.clock().cancel_timer(METRICS_EXPORT_TIMER);
        self.clock().set_timer_ns(
            METRICS_EXPORT_TIMER,
            interval_ns,
            None,
            None,
            None,
            None,
            None,
        )?;
        Ok(())
    }

    fn schedule_segment_seal(&mut self) -> Result<()> {
        if !self.capture.lifecycle.is_segment_mode() || !self.capture.lifecycle.seal.enabled {
            return Ok(());
        }

        let seal = self
            .capture
            .lifecycle
            .resolved_seal()?
            .expect("enabled seal schedule should resolve");
        let now = self.clock().timestamp_ns();
        let next = next_seal_boundary_ns(now.as_u64(), &seal);
        self.clock().cancel_timer(SEGMENT_SEAL_TIMER);
        self.clock()
            .set_time_alert_ns(SEGMENT_SEAL_TIMER, UnixNanos::from(next), None, None)?;
        Ok(())
    }

    fn seal_segment_runtimes(&mut self) -> Result<()> {
        if !self.capture.lifecycle.is_segment_mode() {
            return Ok(());
        }

        optional_flush_all(&self.instrument_runtime)?;
        optional_flush_all(&self.custom_data_runtime)?;
        optional_seal_all(&self.mark_price_runtime)?;
        optional_seal_all(&self.index_price_runtime)?;
        optional_seal_all(&self.funding_rate_runtime)?;
        optional_seal_all(&self.instrument_status_runtime)?;
        optional_seal_all(&self.instrument_close_runtime)?;
        optional_seal_all(&self.option_greeks_runtime)?;
        optional_seal_all(&self.quote_runtime)?;
        optional_seal_all(&self.trade_runtime)?;
        optional_seal_all(&self.bar_runtime)?;
        optional_seal_all(&self.book_delta_runtime)?;
        Ok(())
    }

    /// Start request-style custom data polling (`request_data` only).
    fn start_custom_data_request_jobs(&mut self) -> Result<()> {
        let job_count = self.custom_data_request_jobs.len();
        for index in 0..job_count {
            let fire_immediately = self.custom_data_request_jobs[index].spec.fire_immediately;
            if fire_immediately {
                self.fire_custom_data_request(index)?;
            }
            self.schedule_custom_data_request_timer(index)?;
        }
        Ok(())
    }

    fn schedule_custom_data_request_timer(&mut self, index: usize) -> Result<()> {
        let Some(job) = self.custom_data_request_jobs.get(index) else {
            return Ok(());
        };
        let timer_name = job.timer_name();
        let interval_ns = job.interval_ns();
        self.clock().cancel_timer(&timer_name);
        self.clock().set_timer_ns(
            timer_name.as_str(),
            interval_ns,
            None,
            None,
            None,
            None,
            None,
        )?;
        Ok(())
    }

    fn cancel_custom_data_request_timers(&mut self) {
        let timer_names: Vec<String> = self
            .custom_data_request_jobs
            .iter()
            .map(CustomDataRequestJob::timer_name)
            .collect();
        for timer_name in timer_names {
            self.clock().cancel_timer(&timer_name);
        }
    }

    /// Fire one Nautilus `request_data` for a request job (never `subscribe_data`).
    fn fire_custom_data_request(&mut self, index: usize) -> Result<()> {
        let now_ns = self.clock().timestamp_ns().as_u64();
        let Some(job) = self.custom_data_request_jobs.get_mut(index) else {
            return Ok(());
        };
        if !job.prepare_fire(now_ns) {
            return Ok(());
        }

        let data_type = job.data_type().clone();
        let client_id = ClientId::from(job.client_id_str());
        let type_name = data_type.type_name().to_string();
        let identifier = data_type.identifier().map(str::to_string);
        let polls = job.polls;

        log::info!(
            "custom_data_request fire type={type_name} id={identifier:?} client_id={client_id} poll={polls}"
        );

        // HTTP is owned by the venue DataClient (e.g. DeribitDataClient.http_client).
        DataActor::request_data(self, data_type, client_id, None, None, None, None)?;
        Ok(())
    }

    /// Sink request-response payloads and clear matching in-flight jobs.
    ///
    /// This is the request path only. Live subscribe traffic uses `on_data`.
    fn apply_custom_data_request_response(&mut self, resp: &CustomDataResponse) -> Result<()> {
        let items = resp
            .data
            .downcast_ref::<Vec<CustomData>>()
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        for item in items {
            self.submit_custom_data(item.clone())?;
        }

        let rows = items.len() as u64;
        let mut matched = false;
        for job in &mut self.custom_data_request_jobs {
            if job.matches_data_type(&resp.data_type) {
                job.complete_response(rows);
                matched = true;
                log::info!(
                    "custom_data_request response type={} id={:?} rows={rows} polls={} total_rows={}",
                    resp.data_type.type_name(),
                    resp.data_type.identifier(),
                    job.polls,
                    job.rows
                );
            }
        }

        if !matched && !items.is_empty() {
            log::debug!(
                "custom_data_request response type={} id={:?} rows={rows} had no matching poll job",
                resp.data_type.type_name(),
                resp.data_type.identifier()
            );
        }

        Ok(())
    }
}

impl DataActor for CatalogCaptureActor {
    fn on_start(&mut self) -> Result<()> {
        self.bootstrap_instruments()?;
        let plan = self.plan.clone();
        self.sync_forward_price_targets(&plan);
        // 1) Subscribe streams (includes subscribe-style custom_data only).
        self.subscribe_plan(&plan);
        // 2) Request polls (custom_data_requests only) — separate Nautilus path.
        self.start_custom_data_request_jobs()?;

        if let Some(manager) = &self.dynamic_option_universe {
            self.clock().set_timer_ns(
                OPTION_UNIVERSE_REFRESH_TIMER,
                manager.refresh_interval_secs() * 1_000_000_000,
                None,
                None,
                None,
                None,
                None,
            )?;
        }
        self.schedule_hip4_universe_refresh()?;
        self.schedule_segment_seal()?;
        self.publish_metrics_snapshot();
        self.schedule_metrics_export()?;

        Ok(())
    }

    fn on_stop(&mut self) -> Result<()> {
        self.clock().cancel_timer(OPTION_UNIVERSE_REFRESH_TIMER);
        self.clock().cancel_timer(HIP4_UNIVERSE_REFRESH_TIMER);
        self.clock().cancel_timer(SEGMENT_SEAL_TIMER);
        self.clock().cancel_timer(METRICS_EXPORT_TIMER);
        self.cancel_custom_data_request_timers();
        let _ = self.shutdown_all()?;
        self.publish_metrics_snapshot();
        self.log_capture_metrics_summary();
        Ok(())
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> Result<()> {
        if event.name == OPTION_UNIVERSE_REFRESH_TIMER {
            self.apply_dynamic_option_universe_refresh()?;
        }
        if event.name == HIP4_UNIVERSE_REFRESH_TIMER {
            self.apply_dynamic_hip4_universe_refresh()?;
        }
        if event.name == SEGMENT_SEAL_TIMER {
            self.seal_segment_runtimes()?;
            self.schedule_segment_seal()?;
        }
        if event.name == METRICS_EXPORT_TIMER {
            self.publish_metrics_snapshot();
            self.schedule_metrics_export()?;
        }
        if event.name.starts_with(CUSTOM_DATA_REQUEST_TIMER_PREFIX) {
            if let Some(index) = parse_request_timer_index(event.name.as_str()) {
                self.fire_custom_data_request(index)?;
            }
        }
        Ok(())
    }

    fn on_instrument(&mut self, instrument: &InstrumentAny) -> Result<()> {
        self.submit_instrument(instrument.clone())
    }

    /// Live subscribe path only (`subscribe_data` → stream).
    ///
    /// Request/poll responses must not use this callback; they arrive via
    /// `handle_data_response` / `on_historical_data`.
    fn on_data(&mut self, data: &CustomData) -> Result<()> {
        self.submit_custom_data(data.clone())
    }

    /// Request path completion (`request_data` → `CustomDataResponse`).
    ///
    /// Overrides the default handler so we retain `data_type` for in-flight
    /// job correlation (default only forwards the payload to `on_historical_data`).
    fn handle_data_response(&mut self, resp: &CustomDataResponse) {
        if let Err(error) = self.apply_custom_data_request_response(resp) {
            log::error!("custom_data_request response handling failed: {error:#}");
        }
    }

    /// Fallback request-path sink if a response arrives without our override path.
    fn on_historical_data(&mut self, data: &dyn Any) -> Result<()> {
        if let Some(items) = data.downcast_ref::<Vec<CustomData>>() {
            for item in items {
                self.submit_custom_data(item.clone())?;
            }
        }
        Ok(())
    }

    fn on_mark_price(&mut self, mark_price: &MarkPriceUpdate) -> Result<()> {
        self.submit_mark_price(*mark_price)
    }

    fn on_index_price(&mut self, index_price: &IndexPriceUpdate) -> Result<()> {
        self.submit_index_price(*index_price)
    }

    fn on_funding_rate(&mut self, funding_rate: &FundingRateUpdate) -> Result<()> {
        self.submit_funding_rate(*funding_rate)
    }

    fn on_instrument_status(&mut self, data: &InstrumentStatus) -> Result<()> {
        self.submit_instrument_status(*data)
    }

    fn on_instrument_close(&mut self, close: &InstrumentClose) -> Result<()> {
        self.submit_instrument_close(*close)
    }

    fn on_option_greeks(&mut self, greeks: &OptionGreeks) -> Result<()> {
        if let Some(observer) = &mut self.online_option_metrics {
            for line in observer.on_option_greeks(greeks) {
                log::info!("{line}");
            }
        }
        self.submit_option_greeks(*greeks)?;
        if self.forward_price_targets.contains(&greeks.instrument_id) {
            if let Some(forward_price) = forward_price_from_option_greeks(greeks) {
                self.persist_forward_price(forward_price)?;
            }
        }
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> Result<()> {
        if let Some(observer) = &mut self.online_option_metrics {
            for line in observer.on_quote(quote) {
                log::info!("{line}");
            }
        }
        self.submit_quote(*quote)?;
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> Result<()> {
        self.submit_trade(*trade)?;
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> Result<()> {
        self.submit_bar(*bar)?;
        Ok(())
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> Result<()> {
        self.submit_book_deltas(deltas)
    }
}
