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

use anyhow::Context;
use catalog_capture_core::{
    ForwardPrice, append_forward_price_records, forward_price_record_from_model, item::CaptureItem,
    runtime::FlushResult,
};
use nautilus_common::actor::DataActor;

use super::*;
use crate::actor_runtime::{
    enqueue_seal_runtime, optional_flush_all, optional_submit, submit_capture_item,
};

impl CatalogCaptureActor {
    pub(super) fn submit_audit(&self, file: &'static str, record: serde_json::Value) -> Result<()> {
        let now = self.clock().timestamp_ns().as_u64();
        optional_submit(
            &self.audit_runtime,
            CaptureItem {
                partition_key: PartitionKey::market_data("audit", file),
                event_ts_ns: now,
                init_ts_ns: Some(now),
                row_count: 1,
                estimated_bytes: record.to_string().len(),
                payload: (file, record),
            },
        )
    }

    pub(super) fn submit_instrument(&mut self, instrument: InstrumentAny) -> Result<()> {
        let instrument = self
            .hip4_rolling
            .definition(instrument.id())
            .unwrap_or(instrument);
        let id = instrument.id();
        let rolling_hip4 = self.hip4_rolling.definition(id).is_some();
        if rolling_hip4 {
            self.cache_rc()
                .borrow_mut()
                .add_instrument(instrument.clone())?;
        }
        if rolling_hip4
            && self.hip4_rolling.definitions_recorded.contains(&id)
            && !self.pending_instrument_definitions.contains(&id)
        {
            return Ok(());
        }
        let ts_init = Instrument::ts_init(&instrument).as_u64();
        if self.matched_sports.retiring.contains_key(&id)
            && !self.plan.planned_instrument_ids().contains(&id)
        {
            return Ok(());
        }
        // Static selectors may already have recorded a definition before a
        // matched market takes ownership of an additional stream for the ID.
        let matched = self.matched_sports_inbox.is_some()
            && matches!(id.venue.as_str(), "PREDICT" | "POLYMARKET")
            && self.plan.planned_instrument_ids().contains(&id);
        // Native publishes a definition and a correlated response; PM may publish both
        // outcomes for either request. Late/duplicate versions cannot overlap a sealed file.
        if matched
            && !self.pending_instrument_definitions.contains(&id)
            && self
                .matched_sports
                .instrument_versions
                .get(&id)
                .is_some_and(|last| ts_init <= *last)
        {
            return Ok(());
        }
        let result = optional_submit(
            &self.instrument_runtime,
            CaptureItem {
                partition_key: PartitionKey::market_data(
                    "instruments",
                    Instrument::id(&instrument),
                ),
                event_ts_ns: ts_init,
                init_ts_ns: Some(ts_init),
                row_count: 1,
                estimated_bytes: std::mem::size_of::<InstrumentAny>(),
                payload: instrument,
            },
        );
        if result.is_err() {
            self.pending_instrument_definitions.insert(id);
        } else {
            self.pending_instrument_definitions.remove(&id);
            if rolling_hip4 {
                self.hip4_rolling.definitions_recorded.insert(id);
            }
            if matched {
                self.matched_sports.instrument_versions.insert(id, ts_init);
            }
        }
        result
    }

    pub(super) fn retry_instrument_definitions(&mut self) -> Result<()> {
        if let Some(runtime) = &self.instrument_runtime
            && let Some(affected) = runtime.failures_since(&mut self.instrument_failure_cursor)
        {
            let mut candidates: BTreeSet<_> =
                self.plan.planned_instrument_ids().into_iter().collect();
            for market in self.active_predict_market_ids.values() {
                for outcome in [1, 2] {
                    candidates.insert(nautilus_predict::common::predict_outcome_instrument_id(
                        *market, outcome,
                    )?);
                }
            }
            self.pending_instrument_definitions
                .extend(candidates.into_iter().filter(|id| {
                    affected.contains(&PartitionKey::market_data("instruments", id).stable_key())
                }));
        }
        let mut failure = None;
        for id in self.pending_instrument_definitions.clone() {
            if let Some(instrument) = self.cache().instrument(&id)
                && let Err(error) = self.submit_instrument(instrument)
            {
                failure.get_or_insert(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }

    pub(super) fn submit_quote(&mut self, quote: QuoteTick) -> Result<()> {
        if !self.accepts_market_data(quote.instrument_id, || {
            self.plan
                .quotes
                .iter()
                .any(|spec| spec.instrument_id == quote.instrument_id)
        }) {
            return Ok(());
        }
        submit_capture_item(
            &self.quote_runtime,
            PartitionKey::catalog_data::<QuoteTick>(quote.instrument_id),
            quote.ts_event.as_u64(),
            Some(quote.ts_init.as_u64()),
            quote,
        )
    }

    pub(super) fn submit_custom_data(&mut self, data: CustomData) -> Result<()> {
        self.reap_retired_predict_markets()?;
        let data_type = data.data_type.clone();
        if self.lighter.enabled()
            && data_type.type_name() == "LighterOpenInterest"
            && !self
                .plan
                .custom_data
                .iter()
                .any(|s| s.data_type == data_type)
        {
            return Ok(());
        }
        let ts_init = data.data.ts_init().as_u64();
        let event_ts = data.data.ts_event().as_u64();
        let mut retired_predict_market_id = None;
        let runtime = match predict_output_selector_key(&data_type)? {
            Some(selector_key) => {
                let market_id = data_type
                    .identifier()
                    .ok_or_else(|| anyhow::anyhow!("Predict snapshot is missing market ID"))?
                    .parse::<u64>()
                    .map_err(|_| anyhow::anyhow!("invalid Predict snapshot market ID"))?;
                // A rolling product's successor is confirmed only by its first valid snapshot.
                // Seal at that event—not at a speculative wall-clock boundary—so late valid
                // updates from the retiring market remain in its final segment and the new
                // market begins one clean part without reopening the old identity.
                if let Some(previous_market_id) = self
                    .active_predict_market_ids
                    .get(&selector_key)
                    .copied()
                    .filter(|active_market_id| *active_market_id != market_id)
                {
                    log::info!(
                        "catalog-capture: confirmed Predict rollover interval={}s market {} -> {}; sealing retired segment",
                        selector_key.interval_secs(),
                        previous_market_id,
                        market_id
                    );
                    if let Some(runtime) = self.predict_custom_data_runtimes.get(&selector_key) {
                        enqueue_seal_runtime(runtime)?;
                    }
                    retired_predict_market_id = Some(previous_market_id);
                }
                self.active_predict_market_ids
                    .insert(selector_key.clone(), market_id);
                self.predict_custom_data_runtimes.get(&selector_key)
            }
            _ => Some(self.custom_data_runtime.as_ref().ok_or_else(|| {
                anyhow::anyhow!("custom-data callback received data without a generic writer")
            })?),
        };
        if let Some(runtime) = runtime {
            runtime.submit(CaptureItem {
                partition_key: PartitionKey::custom_data(
                    data_type.type_name(),
                    data_type.identifier().map(str::to_string),
                    data_type.topic(),
                ),
                event_ts_ns: event_ts,
                init_ts_ns: Some(ts_init),
                row_count: 1,
                estimated_bytes: std::mem::size_of::<CustomData>(),
                payload: data,
            })?;
        }

        self.sync_predict_native(&data_type)?;

        // Native successor subscriptions are admitted before retiring the old definitions.
        // Keep them cached while another strategy still owns an old-market subscription.
        if let Some(market_id) = retired_predict_market_id {
            self.retiring_predict_market_ids.insert(market_id);
        }
        Ok(())
    }

    pub(super) fn submit_mark_price(&mut self, data: MarkPriceUpdate) -> Result<()> {
        if !self.accepts_market_data(data.instrument_id, || {
            self.plan
                .mark_prices
                .iter()
                .any(|spec| spec.instrument_id == data.instrument_id)
        }) {
            return Ok(());
        }
        submit_capture_item(
            &self.mark_price_runtime,
            PartitionKey::catalog_data::<MarkPriceUpdate>(data.instrument_id),
            data.ts_event.as_u64(),
            Some(data.ts_init.as_u64()),
            data,
        )
    }

    pub(super) fn submit_index_price(&mut self, data: IndexPriceUpdate) -> Result<()> {
        if !self.accepts_market_data(data.instrument_id, || {
            self.plan
                .index_prices
                .iter()
                .any(|spec| spec.instrument_id == data.instrument_id)
        }) {
            return Ok(());
        }
        submit_capture_item(
            &self.index_price_runtime,
            PartitionKey::catalog_data::<IndexPriceUpdate>(data.instrument_id),
            data.ts_event.as_u64(),
            Some(data.ts_init.as_u64()),
            data,
        )
    }

    pub(super) fn submit_funding_rate(&mut self, data: FundingRateUpdate) -> Result<()> {
        if !self.accepts_market_data(data.instrument_id, || {
            self.plan
                .funding_rates
                .iter()
                .any(|spec| spec.instrument_id == data.instrument_id)
        }) {
            return Ok(());
        }
        submit_capture_item(
            &self.funding_rate_runtime,
            PartitionKey::catalog_data::<FundingRateUpdate>(data.instrument_id),
            data.ts_event.as_u64(),
            Some(data.ts_init.as_u64()),
            data,
        )
    }

    pub(super) fn submit_instrument_status(&mut self, data: InstrumentStatus) -> Result<()> {
        if !self.accepts_market_data(data.instrument_id, || {
            self.plan
                .instrument_statuses
                .iter()
                .any(|spec| spec.instrument_id == data.instrument_id)
        }) {
            return Ok(());
        }
        submit_capture_item(
            &self.instrument_status_runtime,
            PartitionKey::catalog_data::<InstrumentStatus>(data.instrument_id),
            data.ts_event.as_u64(),
            Some(data.ts_init.as_u64()),
            data,
        )
    }

    pub(super) fn submit_instrument_close(&mut self, data: InstrumentClose) -> Result<()> {
        if !self.accepts_market_data(data.instrument_id, || {
            self.plan
                .instrument_closes
                .iter()
                .any(|spec| spec.instrument_id == data.instrument_id)
        }) {
            return Ok(());
        }
        submit_capture_item(
            &self.instrument_close_runtime,
            PartitionKey::catalog_data::<InstrumentClose>(data.instrument_id),
            data.ts_event.as_u64(),
            Some(data.ts_init.as_u64()),
            data,
        )
    }

    pub(super) fn submit_option_greeks(&mut self, data: OptionGreeks) -> Result<()> {
        if !self.accepts_market_data(data.instrument_id, || {
            self.plan
                .option_greeks
                .iter()
                .any(|spec| spec.instrument_id == data.instrument_id)
        }) {
            return Ok(());
        }
        submit_capture_item(
            &self.option_greeks_runtime,
            PartitionKey::catalog_data::<OptionGreeks>(data.instrument_id),
            data.ts_event.as_u64(),
            Some(data.ts_init.as_u64()),
            data,
        )
    }

    pub(super) fn persist_forward_price(&mut self, forward_price: ForwardPrice) -> Result<()> {
        let record =
            forward_price_record_from_model(&forward_price, "option_greeks_underlying_price");
        append_forward_price_records(&self.catalog_root, std::slice::from_ref(&record))?;
        Ok(())
    }

    pub(super) fn submit_trade(&mut self, trade: TradeTick) -> Result<()> {
        if !self.predict_native_accepts(trade.instrument_id, "record_trades")
            && !self.accepts_market_data(trade.instrument_id, || {
                self.plan
                    .trades
                    .iter()
                    .any(|spec| spec.instrument_id == trade.instrument_id)
            })
        {
            return Ok(());
        }
        submit_capture_item(
            &self.trade_runtime,
            PartitionKey::catalog_data::<TradeTick>(trade.instrument_id),
            trade.ts_event.as_u64(),
            Some(trade.ts_init.as_u64()),
            trade,
        )
    }

    pub(super) fn submit_bar(&mut self, bar: Bar) -> Result<()> {
        if !self.accepts_market_data(bar.bar_type.instrument_id(), || {
            self.plan
                .bars
                .iter()
                .any(|spec| spec.bar_type == bar.bar_type)
        }) {
            return Ok(());
        }
        submit_capture_item(
            &self.bar_runtime,
            PartitionKey::catalog_data::<Bar>(bar.bar_type),
            bar.ts_event.as_u64(),
            Some(bar.ts_init.as_u64()),
            bar,
        )
    }

    pub(super) fn submit_book_deltas(&mut self, deltas: &OrderBookDeltas) -> Result<()> {
        self.submit_book_delta_batch(deltas.clone())
    }

    pub(super) fn managed_book_checkpoint(&self, id: InstrumentId) -> Option<OrderBookDeltas> {
        self.cache_ref()
            .order_book(&id)
            .filter(|book| book.update_count > 0)
            .map(|book| book.to_deltas(book.ts_last, self.clock().timestamp_ns()))
    }

    pub(super) fn observe_book_delta_failures(&mut self) {
        if let Some(runtime) = &self.book_delta_runtime
            && let Some(affected) = runtime.failures_since(&mut self.book_delta_failure_cursor)
        {
            for spec in &self.plan.book_deltas {
                let id = spec.instrument_id;
                if affected.contains(&PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key())
                {
                    self.book_delta_baselines.remove(&id);
                    self.book_delta_recovery.insert(id);
                }
            }
        }
    }

    pub(super) fn submit_book_delta_batch(&mut self, mut deltas: OrderBookDeltas) -> Result<()> {
        if !self.accepts_market_data(deltas.instrument_id, || {
            self.plan
                .book_deltas
                .iter()
                .any(|spec| spec.instrument_id == deltas.instrument_id)
        }) {
            return Ok(());
        }
        if deltas.deltas.is_empty() {
            return Ok(());
        }
        let id = deltas.instrument_id;
        if id.venue.as_str() == "BITFINEX"
            && let [clear] = deltas.deltas.as_slice()
            && clear.action == nautilus_model::enums::BookAction::Clear
            && !nautilus_model::enums::RecordFlag::F_SNAPSHOT.matches(clear.flags)
        {
            self.book_delta_baselines.remove(&id);
            // Bitfinex sends a CLEAR without F_SNAPSHOT when a connection loses its baseline.
            // Native clears the managed book; the writer records a boundary only.
            let runtime = self
                .book_delta_runtime
                .as_ref()
                .context("book-delta writer unavailable")?;
            return runtime.mark_source_gap(&PartitionKey::catalog_data::<OrderBookDelta>(id));
        }
        self.observe_book_delta_failures();
        let recovery = self.book_delta_recovery.contains(&id);
        let source_snapshot = nautilus_model::enums::RecordFlag::F_SNAPSHOT.matches(deltas.flags);
        if recovery && !source_snapshot {
            // Native has applied this source message before dispatching the callback.
            // The checkpoint already represents it; do not append that delta again.
            deltas = self.managed_book_checkpoint(id).ok_or_else(|| {
                anyhow::anyhow!(
                    "Book recovery waits for a managed book or source snapshot for {id}"
                )
            })?;
        }
        let row_count = deltas.deltas.len();
        let estimated_bytes = std::mem::size_of::<OrderBookDeltas>()
            .saturating_add(row_count.saturating_mul(std::mem::size_of::<OrderBookDelta>()));
        let item = CaptureItem {
            partition_key: PartitionKey::catalog_data::<OrderBookDelta>(deltas.instrument_id),
            event_ts_ns: deltas.ts_event.as_u64(),
            init_ts_ns: Some(deltas.ts_init.as_u64()),
            row_count,
            estimated_bytes,
            payload: deltas,
        };
        // A Bitfinex source cut is queued before the replacement snapshot. Keep
        // the verified venue frame as source data, even if it is an empty book.
        let result = if recovery && !(id.venue.as_str() == "BITFINEX" && source_snapshot) {
            self.book_delta_runtime
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("book-delta writer unavailable"))?
                .submit_after_partition_seal(item)
        } else {
            optional_submit(&self.book_delta_runtime, item)
        };
        if result.is_err() {
            self.book_delta_baselines.remove(&id);
            self.book_delta_recovery.insert(id);
        } else if recovery || source_snapshot {
            self.book_delta_recovery.remove(&id);
            self.book_delta_baselines.insert(id);
        }
        result
    }

    pub fn flush_all(&mut self) -> Result<Vec<FlushResult>> {
        Ok(vec![
            optional_flush_all(&self.instrument_runtime)?,
            optional_flush_all(&self.audit_runtime)?,
            optional_flush_all(&self.custom_data_runtime)?,
            self.predict_custom_data_runtimes.values().try_fold(
                FlushResult::default(),
                |mut total, runtime| {
                    let result = runtime.flush_all()?;
                    total.rows += result.rows;
                    total.bytes += result.bytes;
                    total.files.extend(result.files);
                    Ok::<_, anyhow::Error>(total)
                },
            )?,
            optional_flush_all(&self.mark_price_runtime)?,
            optional_flush_all(&self.index_price_runtime)?,
            optional_flush_all(&self.funding_rate_runtime)?,
            optional_flush_all(&self.instrument_status_runtime)?,
            optional_flush_all(&self.instrument_close_runtime)?,
            optional_flush_all(&self.option_greeks_runtime)?,
            optional_flush_all(&self.quote_runtime)?,
            optional_flush_all(&self.trade_runtime)?,
            optional_flush_all(&self.bar_runtime)?,
            optional_flush_all(&self.book_delta_runtime)?,
            optional_flush_all(&self.book_depth_runtime)?,
        ])
    }

    pub fn shutdown_all(&mut self) -> Result<Vec<FlushResult>> {
        if self.shutdown_completed {
            return Ok(Vec::new());
        }

        // Signal every writer before waiting on any one sink.
        let mut admission = Vec::new();
        let mut results = Vec::new();
        macro_rules! drain_writers {
            ($($field:ident),+ $(,)?) => {
                $(if let Some(writer) = &mut self.$field {
                    admission.push(writer.begin_shutdown());
                })+
                for writer in self.predict_custom_data_runtimes.values_mut() {
                    admission.push(writer.begin_shutdown());
                }
                $(if let Some(writer) = &mut self.$field {
                    results.push(writer.shutdown());
                })+
                for writer in self.predict_custom_data_runtimes.values_mut() {
                    results.push(writer.shutdown());
                }
            };
        }
        drain_writers!(
            instrument_runtime,
            audit_runtime,
            custom_data_runtime,
            mark_price_runtime,
            index_price_runtime,
            funding_rate_runtime,
            instrument_status_runtime,
            instrument_close_runtime,
            option_greeks_runtime,
            quote_runtime,
            trade_runtime,
            bar_runtime,
            book_delta_runtime,
            book_depth_runtime,
        );
        self.shutdown_completed = true;
        admission.into_iter().collect::<Result<()>>()?;
        results.into_iter().collect()
    }
}
