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

use super::*;
use crate::actor_runtime::{
    optional_flush_all, optional_shutdown, optional_submit, seal_runtime, submit_capture_item,
};
use catalog_capture_core::config::OverflowPolicy;
use catalog_capture_core::{
    ForwardPrice, append_forward_price_records, forward_price_record_from_model, item::CaptureItem,
    runtime::FlushResult,
};
use nautilus_common::component::Component;

impl CatalogCaptureActor {
    pub(super) fn submit_instrument(&mut self, instrument: InstrumentAny) -> Result<()> {
        let ts_init = Instrument::ts_init(&instrument).as_u64();
        optional_submit(
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
        )
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
        let data_type = data.data_type.clone();
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
                    let runtime = self
                        .predict_custom_data_runtimes
                        .get(&selector_key)
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "no Predict segment writer configured for selector {selector_key}"
                            )
                        })?;
                    seal_runtime(runtime)?;
                    retired_predict_market_id = Some(previous_market_id);
                }
                self.active_predict_market_ids
                    .insert(selector_key.clone(), market_id);
                self.predict_custom_data_runtimes
                    .get(&selector_key)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "no Predict segment writer configured for selector {selector_key}"
                        )
                    })?
            }
            _ => self.custom_data_runtime.as_ref().ok_or_else(|| {
                anyhow::anyhow!("custom-data callback received data without a generic writer")
            })?,
        };
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

        // The successor snapshot is now owned by its writer. Only then may the retired
        // market's two definitions leave the Nautilus cache. Disk history is unaffected:
        // its instrument parquet and sealed custom-data segment already remain in the catalog.
        if let Some(market_id) = retired_predict_market_id {
            self.purge_retired_predict_market_from_cache(market_id)?;
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
        if !self.accepts_market_data(trade.instrument_id, || {
            self.plan
                .trades
                .iter()
                .any(|spec| spec.instrument_id == trade.instrument_id)
        }) {
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

    pub(super) fn submit_book_delta_batch(&mut self, deltas: OrderBookDeltas) -> Result<()> {
        if !self.accepts_market_data(deltas.instrument_id, || {
            self.plan
                .book_deltas
                .iter()
                .any(|spec| spec.instrument_id == deltas.instrument_id)
        }) {
            return Ok(());
        }
        let row_count = deltas.deltas.len();
        if row_count == 0 {
            return Ok(());
        }
        let estimated_bytes = std::mem::size_of::<OrderBookDeltas>()
            .saturating_add(row_count.saturating_mul(std::mem::size_of::<OrderBookDelta>()));
        let result = optional_submit(
            &self.book_delta_runtime,
            CaptureItem {
                partition_key: PartitionKey::catalog_data::<OrderBookDelta>(deltas.instrument_id),
                event_ts_ns: deltas.ts_event.as_u64(),
                init_ts_ns: Some(deltas.ts_init.as_u64()),
                row_count,
                estimated_bytes,
                payload: deltas,
            },
        );
        if let Err(error) = &result
            && matches!(self.capture.overflow_policy, OverflowPolicy::FailFast)
        {
            log::error!(
                "catalog-capture: stopping actor after book-delta submission failure: {error}"
            );
            if let Err(stop_error) = self.stop() {
                log::error!(
                    "catalog-capture: failed to stop actor after book-delta submission failure: {stop_error}"
                );
            }
        }
        result
    }

    pub fn flush_all(&mut self) -> Result<Vec<FlushResult>> {
        Ok(vec![
            optional_flush_all(&self.instrument_runtime)?,
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
        ])
    }

    pub fn shutdown_all(&mut self) -> Result<Vec<FlushResult>> {
        if self.shutdown_completed {
            return Ok(Vec::new());
        }

        // Every family must drain even if another writer reports a failure.
        let mut results = vec![
            optional_shutdown(&mut self.instrument_runtime),
            optional_shutdown(&mut self.custom_data_runtime),
            optional_shutdown(&mut self.mark_price_runtime),
            optional_shutdown(&mut self.index_price_runtime),
            optional_shutdown(&mut self.funding_rate_runtime),
            optional_shutdown(&mut self.instrument_status_runtime),
            optional_shutdown(&mut self.instrument_close_runtime),
            optional_shutdown(&mut self.option_greeks_runtime),
            optional_shutdown(&mut self.quote_runtime),
            optional_shutdown(&mut self.trade_runtime),
            optional_shutdown(&mut self.bar_runtime),
            optional_shutdown(&mut self.book_delta_runtime),
        ];
        for mut runtime in std::mem::take(&mut self.predict_custom_data_runtimes).into_values() {
            results.push(runtime.shutdown());
        }
        self.shutdown_completed = true;
        results.into_iter().collect()
    }
}
