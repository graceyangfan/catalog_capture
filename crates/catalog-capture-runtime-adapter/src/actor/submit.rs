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
    optional_flush_all, optional_shutdown, optional_submit, submit_capture_item,
};
use catalog_capture_core::config::OverflowPolicy;
use catalog_capture_core::{
    append_forward_price_records, forward_price_record_from_model, item::CaptureItem,
    runtime::FlushResult, ForwardPrice,
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
        optional_submit(
            &self.custom_data_runtime,
            CaptureItem {
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
            },
        )
    }

    pub(super) fn submit_mark_price(&mut self, data: MarkPriceUpdate) -> Result<()> {
        submit_capture_item(
            &self.mark_price_runtime,
            PartitionKey::catalog_data::<MarkPriceUpdate>(data.instrument_id),
            data.ts_event.as_u64(),
            Some(data.ts_init.as_u64()),
            data,
        )
    }

    pub(super) fn submit_index_price(&mut self, data: IndexPriceUpdate) -> Result<()> {
        submit_capture_item(
            &self.index_price_runtime,
            PartitionKey::catalog_data::<IndexPriceUpdate>(data.instrument_id),
            data.ts_event.as_u64(),
            Some(data.ts_init.as_u64()),
            data,
        )
    }

    pub(super) fn submit_funding_rate(&mut self, data: FundingRateUpdate) -> Result<()> {
        submit_capture_item(
            &self.funding_rate_runtime,
            PartitionKey::catalog_data::<FundingRateUpdate>(data.instrument_id),
            data.ts_event.as_u64(),
            Some(data.ts_init.as_u64()),
            data,
        )
    }

    pub(super) fn submit_instrument_status(&mut self, data: InstrumentStatus) -> Result<()> {
        submit_capture_item(
            &self.instrument_status_runtime,
            PartitionKey::catalog_data::<InstrumentStatus>(data.instrument_id),
            data.ts_event.as_u64(),
            Some(data.ts_init.as_u64()),
            data,
        )
    }

    pub(super) fn submit_instrument_close(&mut self, data: InstrumentClose) -> Result<()> {
        submit_capture_item(
            &self.instrument_close_runtime,
            PartitionKey::catalog_data::<InstrumentClose>(data.instrument_id),
            data.ts_event.as_u64(),
            Some(data.ts_init.as_u64()),
            data,
        )
    }

    pub(super) fn submit_option_greeks(&mut self, data: OptionGreeks) -> Result<()> {
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
        submit_capture_item(
            &self.trade_runtime,
            PartitionKey::catalog_data::<TradeTick>(trade.instrument_id),
            trade.ts_event.as_u64(),
            Some(trade.ts_init.as_u64()),
            trade,
        )
    }

    pub(super) fn submit_bar(&mut self, bar: Bar) -> Result<()> {
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

        let results = vec![
            optional_shutdown(&mut self.instrument_runtime)?,
            optional_shutdown(&mut self.custom_data_runtime)?,
            optional_shutdown(&mut self.mark_price_runtime)?,
            optional_shutdown(&mut self.index_price_runtime)?,
            optional_shutdown(&mut self.funding_rate_runtime)?,
            optional_shutdown(&mut self.instrument_status_runtime)?,
            optional_shutdown(&mut self.instrument_close_runtime)?,
            optional_shutdown(&mut self.option_greeks_runtime)?,
            optional_shutdown(&mut self.quote_runtime)?,
            optional_shutdown(&mut self.trade_runtime)?,
            optional_shutdown(&mut self.bar_runtime)?,
            optional_shutdown(&mut self.book_delta_runtime)?,
        ];
        self.shutdown_completed = true;
        Ok(results)
    }
}
