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

use anyhow::{bail, Result};
use catalog_capture_core::{
    background::BackgroundCaptureRuntime,
    item::{CaptureItem, PartitionKey},
    metrics::CaptureMetrics,
    metrics_export::FamilyCaptureMetrics,
    runtime::FlushResult,
    sink::CaptureSink,
};
use nautilus_model::data::DataType;

pub fn optional_submit<T, S>(
    runtime: &Option<BackgroundCaptureRuntime<T, S>>,
    item: CaptureItem<T>,
) -> Result<()>
where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    let Some(runtime) = runtime.as_ref() else {
        bail!("capture callback received data for a family without an enabled background runtime");
    };
    runtime.submit(item).map(|_| ())
}

pub fn submit_capture_item<T, S>(
    runtime: &Option<BackgroundCaptureRuntime<T, S>>,
    partition_key: PartitionKey,
    event_ts_ns: u64,
    init_ts_ns: Option<u64>,
    payload: T,
) -> Result<()>
where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    optional_submit(
        runtime,
        CaptureItem {
            partition_key,
            event_ts_ns,
            init_ts_ns,
            estimated_bytes: std::mem::size_of::<T>(),
            payload,
        },
    )
}

pub fn optional_flush_all<T, S>(
    runtime: &Option<BackgroundCaptureRuntime<T, S>>,
) -> Result<FlushResult>
where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    match runtime.as_ref() {
        Some(runtime) => runtime.flush_all(),
        None => Ok(FlushResult::default()),
    }
}

pub fn optional_seal_all<T, S>(
    runtime: &Option<BackgroundCaptureRuntime<T, S>>,
) -> Result<FlushResult>
where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    match runtime.as_ref() {
        Some(runtime) => runtime.seal_all(),
        None => Ok(FlushResult::default()),
    }
}

pub fn optional_shutdown<T, S>(
    runtime: &mut Option<BackgroundCaptureRuntime<T, S>>,
) -> Result<FlushResult>
where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    match runtime.take() {
        Some(mut runtime) => runtime.shutdown(),
        None => Ok(FlushResult::default()),
    }
}

pub fn collect_family_metrics<T, S>(
    family: &str,
    runtime: &Option<BackgroundCaptureRuntime<T, S>>,
    families: &mut Vec<FamilyCaptureMetrics>,
    aggregated: &mut CaptureMetrics,
) where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    let Some(runtime) = runtime.as_ref() else {
        return;
    };
    let metrics = runtime.metrics();
    aggregated.merge(&metrics);
    families.push(FamilyCaptureMetrics {
        family: family.to_string(),
        metrics,
    });
}

pub fn custom_data_client_id(data_type: &DataType) -> Option<&'static str> {
    match data_type.type_name() {
        "BinanceFuturesLiquidation" | "BinanceFuturesTicker" => Some("BINANCE"),
        "DeribitVolatilityIndex" => Some("DERIBIT"),
        "HyperliquidOpenInterest" => Some("HYPERLIQUID"),
        _ => None,
    }
}
