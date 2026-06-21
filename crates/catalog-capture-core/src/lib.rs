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

pub mod atm_reference;
pub mod background;
pub mod buffer;
pub mod config;
pub mod forward_price;
pub mod forward_price_metadata;
pub mod item;
pub mod metrics;
pub mod option_universe;
pub mod option_universe_metadata;
pub mod option_universe_readback;
pub mod option_universe_rollover;
pub mod plan;
pub mod runtime;
pub mod sink;

pub use atm_reference::{
    select_cache_atm_reference, select_cache_perp_strike_fallback,
    select_http_perp_ticker_atm_reference, select_strike_reference_from_decimal_string,
    AtmReferenceSource,
};
pub use background::BackgroundCaptureRuntime;
pub use buffer::PartitionBuffer;
pub use config::{CaptureConfig, CompressionKind, LayoutCompatibility, OverflowPolicy};
pub use forward_price::forward_price_from_option_greeks;
pub use forward_price_metadata::{
    append_forward_price_records, forward_price_log_path, forward_price_record_from_model,
    ForwardPriceRecord, FORWARD_PRICES_FILE,
};
pub use item::{CaptureItem, PartitionKey};
pub use metrics::{CaptureMetrics, FlushReason, FlushReasonMetrics};
pub use option_universe::{
    aggregate_open_interest_by_strike, derive_perp_instrument_id, expand_option_universe,
    merge_capture_plans, okx_instrument_family, option_instrument_ids_at_selected_expiry,
    resolve_option_universe, select_nearest_expiry_reference_instrument_id, ExpiryPolicy,
    OptionUniverseFamily, OptionUniverseResolveError, OptionUniverseSpec, OptionUniverseVenueKind,
    ResolvedOptionUniverse, StrikeOpenInterestByStrike, StrikePolicy,
};
pub use option_universe_metadata::{
    append_option_universe_resolution_records, catalog_root_from_uri,
    compute_refresh_rollover_reason, option_universe_resolution_log_path,
    read_option_universe_resolution_records, refresh_resolution_record, startup_resolution_record,
    summarize_option_universe_resolution_records, validate_option_universe_resolution_metadata,
    validate_option_universe_resolution_records, OptionUniverseResolutionEventKind,
    OptionUniverseResolutionRecord, OptionUniverseResolutionSummary,
    OptionUniverseResolutionValidationOptions, OptionUniverseResolutionValidationReport,
    StrikeSelectionProfile, ALL_STRIKES_MIN_SELECTED_STRIKES,
};
pub use option_universe_readback::{
    sample_all_strikes_instrument_ids, validate_option_universe_readback, BarReadbackCount,
    InstrumentReadbackCounts, OptionUniverseReadbackOptions, OptionUniverseReadbackReport,
    ALL_STRIKES_READBACK_SAMPLE_LIMIT,
};
pub use option_universe_rollover::{should_apply_strike_change, StrikeChangeSmoothingState};
pub use plan::{
    BarCaptureSpec, BookDeltasCaptureSpec, CapturePlan, CustomDataCaptureSpec,
    ForwardPriceCaptureSpec, FundingRateCaptureSpec, IndexPriceCaptureSpec, InstrumentCaptureSpec,
    InstrumentCloseCaptureSpec, InstrumentStatusCaptureSpec, MarkPriceCaptureSpec,
    OptionGreeksCaptureSpec, QuoteCaptureSpec, TradeCaptureSpec,
};
pub use runtime::{CaptureRuntime, FlushResult};
pub use sink::{CaptureSink, NautilusCatalogSink};
