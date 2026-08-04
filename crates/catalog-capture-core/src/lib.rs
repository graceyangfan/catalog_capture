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
pub mod budget;
pub mod buffer;
pub mod catalog_layout;
pub mod config;
pub mod forward_price;
pub mod forward_price_metadata;
pub mod hip4;
pub mod item;
pub mod jsonl;
pub mod lifecycle;
pub mod metrics;
pub mod metrics_export;
pub mod option_universe;
pub mod plan;
pub mod runtime;
pub mod sink;

pub use atm_reference::{
    select_cache_atm_reference, select_cache_perp_strike_fallback,
    select_http_perp_ticker_atm_reference, select_strike_reference_from_decimal_string,
    AtmReferenceSource,
};
pub use background::BackgroundCaptureRuntime;
pub use budget::{
    estimate_peak_buffered_bytes, family_partition_counts, format_budget_warning,
    format_buffer_estimate, validate_capture_config, BufferMemoryEstimate, FamilyBufferEstimate,
};
pub use buffer::PartitionBuffer;
pub use config::{CaptureConfig, CompressionKind, LayoutCompatibility, OverflowPolicy};
pub use forward_price::forward_price_from_option_greeks;
pub use forward_price_metadata::{
    append_forward_price_records, forward_price_log_path, forward_price_record_from_model,
    ForwardPriceRecord, FORWARD_PRICES_FILE,
};
pub use hip4::{
    append_hip4_universe_resolution_records, build_resolved_hip4_universe,
    compute_hip4_refresh_rollover_reason, expand_hip4_universe, hip4_perp_instrument_id,
    hip4_refresh_resolution_record, hip4_startup_resolution_record,
    hip4_universe_resolution_log_path, instrument_ids_from_outcomes, next_rotation_delay_secs,
    parse_expiry_to_ns, resolve_hip4_market, validate_hip4_refresh_resolution_record,
    validate_hip4_refresh_rollover_reason, Hip4UniverseFamily, Hip4UniverseResolutionEventKind,
    Hip4UniverseResolutionRecord, Hip4UniverseSpec, ResolveHip4MarketOptions, ResolvedHip4Market,
    ResolvedHip4Universe, HIP4_UNIVERSE_RESOLUTIONS_FILE, REFRESH_ROLLOVER_REASONS,
};
pub use item::{CaptureItem, PartitionKey};
pub use lifecycle::{
    next_seal_boundary_ns, resolve_seal_schedule, should_seal_at, DurabilityConfig,
    LifecycleConfig, LifecycleMode, ResolvedSealSchedule, SealConfigFile, SegmentCaptureSink,
    SegmentLifecycleConfig,
};
pub use metrics::{CaptureMetrics, FlushReason, FlushReasonMetrics};
pub use metrics_export::{
    process_rss_bytes, render_json, render_prometheus, unix_time_ms, CaptureMetricsSnapshot,
    FamilyCaptureMetrics,
};
pub use option_universe::{
    aggregate_open_interest_by_strike, append_option_universe_resolution_records,
    catalog_root_from_uri, compute_refresh_rollover_reason, derive_perp_instrument_id,
    expand_option_universe, merge_capture_plans, okx_instrument_family,
    option_instrument_ids_at_selected_expiry, option_universe_resolution_log_path,
    read_option_universe_resolution_records, refresh_resolution_record, resolve_option_universe,
    sample_all_strikes_instrument_ids, select_nearest_expiry_reference_instrument_id,
    should_apply_strike_change, startup_resolution_record,
    summarize_option_universe_resolution_records, validate_option_universe_readback,
    validate_option_universe_resolution_metadata, validate_option_universe_resolution_records,
    BarReadbackCount, ExpiryPolicy, InstrumentReadbackCounts, OptionUniverseFamily,
    OptionUniverseReadbackOptions, OptionUniverseReadbackReport, OptionUniverseResolutionEventKind,
    OptionUniverseResolutionRecord, OptionUniverseResolutionSummary,
    OptionUniverseResolutionValidationOptions, OptionUniverseResolutionValidationReport,
    OptionUniverseResolveError, OptionUniverseSpec, OptionUniverseVenueKind,
    ResolvedOptionUniverse, StrikeChangeSmoothingState, StrikeOpenInterestByStrike, StrikePolicy,
    StrikeSelectionProfile, ALL_STRIKES_MIN_SELECTED_STRIKES, ALL_STRIKES_READBACK_SAMPLE_LIMIT,
};
pub use plan::{
    capture_plan_difference, instrument_id_difference, plan_instrument_ids, BarCaptureSpec,
    BookDeltasCaptureSpec, CaptureFamilyRuntimeFlags, CapturePlan, CustomDataCaptureSpec,
    CustomDataRequestCaptureSpec, ForwardPriceCaptureSpec, FundingRateCaptureSpec,
    IndexPriceCaptureSpec, InstrumentCaptureSpec, InstrumentCloseCaptureSpec,
    InstrumentStatusCaptureSpec, MarkPriceCaptureSpec, OptionGreeksCaptureSpec, QuoteCaptureSpec,
    RequestOverlapPolicy, TradeCaptureSpec, DEFAULT_CUSTOM_DATA_REQUEST_INTERVAL_SECS,
    DEFAULT_CUSTOM_DATA_REQUEST_TIMEOUT_SECS, DEFAULT_MAX_AGGREGATE_CUSTOM_DATA_REQUEST_RPS,
    MIN_CUSTOM_DATA_REQUEST_INTERVAL_SECS,
};
pub use runtime::{CaptureRuntime, FlushResult};
pub use sink::{
    chunked_catalog_sink_from_config, CaptureSink, CatalogSink, ChunkedCatalogSink,
    NautilusCatalogSink,
};
