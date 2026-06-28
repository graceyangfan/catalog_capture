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

mod expand;
mod metadata;
mod readback;
mod resolve;
mod rollover;
mod spec;

pub use expand::{expand_option_universe, merge_capture_plans};
pub use metadata::{
    append_option_universe_resolution_records, catalog_root_from_uri,
    compute_refresh_rollover_reason, option_universe_resolution_log_path,
    read_option_universe_resolution_records, refresh_resolution_record, startup_resolution_record,
    summarize_option_universe_resolution_records, validate_option_universe_resolution_metadata,
    validate_option_universe_resolution_records, OptionUniverseResolutionEventKind,
    OptionUniverseResolutionRecord, OptionUniverseResolutionSummary,
    OptionUniverseResolutionValidationOptions, OptionUniverseResolutionValidationReport,
    StrikeSelectionProfile, ALL_STRIKES_MIN_SELECTED_STRIKES, OPTION_UNIVERSE_RESOLUTIONS_FILE,
    REFRESH_ROLLOVER_REASONS,
};
pub use readback::{
    sample_all_strikes_instrument_ids, validate_option_universe_readback, BarReadbackCount,
    InstrumentReadbackCounts, OptionUniverseReadbackOptions, OptionUniverseReadbackReport,
    ALL_STRIKES_READBACK_SAMPLE_LIMIT,
};
pub use resolve::{
    aggregate_open_interest_by_strike, option_instrument_ids_at_selected_expiry,
    resolve_option_universe, select_nearest_expiry_reference_instrument_id,
};
pub use rollover::{should_apply_strike_change, StrikeChangeSmoothingState};
pub use spec::{
    derive_perp_instrument_id, okx_instrument_family, ExpiryPolicy, OptionUniverseFamily,
    OptionUniverseResolveError, OptionUniverseSpec, OptionUniverseVenueKind,
    ResolvedOptionUniverse, StrikeOpenInterestByStrike, StrikePolicy,
};
