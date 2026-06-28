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

pub mod expand;
pub mod metadata;
pub mod outcome_meta;
pub mod rollover;
pub mod spec;

pub use expand::expand_hip4_universe;
pub use metadata::{
    append_hip4_universe_resolution_records, compute_hip4_refresh_rollover_reason,
    hip4_universe_resolution_log_path, refresh_resolution_record as hip4_refresh_resolution_record,
    startup_resolution_record as hip4_startup_resolution_record,
    validate_hip4_refresh_resolution_record, validate_hip4_refresh_rollover_reason,
    Hip4UniverseResolutionEventKind, Hip4UniverseResolutionRecord, HIP4_UNIVERSE_RESOLUTIONS_FILE,
    REFRESH_ROLLOVER_REASONS,
};
pub use outcome_meta::{
    hip4_perp_instrument_id, instrument_ids_from_outcomes, parse_expiry_to_ns, resolve_hip4_market,
    ResolveHip4MarketOptions, ResolvedHip4Market,
};
pub use rollover::next_rotation_delay_secs;
pub use spec::{
    build_resolved_hip4_universe, Hip4UniverseFamily, Hip4UniverseSpec, ResolvedHip4Universe,
};
