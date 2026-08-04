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

use std::str::FromStr;

use nautilus_core::UnixNanos;
use nautilus_model::identifiers::InstrumentId;

use super::outcome_meta::{hip4_perp_instrument_id, ResolvedHip4Market};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hip4UniverseFamily {
    Instruments,
    Quotes,
    /// Outcome YES/NO trade ticks (needed for CJP-style replay; polyup uses quotes).
    Trades,
    MarkPrices,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hip4UniverseSpec {
    pub venue_id: String,
    pub underlying: String,
    pub period: String,
    pub market_class: String,
    pub include_fallback: bool,
    pub include_perp_mark: bool,
    pub families: Vec<Hip4UniverseFamily>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedHip4Universe {
    pub resolved_at_ns: UnixNanos,
    pub market: ResolvedHip4Market,
    pub perp_instrument_id: Option<InstrumentId>,
    pub outcome_instrument_ids: Vec<InstrumentId>,
    pub all_instrument_ids: Vec<InstrumentId>,
}

pub fn build_resolved_hip4_universe(
    spec: &Hip4UniverseSpec,
    market: ResolvedHip4Market,
    resolved_at_ns: u64,
) -> ResolvedHip4Universe {
    let outcome_instrument_ids = market
        .instrument_ids
        .iter()
        .filter_map(|instrument_id| InstrumentId::from_str(instrument_id.as_str()).ok())
        .collect::<Vec<_>>();
    let perp_instrument_id = spec
        .include_perp_mark
        .then(|| hip4_perp_instrument_id(&spec.underlying))
        .and_then(|instrument_id| InstrumentId::from_str(instrument_id.as_str()).ok());
    let mut all_instrument_ids = Vec::with_capacity(
        outcome_instrument_ids.len() + usize::from(perp_instrument_id.is_some()),
    );
    if let Some(perp_instrument_id) = perp_instrument_id {
        all_instrument_ids.push(perp_instrument_id);
    }
    all_instrument_ids.extend(outcome_instrument_ids.iter().copied());

    ResolvedHip4Universe {
        resolved_at_ns: UnixNanos::from(resolved_at_ns),
        market,
        perp_instrument_id,
        outcome_instrument_ids,
        all_instrument_ids,
    }
}
