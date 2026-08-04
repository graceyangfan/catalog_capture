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

use serde::{Deserialize, Serialize};

use super::custom::{CustomDataRequestSelector, CustomDataSelector};
use super::hip4::Hip4UniverseSelector;
use super::option_universe::OptionUniverseSelector;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CaptureConfigFile {
    #[serde(default)]
    pub instruments: Vec<InstrumentSelector>,
    #[serde(default)]
    pub quotes: Vec<InstrumentSelector>,
    #[serde(default)]
    pub trades: Vec<InstrumentSelector>,
    #[serde(default)]
    pub mark_prices: Vec<InstrumentSelector>,
    #[serde(default)]
    pub instrument_statuses: Vec<InstrumentSelector>,
    #[serde(default)]
    pub instrument_closes: Vec<InstrumentSelector>,
    #[serde(default)]
    pub option_greeks: Vec<InstrumentSelector>,
    #[serde(default)]
    pub forward_prices: Vec<InstrumentSelector>,
    #[serde(default)]
    pub index_prices: Vec<InstrumentSelector>,
    #[serde(default)]
    pub funding_rates: Vec<InstrumentSelector>,
    #[serde(default)]
    pub bars: Vec<BarSelector>,
    #[serde(default)]
    pub book_deltas: Vec<BookDeltasSelector>,
    /// Subscribe-style custom data only (`subscribe_data` → live `on_data`).
    /// Do not put request-only types (e.g. DeribitBookSummary) here.
    #[serde(default)]
    pub custom_data: Vec<CustomDataSelector>,
    /// Request-style custom data only (`request_data` → response handler).
    /// Do not put stream/subscribe types (e.g. DeribitVolatilityIndex) here.
    #[serde(default)]
    pub custom_data_requests: Vec<CustomDataRequestSelector>,
    #[serde(default)]
    pub option_universe: Vec<OptionUniverseSelector>,
    #[serde(default)]
    pub hip4_universe: Vec<Hip4UniverseSelector>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstrumentSelector {
    pub instrument_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BarSelector {
    pub bar_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookDeltasSelector {
    pub instrument_id: String,
    pub book_type: String,
}
