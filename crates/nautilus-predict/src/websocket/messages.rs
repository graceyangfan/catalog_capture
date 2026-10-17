// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Exact public WebSocket payloads. Wire amounts never pass through floating point.

use rust_decimal::Decimal;
use serde::Deserialize;
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictLastOrderSettledWire {
    pub id: String,
    pub price: Decimal,
    pub kind: String,
    pub market_id: u64,
    pub side: String,
    pub outcome: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PredictSettlementsPendingWire {
    pub asks: Vec<(Decimal, Decimal)>,
    pub bids: Vec<(Decimal, Decimal)>,
}

/// Predict's public `predictOrderbook/{marketId}` payload.
///
/// Decimal tokens are intentionally decoded directly to `Decimal`; no float is used.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictOrderbookWire {
    pub market_id: u64,
    pub update_timestamp_ms: u64,
    pub order_count: u64,
    pub asks: Vec<(Decimal, Decimal)>,
    pub bids: Vec<(Decimal, Decimal)>,
    #[serde(default)]
    pub last_order_settled: Option<PredictLastOrderSettledWire>,
    #[serde(default)]
    pub settlements_pending: Option<PredictSettlementsPendingWire>,
}
