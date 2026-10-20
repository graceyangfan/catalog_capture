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

use nautilus_model::instruments::InstrumentAny;
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct BinaryEvent {
    pub event_id: u64,
    pub title: String,
    pub tag: String,
    pub start_at: u64,
    pub end_at: u64,
    pub metadata: BinaryMetadata,
    pub order_books: Vec<BinaryMarket>,
}
#[derive(Debug, Clone, Deserialize)]
pub struct BinaryMetadata {
    pub market_id: u64,
    pub underlying_market_id: u64,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(flatten)]
    pub oracle: OracleSource,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, serde::Serialize)]
pub struct OracleSource {
    pub symbol: String,
    pub duration: String,
    pub provider: String,
    pub feed_id: String,
}
impl OracleSource {
    pub fn channel(&self) -> String {
        format!("up_down_candle/{}/{}/1s", self.symbol, self.duration)
    }
    pub fn validates(&self, point: &OraclePoint) -> bool {
        self.symbol == point.symbol
            && self.duration == point.duration
            && self.provider == point.provider
            && self.feed_id == point.feed_id
            && point.resolution == "1s"
    }
}
#[derive(Debug, Clone, Deserialize)]
pub struct BinaryMarket {
    pub symbol: String,
    pub market_id: u64,
    pub market_type: String,
    pub status: String,
    pub supported_size_decimals: u8,
    pub supported_price_decimals: u8,
    pub min_base_amount: Decimal,
    pub min_quote_amount: Decimal,
    pub start_timestamp: u64,
    pub is_frozen: bool,
}
#[derive(Debug, Clone)]
pub struct MarketContext {
    pub market_id: u64,
    pub instrument: InstrumentAny,
    pub source: OracleSource,
    pub start_secs: u64,
}
#[derive(Debug, Clone, Deserialize)]
pub struct OraclePoint {
    pub symbol: String,
    pub duration: String,
    pub resolution: String,
    pub timestamp: u64,
    pub close: Decimal,
    pub provider: String,
    pub feed_id: String,
}
#[derive(Debug, Deserialize)]
pub struct BinaryStats {
    pub market_id: u64,
    pub open_interest: Option<Decimal>,
    pub status: String,
}
