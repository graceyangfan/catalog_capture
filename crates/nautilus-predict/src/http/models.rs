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

use serde::Deserialize;

/// Public endpoint response envelope.
#[derive(Debug, Clone, Deserialize)]
pub struct PredictResponse<T> {
    pub success: bool,
    pub data: T,
}

/// Cursor page returned by the public market catalogue.
#[derive(Debug, Clone, Deserialize)]
pub struct PredictListResponse<T> {
    pub success: bool,
    pub cursor: Option<String>,
    pub data: Vec<T>,
}

/// Search and category response envelope.
pub type PredictMarketsResponse = PredictResponse<PredictMarketsData>;

/// Markets returned by search or category lookup.
#[derive(Debug, Clone, Deserialize)]
pub struct PredictMarketsData {
    #[serde(default)]
    pub markets: Vec<PredictMarket>,
}

/// Public market definition used for discovery and instrument conversion.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictMarket {
    pub id: u64,
    pub title: String,
    pub decimal_precision: u8,
    pub outcomes: Vec<PredictOutcome>,
    pub created_at: Option<String>,
    pub variant_data: Option<PredictVariantData>,
}

impl PredictMarket {
    /// Formats operator-facing metadata while retaining the venue title for window parsing.
    #[must_use]
    pub fn display_title(&self) -> String {
        match self
            .variant_data
            .as_ref()
            .filter(|v| v.variant_type == "CRYPTO_UP_DOWN")
        {
            Some(variant) => format!(
                "{} | Start: {}",
                self.title,
                variant
                    .start_price
                    .map_or_else(|| "TBD".to_string(), |price| price.to_string()),
            ),
            None => self.title.clone(),
        }
    }
}

/// Variant-specific discovery metadata, including an optional exact start price.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictVariantData {
    #[serde(rename = "type")]
    pub variant_type: String,
    pub price_feed_symbol: Option<String>,
    #[serde(default, deserialize_with = "deserialize_start_price")]
    pub start_price: Option<rust_decimal::Decimal>,
}

/// Outcome identity and its decimal on-chain token ID.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictOutcome {
    pub name: String,
    pub index_set: u8,
    pub on_chain_id: String,
}

// Read the original JSON token rather than passing a numeric price through f64.
fn deserialize_start_price<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<rust_decimal::Decimal>, D::Error> {
    use serde::de::Error;

    let raw = Option::<Box<serde_json::value::RawValue>>::deserialize(deserializer)?;
    raw.map(|raw| {
        let token = raw.get();
        let text = if token.starts_with('"') {
            serde_json::from_str::<String>(token).map_err(D::Error::custom)?
        } else {
            token.to_owned()
        };
        rust_decimal::Decimal::from_str_exact(&text)
            .or_else(|_| rust_decimal::Decimal::from_scientific(&text))
            .map_err(D::Error::custom)
    })
    .transpose()
}
