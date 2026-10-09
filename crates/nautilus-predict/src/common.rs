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

use std::sync::LazyLock;

use catalog_capture_core::{
    CryptoUpDownSelectorKey, PREDICT_SELECTOR_INTERVAL_SECS, PREDICT_SELECTOR_PRICE_FEED_SYMBOL,
    PREDICT_SELECTOR_TITLE_ASSET, predict_outcome_instrument_id,
};
use nautilus_core::Params;
use nautilus_model::{
    data::DataType,
    identifiers::{InstrumentId, Venue},
};
use serde_json::Value;
use ustr::Ustr;

pub static PREDICT_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from("PREDICT")));

/// Nautilus data-client identity for the public Predict adapter.
pub const PREDICT: &str = "PREDICT";

/// Stable domain routing information for one Predict binary market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PredictMarketContext {
    pub market_id: u64,
    /// Native Predict orderbook prices are for this first (`indexSet = 1`) outcome.
    pub yes_instrument_id: InstrumentId,
    pub no_instrument_id: InstrumentId,
    pub price_precision: u8,
    pub size_precision: u8,
}

impl PredictMarketContext {
    /// Returns the canonical custom-data identity for this market's one native orderbook.
    #[must_use]
    pub fn orderbook_data_type(&self) -> DataType {
        predict_orderbook_data_type(self.market_id)
    }
}

/// Returns the canonical market-scoped custom-data identity for a Predict orderbook.
#[must_use]
pub fn predict_orderbook_data_type(market_id: u64) -> DataType {
    DataType::new(
        "PredictOrderbookSnapshot",
        None,
        Some(market_id.to_string()),
    )
}

/// Canonical market-scoped identity for a rolling crypto Up/Down snapshot.
///
/// The selector identity is routing provenance, not a venue field. It lets the capture actor
/// isolate simultaneous BTC/ETH/SOL 5m streams while retaining the market ID as catalog identity.
#[must_use]
pub fn predict_crypto_up_down_orderbook_data_type(
    market_id: u64,
    selector: &CryptoUpDownSelectorKey,
) -> DataType {
    let mut metadata = Params::new();
    metadata.insert(
        PREDICT_SELECTOR_PRICE_FEED_SYMBOL.to_string(),
        Value::String(selector.price_feed_symbol().to_string()),
    );
    metadata.insert(
        PREDICT_SELECTOR_TITLE_ASSET.to_string(),
        Value::String(selector.title_asset().to_string()),
    );
    metadata.insert(
        PREDICT_SELECTOR_INTERVAL_SECS.to_string(),
        Value::String(selector.interval_secs().to_string()),
    );
    DataType::new(
        "PredictOrderbookSnapshot",
        Some(metadata),
        Some(market_id.to_string()),
    )
}

pub fn instrument_id_from_outcome(market_id: u64, index_set: u8) -> anyhow::Result<InstrumentId> {
    predict_outcome_instrument_id(market_id, index_set)
}

#[cfg(test)]
mod tests {
    use super::predict_crypto_up_down_orderbook_data_type;
    use catalog_capture_core::{
        CryptoUpDownSelectorKey, PREDICT_SELECTOR_INTERVAL_SECS,
        PREDICT_SELECTOR_PRICE_FEED_SYMBOL, PREDICT_SELECTOR_TITLE_ASSET,
    };

    #[test]
    fn dynamic_orderbook_type_retains_interval_provenance() {
        let selector = CryptoUpDownSelectorKey::new("BTC/USDT", "Bitcoin", 300).unwrap();
        let data_type = predict_crypto_up_down_orderbook_data_type(2859346, &selector);
        assert_eq!(data_type.identifier(), Some("2859346"));
        let metadata = data_type.metadata().expect("selector provenance");
        assert_eq!(
            metadata
                .get(PREDICT_SELECTOR_INTERVAL_SECS)
                .and_then(serde_json::Value::as_str),
            Some("300")
        );
        assert_eq!(
            metadata
                .get(PREDICT_SELECTOR_PRICE_FEED_SYMBOL)
                .and_then(serde_json::Value::as_str),
            Some("BTCUSDT")
        );
        assert_eq!(
            metadata
                .get(PREDICT_SELECTOR_TITLE_ASSET)
                .and_then(serde_json::Value::as_str),
            Some("bitcoin")
        );
    }
}
