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

use std::path::Path;

use anyhow::Result;
use nautilus_model::data::ForwardPrice;
use serde::{Deserialize, Serialize};

use crate::jsonl::append_jsonl_records;

pub const FORWARD_PRICES_FILE: &str = "metadata/forward_prices.jsonl";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardPriceRecord {
    pub instrument_id: String,
    pub forward_price: String,
    pub underlying_index: Option<String>,
    pub ts_event_ns: u64,
    pub ts_init_ns: u64,
    pub source: String,
}

pub fn forward_price_log_path(catalog_root: &Path) -> std::path::PathBuf {
    catalog_root.join(FORWARD_PRICES_FILE)
}

pub fn forward_price_record_from_model(
    forward_price: &ForwardPrice,
    source: &str,
) -> ForwardPriceRecord {
    ForwardPriceRecord {
        instrument_id: forward_price.instrument_id.to_string(),
        forward_price: forward_price.forward_price.to_string(),
        underlying_index: forward_price.underlying_index.clone(),
        ts_event_ns: forward_price.ts_event.as_u64(),
        ts_init_ns: forward_price.ts_init.as_u64(),
        source: source.to_string(),
    }
}

pub fn append_forward_price_records(
    catalog_root: &Path,
    records: &[ForwardPriceRecord],
) -> Result<()> {
    append_jsonl_records(
        &forward_price_log_path(catalog_root),
        records,
        "forward price",
    )
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nautilus_core::UnixNanos;
    use nautilus_model::{data::ForwardPrice, identifiers::InstrumentId};
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn append_forward_price_records_writes_jsonl() {
        let temp =
            std::env::temp_dir().join(format!("forward-price-metadata-{}", std::process::id()));
        fs::create_dir_all(&temp).unwrap();

        let forward = ForwardPrice::new(
            InstrumentId::from("BTC-26JUN26-65000-C.DERIBIT"),
            Decimal::from(65_250),
            Some("SYN.BTC-26JUN26".to_string()),
            UnixNanos::from(10),
            UnixNanos::from(11),
        );
        let record = forward_price_record_from_model(&forward, "option_greeks_underlying_price");
        append_forward_price_records(&temp, &[record]).unwrap();

        let contents = fs::read_to_string(forward_price_log_path(&temp)).expect("metadata file");
        assert!(contents.contains("\"instrument_id\":\"BTC-26JUN26-65000-C.DERIBIT\""));
        assert!(contents.contains("\"source\":\"option_greeks_underlying_price\""));

        fs::remove_dir_all(temp).ok();
    }
}
