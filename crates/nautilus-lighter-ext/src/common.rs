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

use anyhow::Context;
use nautilus_model::{data::DataType, identifiers::InstrumentId, types::Price};
use rust_decimal::{Decimal, RoundingStrategy};

pub const LIGHTER: &str = "LIGHTER";
pub fn instrument_id(market_id: u64) -> anyhow::Result<InstrumentId> {
    InstrumentId::from_as_ref(format!("{market_id}-BINARY.LIGHTER")).map_err(Into::into)
}
pub fn market_id(id: InstrumentId) -> anyhow::Result<u64> {
    anyhow::ensure!(
        id.venue.as_str() == LIGHTER,
        "not a Lighter instrument: {id}"
    );
    let value = id
        .symbol
        .as_str()
        .strip_suffix("-BINARY")
        .context("expected <market_id>-BINARY.LIGHTER")?;
    let market: u64 = value.parse()?;
    anyhow::ensure!(
        instrument_id(market)? == id,
        "non-canonical Lighter binary instrument {id}"
    );
    Ok(market)
}
pub fn oracle_price(value: Decimal) -> anyhow::Result<Price> {
    anyhow::ensure!(value > Decimal::ZERO, "oracle must be positive");
    Ok(Price::from_decimal_dp(
        value.round_dp_with_strategy(16, RoundingStrategy::MidpointNearestEven),
        16,
    )?)
}
pub fn open_interest_data_type(market_id: u64) -> DataType {
    DataType::new("LighterOpenInterest", None, Some(market_id.to_string()))
}
pub fn open_interest_market(data_type: &DataType) -> anyhow::Result<u64> {
    anyhow::ensure!(
        data_type.type_name() == "LighterOpenInterest",
        "unsupported custom type {data_type}"
    );
    let id = data_type
        .identifier()
        .context("OI requires a canonical market ID identifier")?;
    let market: u64 = id.parse()?;
    anyhow::ensure!(
        *data_type == open_interest_data_type(market),
        "non-canonical OI selector {data_type}"
    );
    Ok(market)
}

/// A canonical public 15m Up/Down event tag, also safe in an HTTP query.
pub fn validate_updown_tag(tag: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        tag.ends_with("-updown-15m")
            && tag
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-'),
        "invalid Lighter 15m event tag {tag}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quantizes_decimal_oracle_with_ties_to_even() {
        for (source, expected) in [
            ("2392.641853126338412544", "2392.6418531263384125"),
            ("1.000000000000000050", "1.0000000000000000"),
            ("1.000000000000000150", "1.0000000000000002"),
        ] {
            assert_eq!(
                oracle_price(source.parse().unwrap()).unwrap().to_string(),
                expected
            );
        }
        assert!(oracle_price(Decimal::ZERO).is_err());
        assert!(oracle_price(-Decimal::ONE).is_err());
    }
    #[test]
    fn selector_identity_is_canonical() {
        let id = instrument_id(4255).unwrap();
        assert_eq!(market_id(id).unwrap(), 4255);
        assert!(market_id(InstrumentId::from("ETH-PERP.LIGHTER")).is_err());
        assert!(
            open_interest_market(&DataType::new(
                "LighterOpenInterest",
                None,
                Some("04255".into())
            ))
            .is_err()
        );
        assert!(open_interest_market(&DataType::new("LighterOpenInterest", None, None)).is_err());
    }
}
