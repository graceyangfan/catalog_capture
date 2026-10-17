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

//! Pure conversions from verified private REST facts to Nautilus reports.

use alloy::primitives::U256;
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::PositionSide,
    identifiers::AccountId,
    instruments::{BinaryOption, Instrument},
    reports::PositionStatusReport,
};

use super::{
    account_models::PredictPositionData,
    error::{Error, Result},
};
use crate::common::instrument_id_from_outcome;

/// Converts an ERC-1155 holding to a net position for its specific outcome instrument.
///
/// Holdings are interpreted as on-chain shares with the SDK's 18-decimal scale. This must
/// be confirmed against a real private response before execution activation. Quantities
/// are floored at native precision; positive subquantum dust is an error, never inferred flat.
/// `ts_init` is the caller's observation time and also `ts_last`, matching the Hyperliquid
/// snapshot convention; it is not a venue update timestamp. Average price and the REST row
/// ID are not mapped without verified units and position-ID semantics.
///
/// # Errors
///
/// Returns an error for inconsistent instrument/token metadata, unrepresentable amounts
/// or positive holdings below one native quantity quantum.
pub fn parse_position_status_report(
    position: &PredictPositionData,
    instrument: &BinaryOption,
    account_id: AccountId,
    ts_init: UnixNanos,
) -> Result<PositionStatusReport> {
    let index_set = u8::try_from(position.outcome.index_set)
        .map_err(|_| Error::Report("unsupported outcome index set"))?;
    let expected_id = instrument_id_from_outcome(position.market.id, index_set)
        .map_err(|_| Error::Report("unsupported outcome index set"))?;
    let token = U256::from_str_radix(instrument.raw_symbol().as_str(), 10)
        .map_err(|_| Error::Report("instrument raw symbol is not a decimal token ID"))?;
    if expected_id != instrument.id()
        || token != position.outcome.on_chain_id
        || !position.market.outcomes.iter().any(|outcome| {
            outcome.index_set == position.outcome.index_set
                && outcome.on_chain_id == position.outcome.on_chain_id
        })
    {
        return Err(Error::Report(
            "holding and instrument token metadata differ",
        ));
    }
    let quantity =
        crate::common::parse::floor_wei_quantity(position.amount, instrument.size_precision())
            .map_err(|_| Error::Report("holding exceeds Nautilus quantity range"))?;
    if !position.amount.is_zero() && quantity.is_zero() {
        return Err(Error::Report(
            "positive holding is below native quantity quantum",
        ));
    }
    let side = if position.amount.is_zero() {
        PositionSide::Flat
    } else {
        PositionSide::Long
    };
    Ok(PositionStatusReport::new(
        account_id,
        instrument.id(),
        side,
        quantity,
        ts_init,
        ts_init,
        None,
        None,
        None,
    ))
}

#[cfg(test)]
mod tests {
    use nautilus_model::{instruments::InstrumentAny, types::fixed::FIXED_PRECISION};
    use rust_decimal::Decimal;
    use serde_json::json;

    use super::*;
    use crate::{http::models::PredictMarket, providers::instruments_from_market};

    fn fixture(amount: U256, index: u8) -> (PredictPositionData, BinaryOption) {
        let market: PredictMarket = serde_json::from_value(json!({
            "id": 42, "title": "fixture", "decimalPrecision": 3,
            "outcomes": [
                {"name": "Yes", "indexSet": 1, "onChainId": "11"},
                {"name": "No", "indexSet": 2, "onChainId": U256::MAX.to_string()},
            ]
        }))
        .unwrap();
        let token = if index == 1 {
            "11".into()
        } else {
            U256::MAX.to_string()
        };
        let position = serde_json::from_value(json!({
            "id": "venue-row-42", "market": {"id": 42, "isNegRisk": false,
                "isYieldBearing": true, "decimalPrecision": 3, "outcomes": [
                    {"name": "Yes", "indexSet": 1, "onChainId": "11"},
                    {"name": "No", "indexSet": 2, "onChainId": U256::MAX.to_string()},
                ]},
            "outcome": {"name": "held", "indexSet": index, "onChainId": token, "status": "LOST"},
            "amount": amount.to_string(), "valueUsd": "0", "averageBuyPriceUsd": "621000000000000000",
            "pnlUsd": "-1"
        })).unwrap();
        let (_, mut instruments) = instruments_from_market(&market, UnixNanos::default()).unwrap();
        let InstrumentAny::BinaryOption(instrument) = instruments.remove(usize::from(index - 1))
        else {
            panic!("expected binary option")
        };
        (position, instrument)
    }

    fn report(
        position: &PredictPositionData,
        instrument: &BinaryOption,
    ) -> Result<PositionStatusReport> {
        parse_position_status_report(
            position,
            instrument,
            AccountId::new("PREDICT-fixture"),
            UnixNanos::from(123u64),
        )
    }

    #[test]
    fn holdings_report_each_outcome_as_long_and_zero_as_flat() {
        for index in [1, 2] {
            let (position, instrument) = fixture(U256::from(10_250_000_000_000_000_000u128), index);
            let report = report(&position, &instrument).unwrap();
            assert_eq!(report.instrument_id, instrument.id());
            assert_eq!(report.position_side, PositionSide::Long);
            assert_eq!(report.signed_decimal_qty, Decimal::new(1025, 2));
            assert_eq!(report.quantity.as_decimal(), Decimal::new(1025, 2));
            assert_eq!(report.ts_last, UnixNanos::from(123u64));
            assert_eq!(report.ts_init, report.ts_last);
            assert!(report.venue_position_id.is_none());
            assert!(report.avg_px_open.is_none());
        }
        let (position, instrument) = fixture(U256::ZERO, 2);
        let report = report(&position, &instrument).unwrap();
        assert_eq!(report.position_side, PositionSide::Flat);
        assert_eq!(report.signed_decimal_qty, Decimal::ZERO);
    }

    #[test]
    fn wei_at_fixed_precision_boundary_floor_with_positive_dust_rejected() {
        let unit = U256::from(10u64.pow(18 - u32::from(FIXED_PRECISION)));
        let (position, instrument) = fixture(unit, 1);
        let result = report(&position, &instrument).unwrap();
        assert_eq!(
            result.quantity.as_decimal(),
            Decimal::new(1, u32::from(FIXED_PRECISION))
        );
        let (position, instrument) = fixture(unit + U256::from(1), 1);
        assert_eq!(
            report(&position, &instrument)
                .unwrap()
                .quantity
                .as_decimal(),
            Decimal::new(1, u32::from(FIXED_PRECISION))
        );
        let (position, instrument) = fixture(unit - U256::from(1), 1);
        assert!(matches!(
            report(&position, &instrument),
            Err(Error::Report(_))
        ));
        let (position, instrument) = fixture(U256::from(1), 1);
        assert!(matches!(
            report(&position, &instrument),
            Err(Error::Report(_))
        ));
    }

    #[test]
    fn report_conversion_rejects_full_width_quantity_overflow() {
        let (position, instrument) = fixture(U256::MAX, 2);
        assert!(matches!(
            report(&position, &instrument),
            Err(Error::Report(_))
        ));
    }

    #[test]
    fn reports_reject_wrong_market_outcome_and_token_membership() {
        let (position, _) = fixture(U256::ZERO, 1);
        let (_, other_instrument) = fixture(U256::ZERO, 2);
        assert!(matches!(
            report(&position, &other_instrument),
            Err(Error::Report(_))
        ));
        for field in [0, 1, 2, 3] {
            let (mut position, instrument) = fixture(U256::ZERO, 2);
            match field {
                0 => position.market.id = 43,
                1 => position.outcome.on_chain_id = U256::ZERO,
                2 => position.market.outcomes.clear(),
                _ => position.outcome.index_set = 256,
            }
            assert!(matches!(
                report(&position, &instrument),
                Err(Error::Report(_))
            ));
        }
    }
}
