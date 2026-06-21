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

use nautilus_model::data::{ForwardPrice, OptionGreeks};
use rust_decimal::Decimal;

pub fn forward_price_from_option_greeks(greeks: &OptionGreeks) -> Option<ForwardPrice> {
    let underlying = greeks.underlying_price?;
    let forward_price = Decimal::from_f64_retain(underlying)?;
    Some(ForwardPrice::new(
        greeks.instrument_id,
        forward_price,
        None,
        greeks.ts_event,
        greeks.ts_init,
    ))
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::{OptionGreekValues, OptionGreeks},
        enums::GreeksConvention,
        identifiers::InstrumentId,
    };

    use super::*;

    #[test]
    fn derives_forward_price_from_option_greeks_underlying() {
        let greeks = OptionGreeks {
            instrument_id: InstrumentId::from("BTC-26JUN26-65000-C.DERIBIT"),
            convention: GreeksConvention::PriceAdjusted,
            greeks: OptionGreekValues::default(),
            bid_iv: None,
            ask_iv: None,
            mark_iv: None,
            underlying_price: Some(65_250.5),
            open_interest: Some(120.0),
            ts_event: UnixNanos::from(10),
            ts_init: UnixNanos::from(11),
        };

        let forward = forward_price_from_option_greeks(&greeks).expect("forward price");
        assert_eq!(forward.instrument_id, greeks.instrument_id);
        assert_eq!(forward.forward_price.to_string(), "65250.5");
        assert_eq!(forward.ts_event, greeks.ts_event);
    }

    #[test]
    fn returns_none_without_underlying_price() {
        let greeks = OptionGreeks {
            instrument_id: InstrumentId::from("BTC-26JUN26-65000-C.DERIBIT"),
            convention: GreeksConvention::PriceAdjusted,
            greeks: OptionGreekValues::default(),
            bid_iv: None,
            ask_iv: None,
            mark_iv: None,
            underlying_price: None,
            open_interest: None,
            ts_event: UnixNanos::default(),
            ts_init: UnixNanos::default(),
        };

        assert!(forward_price_from_option_greeks(&greeks).is_none());
    }
}
