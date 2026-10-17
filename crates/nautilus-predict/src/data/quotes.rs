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

//! Native top-of-book projection. Full custom snapshots retain their original contract.

use nautilus_model::{data::QuoteTick, identifiers::InstrumentId, types::Price};
use rust_decimal::Decimal;

use crate::{PredictMarketContext, PredictOrderbookSnapshot};

pub(super) fn quote_from_snapshot(
    snapshot: &PredictOrderbookSnapshot,
    context: &PredictMarketContext,
    instrument_id: InstrumentId,
) -> anyhow::Result<Option<QuoteTick>> {
    let (Some(bid), Some(ask)) = (
        snapshot.bids.iter().max_by_key(|level| level.price),
        snapshot.asks.iter().min_by_key(|level| level.price),
    ) else {
        return Ok(None);
    };
    // Missing, crossed or zero-size sides cannot establish a usable native quote.
    if bid.price > ask.price || bid.quantity.is_zero() || ask.quantity.is_zero() {
        return Ok(None);
    }
    let (bid_price, ask_price, bid_size, ask_size) = if instrument_id == context.yes_instrument_id {
        (bid.price, ask.price, bid.quantity, ask.quantity)
    } else {
        anyhow::ensure!(
            instrument_id == context.no_instrument_id,
            "Predict quote outcome does not match its market"
        );
        (
            Price::from_decimal_dp(
                Decimal::ONE - ask.price.as_decimal(),
                context.price_precision,
            )?,
            Price::from_decimal_dp(
                Decimal::ONE - bid.price.as_decimal(),
                context.price_precision,
            )?,
            ask.quantity,
            bid.quantity,
        )
    };
    Ok(Some(QuoteTick::new(
        instrument_id,
        bid_price,
        ask_price,
        bid_size,
        ask_size,
        snapshot.ts_event,
        snapshot.ts_init,
    )))
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;

    use super::*;
    use crate::{PredictOrderbookWire, instrument_id_from_outcome};
    fn inputs() -> (PredictMarketContext, PredictOrderbookSnapshot) {
        let context = PredictMarketContext {
            market_id: 2859346,
            yes_instrument_id: instrument_id_from_outcome(2859346, 1).unwrap(),
            no_instrument_id: instrument_id_from_outcome(2859346, 2).unwrap(),
            price_precision: 2,
            size_precision: nautilus_model::types::fixed::FIXED_PRECISION,
        };
        let wire: PredictOrderbookWire =
            serde_json::from_str(include_str!("../../test_data/orderbook_2859346.json")).unwrap();
        let snapshot =
            PredictOrderbookSnapshot::from_wire(&context, wire, UnixNanos::from(42)).unwrap();
        (context, snapshot)
    }
    #[test]
    fn quote_projection_uses_best_levels_and_complements_no_with_swapped_sizes() {
        let (context, mut book) = inputs();
        book.bids.reverse();
        book.asks.reverse();
        let yes = quote_from_snapshot(&book, &context, context.yes_instrument_id)
            .unwrap()
            .unwrap();
        let no = quote_from_snapshot(&book, &context, context.no_instrument_id)
            .unwrap()
            .unwrap();
        assert_eq!(yes.bid_price, Price::from("0.62"));
        assert_eq!(no.bid_price, Price::from("0.37"));
        assert_eq!(no.ask_price, Price::from("0.38"));
        assert_eq!((no.bid_size, no.ask_size), (yes.ask_size, yes.bid_size));
        assert_eq!((no.ts_event, no.ts_init), (book.ts_event, book.ts_init));
        assert!(
            quote_from_snapshot(&book, &context, instrument_id_from_outcome(99, 1).unwrap())
                .is_err()
        );
    }
    #[test]
    fn one_sided_crossed_or_zero_size_books_do_not_fabricate_quotes() {
        let (context, book) = inputs();
        for mode in 0..4 {
            let mut invalid = book.clone();
            match mode {
                0 => invalid.bids.clear(),
                1 => invalid.asks.clear(),
                2 => invalid.bids[0].price = Price::from("0.99"),
                _ => {
                    invalid.bids[0].quantity =
                        nautilus_model::types::Quantity::zero(context.size_precision)
                }
            }
            assert!(
                quote_from_snapshot(&invalid, &context, context.yes_instrument_id)
                    .unwrap()
                    .is_none()
            );
            assert!(
                quote_from_snapshot(&invalid, &context, context.no_instrument_id)
                    .unwrap()
                    .is_none()
            );
        }
    }
}
