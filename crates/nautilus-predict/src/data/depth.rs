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

//! Requested native L2 snapshots over the existing complete venue snapshot.

use std::num::NonZeroUsize;

use nautilus_model::{
    data::{BookOrder, OrderBookDepth},
    enums::{OrderSide, RecordFlag},
    identifiers::InstrumentId,
    types::Price,
};
use rust_decimal::Decimal;

use crate::{PredictMarketContext, PredictOrderbookSnapshot};

pub(super) fn depth_from_snapshot(
    snapshot: &PredictOrderbookSnapshot,
    context: &PredictMarketContext,
    instrument_id: InstrumentId,
    depth: Option<NonZeroUsize>,
) -> anyhow::Result<OrderBookDepth> {
    let yes = instrument_id == context.yes_instrument_id;
    anyhow::ensure!(
        yes || instrument_id == context.no_instrument_id,
        "Predict depth outcome does not match its market"
    );
    let (source_bids, source_asks) = if yes {
        (&snapshot.bids, &snapshot.asks)
    } else {
        (&snapshot.asks, &snapshot.bids)
    };
    // Let Native keep shallow books inline and allocate only when depth exceeds ten.
    // Predict supplies neither a sequence nor per-price order counts.
    let mut result = OrderBookDepth::new(
        instrument_id,
        [],
        [],
        [],
        [],
        RecordFlag::F_SNAPSHOT as u8,
        0,
        snapshot.ts_event,
        snapshot.ts_init,
    );
    for (levels, side, orders, counts) in [
        (
            source_bids,
            OrderSide::Buy,
            &mut result.bids,
            &mut result.bid_counts,
        ),
        (
            source_asks,
            OrderSide::Sell,
            &mut result.asks,
            &mut result.ask_counts,
        ),
    ] {
        for level in levels.iter().filter(|level| !level.quantity.is_zero()) {
            let price = if yes {
                level.price
            } else {
                Price::from_decimal_dp(
                    Decimal::ONE - level.price.as_decimal(),
                    context.price_precision,
                )?
            };
            orders.push(BookOrder::new(side, price, level.quantity, 0));
        }
        // Only the derived view is sorted/truncated. Recording retains native levels.
        match side {
            OrderSide::Buy => orders.sort_by_key(|order| std::cmp::Reverse(order.price)),
            _ => orders.sort_by_key(|order| order.price),
        }
        if let Some(depth) = depth {
            orders.truncate(depth.get());
        }
        counts.resize(orders.len(), 0);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PredictOrderbookWire, instrument_id_from_outcome};

    #[test]
    fn full_depth_preserves_both_outcomes_and_replaces_disappeared_levels() {
        let context = PredictMarketContext {
            market_id: 2859346,
            yes_instrument_id: instrument_id_from_outcome(2859346, 1).unwrap(),
            no_instrument_id: instrument_id_from_outcome(2859346, 2).unwrap(),
            price_precision: 2,
            size_precision: nautilus_model::types::fixed::FIXED_PRECISION,
        };
        let wire: PredictOrderbookWire =
            serde_json::from_str(include_str!("../../test_data/orderbook_2859346.json")).unwrap();
        let mut snapshot = PredictOrderbookSnapshot::from_wire(&context, wire, 42.into()).unwrap();
        // Native depth is not a fixed ten-level structure.
        snapshot.bids = (1..=20)
            .rev()
            .map(|n| crate::PredictOrderbookLevel {
                price: Price::from_decimal_dp(Decimal::new(n, 2), 2).unwrap(),
                quantity: nautilus_model::types::Quantity::from(n as u64),
            })
            .collect();
        snapshot.bids.reverse();
        snapshot.bids.push(crate::PredictOrderbookLevel {
            price: Price::from("0.99"),
            quantity: nautilus_model::types::Quantity::from(0),
        });
        let original = snapshot.clone();
        let yes =
            depth_from_snapshot(&snapshot, &context, context.yes_instrument_id, None).unwrap();
        let no = depth_from_snapshot(&snapshot, &context, context.no_instrument_id, None).unwrap();
        assert_eq!(yes.bids.len(), 20);
        assert_eq!(yes.bids.first().unwrap().price, Price::from("0.20"));
        assert_eq!(no.asks.first().unwrap().price, Price::from("0.80"));
        assert_eq!(
            no.asks.first().unwrap().size,
            yes.bids.first().unwrap().size
        );
        assert_eq!(
            no.bids.first().unwrap().size,
            yes.asks.first().unwrap().size
        );
        assert_eq!(
            (yes.ts_event, yes.ts_init),
            (snapshot.ts_event, snapshot.ts_init)
        );
        assert!(yes.bid_counts.iter().all(|count| *count == 0));
        assert_eq!(yes.sequence, 0);
        let limited = depth_from_snapshot(
            &snapshot,
            &context,
            context.yes_instrument_id,
            NonZeroUsize::new(3),
        )
        .unwrap();
        assert_eq!(limited.bids.len(), 3);
        assert_eq!(
            serde_json::to_value(&snapshot).unwrap(),
            serde_json::to_value(&original).unwrap()
        );
        let mut book = nautilus_model::orderbook::OrderBook::new(
            context.yes_instrument_id,
            nautilus_model::enums::BookType::L2_MBP,
        );
        book.apply_depth(&yes).unwrap();
        snapshot.bids.clear();
        snapshot.asks.clear();
        book.apply_depth(
            &depth_from_snapshot(&snapshot, &context, context.yes_instrument_id, None).unwrap(),
        )
        .unwrap();
        assert!(book.best_bid_price().is_none() && book.best_ask_price().is_none());
        assert!(
            depth_from_snapshot(
                &snapshot,
                &context,
                instrument_id_from_outcome(99, 1).unwrap(),
                None
            )
            .is_err()
        );
    }
}
