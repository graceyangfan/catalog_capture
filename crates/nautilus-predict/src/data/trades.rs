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

//! Native trade projection and independent per-market trade topic ownership.

use anyhow::Context;
use nautilus_common::messages::DataEvent;
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    data::{Data, TradeTick},
    enums::AggressorSide,
    identifiers::{InstrumentId, TradeId},
    types::{Price, Quantity, fixed::FIXED_PRECISION},
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;

use super::runtime::PredictDataSession;
use crate::PredictMarketContext;

const SCALE: u128 = 1_000_000_000_000_000_000;

impl PredictDataSession {
    pub(super) async fn send_trade_subscription(
        &mut self,
        method: &str,
        market_id: u64,
    ) -> anyhow::Result<()> {
        let request_id = self.next_request_id;
        self.next_request_id = request_id
            .checked_add(1)
            .context("Predict request ID overflow")?;
        self.client
            .send_text(
                json!({"method": method, "requestId": request_id,
            "params": [format!("predictTrades/{market_id}")]})
                .to_string(),
                None,
            )
            .await
            .with_context(|| format!("failed to {method} Predict trades {market_id}"))
    }

    pub(super) fn handle_trades(&self, market_id: u64, data: &str) -> anyhow::Result<()> {
        let owners = self
            .trades
            .iter()
            .filter(|(_, c)| c.market_id == market_id)
            .collect::<Vec<_>>();
        if owners.is_empty() {
            return Ok(());
        }
        let wire: TradeWire =
            serde_json::from_str(data).context("invalid Predict trade payload")?;
        anyhow::ensure!(
            wire.market_id == market_id,
            "Predict trade topic/payload market mismatch"
        );
        let executions = wire.executions()?;
        let ts_event = UnixNanos::from(
            wire.ts_ms
                .checked_mul(1_000_000)
                .context("Predict trade timestamp overflow")?,
        );
        let ts_init = self.clock.get_time_ns();
        // Validate every projection before publishing any leg. A malformed frame cannot
        // leave a partial tape on one outcome. Each UUID identifies a local execution,
        // shared by both outcome views, not a venue or chain match ID.
        let mut ticks = Vec::new();
        for execution in executions {
            let trade_id = TradeId::from(UUID4::new().to_string().as_str());
            for (id, context) in &owners {
                ticks.push(project_trade(
                    &execution, context, **id, trade_id, ts_event, ts_init,
                )?);
            }
        }
        for tick in ticks {
            self.data_sender.send(DataEvent::Data(Data::Trade(tick)))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, serde::Serialize, PartialEq, Eq)]
enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum MatchType {
    Complementary,
    Split,
    Merge,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderWire {
    fill: String,
    price: String,
    outcome_index: u8,
    side: Side,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MakerWire {
    #[serde(flatten)]
    order: OrderWire,
    match_type: MatchType,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TradeWire {
    market_id: u64,
    ts_ms: u64,
    taker: OrderWire,
    makers: Vec<MakerWire>,
}

struct Execution {
    yes_price: u128,
    fill: u128,
    yes_buyer: bool,
}

impl TradeWire {
    fn executions(&self) -> anyhow::Result<Vec<Execution>> {
        let (taker_price, taker_fill) = self.taker.amounts()?;
        anyhow::ensure!(
            !self.makers.is_empty(),
            "Predict trade has no maker executions"
        );
        let yes_buyer = (self.taker.side == Side::Bid) == (self.taker.outcome_index == 1);
        let mut result = Vec::with_capacity(self.makers.len());
        let mut filled = 0u128;
        for maker in &self.makers {
            let (price, fill) = maker.order.amounts()?;
            let same_outcome = maker.order.outcome_index == self.taker.outcome_index;
            let valid = match maker.match_type {
                MatchType::Complementary => same_outcome && maker.order.side != self.taker.side,
                MatchType::Split => {
                    !same_outcome && maker.order.side == Side::Bid && self.taker.side == Side::Bid
                }
                MatchType::Merge => {
                    !same_outcome && maker.order.side == Side::Ask && self.taker.side == Side::Ask
                }
            };
            anyhow::ensure!(
                valid,
                "Predict trade match type contradicts order sides/outcomes"
            );
            let taker_execution_price = if same_outcome { price } else { SCALE - price };
            // Per-maker prices describe each execution. Do not multiply two
            // 18-scale integers or assume a multi-maker taker field is a VWAP.
            if self.makers.len() == 1 {
                anyhow::ensure!(
                    taker_execution_price.abs_diff(taker_price) <= 1,
                    "Predict single-maker trade prices disagree"
                );
            }
            filled = filled
                .checked_add(fill)
                .context("Predict trade size overflow")?;
            result.push(Execution {
                yes_price: if maker.order.outcome_index == 1 {
                    price
                } else {
                    SCALE - price
                },
                fill,
                yes_buyer,
            });
        }
        anyhow::ensure!(
            filled == taker_fill,
            "Predict maker allocations differ from taker fill"
        );
        Ok(result)
    }
}

impl OrderWire {
    fn amounts(&self) -> anyhow::Result<(u128, u128)> {
        anyhow::ensure!(
            [1, 2].contains(&self.outcome_index),
            "unsupported Predict trade outcome"
        );
        let atomic = |s: &str| -> anyhow::Result<u128> {
            anyhow::ensure!(
                !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()),
                "Predict atomic trade amount must be unsigned integer text"
            );
            s.parse()
                .context("Predict atomic trade amount out of range")
        };
        let price = atomic(&self.price)?;
        let fill = atomic(&self.fill)?;
        anyhow::ensure!(
            price <= SCALE && fill > 0,
            "Predict trade price/size outside range"
        );
        Ok((price, fill))
    }
}

fn project_trade(
    execution: &Execution,
    context: &PredictMarketContext,
    id: InstrumentId,
    trade_id: TradeId,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    let yes = id == context.yes_instrument_id;
    anyhow::ensure!(
        yes || id == context.no_instrument_id,
        "Predict trade instrument/market mismatch"
    );
    let atomic_price = if yes {
        execution.yes_price
    } else {
        SCALE - execution.yes_price
    };
    let decimal = Decimal::from_i128_with_scale(atomic_price as i128, 18);
    let price = Price::from_decimal_dp(decimal, context.price_precision)?;
    anyhow::ensure!(
        price.as_decimal() == decimal,
        "Predict execution price exceeds instrument precision"
    );
    // Catalog precision is fixed at 16 (9 for standard builds). Do not feed
    // 18-scale raw values into a standard-scale Quantity or use an f64 roundtrip.
    #[allow(
        clippy::useless_conversion,
        reason = "QuantityRaw is u128 or u64 depending on feature unification"
    )]
    let raw = (execution.fill / 10u128.pow(u32::from(18 - FIXED_PRECISION)))
        .try_into()
        .context("Predict trade quantity exceeds native range")?;
    let size = Quantity::from_raw_checked(raw, FIXED_PRECISION)?;
    let aggressor = if execution.yes_buyer == yes {
        AggressorSide::Buy
    } else {
        AggressorSide::Sell
    };
    TradeTick::new_checked(id, price, size, aggressor, trade_id, ts_event, ts_init)
}

#[cfg(test)]
mod tests {
    use nautilus_serialization::arrow::{DecodeFromRecordBatch, EncodeToRecordBatch};

    use super::*;

    fn context() -> PredictMarketContext {
        PredictMarketContext {
            market_id: 42,
            yes_instrument_id: "42-1.PREDICT".into(),
            no_instrument_id: "42-2.PREDICT".into(),
            price_precision: 2,
            size_precision: FIXED_PRECISION,
        }
    }
    fn wire(outcome: u8, side: Side, kind: &str) -> TradeWire {
        let other = if kind == "COMPLEMENTARY" {
            outcome
        } else {
            3 - outcome
        };
        let maker_side = if kind == "COMPLEMENTARY" {
            if side == Side::Bid {
                Side::Ask
            } else {
                Side::Bid
            }
        } else {
            side
        };
        serde_json::from_value(json!({"marketId":42,"tsMs":123,"taker":{"fill":"11680700000000000001","price":"320000000000000000","outcomeIndex":outcome,"side":side},
          "makers":[{"fill":"11680700000000000001","price": if other==outcome {"320000000000000000"} else {"680000000000000000"},"outcomeIndex":other,"side":maker_side,"matchType":kind}]})).unwrap()
    }
    #[test]
    fn every_match_and_taker_direction_projects_native_ticks_and_roundtrips() {
        let c = context();
        for outcome in [1, 2] {
            for side in [Side::Bid, Side::Ask] {
                for kind in [
                    "COMPLEMENTARY",
                    if side == Side::Bid { "SPLIT" } else { "MERGE" },
                ] {
                    let e = wire(outcome, side, kind).executions().unwrap().remove(0);
                    let yes = project_trade(
                        &e,
                        &c,
                        c.yes_instrument_id,
                        "local".into(),
                        123.into(),
                        124.into(),
                    )
                    .unwrap();
                    let no = project_trade(
                        &e,
                        &c,
                        c.no_instrument_id,
                        yes.trade_id,
                        yes.ts_event,
                        yes.ts_init,
                    )
                    .unwrap();
                    assert_eq!(
                        yes.price,
                        Price::from(if outcome == 1 { "0.32" } else { "0.68" })
                    );
                    assert_eq!(
                        yes.aggressor_side,
                        if (outcome == 1) == (side == Side::Bid) {
                            AggressorSide::Buy
                        } else {
                            AggressorSide::Sell
                        }
                    );
                    assert_ne!(yes.aggressor_side, no.aggressor_side);
                    assert_eq!(yes.size, no.size);
                    assert_eq!(
                        yes.size.as_decimal(),
                        Decimal::from_i128_with_scale(
                            11_680_700_000_000_000_001u128 as i128
                                / 10i128.pow(u32::from(18 - FIXED_PRECISION)),
                            u32::from(FIXED_PRECISION)
                        )
                    );
                    let metadata = yes.metadata();
                    let decoded = TradeTick::decode_batch(
                        &metadata,
                        TradeTick::encode_batch(&metadata, &[yes]).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(decoded, vec![yes]);
                }
            }
        }
    }
    #[test]
    fn recorded_public_match_types_preserve_price_size_and_aggressor() {
        let examples: serde_json::Value =
            serde_json::from_str(include_str!("../../test_data/public_trades.json")).unwrap();
        let c = context();
        for kind in ["COMPLEMENTARY", "SPLIT", "MERGE"] {
            let w: TradeWire = serde_json::from_value(examples[kind]["wire"].clone()).unwrap();
            let e = w.executions().unwrap().remove(0);
            let t = project_trade(
                &e,
                &c,
                c.yes_instrument_id,
                "local".into(),
                1.into(),
                2.into(),
            )
            .unwrap();
            let expected = &examples[kind]["canonical_yes"];
            assert_eq!(
                t.price.as_decimal(),
                expected["price"]
                    .as_str()
                    .unwrap()
                    .parse::<Decimal>()
                    .unwrap()
            );
            assert_eq!(
                t.size.as_decimal(),
                expected["size"]
                    .as_str()
                    .unwrap()
                    .parse::<Decimal>()
                    .unwrap()
            );
            assert_eq!(
                t.aggressor_side,
                if kind == "MERGE" {
                    AggressorSide::Buy
                } else {
                    AggressorSide::Sell
                }
            );
        }
    }

    #[test]
    fn mixed_maker_allocations_emit_execution_prices_without_counting_taker_total() {
        let mut w = wire(1, Side::Bid, "COMPLEMENTARY");
        w.taker.fill = (5 * SCALE).to_string();
        w.taker.price = (62 * SCALE / 100).to_string();
        w.makers[0].order.fill = (2 * SCALE).to_string();
        w.makers[0].order.price = (SCALE / 2).to_string();
        w.makers.push(MakerWire {
            order: OrderWire {
                fill: (3 * SCALE).to_string(),
                price: (3 * SCALE / 10).to_string(),
                outcome_index: 2,
                side: Side::Bid,
            },
            match_type: MatchType::Split,
        });
        let executions = w.executions().unwrap();
        assert_eq!(executions.len(), 2);
        assert_eq!(executions.iter().map(|e| e.fill).sum::<u128>(), 5 * SCALE);
        let c = context();
        let ticks = executions
            .iter()
            .map(|e| {
                project_trade(
                    e,
                    &c,
                    c.yes_instrument_id,
                    "local".into(),
                    1.into(),
                    2.into(),
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(ticks[0].price, Price::from("0.50"));
        assert_eq!(ticks[1].price, Price::from("0.70"));
        assert_eq!(ticks[0].size, Quantity::from(2));
        assert_eq!(ticks[1].size, Quantity::from(3));
        assert!(ticks.iter().all(|t| t.aggressor_side == AggressorSide::Buy));
        w.taker.price = (63 * SCALE / 100).to_string();
        assert_eq!(
            w.executions().unwrap().len(),
            2,
            "changing a taker aggregate must not replace per-maker execution prices"
        );
    }

    #[test]
    fn large_atomic_fills_do_not_require_overflowing_notional_products() {
        let mut w = wire(2, Side::Bid, "SPLIT");
        let fill = 1_000_000 * SCALE;
        w.taker.fill = fill.to_string();
        w.makers[0].order.fill = fill.to_string();
        let e = w.executions().unwrap().remove(0);
        let c = context();
        let tick = project_trade(
            &e,
            &c,
            c.yes_instrument_id,
            "local".into(),
            1.into(),
            2.into(),
        )
        .unwrap();
        assert_eq!(tick.size, Quantity::from(1_000_000));
        assert_eq!(tick.price, Price::from("0.68"));
    }

    #[test]
    fn malformed_allocations_and_zero_dust_are_rejected_before_publication() {
        let c = context();
        for mode in 0..6 {
            let mut w = wire(2, Side::Bid, "SPLIT");
            match mode {
                0 => w.taker.outcome_index = 3,
                1 => w.makers[0].order.fill = "1".into(),
                2 => w.makers[0].order.side = Side::Ask,
                3 => w.taker.fill = "-1".into(),
                4 => w.taker.price = (SCALE + 1).to_string(),
                _ => w.makers.clear(),
            }
            assert!(w.executions().is_err());
        }
        assert!(
            project_trade(
                &Execution {
                    fill: 1,
                    yes_price: SCALE / 2,
                    yes_buyer: true
                },
                &c,
                c.yes_instrument_id,
                "local".into(),
                1.into(),
                2.into()
            )
            .is_err()
        );
    }
}
