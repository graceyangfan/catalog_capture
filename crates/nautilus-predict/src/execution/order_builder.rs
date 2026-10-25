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

//! Exact native LIMIT/MARKET terms and wei conversion. No RPC or order state is owned here.

use alloy::primitives::U256;
use nautilus_model::{
    enums::{OrderSide, OrderStatus, OrderType, TimeInForce},
    instruments::{BinaryOption, Instrument},
    orders::{Order, OrderAny},
    types::Currency,
};
use rust_decimal::Decimal;

#[derive(Clone, Copy, Debug)]
pub(super) struct OrderTerms {
    pub cost: Decimal,
    pub price: U256,
    pub shares: U256,
    pub expiry: U256,
    pub market: Option<crate::common::market::PredictMarketOrderAmounts>,
}

pub(super) fn decimal_wei(value: Decimal) -> anyhow::Result<U256> {
    let value = value.normalize();
    anyhow::ensure!(
        value > Decimal::ZERO && value.scale() <= 18,
        "Predict amount is nonpositive or exceeds wei precision"
    );
    U256::from(value.mantissa().unsigned_abs())
        .checked_mul(U256::from(10).pow(U256::from(18 - value.scale())))
        .ok_or_else(|| anyhow::anyhow!("Predict amount exceeds U256"))
}

/// Native command params carry venue-specific inputs without changing MarketOrder.
/// A decimal string keeps the strategy price exact; it is a reference before slippage.
pub(super) fn market_price(
    params: Option<&nautilus_core::Params>,
    order: &OrderAny,
) -> anyhow::Result<Option<U256>> {
    anyhow::ensure!(
        params
            .and_then(|params| params.get("isFillOrKill"))
            .is_none(),
        "Predict FOK must use native TimeInForce::Fok, not params[isFillOrKill]"
    );
    let Some(value) = params.and_then(|params| params.get("market_price")) else {
        anyhow::ensure!(
            order.order_type() != OrderType::Market,
            "Predict MARKET requires strategy market_price"
        );
        return Ok(None);
    };
    anyhow::ensure!(
        order.order_type() == OrderType::Market,
        "Predict market_price requires MARKET"
    );
    let price = value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Predict market_price must be a decimal string"))?;
    let price = Decimal::from_str_exact(price)?;
    Ok(Some(decimal_wei(price)?))
}

/// Match the Native Hyperliquid parameter name while rejecting lossy or ignored input.
/// Omission uses the official SDK's zero extra slippage; no client policy is applied.
pub(super) fn market_order_slippage_bps(
    params: Option<&nautilus_core::Params>,
    order: &OrderAny,
) -> anyhow::Result<u32> {
    // Minimum-out changes signed BUY economics and the venue's target sizing.
    // Native currently supports the default model only; never ignore a caller's mode.
    for key in ["isMinAmountOut", "is_min_amount_out"] {
        anyhow::ensure!(
            params.and_then(|p| p.get(key)).is_none(),
            "Predict Native minimum-out MARKET model is unsupported; params[{key}] cannot select a signed amount model"
        );
    }
    anyhow::ensure!(
        params.and_then(|p| p.get("slippageBps")).is_none(),
        "Predict Native slippage uses params[market_order_slippage_bps], not HTTP slippageBps"
    );
    anyhow::ensure!(
        params.and_then(|p| p.get("market_slippage_bps")).is_none(),
        "Predict per-order slippage uses params[market_order_slippage_bps]"
    );
    let Some(value) = params.and_then(|p| p.get("market_order_slippage_bps")) else {
        return Ok(0);
    };
    anyhow::ensure!(
        order.order_type() == OrderType::Market,
        "Predict market_order_slippage_bps requires MARKET"
    );
    let bps = value.as_u64().filter(|bps| *bps < 10_000).ok_or_else(|| {
        anyhow::anyhow!("Predict market_order_slippage_bps must be an integer from 0 to 9999")
    })?;
    Ok(u32::try_from(bps)?)
}

/// STP is venue-specific; post-only remains on the Native order itself.
pub(super) fn limit_options(
    params: Option<&nautilus_core::Params>,
    order: &OrderAny,
) -> anyhow::Result<crate::http::command_models::PredictLimitOrderOptions> {
    anyhow::ensure!(
        params.and_then(|params| params.get("isPostOnly")).is_none(),
        "Predict post-only must use the Native LIMIT is_post_only field, not params[isPostOnly]"
    );
    anyhow::ensure!(
        params
            .and_then(|params| params.get("selfTradePrevention"))
            .is_none(),
        "Predict Native STP uses params[self_trade_prevention], not the HTTP selfTradePrevention key"
    );
    let stp = params
        .and_then(|params| params.get("self_trade_prevention"))
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .map_err(|_| {
            anyhow::anyhow!(
                "Predict self_trade_prevention must be CANCEL_MAKER/CANCEL_TAKER/CANCEL_BOTH"
            )
        })?;
    anyhow::ensure!(
        stp.is_none() || order.order_type() == OrderType::Limit,
        "Predict self_trade_prevention currently requires LIMIT"
    );
    Ok(crate::http::command_models::PredictLimitOrderOptions {
        is_post_only: order.is_post_only(),
        self_trade_prevention: stp,
    })
}

fn significant_digits(value: Decimal) -> usize {
    value
        .normalize()
        .mantissa()
        .unsigned_abs()
        .to_string()
        .trim_end_matches('0')
        .len()
}

pub(super) fn terms(
    order: &OrderAny,
    instrument: &BinaryOption,
    market_order_slippage_bps: u32,
    now: u64,
    market_price: Option<U256>,
) -> anyhow::Result<OrderTerms> {
    anyhow::ensure!(
        order.status() == OrderStatus::Initialized,
        "Predict submit requires an initialized order"
    );
    anyhow::ensure!(
        matches!(order.order_type(), OrderType::Limit | OrderType::Market),
        "Predict order type is unsupported; use LIMIT or MARKET"
    );
    anyhow::ensure!(
        matches!(order.order_side(), OrderSide::Buy | OrderSide::Sell),
        "Predict order side is unsupported"
    );
    anyhow::ensure!(
        match order.order_type() {
            OrderType::Limit =>
                matches!(order.time_in_force(), TimeInForce::Gtc | TimeInForce::Gtd),
            OrderType::Market =>
                matches!(order.time_in_force(), TimeInForce::Gtc | TimeInForce::Fok),
            _ => false,
        },
        "Predict LIMIT requires GTC/GTD; MARKET requires GTC/FOK"
    );
    anyhow::ensure!(
        !order.is_reduce_only(),
        "Predict venue reduce-only is unsupported"
    );
    let quote_buy = order.is_quote_quantity();
    let original_budget = super::context::OrderView::quote_buy_budget(order);
    anyhow::ensure!(
        !quote_buy
            || (order.order_type() == OrderType::Market && order.order_side() == OrderSide::Buy),
        "Predict quote_quantity requires MARKET BUY"
    );
    anyhow::ensure!(
        (original_budget.is_none() && !quote_buy) || market_order_slippage_bps == 0,
        "Predict quote_quantity MARKET BUY requires market_order_slippage_bps=0 to preserve the quote budget"
    );
    anyhow::ensure!(
        !quote_buy || original_budget.is_some(),
        "Predict quote sizing requires original Native MARKET BUY quote intent"
    );
    let px = if order.order_type() == OrderType::Market {
        Decimal::ONE
    } else {
        order
            .price()
            .ok_or_else(|| anyhow::anyhow!("Predict limit price is missing"))?
            .as_decimal()
    };
    let qty = order.quantity().as_decimal();
    // MARKET's shared SDK amount builder checks its quantity/price bounds once.
    // LIMIT keeps exact native terms instead of silently applying SDK truncation.
    if order.order_type() == OrderType::Limit {
        anyhow::ensure!(
            px > Decimal::ZERO && px <= Decimal::ONE,
            "Predict limit price must be positive and at most one"
        );
        anyhow::ensure!(
            qty >= Decimal::new(1, 2),
            "Predict SDK quantity minimum is 0.01 shares"
        );
        anyhow::ensure!(
            significant_digits(px) <= 3 && significant_digits(qty) <= 5,
            "Predict SDK amount preparation would change original native terms"
        );
    }
    anyhow::ensure!(
        px % instrument.price_increment().as_decimal() == Decimal::ZERO
            && (quote_buy || qty % instrument.size_increment().as_decimal() == Decimal::ZERO),
        "Predict price or quantity does not match the native instrument increment"
    );
    let input = decimal_wei(qty)?;
    let market = if quote_buy {
        anyhow::ensure!(
            qty.round_dp(u32::from(Currency::USDT().precision)) == qty,
            "Predict quote amount requires exact native USDT precision"
        );
        Some(
            crate::common::market::PredictMarketOrderAmounts::from_value_price(
                input,
                market_price.ok_or_else(|| {
                    anyhow::anyhow!("Predict MARKET requires strategy market_price")
                })?,
            )?,
        )
    } else if order.order_type() == OrderType::Market {
        Some(
            crate::common::market::PredictMarketOrderAmounts::from_price(
                if order.order_side() == OrderSide::Buy {
                    crate::common::enums::PredictOrderSide::Buy
                } else {
                    crate::common::enums::PredictOrderSide::Sell
                },
                input,
                market_price.ok_or_else(|| {
                    anyhow::anyhow!("Predict MARKET requires strategy market_price")
                })?,
                market_order_slippage_bps,
            )?,
        )
    } else {
        None
    };
    let shares = market.map_or(input, |amounts| amounts.amount);
    if quote_buy {
        let derived = crate::common::parse::exact_wei(shares)?;
        super::parse::quantity(derived, instrument.size_precision())?;
        anyhow::ensure!(
            derived % instrument.size_increment().as_decimal() == Decimal::ZERO,
            "Predict derived shares do not match native instrument increment"
        );
    }
    let cost = if let Some(amounts) = market {
        crate::common::parse::exact_wei(if order.order_side() == OrderSide::Buy {
            amounts.maker_amount
        } else {
            amounts.taker_amount
        })?
    } else {
        px.checked_mul(qty)
            .ok_or_else(|| anyhow::anyhow!("Predict notional overflow"))?
    };
    anyhow::ensure!(
        original_budget.is_none_or(|budget| cost <= budget),
        "Predict signed collateral exceeds original Native quote budget"
    );
    anyhow::ensure!(
        instrument.quote_currency() == Currency::USDT()
            && (order.order_type() == OrderType::Market
                || cost.round_dp(u32::from(Currency::USDT().precision)) == cost),
        "Predict collateral requires exact native USDT representation"
    );
    let expiry = match order.time_in_force() {
        TimeInForce::Gtc | TimeInForce::Fok if order.order_type() == OrderType::Market => {
            now / 1_000_000_000 + 300
        }
        TimeInForce::Gtc => 4_102_444_800,
        TimeInForce::Gtd => {
            let expiry = order
                .expire_time()
                .ok_or_else(|| anyhow::anyhow!("Predict GTD expiry is missing"))?
                .as_u64();
            anyhow::ensure!(
                expiry % 1_000_000_000 == 0,
                "Predict expiry requires whole seconds"
            );
            expiry / 1_000_000_000
        }
        _ => unreachable!(),
    };
    anyhow::ensure!(
        expiry > now / 1_000_000_000 && instrument.expiration_ns.as_u64() > now,
        "Predict order or instrument has expired"
    );
    anyhow::ensure!(
        order.time_in_force() != TimeInForce::Gtd || expiry > now / 1_000_000_000 + 120,
        "Predict GTD expiry must be more than 2 minutes in the future"
    );
    anyhow::ensure!(
        cost >= Decimal::ONE,
        "Predict order is below the venue minimum of 1 USDT"
    );
    Ok(OrderTerms {
        cost,
        price: decimal_wei(px)?,
        shares,
        expiry: U256::from(expiry),
        market,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wei_preserves_eighteen_decimals_and_normalizes_product_trailing_zeros() {
        let one_wei = Decimal::from_str_exact("0.000000000000000001").unwrap();
        assert_eq!(decimal_wei(one_wei).unwrap(), U256::from(1));
        let shares = Decimal::from_str_exact("196.0000000000000000").unwrap();
        let price = Decimal::from_str_exact("0.013").unwrap();
        assert_eq!(
            decimal_wei(shares * price).unwrap(),
            U256::from(2_548_000_000_000_000_000u128)
        );
        assert!(decimal_wei(Decimal::from_str_exact("0.0000000000000000001").unwrap()).is_err());
        assert!(decimal_wei(Decimal::ZERO).is_err());
        assert!(decimal_wei(-one_wei).is_err());
    }
}
