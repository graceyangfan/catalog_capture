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

//! Exact native LIMIT / SELL MARKET validation and wei conversion. No RPC or order state is owned here.

use alloy::primitives::U256;
use nautilus_model::{
    enums::{OrderSide, OrderStatus, OrderType, TimeInForce},
    instruments::{BinaryOption, Instrument},
    orders::{Order, OrderAny},
    types::Currency,
};
use rust_decimal::Decimal;

use crate::config::PredictSubmissionConfig;

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
    policy: &PredictSubmissionConfig,
    now: u64,
) -> anyhow::Result<(Decimal, U256, U256, U256)> {
    anyhow::ensure!(
        order.status() == OrderStatus::Initialized,
        "Predict submit requires an initialized order"
    );
    anyhow::ensure!(
        matches!(order.order_type(), OrderType::Limit | OrderType::Market),
        "Predict order type is unsupported; use LIMIT or SELL MARKET"
    );
    anyhow::ensure!(
        matches!(order.order_side(), OrderSide::Buy | OrderSide::Sell)
            && (order.order_type() != OrderType::Market || order.order_side() == OrderSide::Sell),
        "Predict MARKET submission supports SELL only"
    );
    anyhow::ensure!(
        matches!(order.time_in_force(), TimeInForce::Gtc | TimeInForce::Gtd)
            && (order.order_type() != OrderType::Market
                || order.time_in_force() == TimeInForce::Gtc),
        "Predict LIMIT requires GTC/GTD; MARKET currently requires GTC"
    );
    anyhow::ensure!(
        !order.is_quote_quantity() && !order.is_reduce_only(),
        "Predict requires base quantity; venue reduce-only is unsupported"
    );
    anyhow::ensure!(
        !policy.require_post_only || order.is_post_only(),
        "Predict configured submission policy requires post-only"
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
    anyhow::ensure!(
        px % instrument.price_increment().as_decimal() == Decimal::ZERO
            && qty % instrument.size_increment().as_decimal() == Decimal::ZERO,
        "Predict price or quantity does not match the native instrument increment"
    );
    let cost = if order.order_type() == OrderType::Market {
        Decimal::ZERO // Fresh depth determines SELL proceeds in preflight; no cash is spent.
    } else {
        px.checked_mul(qty)
            .ok_or_else(|| anyhow::anyhow!("Predict notional overflow"))?
    };
    anyhow::ensure!(
        instrument.quote_currency() == Currency::USDT()
            && (order.order_type() == OrderType::Market
                || cost.round_dp(u32::from(Currency::USDT().precision)) == cost),
        "Predict collateral requires exact native USDT representation"
    );
    anyhow::ensure!(
        order.order_side() == OrderSide::Sell || cost <= policy.max_order_usdt,
        "Predict order exceeds its collateral cap"
    );
    let expiry = match order.time_in_force() {
        TimeInForce::Gtc if order.order_type() == OrderType::Market => now / 1_000_000_000 + 300,
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
        order.order_type() == OrderType::Market || cost >= Decimal::ONE,
        "Predict order is below the venue minimum of 1 USDT"
    );
    Ok((
        cost,
        decimal_wei(px)?,
        decimal_wei(qty)?,
        U256::from(expiry),
    ))
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
