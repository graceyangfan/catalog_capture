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

//! Stateless resource views. Native owns balances/orders; this module owns no ledger.
//! Reported cash uses Native leaves notional; command admission does not audit funds.
use alloy::primitives::B256;
use nautilus_model::{
    accounts::{Account, cash::CashAccount},
    enums::{OrderSide, OrderType},
    instruments::InstrumentAny,
    types::{Currency, Money, Price},
};
use rust_decimal::Decimal;

use crate::{
    common::{enums::PredictOrderSide, parse::exact_wei},
    http::account_models::{PredictOrderData, PredictOrderStatus},
};

/// Validate the supported OPEN resource view before reporting or command admission.
/// These are original signed amounts, not remaining quantity: partial fills do not
/// reduce them. A zero amount cannot describe a supported priced order. Refuse an
/// unaccountable resource snapshot rather than treating it as available capacity.
pub(super) fn open_order_hash(row: &PredictOrderData) -> anyhow::Result<B256> {
    anyhow::ensure!(
        row.status == PredictOrderStatus::Open
            && row.currency == "USDT"
            && !row.order.fields.maker_amount.is_zero()
            && !row.order.fields.taker_amount.is_zero(),
        "Predict open-order snapshot has an invalid status, currency or zero signed amount"
    );
    row.order
        .hash
        .ok_or_else(|| anyhow::anyhow!("Predict open order lacks its exchange hash"))
}

fn signed_collateral(row: &PredictOrderData) -> anyhow::Result<Decimal> {
    match row.order.fields.side {
        PredictOrderSide::Buy => exact_wei(row.order.fields.maker_amount),
        PredictOrderSide::Sell => Ok(Decimal::ZERO),
    }
}

/// Unknown/market BUY budgets are not reduced using ambiguous REST filled-amount units.
pub(super) fn reported_signed_reservation(row: &PredictOrderData) -> anyhow::Result<Money> {
    external_reservation(signed_collateral(row)?)
}

fn native_buy_price(order: &impl super::context::OrderView) -> anyhow::Result<Price> {
    if order.order_type() == OrderType::Market {
        // No signed budget is available: binary BUY cost is bounded by one USDT/share.
        Ok(Price::from("1"))
    } else {
        order.price().ok_or_else(|| {
            anyhow::anyhow!("Predict local BUY reservation requires an explicit price")
        })
    }
}

pub(super) fn native_inventory(order: &impl super::context::OrderView) -> Decimal {
    if order.order_side() == OrderSide::Sell {
        order.leaves_qty().as_decimal()
    } else {
        Decimal::ZERO
    }
}

pub(super) fn native_reservation(
    cash: &CashAccount,
    order: &impl super::context::OrderView,
    instrument: &InstrumentAny,
    market_buy_collateral: Option<Decimal>,
) -> anyhow::Result<Money> {
    anyhow::ensure!(
        matches!(instrument, InstrumentAny::BinaryOption(_)) && !order.is_quote_quantity(),
        "Predict reservations require native base-quantity binary orders"
    );
    if order.order_side() == OrderSide::Sell {
        return Ok(Money::zero(Currency::USDT()));
    }
    if order.order_type() == OrderType::Market {
        if let Some(collateral) = market_buy_collateral {
            // Native filled/leaves quantities are gross shares. The immutable signed
            // budget bounds the unfilled fraction; REST amountFilled units are unused.
            let quantity = crate::common::parse::decimal_to_wei(order.quantity().as_decimal())?;
            let leaves = crate::common::parse::decimal_to_wei(order.leaves_qty().as_decimal())?;
            let budget = crate::common::parse::decimal_to_wei(collateral)?;
            let numerator = budget
                .checked_mul(leaves)
                .and_then(|value| value.checked_add(quantity - alloy::primitives::U256::from(1)))
                .ok_or_else(|| anyhow::anyhow!("Predict MARKET reservation overflow"))?;
            return external_reservation(exact_wei(numerator / quantity)?);
        }
        // Recovery can observe a MARKET before the authenticated OPEN snapshot. With
        // no original signed budget in the native order, reserve the binary maximum
        // of one USDT/share. Never infer the budget from a current/average quote.
        return external_reservation(order.leaves_qty().as_decimal());
    }
    let price = native_buy_price(order)?;
    let reservation = cash.calculate_balance_locked(
        instrument,
        order.order_side(),
        order.leaves_qty(),
        price,
        None,
    )?;
    anyhow::ensure!(
        reservation.currency == Currency::USDT()
            && reservation.currency.precision == Currency::USDT().precision,
        "Predict native reservation has an unexpected currency scale"
    );
    Ok(reservation)
}

fn external_reservation(amount: Decimal) -> anyhow::Result<Money> {
    let currency = Currency::USDT();
    let amount = amount.round_dp_with_strategy(
        u32::from(currency.precision),
        rust_decimal::RoundingStrategy::ToPositiveInfinity,
    );
    Money::from_decimal(amount, currency)
        .map_err(|_| anyhow::anyhow!("Predict external reservation exceeds native money range"))
}

#[cfg(test)]
#[path = "../../tests/unit/resources.rs"]
mod tests;

#[cfg(test)]
use nautilus_model::orders::Order;
