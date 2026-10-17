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
//! Reported cash uses Native leaves notional. Admission also covers signed budgets
//! and pending writes; its fee allowance is a conservative bound, not a charged fee.
use alloy::primitives::{B256, U256};
use nautilus_model::{
    accounts::{Account, cash::CashAccount},
    enums::{OrderSide, OrderType},
    instruments::InstrumentAny,
    orders::{Order, OrderAny},
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

/// Preserve the existing worst-case signed BUY budget plus fee-rate allowance.
/// A BUY share fee is not an extra collateral debit; this is only admission headroom.
pub(super) fn signed_admission_collateral(row: &PredictOrderData) -> anyhow::Result<Decimal> {
    let collateral = signed_collateral(row)?;
    if row.order.fields.side != PredictOrderSide::Buy {
        return Ok(collateral);
    }
    let bps: u32 = row
        .order
        .fields
        .fee_rate_bps
        .try_into()
        .map_err(|_| anyhow::anyhow!("Predict fee exceeds supported range"))?;
    anyhow::ensure!(bps <= 10_000, "Predict fee exceeds supported range");
    collateral
        .checked_mul(Decimal::ONE + Decimal::from(bps) / Decimal::from(10_000))
        .ok_or_else(|| anyhow::anyhow!("Predict reserve overflow"))
}

/// Reserve the full signed SELL quantity for this token, including partial REST rows.
pub(super) fn signed_inventory(row: &PredictOrderData, token: U256) -> U256 {
    if row.order.fields.side == PredictOrderSide::Sell && row.order.fields.token_id == token {
        row.order.fields.maker_amount
    } else {
        U256::ZERO
    }
}

fn native_buy_price(order: &OrderAny) -> anyhow::Result<Price> {
    if order.order_type() == OrderType::Market {
        // No signed budget is available: binary BUY cost is bounded by one USDT/share.
        Ok(Price::from("1"))
    } else {
        order.price().ok_or_else(|| {
            anyhow::anyhow!("Predict local BUY reservation requires an explicit price")
        })
    }
}

/// Exact pre-Money notional for an unrepresented local BUY; no fee is charged here.
pub(super) fn native_admission_collateral(order: &OrderAny) -> anyhow::Result<Decimal> {
    if order.order_side() != OrderSide::Buy {
        return Ok(Decimal::ZERO);
    }
    native_buy_price(order)?
        .as_decimal()
        .checked_mul(order.leaves_qty().as_decimal())
        .ok_or_else(|| anyhow::anyhow!("Predict native BUY reserve overflow"))
}

pub(super) fn native_inventory(order: &OrderAny) -> Decimal {
    if order.order_side() == OrderSide::Sell {
        order.leaves_qty().as_decimal()
    } else {
        Decimal::ZERO
    }
}

pub(super) fn native_reservation(
    cash: &CashAccount,
    order: &OrderAny,
    instrument: &InstrumentAny,
) -> anyhow::Result<Money> {
    anyhow::ensure!(
        matches!(instrument, InstrumentAny::BinaryOption(_)) && !order.is_quote_quantity(),
        "Predict reservations require native base-quantity binary orders"
    );
    if order.order_side() == OrderSide::Sell {
        return Ok(Money::zero(Currency::USDT()));
    }
    if order.order_type() == OrderType::Market {
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
