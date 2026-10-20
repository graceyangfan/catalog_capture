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

//! Pure conversion using Nautilus monetary types. Reservation evidence is a required input.

use nautilus_model::types::{AccountBalance, Currency, Money};

use super::models::{PredictBalanceSnapshot, PredictCollateralSnapshot};
use crate::common::{contracts::collateral_contract, parse::exact_wei};

/// Converts verified collateral into an account balance using explicit reservation evidence.
/// No free balance or zero lock is inferred from allowance or wallet balance alone. The caller
/// owns reservation completeness and snapshot freshness; this function never publishes state.
///
/// # Errors
/// Rejects context mismatches, dust requiring rounding, overflow and invalid reservations.
pub fn parse_collateral_balance(
    snapshot: &PredictCollateralSnapshot,
    locked: Money,
) -> anyhow::Result<AccountBalance> {
    parse_balance(
        snapshot.environment,
        snapshot.collateral,
        snapshot.account_address,
        snapshot.block_hash,
        snapshot.decimals,
        snapshot.balance_wei,
        locked,
    )
}

/// Converts minimal balance evidence with explicit reservations, using the same exact monetary policy.
///
/// # Errors
/// Invalid collateral, monetary scale, reservations, precision or range return errors.
pub fn parse_balance_snapshot(
    snapshot: &PredictBalanceSnapshot,
    locked: Money,
) -> anyhow::Result<AccountBalance> {
    parse_balance(
        snapshot.environment,
        snapshot.collateral,
        snapshot.account_address,
        snapshot.block_hash,
        snapshot.decimals,
        snapshot.balance_wei,
        locked,
    )
}

fn parse_balance(
    environment: crate::common::environment::PredictEnvironment,
    collateral: alloy::primitives::Address,
    account: alloy::primitives::Address,
    block_hash: alloy::primitives::B256,
    decimals: u8,
    balance: alloy::primitives::U256,
    locked: Money,
) -> anyhow::Result<AccountBalance> {
    anyhow::ensure!(
        collateral == collateral_contract(environment)
            && !account.is_zero()
            && !block_hash.is_zero()
            && decimals == 18
            && locked.currency == Currency::USDT()
            && locked.currency.precision == Currency::USDT().precision,
        "invalid Predict collateral or reservation context"
    );
    let total = exact_wei(balance)?;
    anyhow::ensure!(
        locked.as_decimal() >= rust_decimal::Decimal::ZERO && locked.as_decimal() <= total,
        "Predict reservations are negative or exceed collateral"
    );
    let total = Money::from_decimal_exact(total, locked.currency)
        .map_err(|_| anyhow::anyhow!("Predict collateral balance requires monetary rounding"))?;
    let free = total
        .checked_sub(locked)
        .ok_or_else(|| anyhow::anyhow!("Predict collateral free balance exceeds Nautilus range"))?;
    AccountBalance::new_checked(total, locked, free).map_err(|_| {
        anyhow::anyhow!("Predict collateral balance exceeds Nautilus range or reservations")
    })
}
