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

//! Exact protocol amounts shared by private REST, wallet events and chain snapshots.

use alloy::primitives::U256;
use rust_decimal::Decimal;

pub(crate) fn exact_wei(value: U256) -> anyhow::Result<Decimal> {
    let divisor = U256::from(1_000_000_000_000_000_000u64);
    let whole = value / divisor;
    let fraction = (value % divisor).to::<u64>();
    let text = format!("{whole}.{fraction:018}");
    Decimal::from_str_exact(text.trim_end_matches('0').trim_end_matches('.'))
        .map_err(|_| anyhow::anyhow!("Predict wei amount exceeds decimal precision or range"))
}

/// Floors positive atomic shares at the native instrument precision before constructing Quantity.
/// No floating point or Decimal multiplication by 1e18 is involved.
pub(crate) fn floor_wei_quantity(
    value: U256,
    precision: u8,
) -> anyhow::Result<nautilus_model::types::Quantity> {
    anyhow::ensure!(
        precision <= 18,
        "Predict quantity precision exceeds atomic precision"
    );
    let unit = U256::from(10u64.pow(18 - u32::from(precision)));
    let rounded = value / unit * unit;
    nautilus_model::types::Quantity::from_decimal_dp(exact_wei(rounded)?, precision)
        .map_err(|_| anyhow::anyhow!("Predict quantity exceeds Nautilus range"))
}

pub(crate) fn decimal_to_wei(value: Decimal) -> anyhow::Result<U256> {
    anyhow::ensure!(
        value >= Decimal::ZERO && value.scale() <= 18,
        "Predict amount is outside atomic precision"
    );
    U256::from(value.mantissa() as u128)
        .checked_mul(U256::from(10u64.pow(18 - value.scale())))
        .ok_or_else(|| anyhow::anyhow!("Predict atomic amount overflow"))
}
