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

use alloy::primitives::{Address, B256, U256};
use nautilus_core::UnixNanos;

use crate::common::{contracts::PredictExchangeType, environment::PredictEnvironment};

/// Minimal balance evidence for reported cash accounts. Approvals are a separate admission concern.
#[derive(Clone, Debug)]
pub struct PredictBalanceSnapshot {
    pub environment: PredictEnvironment,
    pub account_address: Address,
    pub collateral: Address,
    pub block_number: u64,
    pub block_hash: B256,
    pub block_timestamp: UnixNanos,
    pub decimals: u8,
    pub balance_wei: U256,
    pub ts_observed: UnixNanos,
}

/// Balance and exchange allowances read at one canonical block hash. Allowances are not funds
/// or locked balances, and this snapshot says nothing about ERC-1155 sell approvals.
#[derive(Clone, Debug)]
pub struct PredictCollateralSnapshot {
    pub environment: PredictEnvironment,
    pub account_address: Address,
    pub collateral: Address,
    pub block_number: u64,
    pub block_hash: B256,
    pub decimals: u8,
    pub balance_wei: U256,
    pub allowances: [(PredictExchangeType, U256); 4],
    pub ts_observed: UnixNanos,
}
