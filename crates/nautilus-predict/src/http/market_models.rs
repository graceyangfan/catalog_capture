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

//! Execution metadata is strict and independent of the recorder's permissive DTO.
use alloy::primitives::U256;
use serde::Deserialize;

use crate::common::{contracts::PredictExchangeType, serde::decimal_u256};

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictExecutionMarket {
    pub id: u64,
    pub trading_status: String,
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
    pub fee_rate_bps: u32,
    pub decimal_precision: u8,
    pub outcomes: Vec<PredictExecutionOutcome>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictExecutionOutcome {
    pub index_set: u8,
    #[serde(with = "decimal_u256")]
    pub on_chain_id: U256,
}

impl PredictExecutionMarket {
    #[must_use]
    pub fn exchange(&self) -> PredictExchangeType {
        PredictExchangeType::from_market_flags(self.is_neg_risk, self.is_yield_bearing)
    }
}
