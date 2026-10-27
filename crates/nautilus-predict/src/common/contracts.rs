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

//! Exchange identities from official SDK 1.3.8, Constants.ts (5ff2b4a).

use alloy::primitives::{Address, address};

use super::environment::PredictEnvironment;

/// Collateral contract from the official SDK. Both supported deployments use 18 decimals.
#[must_use]
pub const fn collateral_contract(environment: PredictEnvironment) -> Address {
    match environment {
        PredictEnvironment::BnbMainnet => address!("55d398326f99059fF775485246999027B3197955"),
        PredictEnvironment::BnbTestnet => address!("B32171ecD878607FFc4F8FC0bCcE6852BB3149E0"),
    }
}

/// Market-dependent exchange selection. It remains a per-order input to a shared signer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredictExchangeType {
    /// Standard conditional-token market.
    Standard,
    /// Standard negative-risk market.
    NegRisk,
    /// Yield-bearing conditional-token market.
    YieldBearing,
    /// Yield-bearing negative-risk market.
    YieldBearingNegRisk,
}

impl PredictExchangeType {
    pub(crate) const ALL: [Self; 4] = [
        Self::Standard,
        Self::NegRisk,
        Self::YieldBearing,
        Self::YieldBearingNegRisk,
    ];

    /// Selects the exchange using the two flags returned in venue market metadata.
    #[must_use]
    pub const fn from_market_flags(is_neg_risk: bool, is_yield_bearing: bool) -> Self {
        match (is_neg_risk, is_yield_bearing) {
            (false, false) => Self::Standard,
            (true, false) => Self::NegRisk,
            (false, true) => Self::YieldBearing,
            (true, true) => Self::YieldBearingNegRisk,
        }
    }

    /// Returns the official verifying contract for this environment and market type.
    #[must_use]
    pub const fn contract(self, environment: PredictEnvironment) -> Address {
        match environment {
            PredictEnvironment::BnbMainnet => match self {
                Self::Standard => address!("8BC070BEdAB741406F4B1Eb65A72bee27894B689"),
                Self::NegRisk => address!("365fb81bd4A24D6303cd2F19c349dE6894D8d58A"),
                Self::YieldBearing => address!("6bEb5a40C032AFc305961162d8204CDA16DECFa5"),
                Self::YieldBearingNegRisk => address!("8A289d458f5a134bA40015085A8F50Ffb681B41d"),
            },
            PredictEnvironment::BnbTestnet => match self {
                Self::Standard => address!("2A6413639BD3d73a20ed8C95F634Ce198ABbd2d7"),
                Self::NegRisk => address!("d690b2bd441bE36431F6F6639D7Ad351e7B29680"),
                Self::YieldBearing => address!("8a6B4Fa700A1e310b106E7a48bAFa29111f66e89"),
                Self::YieldBearingNegRisk => address!("95D5113bc50eD201e319101bbca3e0E250662fCC"),
            },
        }
    }

    /// Conditional-token contract selected exactly as SDK Constants.ts.
    #[must_use]
    pub const fn conditional_tokens(self, environment: PredictEnvironment) -> Address {
        match (environment, self) {
            (PredictEnvironment::BnbMainnet, Self::Standard | Self::NegRisk) => {
                address!("22DA1810B194ca018378464a58f6Ac2B10C9d244")
            }
            (PredictEnvironment::BnbMainnet, Self::YieldBearing) => {
                address!("9400F8Ad57e9e0F352345935d6D3175975eb1d9F")
            }
            (PredictEnvironment::BnbMainnet, Self::YieldBearingNegRisk) => {
                address!("F64b0b318AAf83BD9071110af24D24445719A07F")
            }
            (PredictEnvironment::BnbTestnet, Self::Standard | Self::NegRisk) => {
                address!("2827AAef52D71910E8FBad2FfeBC1B6C2DA37743")
            }
            (PredictEnvironment::BnbTestnet, Self::YieldBearing) => {
                address!("38BF1cbD66d174bb5F3037d7068E708861D68D7f")
            }
            (PredictEnvironment::BnbTestnet, Self::YieldBearingNegRisk) => {
                address!("26e865CbaAe99b62fbF9D18B55c25B5E079A93D5")
            }
        }
    }

    pub(crate) const fn cache_index(self) -> usize {
        match self {
            Self::Standard => 0,
            Self::NegRisk => 1,
            Self::YieldBearing => 2,
            Self::YieldBearingNegRisk => 3,
        }
    }
}
