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

//! Explicit environment identity shared by private transport and wallet signing.

/// Supported environments from the official SDK and REST documentation.
/// URL overrides change routing while preserving this identity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PredictEnvironment {
    /// BNB mainnet, with an API key required for private REST requests.
    #[default]
    BnbMainnet,
    /// BNB testnet, where an API key is optional for private REST requests.
    BnbTestnet,
}

impl PredictEnvironment {
    /// Returns the chain ID used in the EIP-712 domain.
    #[must_use]
    pub const fn chain_id(self) -> u64 {
        match self {
            Self::BnbMainnet => 56,
            Self::BnbTestnet => 97,
        }
    }

    /// Returns the official REST origin.
    #[must_use]
    pub const fn http_url(self) -> &'static str {
        match self {
            Self::BnbMainnet => "https://api.predict.fun",
            Self::BnbTestnet => "https://api-testnet.predict.fun",
        }
    }
}
