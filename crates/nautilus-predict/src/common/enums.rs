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

//! Numeric wire enums for Predict orders.

use serde::{Deserialize, Serialize};

use crate::http::error::{Error, Result};

/// Venue order side, distinct from Nautilus domain enums.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum PredictOrderSide {
    /// Buy outcome tokens, encoded as zero.
    Buy,
    /// Sell outcome tokens, encoded as one.
    Sell,
}

impl From<PredictOrderSide> for u8 {
    fn from(side: PredictOrderSide) -> Self {
        match side {
            PredictOrderSide::Buy => 0,
            PredictOrderSide::Sell => 1,
        }
    }
}

impl TryFrom<u8> for PredictOrderSide {
    type Error = Error;
    fn try_from(side: u8) -> Result<Self> {
        match side {
            0 => Ok(Self::Buy),
            1 => Ok(Self::Sell),
            _ => Err(Error::BadRequest("unsupported Predict order side")),
        }
    }
}

/// REST signature type zero covers both EOA and EIP-1271 Predict Account orders.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum PredictSignatureType {
    /// EOA or Predict Account contract signature, encoded as zero.
    Eoa,
}

impl From<PredictSignatureType> for u8 {
    fn from(_: PredictSignatureType) -> Self {
        0
    }
}

impl TryFrom<u8> for PredictSignatureType {
    type Error = Error;
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Eoa),
            _ => Err(Error::BadRequest("unsupported Predict signature type")),
        }
    }
}
