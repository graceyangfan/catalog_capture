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

//! Private REST wire models from dev.predict.fun, snapshot 2026-10-13.
//! Account identity and points are deliberately distinct from trading balances.

use alloy::primitives::Address;
use nautilus_core::string::secret::SecretString;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// Signed challenge submitted to the JWT authentication endpoint.
#[derive(Debug, Serialize)]
pub struct PredictAuthRequest {
    /// EOA or Predict Account deposit address whose identity is authenticated.
    pub signer: Address,
    /// Exact challenge text returned by the venue.
    pub message: String,
    /// Wire hex signature, redacted from debug output.
    pub signature: SecretString,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PredictAuthMessage {
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PredictAuthTokenData {
    pub token: SecretString,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PredictPrivateResponse<'a> {
    pub success: bool,
    #[serde(borrow)]
    pub data: Option<&'a RawValue>,
    #[serde(borrow, default, deserialize_with = "deserialize_cursor")]
    pub cursor: Option<&'a RawValue>,
}

// Preserve the distinction between a missing cursor and an explicit JSON null. Borrowing
// its raw value also lets a failed envelope be classified before success fields are decoded.
fn deserialize_cursor<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(deserializer).map(Some)
}

/// Connected account identity and non-cash metadata from `/v1/account`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictAccount {
    /// Account display name.
    pub name: String,
    /// Wallet identity associated with the JWT.
    pub address: Address,
    /// Optional account image URL.
    pub image_url: Option<String>,
    /// Account referral metadata.
    pub referral: PredictAccountReferral,
    /// Program points, with no trading-balance semantics.
    pub points: PredictAccountPoints,
}

/// Referral program metadata returned by the venue.
#[derive(Debug, Deserialize)]
pub struct PredictAccountReferral {
    /// Referral code when available.
    pub code: Option<String>,
    /// Wire status of the referral program.
    pub status: PredictReferralStatus,
}

/// Venue referral program status.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PredictReferralStatus {
    /// Referral program is locked.
    Locked,
    /// Referral program is unlocked.
    Unlocked,
}

/// Non-cash program points, decoded as a decimal.
#[derive(Debug, Deserialize)]
pub struct PredictAccountPoints {
    /// Total program points.
    pub total: Decimal,
}

/// REST orderbook admission projection; unlike the WebSocket payload it has no orderCount.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictExecutionOrderbook {
    pub market_id: u64,
    pub update_timestamp_ms: u64,
    pub asks: Vec<(Decimal, Decimal)>,
    pub bids: Vec<(Decimal, Decimal)>,
}
