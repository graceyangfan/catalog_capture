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

//! Read-only order, holding and match DTOs. These retain venue facts without implying
//! Nautilus execution events, order admission or settlement reconciliation.

use alloy::primitives::{Address, B256, U256};
use nautilus_core::string::secret::SecretString;
use rust_decimal::Decimal;
use serde::Deserialize;

use super::order_models::PredictOrder;
use crate::common::serde::decimal_u256;

pub use crate::common::enums::{PredictFeeType, PredictOrderStrategy, PredictQuoteType};

/// One venue page. The opaque cursor is passed unchanged to the next query.
/// A null cursor (or omitted optional positions cursor) ends pagination; an empty
/// data page may still have a next cursor.
#[derive(Debug)]
pub struct PredictPage<T> {
    pub data: Vec<T>,
    pub cursor: Option<String>,
}

/// Inbound signed contract fields, separate from a locally verified immutable signed order.
#[derive(Debug, Deserialize)]
pub struct PredictRestOrder {
    #[serde(flatten)]
    pub fields: PredictOrder,
    /// Optional in the endpoint schema; never replaced with the venue's decimal order ID.
    pub hash: Option<B256>,
    pub signature: SecretString,
}

/// Order lifecycle status returned by private REST endpoints.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PredictOrderStatus {
    Open,
    Filled,
    Expired,
    Cancelled,
    Invalidated,
}

/// Order state from `/v1/orders`. Amount strings are preserved as supplied by the venue;
/// endpoint schemas do not specify their units. No accepted/updated timestamp is provided.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictOrderData {
    pub order: PredictRestOrder,
    pub id: String,
    pub market_id: u64,
    pub currency: String,
    pub amount: String,
    pub amount_filled: String,
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
    pub strategy: PredictOrderStrategy,
    pub status: PredictOrderStatus,
    // Endpoint schema permits a number; the standalone schema narrows this to int32.
    pub reward_earning_rate: Decimal,
}

/// Market metadata embedded in account holdings and match history.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictAccountMarket {
    pub id: u64,
    pub is_neg_risk: bool,
    pub is_yield_bearing: bool,
    pub decimal_precision: u8,
    pub outcomes: Vec<PredictAccountOutcome>,
}

/// Outcome metadata embedded in an account-market projection.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictAccountOutcome {
    pub name: String,
    pub index_set: u32,
    #[serde(with = "decimal_u256")]
    pub on_chain_id: U256,
    pub status: Option<PredictOutcomeStatus>,
}

/// Resolved outcome result reported by the venue.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PredictOutcomeStatus {
    Won,
    Lost,
}

/// ERC-1155 holding with an integer share amount and unscaled monetary strings.
/// The endpoint supplies no update timestamp.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictPositionData {
    pub id: String,
    pub market: PredictAccountMarket,
    pub outcome: PredictAccountOutcome,
    #[serde(with = "decimal_u256")]
    pub amount: U256,
    pub value_usd: String,
    pub average_buy_price_usd: String,
    pub pnl_usd: String,
}

/// A fee in the asset identified by `fee_type`, not necessarily collateral currency.
#[derive(Debug, Deserialize)]
pub struct PredictMatchFee {
    /// Integer wei; kept at full precision before any Nautilus Money conversion.
    #[serde(with = "decimal_u256")]
    pub amount: U256,
    #[serde(rename = "type")]
    pub fee_type: PredictFeeType,
}

/// One participant in a match, retaining its own outcome, amount, price and fee.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictMatchLeg {
    pub quote_type: PredictQuoteType,
    pub amount: String,
    pub price: String,
    pub outcome: PredictAccountOutcome,
    pub signer: Address,
    /// Present in OrderFillData, omitted by the match endpoint's inline schema.
    /// Missing hashes prevent reliable FillReport association; no identifier is invented.
    pub hash: Option<B256>,
    pub fee: PredictMatchFee,
}

/// Match event, not one account fill. Multiple maker legs may belong to the same wallet;
/// aggregate price/amount cannot be substituted for an individual leg's values.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictMatchData {
    pub market: PredictAccountMarket,
    pub taker: PredictMatchLeg,
    pub amount_filled: String,
    pub price_executed: String,
    pub makers: Vec<PredictMatchLeg>,
    pub transaction_hash: B256,
    /// Added in the standalone schema; absent in the older endpoint schema.
    pub settlement_id: Option<String>,
    pub executed_at: String,
}
