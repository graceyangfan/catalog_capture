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

//! Private wallet event DTOs from the official Predict WebSocket documentation.

use alloy::primitives::{Address, B256, U256};
use nautilus_core::UnixNanos;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, de::Error as _};

use super::error::{PredictWalletWsError, Result};
use crate::common::{
    enums::{PredictFeeType, PredictOrderStrategy, PredictQuoteType},
    serde::decimal_u256,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PredictWalletOutcome {
    Yes,
    No,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PredictWalletTransactionKind {
    Sale,
    Purchase,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PredictWalletRejectionReason {
    NoMarketMatch,
    RejectedDuplicate,
    RejectedPostOnly,
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PredictWalletCancellationReason {
    SelfTradePrevention,
    #[serde(other)]
    Unknown,
}

/// Per-order human-readable quantities and prices; this is not the aggregate match price.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictWalletOrderDetails {
    pub market_id: u64,
    pub outcome_index: u8,
    pub market_question: String,
    pub outcome: PredictWalletOutcome,
    #[serde(deserialize_with = "wallet_quote_type")]
    pub quote_type: PredictQuoteType,
    #[serde(deserialize_with = "decimal_string")]
    pub quantity: Decimal,
    #[serde(deserialize_with = "decimal_string")]
    pub quantity_filled: Decimal,
    #[serde(deserialize_with = "decimal_string")]
    pub price: Decimal,
    #[serde(deserialize_with = "decimal_string")]
    pub value: Decimal,
    #[serde(deserialize_with = "decimal_string")]
    pub value_filled: Decimal,
    pub strategy_type: PredictOrderStrategy,
    pub category_slug: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictWalletOrder {
    /// Venue decimal ID remains distinct from the exchange hash.
    pub order_id: String,
    pub order_hash: B256,
    pub wallet_address: Address,
    pub timestamp: u64,
    pub details: PredictWalletOrderDetails,
}

impl PredictWalletOrder {
    /// Converts the venue's epoch milliseconds with overflow validation.
    ///
    /// # Errors
    ///
    /// Returns an error when the timestamp cannot be represented in nanoseconds.
    pub fn timestamp_ns(&self) -> Result<UnixNanos> {
        self.timestamp
            .checked_mul(1_000_000)
            .map(UnixNanos::from)
            .ok_or(PredictWalletWsError::Protocol)
    }
}

/// Integer wei facts for one order's settlement; precision remains full U256.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictWalletFill {
    #[serde(with = "decimal_u256")]
    pub executed_price_wei: U256,
    #[serde(with = "decimal_u256")]
    pub executed_size_wei: U256,
    #[serde(with = "decimal_u256")]
    pub executed_value_wei: U256,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictWalletFee {
    #[serde(with = "decimal_u256")]
    pub amount_wei: U256,
    #[serde(rename = "type")]
    pub fee_type: PredictFeeType,
}

/// Submission is pending settlement, success is confirmed, failure does not confirm a fill.
/// Missing maker/fee fields do not imply taker liquidity or zero commission.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum PredictWalletEventKind {
    OrderAccepted,
    OrderNotAccepted {
        reason: PredictWalletRejectionReason,
    },
    OrderExpired,
    OrderCancelled {
        reason: Option<PredictWalletCancellationReason>,
    },
    OrderTransactionSubmitted {
        kind: PredictWalletTransactionKind,
        settlement_id: String,
        fill: PredictWalletFill,
    },
    /// Venue match planning precedes settlement; it confirms neither a fill nor cancellation.
    OrderMarketMatchPlan {
        #[serde(with = "decimal_u256")]
        amount: U256,
        #[serde(with = "decimal_u256")]
        amount_filled: U256,
        #[serde(with = "decimal_u256")]
        planned_fill: U256,
        #[serde(with = "decimal_u256")]
        remainder_cancelled: U256,
        settlement_ids: Vec<String>,
    },
    OrderTransactionSuccess {
        settlement_id: String,
        fill: PredictWalletFill,
        is_maker: Option<bool>,
        fee: Option<PredictWalletFee>,
    },
    OrderTransactionFailed {
        settlement_id: String,
        fill: PredictWalletFill,
    },
    #[serde(other)]
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct PredictWalletEvent {
    #[serde(flatten)]
    pub order: PredictWalletOrder,
    #[serde(flatten)]
    pub kind: PredictWalletEventKind,
}

/// Delivers subscription control and venue facts without publishing execution events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PredictWalletStreamMessage {
    /// Every subscription needs REST reconciliation: the wallet topic provides no snapshot.
    Subscribed { connection_epoch: u64 },
    Wallet {
        connection_epoch: u64,
        event: Box<PredictWalletEvent>,
    },
}

impl PredictWalletStreamMessage {
    pub(crate) fn connection_epoch(&self) -> u64 {
        match self {
            Self::Subscribed { connection_epoch }
            | Self::Wallet {
                connection_epoch, ..
            } => *connection_epoch,
        }
    }
}

fn decimal_string<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Decimal, D::Error> {
    let value = String::deserialize(deserializer)?;
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.')
    {
        return Err(D::Error::custom(
            "wallet decimal requires an unsigned decimal string",
        ));
    }
    Decimal::from_str_exact(&value)
        .map_err(|_| D::Error::custom("wallet decimal exceeds precision or range"))
}

fn wallet_quote_type<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<PredictQuoteType, D::Error> {
    match String::deserialize(deserializer)?.as_str() {
        "ASK" => Ok(PredictQuoteType::Ask),
        "BID" => Ok(PredictQuoteType::Bid),
        _ => Err(D::Error::custom("wallet quote type requires ASK or BID")),
    }
}
