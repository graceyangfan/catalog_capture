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

//! Write DTOs preserve signed values; receipts are transport evidence, not fills.

use std::collections::HashSet;

use alloy::primitives::{B256, U256};
use serde::{Deserialize, Serialize};

use super::{
    error::{Error, PredictDecodeErrorKind, Result},
    order_models::{PredictContractOrder, PredictSignedOrder},
};
use crate::common::{
    enums::PredictOrderSide, market::PredictMarketOrderAmounts, serde::decimal_u256,
};

/// Borrowed LIMIT or quantity-based MARKET request. Numeric values are exact SDK wei.
/// Market admission, approvals, fee policy and nonce verification remain the caller's responsibility.
#[derive(Debug, Serialize)]
pub struct PredictCreateOrderRequest<'a> {
    data: PredictCreateOrderData<'a>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PredictCreateOrderData<'a> {
    price_per_share: String,
    strategy: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_post_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slippage_bps: Option<u32>,
    order: PredictContractOrder<'a>,
}

impl<'a> PredictCreateOrderRequest<'a> {
    /// Constructs an exact LIMIT request without changing any signed amount.
    ///
    /// # Errors
    /// Zero amounts, prices outside (0, 1], inexact signed price/quantity and overflow are rejected.
    pub fn limit(
        signed: &'a PredictSignedOrder,
        price_per_share_wei: U256,
        is_post_only: bool,
    ) -> Result<Self> {
        let scale = U256::from(1_000_000_000_000_000_000u64);
        let order = signed.order();
        let (shares, collateral) = match order.side {
            PredictOrderSide::Buy => (order.taker_amount, order.maker_amount),
            PredictOrderSide::Sell => (order.maker_amount, order.taker_amount),
        };
        if price_per_share_wei.is_zero()
            || price_per_share_wei > scale
            || shares.is_zero()
            || collateral.is_zero()
        {
            return Err(Error::BadRequest(
                "LIMIT price and amounts are outside the supported range",
            ));
        }
        let product = price_per_share_wei
            .checked_mul(shares)
            .ok_or(Error::BadRequest("LIMIT amount multiplication overflow"))?;
        if product % scale != U256::ZERO || product / scale != collateral {
            return Err(Error::BadRequest(
                "LIMIT price differs from exact signed amounts",
            ));
        }
        Ok(Self {
            data: PredictCreateOrderData {
                price_per_share: price_per_share_wei.to_string(),
                strategy: "LIMIT",
                is_post_only: Some(is_post_only),
                slippage_bps: None,
                order: signed.contract_order()?,
            },
        })
    }

    /// Builds the SDK's quantity-based MARKET envelope with default (non minimum-out)
    /// slippage. The signing primitive does not establish inventory, balance or exit readiness.
    ///
    /// # Errors
    /// Signed side/amounts differing from prepared SDK terms, invalid average prices or
    /// unsupported expiration are rejected without changing the signed payload.
    pub fn market(
        signed: &'a PredictSignedOrder,
        amounts: &PredictMarketOrderAmounts,
    ) -> Result<Self> {
        amounts
            .validate()
            .map_err(|_| Error::BadRequest("invalid MARKET amount metadata"))?;
        let scale = U256::from(1_000_000_000_000_000_000u64);
        let order = signed.order();
        let shares = match amounts.side {
            PredictOrderSide::Buy => amounts.taker_amount,
            PredictOrderSide::Sell => amounts.maker_amount,
        };
        if order.side != amounts.side
            || order.maker_amount != amounts.maker_amount
            || order.taker_amount != amounts.taker_amount
            || shares != amounts.amount
            || amounts.amount < scale / U256::from(100)
            || amounts.price_per_share.is_zero()
            || amounts.price_per_share > scale
            || amounts.last_price.is_zero()
            || amounts.last_price > scale
            || amounts.maker_amount.is_zero()
            || amounts.taker_amount.is_zero()
            || amounts.slippage_bps >= 10_000
            || order.expiration.is_zero()
        {
            return Err(Error::BadRequest(
                "MARKET metadata differs from signed SDK amounts",
            ));
        }
        Ok(Self {
            data: PredictCreateOrderData {
                price_per_share: amounts.price_per_share.to_string(),
                strategy: "MARKET",
                is_post_only: None,
                slippage_bps: Some(amounts.slippage_bps),
                order: signed.contract_order()?,
            },
        })
    }
}

/// POST /v1/orders acknowledgement. The opaque code is not interpreted as an order status.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictCreateOrderReceipt {
    pub code: String,
    #[serde(with = "decimal_u256")]
    pub order_id: U256,
    pub order_hash: B256,
    /// Venue removal lock metadata; absence does not establish immediate removability.
    pub removal_locked_until: Option<String>,
}

/// Exact partition returned by /v1/orders/remove, separate from the standard data envelope.
/// `noop` includes filled, previously removed and cancelled orders; it is not cancellation proof.
#[derive(Debug, Deserialize)]
pub struct PredictRemoveOrdersReceipt {
    pub success: bool,
    pub removed: Vec<String>,
    pub noop: Vec<String>,
}

impl PredictRemoveOrdersReceipt {
    pub(crate) fn validate(&self, endpoint: &'static str, ids: &[U256]) -> Result<()> {
        if !self.success {
            if !self.removed.is_empty() {
                return Err(Error::decode(
                    endpoint,
                    PredictDecodeErrorKind::InvalidAcknowledgement,
                ));
            }
            return Err(Error::Venue { endpoint });
        }
        let expected: HashSet<_> = ids.iter().copied().collect();
        let mut seen = HashSet::new();
        for value in self.removed.iter().chain(&self.noop) {
            let id = decimal_u256::parse::<serde::de::value::Error>(value).map_err(|_| {
                Error::decode(endpoint, PredictDecodeErrorKind::InvalidAcknowledgement)
            })?;
            if !expected.contains(&id) || !seen.insert(id) || value != &id.to_string() {
                return Err(Error::decode(
                    endpoint,
                    PredictDecodeErrorKind::InvalidAcknowledgement,
                ));
            }
        }
        if seen != expected {
            return Err(Error::decode(
                endpoint,
                PredictDecodeErrorKind::InvalidAcknowledgement,
            ));
        }
        Ok(())
    }
}
