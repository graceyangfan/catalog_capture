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

//! Canonical signed-order payloads. Integer protocol values retain full uint256 precision.

use alloy::primitives::{Address, B256, U256};
use nautilus_core::string::secret::SecretString;
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};

use super::error::{Error, Result};
use crate::common::{
    contracts::PredictExchangeType,
    enums::{PredictOrderSide, PredictSignatureType},
    environment::PredictEnvironment,
    serde::decimal_u256,
};

/// Unsigned canonical order with explicit salt, amounts and expiration. This is a signing
/// primitive; venue admission checks and Nautilus price/quantity conversion belong to execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictOrder {
    /// Caller-generated salt; encoded on the wire as a decimal string.
    #[serde(with = "decimal_u256")]
    pub salt: U256,
    /// Wallet or deposit account providing the assets.
    pub maker: Address,
    /// API signer identity, equal to maker for supported wallet modes.
    pub signer: Address,
    /// Zero address denotes a public order.
    pub taker: Address,
    /// Full-width ERC-1155 outcome token ID.
    #[serde(with = "decimal_u256")]
    pub token_id: U256,
    /// Maker asset amount in integer wei.
    #[serde(with = "decimal_u256")]
    pub maker_amount: U256,
    /// Taker asset amount in integer wei.
    #[serde(with = "decimal_u256")]
    pub taker_amount: U256,
    /// Explicit Unix expiration in seconds; no clock or default is applied by the signer.
    #[serde(
        serialize_with = "decimal_u256::serialize",
        deserialize_with = "deserialize_expiration"
    )]
    pub expiration: U256,
    /// On-chain cancellation nonce supplied by the caller.
    #[serde(with = "decimal_u256")]
    pub nonce: U256,
    /// Market fee rate in basis points supplied by the caller.
    #[serde(with = "decimal_u256")]
    pub fee_rate_bps: U256,
    /// Numeric venue side.
    pub side: PredictOrderSide,
    /// Numeric signature mode, including EIP-1271 under value zero.
    pub signature_type: PredictSignatureType,
}

/// Immutable signed payload. Context metadata is retained for later submission checks and
/// excluded from the canonical SDK JSON. Use [`Self::contract_order`] for the REST format,
/// which requires an int64 expiration. Only the order signer constructs this type.
#[derive(Clone, Debug, Serialize)]
pub struct PredictSignedOrder {
    #[serde(flatten)]
    order: PredictOrder,
    hash: B256,
    signature: SecretString,
    #[serde(skip)]
    environment: PredictEnvironment,
    #[serde(skip)]
    exchange: PredictExchangeType,
}

impl PredictSignedOrder {
    /// Returns the immutable order covered by the signature.
    #[must_use]
    pub fn order(&self) -> &PredictOrder {
        &self.order
    }
    /// Returns the exchange EIP-712 order hash, before Kernel account wrapping.
    #[must_use]
    pub fn hash(&self) -> B256 {
        self.hash
    }
    /// Returns the wire signature with redacted debug output.
    #[must_use]
    pub fn signature(&self) -> &SecretString {
        &self.signature
    }
    /// Returns the environment used for signing.
    #[must_use]
    pub fn environment(&self) -> PredictEnvironment {
        self.environment
    }
    /// Returns the market-dependent exchange used for signing.
    #[must_use]
    pub fn exchange(&self) -> PredictExchangeType {
        self.exchange
    }

    /// Prepares the nested REST ContractOrder, preserving signed values while converting
    /// expiration to the int64 field required by the current OpenAPI schema.
    ///
    /// # Errors
    ///
    /// Returns an error if expiration cannot be represented as a nonnegative int64 timestamp.
    pub fn contract_order(&self) -> Result<PredictContractOrder<'_>> {
        let seconds: u64 = self
            .order
            .expiration
            .try_into()
            .map_err(|_| Error::BadRequest("expiration exceeds REST int64 range"))?;
        let expiration = i64::try_from(seconds)
            .map_err(|_| Error::BadRequest("expiration exceeds REST int64 range"))?;
        Ok(PredictContractOrder {
            signed: self,
            expiration,
        })
    }

    pub(crate) fn new(
        order: PredictOrder,
        hash: B256,
        signature: SecretString,
        environment: PredictEnvironment,
        exchange: PredictExchangeType,
    ) -> Self {
        Self {
            order,
            hash,
            signature,
            environment,
            exchange,
        }
    }
}

/// Borrowed nested REST order payload. The full CreateOrderRequest also requires strategy
/// and price metadata, supplied by the command request builder.
#[derive(Debug)]
pub struct PredictContractOrder<'a> {
    signed: &'a PredictSignedOrder,
    expiration: i64,
}

impl Serialize for PredictContractOrder<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let order = self.signed.order();
        let mut map = serializer.serialize_map(Some(14))?;
        map.serialize_entry("salt", &order.salt.to_string())?;
        map.serialize_entry("maker", &order.maker)?;
        map.serialize_entry("signer", &order.signer)?;
        map.serialize_entry("taker", &order.taker)?;
        map.serialize_entry("tokenId", &order.token_id.to_string())?;
        map.serialize_entry("makerAmount", &order.maker_amount.to_string())?;
        map.serialize_entry("takerAmount", &order.taker_amount.to_string())?;
        map.serialize_entry("expiration", &self.expiration)?;
        map.serialize_entry("nonce", &order.nonce.to_string())?;
        map.serialize_entry("feeRateBps", &order.fee_rate_bps.to_string())?;
        map.serialize_entry("side", &order.side)?;
        map.serialize_entry("signatureType", &order.signature_type)?;
        map.serialize_entry("hash", &self.signed.hash())?;
        map.serialize_entry("signature", self.signed.signature())?;
        map.end()
    }
}

fn deserialize_expiration<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<U256, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Expiration {
        Decimal(String),
        Seconds(u64),
    }
    match Expiration::deserialize(deserializer)? {
        Expiration::Decimal(value) => decimal_u256::parse(&value),
        Expiration::Seconds(seconds) => Ok(U256::from(seconds)),
    }
}
