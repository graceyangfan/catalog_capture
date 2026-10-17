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

//! EIP-712 order signing matching official SDK buildTypedData/signTypedDataOrder.
//! Domains are cached per environment and exchange; amounts, token IDs, nonce, salt and
//! expiration remain per-order inputs. No JSON representation participates in the digest.

use std::sync::Arc;

use alloy::{
    primitives::{Address, B256, Keccak256, U256, keccak256},
    sol,
    sol_types::{SolStruct, eip712_domain},
};

use super::PredictAuthSigner;
use crate::{
    common::contracts::PredictExchangeType,
    http::{
        error::{Error, Result},
        order_models::{PredictOrder, PredictSignedOrder},
    },
};

// Canonical type declaration is the source of truth for the cached EIP-712 type hash.
sol! {
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        address taker;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint256 expiration;
        uint256 nonce;
        uint256 feeRateBps;
        uint8 side;
        uint8 signatureType;
    }
}

/// Order signer sharing the parsed wallet and immutable Kernel context with authentication.
/// Construction does not verify on-chain ownership, balances, approvals, expiry or REST limits.
#[derive(Debug)]
pub struct PredictOrderSigner {
    wallet: Arc<PredictAuthSigner>,
    hasher: PredictOrderHasher,
}

/// Public EIP-712 hashing context; contains no wallet or signing authority.
#[derive(Clone, Debug)]
pub struct PredictOrderHasher {
    domains: [B256; 4],
    type_hash: B256,
}

impl PredictOrderHasher {
    /// Caches the public domains for an explicit chain environment.
    #[must_use]
    pub fn new(environment: crate::common::environment::PredictEnvironment) -> Self {
        let domains = [
            PredictExchangeType::Standard,
            PredictExchangeType::NegRisk,
            PredictExchangeType::YieldBearing,
            PredictExchangeType::YieldBearingNegRisk,
        ]
        .map(|exchange| {
            eip712_domain! {
                name: "predict.fun CTF Exchange",
                version: "1",
                chain_id: environment.chain_id(),
                verifying_contract: exchange.contract(environment),
            }
            .separator()
        });
        Self {
            domains,
            type_hash: keccak256(Order::eip712_root_type().as_bytes()),
        }
    }

    /// Returns the exchange EIP-712 order hash for the selected market type.
    /// This original hash is retained even when a Predict Account wraps it for signing.
    #[must_use]
    pub fn order_hash(&self, order: &PredictOrder, exchange: PredictExchangeType) -> B256 {
        // Static ABI words retain full uint256 values and left-pad addresses. Streaming the
        // words avoids JSON materialization and a heap allocation for ABI concatenation.
        let words = [
            order.salt.to_be_bytes::<32>(),
            address_word(order.maker),
            address_word(order.signer),
            address_word(order.taker),
            order.token_id.to_be_bytes(),
            order.maker_amount.to_be_bytes(),
            order.taker_amount.to_be_bytes(),
            order.expiration.to_be_bytes(),
            order.nonce.to_be_bytes(),
            order.fee_rate_bps.to_be_bytes(),
            U256::from(u8::from(order.side)).to_be_bytes(),
            U256::from(u8::from(order.signature_type)).to_be_bytes(),
        ];
        let mut hasher = Keccak256::new();
        hasher.update(self.type_hash);
        for word in words {
            hasher.update(word);
        }
        let struct_hash = hasher.finalize();
        let mut digest = [0u8; 66];
        digest[..2].copy_from_slice(&[0x19, 0x01]);
        digest[2..34].copy_from_slice(self.domains[exchange.cache_index()].as_slice());
        digest[34..].copy_from_slice(struct_hash.as_slice());
        keccak256(digest)
    }
}

impl PredictOrderSigner {
    /// Shares wallet identity while hashing remains a public, read-only operation.
    #[must_use]
    pub fn new(wallet: Arc<PredictAuthSigner>) -> Self {
        let hasher = PredictOrderHasher::new(wallet.environment());
        Self { wallet, hasher }
    }

    /// Returns the original exchange hash, before any Predict Account signature wrapping.
    #[must_use]
    pub fn order_hash(&self, order: &PredictOrder, exchange: PredictExchangeType) -> B256 {
        self.hasher.order_hash(order, exchange)
    }

    /// Validates wallet binding and signs an order for the selected exchange.
    ///
    /// # Errors
    ///
    /// Returns an error if maker/signer differs from the wallet identity, taker is nonzero,
    /// or wallet signing fails. Numeric admission checks belong to the execution layer.
    pub fn sign_order(
        &self,
        order: &PredictOrder,
        exchange: PredictExchangeType,
    ) -> Result<PredictSignedOrder> {
        let expected = self.wallet.account_address();
        if order.maker != expected || order.signer != expected {
            return Err(Error::BadRequest(
                "order maker and signer must match the wallet identity",
            ));
        }
        if !order.taker.is_zero() {
            return Err(Error::BadRequest(
                "Predict REST supports only public orders with a zero taker",
            ));
        }
        let hash = self.order_hash(order, exchange);
        let signature = self.wallet.sign_order_hash(hash)?;
        Ok(PredictSignedOrder::new(
            *order,
            hash,
            signature,
            self.wallet.environment(),
            exchange,
        ))
    }
}

fn address_word(address: Address) -> [u8; 32] {
    let mut word = [0u8; 32];
    word[12..].copy_from_slice(address.as_slice());
    word
}
