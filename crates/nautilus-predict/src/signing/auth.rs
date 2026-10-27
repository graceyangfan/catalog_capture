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

//! Authentication signing matching official SDK `signPredictAccountMessage`.
//!
//! Reference: PredictDotFun/sdk 1.3.8 (5ff2b4a), OrderBuilder.ts and internal/Utils.ts.
//! EOA authentication uses EIP-191. Predict Accounts hash the text with EIP-191, wrap it
//! in the Kernel EIP-712 domain, then EIP-191 sign the *32 digest bytes*, not their hex text.
//! The account signature is `0x01 || ECDSA_VALIDATOR || signature` (86 bytes).

use std::{fmt, str::FromStr};

use alloy::{
    primitives::{Address, B256, address, eip191_hash_message, hex, keccak256},
    signers::{SignerSync, local::PrivateKeySigner},
    sol,
    sol_types::{SolStruct, eip712_domain},
};
use nautilus_core::string::secret::SecretString;

use crate::{
    common::environment::PredictEnvironment,
    http::error::{Error, Result},
};

const ECDSA_VALIDATOR: Address = address!("845ADb2C711129d4f3966735eD98a9F09fC4cE57");

sol! {
    struct Kernel {
        bytes32 hash;
    }
}

/// Local wallet signer. A Predict Account uses its deposit address as the API signer,
/// while the exported Privy wallet key signs the Kernel digest.
/// Construction is offline; it does not check the account's validator ownership on chain.
/// The venue verifies the signature when exchanging it for a JWT.
///
/// The private key is consumed as `SecretString` and erased when parsing finishes. Alloy
/// retains the signing scalar in its k256 key, which zeroizes its secret material on drop.
pub struct PredictAuthSigner {
    signer: PrivateKeySigner,
    environment: PredictEnvironment,
    predict_account: Option<Address>,
    kernel: Option<KernelSigningContext>,
}

impl PredictAuthSigner {
    /// Creates a wallet signer and precomputes its immutable Kernel context, when applicable.
    ///
    /// # Errors
    ///
    /// Returns an error if the private key or Predict Account address is invalid.
    pub fn new(
        private_key: SecretString,
        environment: PredictEnvironment,
        predict_account: Option<&str>,
    ) -> Result<Self> {
        let key = private_key.expose_secret();
        let key = key.strip_prefix("0x").unwrap_or(key);
        if key.len() != 64 || !key.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(Error::BadRequest("private key must be 32 bytes of hex"));
        }
        let signer = PrivateKeySigner::from_str(key)
            .map_err(|_| Error::BadRequest("invalid secp256k1 private key"))?;
        let predict_account = predict_account
            .map(|value| {
                Address::from_str(value)
                    .map_err(|_| Error::BadRequest("invalid Predict Account address"))
                    .and_then(|address| {
                        if address.is_zero() {
                            Err(Error::BadRequest("Predict Account address cannot be zero"))
                        } else {
                            Ok(address)
                        }
                    })
            })
            .transpose()?;
        Ok(Self {
            signer,
            environment,
            predict_account,
            kernel: predict_account.map(|account| KernelSigningContext::new(environment, account)),
        })
    }

    /// Returns the owner wallet address derived from the signing key.
    #[must_use]
    pub fn owner_address(&self) -> Address {
        self.signer.address()
    }

    /// Returns the API signer identity: the deposit address or the EOA owner address.
    #[must_use]
    pub fn account_address(&self) -> Address {
        self.predict_account.unwrap_or_else(|| self.owner_address())
    }

    /// Returns the environment bound to this signer and its cached domain.
    #[must_use]
    pub fn environment(&self) -> PredictEnvironment {
        self.environment
    }

    /// Signs the exact dynamic message returned by `/v1/auth/message`, without trimming it.
    ///
    /// # Errors
    ///
    /// Returns an error if the wallet cannot sign the message.
    pub fn sign_auth_message(&self, message: &str) -> Result<SecretString> {
        let signature = match &self.kernel {
            None => hex::encode_prefixed(
                self.signer
                    .sign_message_sync(message.as_bytes())
                    .map_err(|_| Error::Signing)?
                    .as_bytes(),
            ),
            Some(kernel) => {
                return self.sign_kernel_hash(kernel, eip191_hash_message(message));
            }
        };
        Ok(signature.into())
    }

    /// Order hashes are raw EIP-712 digests. EOA signs directly; Predict Account wraps the
    /// raw bytes without the authentication text's inner EIP-191 hash.
    pub(crate) fn sign_order_hash(&self, hash: B256) -> Result<SecretString> {
        match &self.kernel {
            None => self
                .signer
                .sign_hash_sync(&hash)
                .map(|signature| hex::encode_prefixed(signature.as_bytes()).into())
                .map_err(|_| Error::Signing),
            Some(kernel) => self.sign_kernel_hash(kernel, hash),
        }
    }

    fn sign_kernel_hash(&self, kernel: &KernelSigningContext, hash: B256) -> Result<SecretString> {
        let digest = kernel.wrap_hash(hash);
        let signature = self
            .signer
            .sign_message_sync(digest.as_slice())
            .map_err(|_| Error::Signing)?;
        let mut bytes = [0u8; 86];
        bytes[0] = 1;
        bytes[1..21].copy_from_slice(ECDSA_VALIDATOR.as_slice());
        bytes[21..].copy_from_slice(&signature.as_bytes());
        Ok(hex::encode_prefixed(bytes).into())
    }
}

impl fmt::Debug for PredictAuthSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PredictAuthSigner")
            .field("owner_address", &self.owner_address())
            .field("environment", &self.environment)
            .field("predict_account", &self.predict_account)
            .finish_non_exhaustive()
    }
}

/// Account-bound static hashes. Message hashes remain request-scoped; scratch buffers are
/// stack-local so concurrent signing never shares mutable state or takes a buffer lock.
struct KernelSigningContext {
    domain_separator: B256,
    type_hash: B256,
}

impl KernelSigningContext {
    fn new(environment: PredictEnvironment, account: Address) -> Self {
        let domain = eip712_domain! {
            name: "Kernel",
            version: "0.3.1",
            chain_id: environment.chain_id(),
            verifying_contract: account,
        };
        Self {
            domain_separator: domain.separator(),
            type_hash: Kernel { hash: B256::ZERO }.eip712_type_hash(),
        }
    }

    fn wrap_hash(&self, message_hash: B256) -> B256 {
        // Kernel has exactly one bytes32 field: ABI encoding is two consecutive words.
        let mut encoded = [0u8; 64];
        encoded[..32].copy_from_slice(self.type_hash.as_slice());
        encoded[32..].copy_from_slice(message_hash.as_slice());
        let struct_hash = keccak256(encoded);
        let mut digest = [0u8; 66];
        digest[..2].copy_from_slice(&[0x19, 0x01]);
        digest[2..34].copy_from_slice(self.domain_separator.as_slice());
        digest[34..].copy_from_slice(struct_hash.as_slice());
        keccak256(digest)
    }
}
