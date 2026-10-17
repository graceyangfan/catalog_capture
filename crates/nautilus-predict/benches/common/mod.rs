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

//! Frozen pre-cache reference for paired offline benchmarks. Never used by production.

use alloy::{
    primitives::{Address, address, eip191_hash_message},
    signers::{SignerSync, local::PrivateKeySigner},
    sol,
    sol_types::{SolStruct, eip712_domain},
};
use nautilus_core::string::secret::SecretString;
use nautilus_predict::{
    PredictEnvironment,
    http::error::{Error, Result},
};

sol! {
    struct Kernel {
        bytes32 hash;
    }
}

pub(crate) struct ReferenceAuthSigner {
    signer: PrivateKeySigner,
    environment: PredictEnvironment,
    account: Option<Address>,
}

impl ReferenceAuthSigner {
    pub(crate) fn new(key: &str, environment: PredictEnvironment, account: Option<&str>) -> Self {
        Self {
            signer: key.parse().unwrap(),
            environment,
            account: account.map(|address| address.parse().unwrap()),
        }
    }

    pub(crate) fn sign_auth_message(&self, message: &str) -> Result<SecretString> {
        let signature = match self.account {
            None => self
                .signer
                .sign_message_sync(message.as_bytes())
                .map_err(|_| Error::Signing)?
                .to_string(),
            Some(account) => {
                let domain = eip712_domain! {
                    name: "Kernel",
                    version: "0.3.1",
                    chain_id: self.environment.chain_id(),
                    verifying_contract: account,
                };
                let digest = Kernel {
                    hash: eip191_hash_message(message),
                }
                .eip712_signing_hash(&domain);
                let signature = self
                    .signer
                    .sign_message_sync(digest.as_slice())
                    .map_err(|_| Error::Signing)?;
                let validator = address!("845ADb2C711129d4f3966735eD98a9F09fC4cE57");
                format!(
                    "0x01{}{}",
                    validator.to_string().trim_start_matches("0x"),
                    signature.to_string().trim_start_matches("0x")
                )
                .to_lowercase()
            }
        };
        Ok(signature.into())
    }
}
