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

#![cfg(feature = "execution")]

use alloy::primitives::{Address, Signature};
use nautilus_core::string::secret::SecretString;
use nautilus_predict::{
    PredictEnvironment, common::credential::PredictAuthToken,
    http::execution_models::PredictAuthRequest, signing::PredictAuthSigner,
};
use serde::Deserialize;

// Hardhat public development key #0. Never load credentials from env in these tests.
const PUBLIC_TEST_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

#[derive(Deserialize)]
struct Vectors {
    vectors: Vec<Vector>,
}

#[derive(Deserialize)]
struct Vector {
    chain_id: u64,
    predict_account: Option<String>,
    message: String,
    signer: String,
    signature: String,
}

#[test]
fn auth_signatures_match_official_sdk_vectors_byte_for_byte() {
    let fixtures: Vectors =
        serde_json::from_str(include_str!("fixtures/auth_vectors.json")).unwrap();
    assert_eq!(fixtures.vectors.len(), 16);
    for vector in fixtures.vectors {
        let chain = match vector.chain_id {
            56 => PredictEnvironment::BnbMainnet,
            97 => PredictEnvironment::BnbTestnet,
            _ => panic!("unexpected SDK chain"),
        };
        let signer = PredictAuthSigner::new(
            PUBLIC_TEST_KEY.into(),
            chain,
            vector.predict_account.as_deref(),
        )
        .unwrap();
        let signature = signer.sign_auth_message(&vector.message).unwrap();
        assert_eq!(
            signer.account_address(),
            vector.signer.parse::<Address>().unwrap()
        );
        assert_eq!(
            signature.expose_secret(),
            vector.signature,
            "chain={}, account={:?}, message={:?}",
            vector.chain_id,
            vector.predict_account,
            vector.message
        );
        let signature_bytes = (vector.signature.len() - 2) / 2;
        assert_eq!(
            signature_bytes,
            if vector.predict_account.is_some() {
                86
            } else {
                65
            }
        );
        if vector.predict_account.is_none() {
            let signature: Signature = vector.signature.parse().unwrap();
            assert_eq!(
                signature.recover_address_from_msg(vector.message).unwrap(),
                signer.owner_address()
            );
        }
    }
}

#[test]
fn reused_signers_preserve_sdk_results_when_messages_and_contexts_alternate() {
    let fixtures: Vectors =
        serde_json::from_str(include_str!("fixtures/auth_vectors.json")).unwrap();
    for vectors in fixtures.vectors.as_chunks::<4>().0 {
        let chain = if vectors[0].chain_id == 56 {
            PredictEnvironment::BnbMainnet
        } else {
            PredictEnvironment::BnbTestnet
        };
        let signer = PredictAuthSigner::new(
            PUBLIC_TEST_KEY.into(),
            chain,
            vectors[0].predict_account.as_deref(),
        )
        .unwrap();
        for index in [0, 1, 2, 3, 1, 0] {
            assert_eq!(
                signer
                    .sign_auth_message(&vectors[index].message)
                    .unwrap()
                    .expose_secret(),
                vectors[index].signature
            );
        }
    }
    let first = PredictAuthSigner::new(
        PUBLIC_TEST_KEY.into(),
        PredictEnvironment::BnbMainnet,
        Some("0x1111111111111111111111111111111111111111"),
    )
    .unwrap();
    let second = PredictAuthSigner::new(
        PUBLIC_TEST_KEY.into(),
        PredictEnvironment::BnbMainnet,
        Some("0x2222222222222222222222222222222222222222"),
    )
    .unwrap();
    let expected = first.sign_auth_message("same dynamic challenge").unwrap();
    assert_ne!(
        expected,
        second.sign_auth_message("same dynamic challenge").unwrap()
    );
    assert_eq!(
        expected,
        first.sign_auth_message("same dynamic challenge").unwrap()
    );
}

#[test]
fn signer_and_credentials_redact_secrets() {
    let signer =
        PredictAuthSigner::new(PUBLIC_TEST_KEY.into(), PredictEnvironment::BnbMainnet, None)
            .unwrap();
    let request = PredictAuthRequest {
        signer: signer.account_address(),
        message: "dynamic challenge".into(),
        signature: signer.sign_auth_message("dynamic challenge").unwrap(),
    };
    let token = PredictAuthToken::new("jwt-secret-sentinel".into()).unwrap();
    assert!(!format!("{signer:?}").contains(PUBLIC_TEST_KEY.trim_start_matches("0x")));
    assert!(!format!("{request:?}").contains(request.signature.expose_secret()));
    assert!(!format!("{token:?}").contains(token.expose_secret()));
}

#[test]
fn invalid_keys_addresses_and_header_credentials_are_rejected() {
    for key in ["", "00", "not-a-key", &"0".repeat(64), &"f".repeat(64)] {
        let error =
            PredictAuthSigner::new(key.into(), PredictEnvironment::BnbMainnet, None).unwrap_err();
        assert!(!format!("{error:?}").contains("not-a-key"));
    }
    for address in [
        "invalid-address",
        "0x0000000000000000000000000000000000000000",
    ] {
        assert!(
            PredictAuthSigner::new(
                PUBLIC_TEST_KEY.into(),
                PredictEnvironment::BnbMainnet,
                Some(address)
            )
            .is_err()
        );
    }
    for token in [
        "",
        " ",
        "jwt\r\ninjected: header",
        "jwt with spaces",
        "非ascii",
    ] {
        assert!(PredictAuthToken::new(SecretString::from(token)).is_err());
    }
}
