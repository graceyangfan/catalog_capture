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

use std::{collections::HashSet, sync::Arc};

use alloy::primitives::{Address, B256, Signature, U256};
use nautilus_predict::{
    PredictEnvironment,
    common::contracts::PredictExchangeType,
    http::{error::Error, order_models::PredictOrder},
    signing::{PredictAuthSigner, PredictOrderSigner},
};
use serde::Deserialize;
use serde_json::{Value, json};

// Public Hardhat development key #0, never an operator credential.
const TEST_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

#[derive(Deserialize)]
struct Fixtures {
    vectors: Vec<Vector>,
}

#[derive(Deserialize)]
struct Vector {
    chain_id: u64,
    predict_account: Option<String>,
    is_neg_risk: bool,
    is_yield_bearing: bool,
    verifying_contract: Address,
    order: PredictOrder,
    hash: B256,
    signature: String,
}

fn fixtures() -> Vec<Vector> {
    serde_json::from_str::<Fixtures>(include_str!("fixtures/order_vectors.json"))
        .unwrap()
        .vectors
}

fn wallet(vector: &Vector) -> Arc<PredictAuthSigner> {
    let environment = match vector.chain_id {
        56 => PredictEnvironment::BnbMainnet,
        97 => PredictEnvironment::BnbTestnet,
        _ => panic!("unsupported fixture environment"),
    };
    Arc::new(
        PredictAuthSigner::new(
            TEST_KEY.into(),
            environment,
            vector.predict_account.as_deref(),
        )
        .unwrap(),
    )
}

#[test]
fn exchange_hash_signature_and_wire_payload_match_official_sdk() {
    let vectors = fixtures();
    assert_eq!(vectors.len(), 32);
    for vector in vectors {
        let wallet = wallet(&vector);
        let signer = PredictOrderSigner::new(Arc::clone(&wallet));
        let exchange =
            PredictExchangeType::from_market_flags(vector.is_neg_risk, vector.is_yield_bearing);
        assert_eq!(
            exchange.contract(wallet.environment()),
            vector.verifying_contract
        );
        assert_eq!(signer.order_hash(&vector.order, exchange), vector.hash);
        // Read-only hashing must reproduce every SDK vector without constructing a wallet.
        let hasher = nautilus_predict::signing::PredictOrderHasher::new(wallet.environment());
        assert_eq!(hasher.order_hash(&vector.order, exchange), vector.hash);
        let signed = signer.sign_order(&vector.order, exchange).unwrap();
        assert_eq!(signed.signature().expose_secret(), vector.signature);
        assert_eq!(signed.hash(), vector.hash);
        assert_eq!(signed.order(), &vector.order);
        assert_eq!(signed.environment(), wallet.environment());
        assert_eq!(signed.exchange(), exchange);
        let mut expected = serde_json::to_value(vector.order).unwrap();
        expected["hash"] = json!(vector.hash);
        expected["signature"] = json!(vector.signature);
        assert_eq!(serde_json::to_value(&signed).unwrap(), expected);
        expected["expiration"] = json!(4102444800i64);
        assert_eq!(
            serde_json::to_value(signed.contract_order().unwrap()).unwrap(),
            expected
        );
        assert_eq!(
            serde_json::from_value::<PredictOrder>(expected).unwrap(),
            vector.order
        );
        assert!(!format!("{signed:?}").contains(signed.signature().expose_secret()));
        assert!(!format!("{signer:?}").contains(TEST_KEY.trim_start_matches("0x")));
        let bytes = if vector.predict_account.is_none() {
            65
        } else {
            86
        };
        assert_eq!(signed.signature().expose_secret().len(), 2 + 2 * bytes);
        if vector.predict_account.is_none() {
            let signature: Signature = vector.signature.parse().unwrap();
            assert_eq!(
                signature
                    .recover_address_from_prehash(&vector.hash)
                    .unwrap(),
                wallet.owner_address()
            );
        }
    }
}

#[test]
fn one_signer_switches_all_exchange_domains_without_stale_hashes() {
    let vectors = fixtures();
    for context in vectors.as_chunks::<8>().0 {
        let signer = PredictOrderSigner::new(wallet(&context[0]));
        let mut buy_hashes = HashSet::new();
        for index in [0, 2, 4, 6, 1, 3, 5, 7, 6, 0] {
            let vector = &context[index];
            let exchange =
                PredictExchangeType::from_market_flags(vector.is_neg_risk, vector.is_yield_bearing);
            let signed = signer.sign_order(&vector.order, exchange).unwrap();
            assert_eq!(signed.hash(), vector.hash);
            assert_eq!(signed.signature().expose_secret(), vector.signature);
            if index % 2 == 0 {
                buy_hashes.insert(signed.hash());
            }
        }
        assert_eq!(buy_hashes.len(), 4);
    }
    assert_ne!(vectors[0].hash, vectors[16].hash);
}

#[test]
fn order_hash_input_does_not_follow_authentication_text_semantics() {
    let vectors = fixtures();
    for index in [0, 8, 16, 24] {
        let vector = &vectors[index];
        let wallet = wallet(vector);
        let signed = PredictOrderSigner::new(Arc::clone(&wallet))
            .sign_order(&vector.order, PredictExchangeType::Standard)
            .unwrap();
        assert_ne!(
            signed.signature(),
            &wallet.sign_auth_message(&vector.hash.to_string()).unwrap()
        );
        // Detached edits cannot alter the signed payload retained by the caller.
        let mut edited = *signed.order();
        edited.maker_amount += U256::from(1);
        let signer = PredictOrderSigner::new(wallet);
        assert_ne!(
            signer.order_hash(&edited, PredictExchangeType::Standard),
            signed.hash()
        );
        assert_eq!(signed.order(), &vector.order);
    }
}

#[test]
fn wallet_binding_and_public_taker_are_validated_before_signing() {
    for vector in [fixtures().remove(0), fixtures().remove(8)] {
        let signer = PredictOrderSigner::new(wallet(&vector));
        for field in ["maker", "signer", "taker"] {
            let mut order = vector.order;
            let unrelated = "0x2222222222222222222222222222222222222222"
                .parse()
                .unwrap();
            match field {
                "maker" => order.maker = unrelated,
                "signer" => order.signer = unrelated,
                _ => order.taker = unrelated,
            }
            assert!(matches!(
                signer.sign_order(&order, PredictExchangeType::Standard),
                Err(Error::BadRequest(_))
            ));
        }
    }
}

#[test]
fn wire_integers_keep_full_precision_and_reject_lossy_encodings() {
    let vectors = fixtures();
    assert_eq!(vectors[1].order.token_id, U256::MAX);
    let valid = serde_json::to_value(vectors[1].order).unwrap();
    for name in [
        "salt",
        "tokenId",
        "makerAmount",
        "takerAmount",
        "nonce",
        "feeRateBps",
    ] {
        assert!(valid[name].is_string());
        for value in [
            json!(12),
            json!(1.5),
            json!("-1"),
            json!("0x123"),
            json!("1e18"),
            json!(""),
            json!("115792089237316195423570985008687907853269984665640564039457584007913129639936"),
        ] {
            let mut invalid = valid.clone();
            invalid[name] = value;
            assert!(
                serde_json::from_value::<PredictOrder>(invalid).is_err(),
                "field={name}"
            );
        }
    }
    for (field, value) in [
        ("side", json!(2)),
        ("signatureType", json!(1)),
        ("signatureType", json!(2)),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        assert!(serde_json::from_value::<PredictOrder>(invalid).is_err());
    }
    let roundtrip: PredictOrder = serde_json::from_value::<PredictOrder>(valid.clone()).unwrap();
    assert_eq!(serde_json::to_value(roundtrip).unwrap(), valid);
    let raw: Value = serde_json::from_str(include_str!("fixtures/order_vectors.json")).unwrap();
    assert_eq!(valid, raw["vectors"][1]["order"]);
}

#[test]
fn expiration_converts_to_rest_int64_without_truncation_or_float_coercion() {
    let vector = fixtures().remove(0);
    let signer = PredictOrderSigner::new(wallet(&vector));
    for value in [U256::from(i64::MAX as u64) + U256::from(1), U256::MAX] {
        let mut order = vector.order;
        order.expiration = value;
        assert!(
            signer
                .sign_order(&order, PredictExchangeType::Standard)
                .unwrap()
                .contract_order()
                .is_err()
        );
    }
    let mut order = vector.order;
    order.expiration = U256::from(i64::MAX as u64);
    let signed = signer
        .sign_order(&order, PredictExchangeType::Standard)
        .unwrap();
    assert_eq!(
        serde_json::to_value(signed.contract_order().unwrap()).unwrap()["expiration"],
        json!(i64::MAX)
    );
    let original = serde_json::to_value(vector.order).unwrap();
    for value in [
        json!(-1),
        json!(1.5),
        json!("1e18"),
        json!("-1"),
        json!("0x123"),
        json!(""),
    ] {
        let mut invalid = original.clone();
        invalid["expiration"] = value;
        assert!(serde_json::from_value::<PredictOrder>(invalid).is_err());
    }
}

#[test]
fn each_variable_signed_field_changes_the_digest() {
    let vector = fixtures().remove(0);
    let signer = PredictOrderSigner::new(wallet(&vector));
    let original = serde_json::to_value(vector.order).unwrap();
    for field in [
        "salt",
        "tokenId",
        "makerAmount",
        "takerAmount",
        "expiration",
        "nonce",
        "feeRateBps",
        "maker",
        "signer",
        "taker",
        "side",
    ] {
        let mut changed = original.clone();
        changed[field] = match field {
            "maker" | "signer" | "taker" => json!("0x2222222222222222222222222222222222222222"),
            "side" => json!(1),
            _ => json!("2"),
        };
        let changed: PredictOrder = serde_json::from_value(changed).unwrap();
        assert_ne!(
            signer.order_hash(&changed, PredictExchangeType::Standard),
            vector.hash,
            "field={field}"
        );
    }
}

#[test]
fn shared_signer_is_deterministic_under_concurrent_exchange_switching() {
    let vectors = fixtures();
    let signer = Arc::new(PredictOrderSigner::new(wallet(&vectors[8])));
    std::thread::scope(|scope| {
        for vector in &vectors[8..16] {
            let signer = Arc::clone(&signer);
            scope.spawn(move || {
                let exchange = PredictExchangeType::from_market_flags(
                    vector.is_neg_risk,
                    vector.is_yield_bearing,
                );
                for _ in 0..8 {
                    let signed = signer.sign_order(&vector.order, exchange).unwrap();
                    assert_eq!(signed.hash(), vector.hash);
                    assert_eq!(signed.signature().expose_secret(), vector.signature);
                }
            });
        }
    });
}

#[test]
fn minimum_resting_requests_match_sdk_with_default_and_uuid_salts_in_all_domains() {
    use nautilus_predict::http::command_models::PredictCreateOrderRequest;
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/runtime_limit_requests.json")).unwrap();
    let vectors = fixture["vectors"].as_array().unwrap();
    assert_eq!(vectors.len(), 48);
    for vector in vectors {
        let wallet = Arc::new(
            PredictAuthSigner::new(
                TEST_KEY.into(),
                PredictEnvironment::BnbMainnet,
                vector["predictAccount"].as_str(),
            )
            .unwrap(),
        );
        let expected = &vector["body"];
        let order: PredictOrder =
            serde_json::from_value(expected["data"]["order"].clone()).unwrap();
        let exchange = PredictExchangeType::from_market_flags(
            vector["isNegRisk"].as_bool().unwrap(),
            vector["isYieldBearing"].as_bool().unwrap(),
        );
        let signed = PredictOrderSigner::new(wallet)
            .sign_order(&order, exchange)
            .unwrap();
        let price = expected["data"]["pricePerShare"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let request = PredictCreateOrderRequest::limit(&signed, price, true).unwrap();
        assert_eq!(serde_json::to_value(request).unwrap(), *expected);
        assert_eq!(order.maker_amount, U256::from(1_000_000_000_000_000_000u64));
        assert_eq!(order.fee_rate_bps, U256::from(200));
        assert_eq!(order.expiration, U256::from(4_102_444_800u64));
        if vector["saltProfile"] == "runtime_uuid" {
            assert!(order.salt > U256::from(2_147_483_648u64));
        } else if vector["saltProfile"] == "runtime_sdk_range" {
            assert!(order.salt < U256::from(2_147_483_648u64));
        }
    }
}
