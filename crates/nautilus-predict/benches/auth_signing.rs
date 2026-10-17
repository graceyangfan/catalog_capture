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

//! Offline authentication benchmarks. Signers are reused, matching a live session's lifetime.

mod common;

use std::hint::black_box;

use common::ReferenceAuthSigner;
use criterion::{Criterion, criterion_group, criterion_main};
use nautilus_predict::signing::{PredictAuthSigner, PredictChain};

// Public Hardhat development key #0, never an operator credential.
const TEST_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const DEPOSIT: &str = "0x1111111111111111111111111111111111111111";
const MESSAGE: &str = "Predict.fun authentication\n nonce: offline-fixture ";

fn bench_auth_signing(c: &mut Criterion) {
    // Both implementations must reproduce every official SDK fixture before timings are accepted.
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("../tests/fixtures/auth_vectors.json")).unwrap();
    for vector in fixtures["vectors"].as_array().unwrap() {
        let chain = if vector["chain_id"] == 56 {
            PredictChain::BnbMainnet
        } else {
            PredictChain::BnbTestnet
        };
        let account = vector["predict_account"].as_str();
        let cached = PredictAuthSigner::new(TEST_KEY.into(), chain, account).unwrap();
        let reference = ReferenceAuthSigner::new(TEST_KEY, chain, account);
        let message = vector["message"].as_str().unwrap();
        let expected = vector["signature"].as_str().unwrap();
        assert_eq!(
            cached.sign_auth_message(message).unwrap().expose_secret(),
            expected
        );
        assert_eq!(
            reference
                .sign_auth_message(message)
                .unwrap()
                .expose_secret(),
            expected
        );
    }
    for (name, chain, account) in [
        ("auth/eoa", PredictChain::BnbMainnet, None),
        (
            "auth/account_mainnet",
            PredictChain::BnbMainnet,
            Some(DEPOSIT),
        ),
        (
            "auth/account_testnet",
            PredictChain::BnbTestnet,
            Some(DEPOSIT),
        ),
    ] {
        let signer = PredictAuthSigner::new(TEST_KEY.into(), chain, account).unwrap();
        let reference = ReferenceAuthSigner::new(TEST_KEY, chain, account);
        c.bench_function(&format!("{name}/uncached"), |b| {
            b.iter(|| reference.sign_auth_message(black_box(MESSAGE)).unwrap());
        });
        c.bench_function(name, |b| {
            b.iter(|| signer.sign_auth_message(black_box(MESSAGE)).unwrap());
        });
    }
}

criterion_group!(benches, bench_auth_signing);
criterion_main!(benches);
