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

//! Offline order hashing, signing and nested REST payload preparation.

use std::{hint::black_box, sync::Arc};

use criterion::{Criterion, criterion_group, criterion_main};
use nautilus_predict::{
    PredictEnvironment,
    common::contracts::PredictExchangeType,
    http::order_models::PredictOrder,
    signing::{PredictAuthSigner, PredictOrderSigner},
};

// Public Hardhat fixture, never an operator credential.
const TEST_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

fn bench_order_signing(c: &mut Criterion) {
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("../tests/fixtures/order_vectors.json")).unwrap();
    for (name, index) in [("eoa", 0), ("account", 8)] {
        let vector = &fixtures["vectors"][index];
        let order: PredictOrder = serde_json::from_value(vector["order"].clone()).unwrap();
        let wallet = Arc::new(
            PredictAuthSigner::new(
                TEST_KEY.into(),
                PredictEnvironment::BnbMainnet,
                vector["predict_account"].as_str(),
            )
            .unwrap(),
        );
        let signer = PredictOrderSigner::new(wallet);
        assert_eq!(
            signer
                .order_hash(&order, PredictExchangeType::Standard)
                .to_string(),
            vector["hash"].as_str().unwrap()
        );
        assert_eq!(
            signer
                .sign_order(&order, PredictExchangeType::Standard)
                .unwrap()
                .signature()
                .expose_secret(),
            vector["signature"].as_str().unwrap()
        );
        if index == 0 {
            c.bench_function("order/hash", |b| {
                b.iter(|| {
                    signer.order_hash(black_box(&order), black_box(PredictExchangeType::Standard))
                })
            });
        }
        c.bench_function(&format!("order/sign_{name}"), |b| {
            b.iter(|| {
                signer
                    .sign_order(black_box(&order), black_box(PredictExchangeType::Standard))
                    .unwrap()
            });
        });
        c.bench_function(&format!("order/prepare_rest_{name}"), |b| {
            b.iter(|| {
                let signed = signer
                    .sign_order(black_box(&order), black_box(PredictExchangeType::Standard))
                    .unwrap();
                serde_json::to_string(&signed.contract_order().unwrap()).unwrap()
            });
        });
    }
}

criterion_group!(benches, bench_order_signing);
criterion_main!(benches);
