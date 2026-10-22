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

//! Measure the production routing insertion with a fixed retained route population.
#[path = "../src/execution/routing.rs"]
#[allow(dead_code, unused_imports)]
mod routing;

use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use alloy::primitives::U256;
#[path = "../src/common/serde.rs"]
#[allow(dead_code)]
pub(crate) mod wire_serde;
mod common {
    pub(crate) use super::wire_serde as serde;
}
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use nautilus_model::identifiers::VenueOrderId;

fn bench_routes(c: &mut Criterion) {
    let mut group = c.benchmark_group("execution/rest_route_insert");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(200));
    for retained in [0, 1_000, 10_000] {
        let ids = routing::RestOrderIds::default();
        let routes = (0..retained)
            .map(|n| (VenueOrderId::new(format!("HISTORY-{n}")), U256::from(n)))
            .collect::<Vec<_>>();
        routing::register_many(&ids, &routes).unwrap();
        let target = VenueOrderId::from("TARGET");
        group.bench_with_input(BenchmarkId::from_parameter(retained), &retained, |b, _| {
            b.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iterations {
                    let started = Instant::now();
                    routing::register(black_box(&ids), target, U256::from(99)).unwrap();
                    elapsed += started.elapsed();
                    // Teardown is deliberately untimed; each insertion sees the same history.
                    ids.lock().unwrap().remove(&target);
                }
                elapsed
            });
        });
    }
    group.finish();
}
fn bench_numeric_ids(c: &mut Criterion) {
    let mut group = c.benchmark_group("execution/rest_numeric_id_parse");
    group.sample_size(30);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(300));
    for value in [
        "1234567890",
        "115792089237316195423570985008687907853269984665640564039457584007913129639935",
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(value.len()),
            value,
            |b, value| {
                b.iter(|| black_box(routing::parse_id(black_box(value)).unwrap()));
            },
        );
    }
    group.finish();
}
criterion_group!(benches, bench_routes, bench_numeric_ids);
criterion_main!(benches);
