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

//! Producer-only microbenchmarks. Collection/sorting is excluded from timed chunks.

use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use criterion::{Criterion, criterion_group, criterion_main};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::events::OrderAccepted;
use nautilus_predict::latency::{LatencyMetric, PredictLatency};

fn bench_latency(c: &mut Criterion) {
    let owner = "TESTER-001".into();
    let account = "PREDICT-bench".into();
    let event = OrderAccepted::new(
        owner,
        "BENCH-001".into(),
        "42-1.PREDICT".into(),
        "ORDER-1".into(),
        "12345".into(),
        account,
        UUID4::new(),
        UnixNanos::from(1),
        UnixNanos::from(1),
        false,
    );
    let disabled: Option<PredictLatency> = None;
    c.bench_function("latency/disabled_branch", |b| {
        b.iter(|| {
            if let Some(probe) = black_box(&disabled) {
                probe.timer(LatencyMetric::CreateRequest).finish();
            }
        })
    });
    for callback in [false, true] {
        let (probe, mut collector) = PredictLatency::channel(owner, account, 64).unwrap();
        let name = if callback {
            "latency/callback_queued"
        } else {
            "latency/timer_queued"
        };
        c.bench_function(name, |b| {
            b.iter_custom(|iterations| {
                let mut elapsed = Duration::ZERO;
                let mut remaining = iterations;
                while remaining > 0 {
                    let chunk = remaining.min(32);
                    let start = Instant::now();
                    for _ in 0..chunk {
                        if callback {
                            probe.strategy_callback(black_box(&event));
                        } else {
                            probe.timer(LatencyMetric::CreateRequest).finish();
                        }
                    }
                    elapsed += start.elapsed();
                    black_box(collector.snapshot());
                    remaining -= chunk;
                }
                elapsed
            })
        });
        assert_eq!(collector.snapshot().dropped_samples, 0);
    }
    // A stalled collector must remain a bounded, non-waiting path.
    let (probe, mut collector) = PredictLatency::channel(owner, account, 1).unwrap();
    probe.timer(LatencyMetric::CreateRequest).finish();
    c.bench_function("latency/timer_full_queue", |b| {
        b.iter(|| {
            probe.timer(LatencyMetric::CreateRequest).finish();
        })
    });
    c.bench_function("latency/callback_full_queue", |b| {
        b.iter(|| {
            probe.strategy_callback(black_box(&event));
        })
    });
    assert!(collector.snapshot().dropped_samples > 0);
}

criterion_group!(benches, bench_latency);
criterion_main!(benches);
