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

//! Exercise the real client entry with retained Native orders; no private writes.
// The loopback venue and lifecycle are shared with integration tests.
#[allow(dead_code, unused_imports)]
#[path = "../tests/execution.rs"]
mod execution;

use criterion::{Criterion, criterion_group, criterion_main};
fn bench_execution(c: &mut Criterion) {
    execution::entry_benchmark::run(c);
    execution::entry_benchmark::flow(c);
    execution::entry_benchmark::retained_flow(c);
    execution::entry_benchmark::cancel_feedback(c);
    execution::entry_benchmark::wallet_dispatch(c);
    execution::entry_benchmark::market_preparation(c);
}
criterion_group!(benches, bench_execution);
criterion_main!(benches);
