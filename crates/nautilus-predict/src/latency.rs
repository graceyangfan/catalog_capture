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

//! Optional, bounded latency telemetry. Producers never wait for the collector.
//! Native owns execution state; correlation here is disposable measurement data only.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use nautilus_common::{cache::fifo::FifoCacheMap, messages::ExecutionReport};
use nautilus_model::{
    enums::OrderStatus,
    events::OrderEvent,
    identifiers::{AccountId, ClientOrderId, TradeId, TraderId},
};
use serde::Serialize;
use tokio::sync::mpsc;

/// Monotonic local intervals; overlapping intervals must not be summed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyMetric {
    CommandToPost,
    DecisionToPost,
    /// Legacy strict-funding path; no observations on the current command path.
    AdmissionWait,
    Preparation,
    MarketGet,
    MarketBookGet,
    OpenCollection,
    /// Legacy per-command OPEN collection; background OPEN uses OpenCollection.
    AdmissionOpenCollection,
    RpcHttp,
    /// Legacy per-command funding RPC collection.
    ResourceCollection,
    CreateRequest,
    CancelRequest,
    CancelToStrategyCallback,
    WsToNativeSend,
    WsToStrategyCallback,
    /// Confirmed WS fills only, ending inside the actual strategy callback.
    WsFillToStrategyCallback,
    WsMatchToDataSend,
    WsMatchToStrategyCallback,
    DataMatchSendToStrategyCallback,
    WsFillToNativeSend,
    NativeFillSendToStrategyCallback,
    NativeSendToStrategyCallback,
}
/// Counts include failed attempts and background/startup collectors.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LatencyCounter {
    MetadataHit,
    MetadataMiss,
    OpenPageRequests,
    RpcRequests,
    CreateCalls,
    RemoveCalls,
    AbandonedTimings,
    OrderbookRequests,
    MetadataRequests,
}
const COUNTERS: [LatencyCounter; 9] = [
    LatencyCounter::MetadataHit,
    LatencyCounter::MetadataMiss,
    LatencyCounter::OpenPageRequests,
    LatencyCounter::RpcRequests,
    LatencyCounter::CreateCalls,
    LatencyCounter::RemoveCalls,
    LatencyCounter::AbandonedTimings,
    LatencyCounter::OrderbookRequests,
    LatencyCounter::MetadataRequests,
];
const PENDING_LIMIT: usize = 4096;
const CORRELATION_TTL: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum FeedbackKind {
    Accepted,
    Canceled,
    Rejected,
    Expired,
    Filled,
    Matched,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct FeedbackKey {
    client: ClientOrderId,
    kind: FeedbackKind,
    venue_time: u64,
    trade: Option<TradeId>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum TraceKey {
    Command(ClientOrderId),
    Cancel(ClientOrderId),
    Feedback(FeedbackKey),
}
#[derive(Clone, Copy, Debug)]
enum Sample {
    Duration(LatencyMetric, u64),
    Decision(ClientOrderId, Instant),
    CancelDecision(ClientOrderId, Instant),
    Post(ClientOrderId, Instant),
    Feedback(FeedbackKey, Instant, Instant),
    Callback(FeedbackKey, Instant),
}
#[derive(Debug)]
struct Shared {
    trader: TraderId,
    account: AccountId,
    dropped: AtomicU64,
    counters: [AtomicU64; COUNTERS.len()],
}

/// Clone into an execution config and the participating strategy. None in the config disables it.
#[derive(Clone, Debug)]
pub struct PredictLatency {
    tx: mpsc::Sender<Sample>,
    shared: Arc<Shared>,
}
impl PredictLatency {
    /// Creates a bounded telemetry channel and its cold-path collector.
    ///
    /// # Errors
    /// Capacity must be 1..=65536. It also bounds retained observations per metric.
    pub fn channel(
        trader: TraderId,
        account: AccountId,
        capacity: usize,
    ) -> anyhow::Result<(Self, PredictLatencyCollector)> {
        anyhow::ensure!(
            (1..=65536).contains(&capacity),
            "invalid Predict latency capacity"
        );
        let (tx, rx) = mpsc::channel(capacity);
        let shared = Arc::new(Shared {
            trader,
            account,
            dropped: AtomicU64::new(0),
            counters: std::array::from_fn(|_| AtomicU64::new(0)),
        });
        Ok((
            Self {
                tx,
                shared: shared.clone(),
            },
            PredictLatencyCollector {
                rx,
                shared,
                capacity,
                observations: BTreeMap::new(),
                pending: FifoCacheMap::new(),
                correlations_lost: 0,
                evicted: 0,
                invalid_intervals: 0,
            },
        ))
    }
    pub(crate) fn matches_owner(&self, trader: TraderId, account: AccountId) -> bool {
        self.shared.trader == trader && self.shared.account == account
    }
    fn send(&self, sample: Sample) {
        if self.tx.try_send(sample).is_err() {
            self.shared.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub(crate) fn count(&self, counter: LatencyCounter) {
        self.shared.counters[counter as usize].fetch_add(1, Ordering::Relaxed);
    }
    /// Starts a local interval. Dropping an unfinished timer counts abandonment, not a completed observation.
    pub fn timer(&self, metric: LatencyMetric) -> LatencyTimer<'_> {
        LatencyTimer {
            probe: self,
            metric,
            started: Instant::now(),
            finished: false,
        }
    }
    /// Marks the strategy's decision start, after the native order ID becomes available.
    /// `started` may be captured before signal evaluation. Never pass a venue or wall-clock timestamp.
    pub fn decision(&self, client: ClientOrderId, started: Instant) {
        self.send(Sample::Decision(client, started));
    }
    /// Marks the start of a strategy cancellation; terminal callbacks may originate from REST or WS.
    pub fn cancel_decision(&self, client: ClientOrderId, started: Instant) {
        self.send(Sample::CancelDecision(client, started));
    }
    /// Call as the first action in the actual Native strategy order callback.
    /// Unmatched/reconciliation events do not invent a WS receive time.
    pub fn strategy_callback(&self, event: &dyn OrderEvent) {
        let at = Instant::now();
        if event.trader_id() != self.shared.trader
            || event.account_id() != Some(self.shared.account)
        {
            return;
        }
        let kind = match event.type_name() {
            "OrderAccepted" => FeedbackKind::Accepted,
            "OrderCanceled" => FeedbackKind::Canceled,
            "OrderRejected" => FeedbackKind::Rejected,
            "OrderExpired" => FeedbackKind::Expired,
            "OrderFilled" => FeedbackKind::Filled,
            _ => return,
        };
        self.send(Sample::Callback(
            FeedbackKey {
                client: event.client_order_id(),
                kind,
                venue_time: event.ts_event().as_u64(),
                trade: event.trade_id(),
            },
            at,
        ));
    }
    /// Call first in on_data for an early Submitted settlement notification.
    pub fn strategy_settlement_callback(&self, update: &crate::PredictSettlementUpdate) {
        if update.account_id == self.shared.account
            && update.status == crate::PredictSettlementStatus::Submitted
        {
            self.send(Sample::Callback(
                FeedbackKey {
                    client: update.client_order_id,
                    kind: FeedbackKind::Matched,
                    venue_time: update.ts_event.as_u64(),
                    trade: Some(update.trade_id),
                },
                Instant::now(),
            ));
        }
    }
    pub(crate) fn match_feedback(
        &self,
        update: &crate::PredictSettlementUpdate,
        received: Instant,
        sent: Instant,
    ) {
        if update.status == crate::PredictSettlementStatus::Submitted {
            self.send(Sample::Feedback(
                FeedbackKey {
                    client: update.client_order_id,
                    kind: FeedbackKind::Matched,
                    venue_time: update.ts_event.as_u64(),
                    trade: Some(update.trade_id),
                },
                received,
                sent,
            ));
        }
    }
    pub(crate) fn post(&self, client: ClientOrderId, command_started: Instant) {
        let now = Instant::now();
        self.elapsed(LatencyMetric::CommandToPost, command_started, now);
        self.send(Sample::Post(client, now));
    }
    pub(crate) fn prepare_feedback(
        &self,
        report: &ExecutionReport,
        received: Instant,
        native_send: Instant,
    ) -> Option<FeedbackTiming> {
        let key = match report {
            ExecutionReport::Fill(fill) => {
                return self.prepare_fill_feedback(fill, received, native_send);
            }
            ExecutionReport::Order(order) => {
                let kind = match order.order_status {
                    OrderStatus::Accepted => Some((FeedbackKind::Accepted, order.ts_accepted)),
                    OrderStatus::Canceled => Some((FeedbackKind::Canceled, order.ts_last)),
                    OrderStatus::Rejected => Some((FeedbackKind::Rejected, order.ts_last)),
                    OrderStatus::Expired => Some((FeedbackKind::Expired, order.ts_last)),
                    _ => None,
                };
                order
                    .client_order_id
                    .zip(kind)
                    .map(|(client, (kind, time))| FeedbackKey {
                        client,
                        kind,
                        venue_time: time.as_u64(),
                        trade: None,
                    })
            }
            _ => None,
        };
        key.map(|key| FeedbackTiming {
            key,
            received,
            native_send,
        })
    }
    pub(crate) fn prepare_fill_feedback(
        &self,
        fill: &nautilus_model::reports::FillReport,
        received: Instant,
        native_send: Instant,
    ) -> Option<FeedbackTiming> {
        fill.client_order_id.map(|client| FeedbackTiming {
            key: FeedbackKey {
                client,
                kind: FeedbackKind::Filled,
                venue_time: fill.ts_event.as_u64(),
                trade: Some(fill.trade_id),
            },
            received,
            native_send,
        })
    }
    pub(crate) fn feedback(&self, timing: FeedbackTiming) {
        self.send(Sample::Feedback(
            timing.key,
            timing.received,
            timing.native_send,
        ));
    }
    fn elapsed(&self, metric: LatencyMetric, start: Instant, end: Instant) {
        if let Some(elapsed) = end.checked_duration_since(start) {
            self.send(Sample::Duration(metric, nanos(elapsed)));
        }
    }
}

pub(crate) struct FeedbackTiming {
    key: FeedbackKey,
    received: Instant,
    native_send: Instant,
}

/// A lightweight optional producer timer. No formatting, histogram or IO is performed here.
#[must_use]
pub struct LatencyTimer<'a> {
    probe: &'a PredictLatency,
    metric: LatencyMetric,
    started: Instant,
    finished: bool,
}
impl LatencyTimer<'_> {
    pub fn finish(mut self) {
        self.probe
            .elapsed(self.metric, self.started, Instant::now());
        self.finished = true;
    }
}
impl Drop for LatencyTimer<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.probe.count(LatencyCounter::AbandonedTimings);
        }
    }
}

#[derive(Debug, Serialize)]
pub struct LatencyDistribution {
    pub completed_observations: u64,
    pub retained_observations: usize,
    pub min_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}
#[derive(Debug, Serialize)]
pub struct PredictLatencySnapshot {
    pub distributions: BTreeMap<LatencyMetric, LatencyDistribution>,
    pub counters: BTreeMap<String, u64>,
    pub dropped_samples: u64,
    pub pending_correlations: usize,
    pub retained_correlations: usize,
    pub evicted_correlations: u64,
    pub lost_correlations: u64,
    pub invalid_intervals: u64,
}
#[derive(Default)]
struct Observations {
    total: u64,
    samples: VecDeque<u64>,
}
struct Trace {
    times: [Option<Instant>; 3],
    emitted: u8,
    last: Instant,
}
/// Drain/sort on a dedicated monitoring thread or after the run, never on the LiveNode executor.
/// Storage is bounded even if a callback never arrives or the collector is stalled.
pub struct PredictLatencyCollector {
    rx: mpsc::Receiver<Sample>,
    shared: Arc<Shared>,
    capacity: usize,
    observations: BTreeMap<LatencyMetric, Observations>,
    pending: FifoCacheMap<TraceKey, Trace, PENDING_LIMIT>,
    correlations_lost: u64,
    evicted: u64,
    invalid_intervals: u64,
}
impl PredictLatencyCollector {
    /// Drains at most one channel capacity per call, so a busy producer cannot monopolize the monitor.
    /// Percentiles are nearest-rank over the last `capacity` completed observations per metric.
    pub fn snapshot(&mut self) -> PredictLatencySnapshot {
        self.expire(Instant::now());
        for _ in 0..self.capacity {
            match self.rx.try_recv() {
                Ok(sample) => self.accept(sample),
                Err(_) => break,
            }
        }
        let distributions = self
            .observations
            .iter()
            .filter(|(_, v)| !v.samples.is_empty())
            .map(|(metric, v)| {
                let mut sorted = v.samples.iter().copied().collect::<Vec<_>>();
                sorted.sort_unstable();
                let quantile =
                    |p: usize| sorted[(sorted.len() * p).div_ceil(100).saturating_sub(1)];
                (
                    *metric,
                    LatencyDistribution {
                        completed_observations: v.total,
                        retained_observations: sorted.len(),
                        min_ns: sorted[0],
                        p50_ns: quantile(50),
                        p95_ns: quantile(95),
                        p99_ns: quantile(99),
                        max_ns: *sorted.last().expect("non-empty"),
                    },
                )
            })
            .collect();
        PredictLatencySnapshot {
            distributions,
            counters: COUNTERS
                .into_iter()
                .map(|counter| {
                    (
                        serde_json::to_value(counter)
                            .expect("counter serialization")
                            .as_str()
                            .expect("counter name")
                            .to_owned(),
                        self.shared.counters[counter as usize].load(Ordering::Relaxed),
                    )
                })
                .collect(),
            dropped_samples: self.shared.dropped.load(Ordering::Relaxed),
            pending_correlations: self
                .pending
                .iter()
                .filter(|(key, trace)| !Self::complete(**key, trace))
                .count(),
            retained_correlations: self.pending.len(),
            evicted_correlations: self.evicted,
            lost_correlations: self.correlations_lost,
            invalid_intervals: self.invalid_intervals,
        }
    }
    fn observe(&mut self, metric: LatencyMetric, value: u64) {
        let observations = self.observations.entry(metric).or_default();
        observations.total += 1;
        if observations.samples.len() == self.capacity {
            observations.samples.pop_front();
        }
        observations.samples.push_back(value);
    }
    fn accept(&mut self, sample: Sample) {
        match sample {
            Sample::Duration(metric, value) => self.observe(metric, value),
            Sample::Decision(client, time) => self.point(TraceKey::Command(client), 0, time),
            Sample::CancelDecision(client, time) => self.point(TraceKey::Cancel(client), 0, time),
            Sample::Post(client, time) => self.point(TraceKey::Command(client), 1, time),
            Sample::Feedback(key, received, send) => {
                self.point(TraceKey::Feedback(key), 0, received);
                self.point(TraceKey::Feedback(key), 1, send);
            }
            Sample::Callback(key, time) => {
                if key.kind == FeedbackKind::Canceled {
                    self.point(TraceKey::Cancel(key.client), 1, time);
                }
                self.point(TraceKey::Feedback(key), 2, time);
            }
        }
    }
    fn point(&mut self, key: TraceKey, index: usize, time: Instant) {
        if !self.pending.contains_key(&key) {
            if self.pending.len() == PENDING_LIMIT {
                self.evicted += 1;
            }
            self.pending.insert(
                key,
                Trace {
                    times: [None; 3],
                    emitted: 0,
                    last: time,
                },
            );
        }
        let trace = self.pending.get_mut(&key).expect("trace installed");
        trace.last = trace.last.max(time);
        // A replay cannot turn an earlier receive/callback into a later measurement.
        if trace.times[index].is_none() {
            trace.times[index] = Some(time);
        }
        let pairs: &[(usize, usize, LatencyMetric)] = match key {
            TraceKey::Command(_) => &[(0, 1, LatencyMetric::DecisionToPost)],
            TraceKey::Cancel(_) => &[(0, 1, LatencyMetric::CancelToStrategyCallback)],
            TraceKey::Feedback(FeedbackKey {
                kind: FeedbackKind::Matched,
                ..
            }) => &[
                (0, 1, LatencyMetric::WsMatchToDataSend),
                (0, 2, LatencyMetric::WsMatchToStrategyCallback),
                (1, 2, LatencyMetric::DataMatchSendToStrategyCallback),
            ],
            TraceKey::Feedback(_) => &[
                (0, 1, LatencyMetric::WsToNativeSend),
                (0, 2, LatencyMetric::WsToStrategyCallback),
                (1, 2, LatencyMetric::NativeSendToStrategyCallback),
            ],
        };
        let mut values = Vec::new();
        for (bit, (start, end, metric)) in pairs.iter().enumerate() {
            if trace.emitted & (1 << bit) == 0
                && let (Some(start), Some(end)) = (trace.times[*start], trace.times[*end])
            {
                trace.emitted |= 1 << bit;
                values.push((*metric, end.checked_duration_since(start)));
            }
        }
        for (metric, value) in values {
            if let Some(value) = value {
                self.observe(metric, nanos(value));
                if matches!(
                    key,
                    TraceKey::Feedback(FeedbackKey {
                        kind: FeedbackKind::Filled,
                        ..
                    })
                ) {
                    let fill_metric = match metric {
                        LatencyMetric::WsToNativeSend => Some(LatencyMetric::WsFillToNativeSend),
                        LatencyMetric::WsToStrategyCallback => {
                            Some(LatencyMetric::WsFillToStrategyCallback)
                        }
                        LatencyMetric::NativeSendToStrategyCallback => {
                            Some(LatencyMetric::NativeFillSendToStrategyCallback)
                        }
                        _ => None,
                    };
                    if let Some(metric) = fill_metric {
                        self.observe(metric, nanos(value));
                    }
                }
            } else {
                self.invalid_intervals += 1;
            }
        }
    }
    fn complete(key: TraceKey, trace: &Trace) -> bool {
        match key {
            TraceKey::Command(_) | TraceKey::Cancel(_) => trace.emitted == 1,
            TraceKey::Feedback(_) => trace.emitted == 7,
        }
    }
    fn expire(&mut self, now: Instant) {
        let expired = |trace: &Trace| {
            now.checked_duration_since(trace.last)
                .is_some_and(|age| age >= CORRELATION_TTL)
        };
        if self.pending.iter().any(|(_, trace)| expired(trace)) {
            // Native FIFO drain preserves insertion order; rebuild once rather than O(n²) removals.
            for (key, trace) in self.pending.drain() {
                if expired(&trace) {
                    if !Self::complete(key, &trace) {
                        self.correlations_lost += 1;
                    }
                } else {
                    self.pending.insert(key, trace);
                }
            }
        }
    }
}
fn nanos(value: Duration) -> u64 {
    u64::try_from(value.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
#[path = "../tests/unit/latency.rs"]
mod tests;
