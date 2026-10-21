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

use super::*;
fn channel(capacity: usize) -> (PredictLatency, PredictLatencyCollector) {
    PredictLatency::channel("TESTER-001".into(), "PREDICT-test".into(), capacity).unwrap()
}
fn key(trade: &str) -> FeedbackKey {
    FeedbackKey {
        client: "ORDER-1".into(),
        kind: FeedbackKind::Filled,
        venue_time: 42,
        trade: Some(trade.into()),
    }
}
#[test]
fn receive_and_callback_match_out_of_order_without_collapsing_equal_time_trades() {
    let (probe, mut collector) = channel(64);
    let start = Instant::now();
    for (trade, offset) in [("TRADE-1", 10), ("TRADE-2", 20)] {
        probe.send(Sample::Callback(
            key(trade),
            start + Duration::from_nanos(offset + 3),
        ));
        probe.send(Sample::Feedback(
            key(trade),
            start,
            start + Duration::from_nanos(offset),
        ));
        // Replayed marks and callbacks must not add a second observation.
        probe.send(Sample::Feedback(
            key(trade),
            start,
            start + Duration::from_nanos(offset),
        ));
        probe.send(Sample::Callback(
            key(trade),
            start + Duration::from_nanos(offset + 3),
        ));
    }
    let stats = collector.snapshot();
    let native = &stats.distributions[&LatencyMetric::WsToNativeSend];
    assert_eq!(native.completed_observations, 2);
    assert_eq!((native.p50_ns, native.p95_ns, native.p99_ns), (10, 20, 20));
    assert_eq!(
        stats.distributions[&LatencyMetric::NativeSendToStrategyCallback].min_ns,
        3
    );
    assert_eq!(
        stats.distributions[&LatencyMetric::WsToStrategyCallback].max_ns,
        23
    );
    assert_eq!(
        stats.distributions[&LatencyMetric::WsFillToStrategyCallback].completed_observations,
        2
    );
    assert_eq!(
        stats.distributions[&LatencyMetric::WsFillToStrategyCallback].max_ns,
        23
    );
    assert_eq!(
        stats.distributions[&LatencyMetric::WsFillToNativeSend].completed_observations,
        2
    );
    assert_eq!(
        stats.distributions[&LatencyMetric::WsFillToNativeSend].p95_ns,
        20
    );
    assert_eq!(
        stats.distributions[&LatencyMetric::NativeFillSendToStrategyCallback].min_ns,
        3
    );
    assert_eq!(stats.pending_correlations, 0);
    assert_eq!(stats.invalid_intervals, 0);
}
#[test]
fn stalled_or_closed_collector_drops_telemetry_without_waiting_or_unbounded_storage() {
    let (probe, mut collector) = channel(1);
    probe.send(Sample::Duration(LatencyMetric::RpcHttp, 7));
    for _ in 0..1000 {
        probe.send(Sample::Duration(LatencyMetric::RpcHttp, 99));
    }
    probe.count(LatencyCounter::RpcRequests);
    let stats = collector.snapshot();
    assert_eq!(stats.dropped_samples, 1000);
    assert_eq!(stats.counters["rpc_requests"], 1);
    assert_eq!(stats.distributions[&LatencyMetric::RpcHttp].max_ns, 7);
    probe.send(Sample::Duration(LatencyMetric::RpcHttp, 9));
    let stats = collector.snapshot();
    assert_eq!(
        stats.distributions[&LatencyMetric::RpcHttp].completed_observations,
        2
    );
    assert_eq!(
        stats.distributions[&LatencyMetric::RpcHttp].retained_observations,
        1
    );
    drop(collector);
    probe.send(Sample::Duration(LatencyMetric::RpcHttp, 11));
    assert_eq!(probe.shared.dropped.load(Ordering::Relaxed), 1001);
}
#[test]
fn absent_or_late_counterparts_are_bounded_and_never_report_zero_latency() {
    let (probe, mut collector) = channel(64);
    let start = Instant::now();
    probe.send(Sample::Decision("ORDER-1".into(), start));
    probe.send(Sample::Callback(key("TRADE-1"), start));
    let stats = collector.snapshot();
    assert!(stats.distributions.is_empty());
    assert_eq!(stats.pending_correlations, 2);
    collector.expire(start + CORRELATION_TTL);
    assert_eq!(collector.snapshot().lost_correlations, 2);
    assert_eq!(collector.snapshot().retained_correlations, 0);
    for n in 0..=PENDING_LIMIT {
        collector.accept(Sample::Decision(
            ClientOrderId::new(format!("BOUND-{n}")),
            start,
        ));
    }
    let stats = collector.snapshot();
    assert_eq!(stats.retained_correlations, PENDING_LIMIT);
    assert_eq!(stats.evicted_correlations, 1);
}
#[test]
fn invalid_intervals_and_abandoned_spans_are_visible_and_not_percentiles() {
    let (probe, mut collector) = channel(16);
    let start = Instant::now();
    probe.send(Sample::Decision(
        "ORDER-1".into(),
        start + Duration::from_secs(1),
    ));
    probe.send(Sample::Post("ORDER-1".into(), start));
    drop(probe.timer(LatencyMetric::CreateRequest));
    let stats = collector.snapshot();
    assert_eq!(stats.invalid_intervals, 1);
    assert!(stats.distributions.is_empty());
    assert_eq!(stats.counters["abandoned_timings"], 1);
}
#[test]
fn completed_timers_and_decision_marks_are_monotonic_and_bound_to_the_owner() {
    let (probe, mut collector) = channel(16);
    probe.timer(LatencyMetric::Preparation).finish();
    let start = Instant::now();
    probe.decision("ORDER-1".into(), start);
    probe.post("ORDER-1".into(), start);
    let stats = collector.snapshot();
    assert_eq!(
        stats.distributions[&LatencyMetric::Preparation].completed_observations,
        1
    );
    assert_eq!(
        stats.distributions[&LatencyMetric::DecisionToPost].completed_observations,
        1
    );
    assert_eq!(stats.counters["abandoned_timings"], 0);
    let serialized = serde_json::to_value(&stats).unwrap();
    assert_eq!(
        serialized["distributions"]["preparation"]["completed_observations"],
        1
    );
    assert!(probe.matches_owner("TESTER-001".into(), "PREDICT-test".into()));
    assert!(!probe.matches_owner("OTHER-001".into(), "PREDICT-test".into()));
    assert!(PredictLatency::channel("TESTER-001".into(), "PREDICT-test".into(), 0).is_err());
    assert!(PredictLatency::channel("TESTER-001".into(), "PREDICT-test".into(), 65537).is_err());
}

#[test]
fn actual_callback_hook_ignores_foreign_traders_and_accounts() {
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::events::OrderAccepted;
    let (probe, mut collector) = channel(16);
    let mut event = OrderAccepted::new(
        "TESTER-001".into(),
        "TEST-001".into(),
        "42-1.PREDICT".into(),
        "ORDER-1".into(),
        "12345".into(),
        "PREDICT-test".into(),
        UUID4::new(),
        UnixNanos::from(1),
        UnixNanos::from(1),
        false,
    );
    probe.strategy_callback(&event);
    event.trader_id = "OTHER-001".into();
    event.client_order_id = "ORDER-2".into();
    probe.strategy_callback(&event);
    event.trader_id = "TESTER-001".into();
    event.account_id = "PREDICT-other".into();
    event.client_order_id = "ORDER-3".into();
    probe.strategy_callback(&event);
    assert_eq!(collector.snapshot().pending_correlations, 1);
    assert_eq!(probe.shared.dropped.load(Ordering::Relaxed), 0);
}

#[test]
fn rest_cancel_callback_matches_decision_without_inventing_ws_latency() {
    let (probe, mut collector) = channel(16);
    let start = Instant::now();
    let key = FeedbackKey {
        kind: FeedbackKind::Canceled,
        trade: None,
        ..key("unused")
    };
    probe.send(Sample::Callback(key, start + Duration::from_nanos(10)));
    probe.cancel_decision(key.client, start);
    probe.send(Sample::Callback(key, start + Duration::from_nanos(20)));
    let stats = collector.snapshot();
    assert_eq!(
        stats.distributions[&LatencyMetric::CancelToStrategyCallback].completed_observations,
        1
    );
    assert_eq!(
        stats.distributions[&LatencyMetric::CancelToStrategyCallback].p99_ns,
        10
    );
    assert!(
        !stats
            .distributions
            .contains_key(&LatencyMetric::WsToStrategyCallback)
    );
    assert_eq!(stats.pending_correlations, 1);
}
