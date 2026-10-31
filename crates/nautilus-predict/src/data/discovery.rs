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

//! Bounded, cancellable rolling-market discovery and first-snapshot handover.

use std::time::Duration;

use ahash::AHashMap;
use anyhow::Context;
use nautilus_common::messages::DataEvent;
use nautilus_model::data::Data;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::runtime::PredictDataSession;
use crate::{
    PredictCryptoUpDownSelector, SelectedCryptoUpDownMarket, common::CryptoUpDownSelectorKey,
    discover_crypto_up_down_market, http::models::PredictMarket,
    providers::instruments_from_market,
};

pub(super) const MAX_DISCOVERY_REQUESTS: usize = 4;

#[derive(Debug, Default)]
pub(super) struct DiscoveryScheduler {
    pub(super) next_version: u64,
    pub(super) inflight: AHashMap<CryptoUpDownSelectorKey, (u64, CancellationToken)>,
}

impl DiscoveryScheduler {
    pub(super) fn due_keys(
        &self,
        subscriptions: &AHashMap<CryptoUpDownSelectorKey, DynamicSubscription>,
        now_ns: u64,
    ) -> Vec<CryptoUpDownSelectorKey> {
        let mut due = subscriptions
            .iter()
            .filter(|(key, state)| {
                state.next_refresh_ns <= now_ns && !self.inflight.contains_key(*key)
            })
            .map(|(key, state)| (state.next_refresh_ns, key.clone()))
            .collect::<Vec<_>>();
        due.sort();
        due.into_iter()
            .take(MAX_DISCOVERY_REQUESTS.saturating_sub(self.inflight.len()))
            .map(|(_, key)| key)
            .collect()
    }

    pub(super) fn version(&mut self) -> anyhow::Result<u64> {
        self.next_version = self
            .next_version
            .checked_add(1)
            .context("Predict selector version overflow")?;
        Ok(self.next_version)
    }

    pub(super) fn cancel(&mut self, key: &CryptoUpDownSelectorKey) {
        if let Some((_, cancellation)) = self.inflight.remove(key) {
            cancellation.cancel();
        }
    }

    pub(super) fn complete(&mut self, key: &CryptoUpDownSelectorKey, version: u64) -> bool {
        if self
            .inflight
            .get(key)
            .is_some_and(|(active, _)| *active == version)
        {
            self.inflight.remove(key);
            true
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub(super) struct DiscoveryResult {
    pub(super) key: CryptoUpDownSelectorKey,
    pub(super) version: u64,
    pub(super) result: anyhow::Result<(SelectedCryptoUpDownMarket, Option<PredictMarket>)>,
}

#[derive(Debug, Clone)]
pub(super) struct DynamicSubscription {
    pub(super) selector: PredictCryptoUpDownSelector,
    pub(super) owners: std::collections::BTreeSet<String>,
    pub(super) version: u64,
    pub(super) current_market_id: Option<u64>,
    pub(super) pending_market_id: Option<u64>,
    /// Earliest UTC instant at which discovery must run again.
    ///
    /// A selected market window is authoritative for the normal rollover deadline.
    /// Only failed discovery uses the short cadence, measured from completion.
    pub(super) next_refresh_ns: u64,
}

impl PredictDataSession {
    pub(super) fn schedule_discovery(&mut self) -> anyhow::Result<()> {
        let now_ns = self.clock.get_time_ns().as_u64();
        for key in self.discovery.due_keys(&self.dynamic_subscriptions, now_ns) {
            let state = &self.dynamic_subscriptions[&key];
            let selector = state.selector.clone();
            let version = state.version;
            let known = [state.current_market_id, state.pending_market_id];
            let http = self.http_client.clone();
            let sender = self.discovery_tx.clone();
            let cancellation = self.cancellation.child_token();
            let task_cancel = cancellation.clone();
            let timeout = self.discovery_timeout;
            let request = async move {
                let selected = discover_crypto_up_down_market(&http, &selector, now_ns).await?;
                let market = if known.contains(&Some(selected.market_id)) {
                    None
                } else {
                    Some(http.request_market(selected.market_id).await?)
                };
                Ok((selected, market))
            };
            self.task_spawner.spawn(run_discovery_request(
                key.clone(),
                version,
                task_cancel,
                sender,
                timeout,
                request,
            ))?;
            self.discovery.inflight.insert(key, (version, cancellation));
        }
        Ok(())
    }

    pub(super) async fn apply_discovery_result(
        &mut self,
        result: DiscoveryResult,
    ) -> anyhow::Result<()> {
        let key = &result.key;
        if !self.discovery.complete(key, result.version)
            || !self
                .dynamic_subscriptions
                .get(key)
                .is_some_and(|state| state.version == result.version)
            || self.cancellation.is_cancelled()
        {
            return Ok(());
        }
        let now_ns = self.clock.get_time_ns().as_u64();
        // Every completion, including errors, gets a future deadline. Never catch up old ticks.
        self.dynamic_subscriptions
            .get_mut(key)
            .expect("validated selector")
            .next_refresh_ns =
            now_ns.saturating_add(self.dynamic_refresh_secs.saturating_mul(1_000_000_000));
        let (selected, market) = result
            .result
            .with_context(|| format!("Predict discovery failed for {key}"))?;
        if let Some(deadline) = inactive_window_deadline(
            selected.window.start_ns,
            selected.window.end_ns,
            now_ns,
            self.dynamic_refresh_secs,
        ) {
            self.dynamic_subscriptions
                .get_mut(key)
                .expect("validated selector")
                .next_refresh_ns = deadline;
            return Ok(()); // Neither stale nor future windows may activate.
        }
        let (current_market_id, pending_market_id) = self
            .dynamic_subscriptions
            .get(key)
            .map(|state| (state.current_market_id, state.pending_market_id))
            .context("Predict dynamic selector disappeared during discovery")?;
        let data_type = crate::predict_crypto_up_down_orderbook_data_type(selected.market_id, key);
        // Reject before publishing definitions or releasing any pending handover.
        self.ensure_custom_data_type(selected.market_id, &data_type)?;
        if current_market_id == Some(selected.market_id) {
            // A transient discovery regression can select the confirmed market while a previous
            // successor is still pending. The confirmed market remains valid, but the stale
            // successor must not keep a socket subscription or registry entry alive.
            if let Some(stale_pending) = pending_market_id
                && Some(stale_pending) != current_market_id
            {
                self.unsubscribe_market(stale_pending).await?;
                if let Some(state) = self.dynamic_subscriptions.get_mut(key)
                    && state.pending_market_id == Some(stale_pending)
                {
                    state.pending_market_id = None;
                }
                self.remove_dynamic_market_if_unused(stale_pending);
            }
            self.set_dynamic_refresh_deadline(key, selected.window.end_ns, now_ns, false)?;
            return Ok(());
        }
        if pending_market_id == Some(selected.market_id) {
            // A cached initial snapshot is best-effort. Repeated REST queries cannot make
            // it appear; retain the subscription and let network reconnect replay it.
            self.set_dynamic_refresh_deadline(key, selected.window.end_ns, now_ns, false)?;
            return Ok(());
        }
        if let Some(stale_pending) = pending_market_id {
            self.unsubscribe_market(stale_pending).await?;
        }
        let market =
            market.context("Predict discovery result is missing new market definitions")?;
        let (context, instruments) = instruments_from_market(&market, self.clock.get_time_ns())?;
        let registered = self
            .dynamic_registry
            .write()
            .expect("Predict dynamic registry lock poisoned")
            .register(self.dynamic_generation, context.clone(), &instruments);
        if !registered {
            // The client was stopped or reset while the REST request was in flight. Do not
            // publish a stale definition into the next Nautilus client generation.
            return Ok(());
        }
        for instrument in instruments {
            self.data_sender
                .send(DataEvent::Data(Data::Instrument(Box::new(instrument))))?;
        }
        self.subscribe_custom_market(selected.market_id, data_type)
            .await?;
        let state = self
            .dynamic_subscriptions
            .get_mut(key)
            .context("Predict dynamic selector disappeared before activation")?;
        state.pending_market_id = Some(selected.market_id);
        state.next_refresh_ns = selected.window.end_ns;
        if let Some(stale_pending) = pending_market_id {
            self.remove_dynamic_market_if_unused(stale_pending);
        }
        Ok(())
    }

    pub(super) fn set_dynamic_refresh_deadline(
        &mut self,
        key: &CryptoUpDownSelectorKey,
        window_end_ns: u64,
        now_ns: u64,
        retry_pending: bool,
    ) -> anyhow::Result<()> {
        let state = self
            .dynamic_subscriptions
            .get_mut(key)
            .with_context(|| format!("missing Predict dynamic selector {key}"))?;
        state.next_refresh_ns = next_dynamic_refresh_ns(
            window_end_ns,
            now_ns,
            self.dynamic_refresh_secs,
            retry_pending,
        );
        Ok(())
    }

    pub(super) async fn stop_dynamic_subscription(
        &mut self,
        key: &CryptoUpDownSelectorKey,
    ) -> anyhow::Result<()> {
        self.discovery.cancel(key);
        let Some(state) = self.dynamic_subscriptions.remove(key) else {
            return Ok(());
        };
        if let Some(market_id) = state.pending_market_id {
            self.unsubscribe_market(market_id).await?;
        }
        if let Some(market_id) = state.current_market_id
            && Some(market_id) != state.pending_market_id
        {
            self.unsubscribe_market(market_id).await?;
        }
        for market_id in [state.pending_market_id, state.current_market_id]
            .into_iter()
            .flatten()
        {
            self.remove_dynamic_market_if_unused(market_id);
        }
        Ok(())
    }

    pub(super) fn remove_dynamic_market_if_unused(&self, market_id: u64) {
        let still_referenced = self.dynamic_subscriptions.values().any(|state| {
            state.current_market_id == Some(market_id) || state.pending_market_id == Some(market_id)
        });
        let still_referenced = still_referenced
            || self.subscriptions.contains_key(&market_id)
            || self
                .trades
                .values()
                .any(|context| context.market_id == market_id);
        if !still_referenced {
            self.dynamic_registry
                .write()
                .expect("Predict dynamic registry lock poisoned")
                .remove(self.dynamic_generation, market_id);
        }
    }
}
impl PredictDataSession {
    pub(super) async fn confirm_dynamic_snapshot(&mut self, market_id: u64) -> anyhow::Result<()> {
        let ready = self
            .dynamic_subscriptions
            .iter()
            .filter_map(|(key, state)| {
                (state.pending_market_id == Some(market_id))
                    .then_some((key.clone(), state.current_market_id))
            })
            .collect::<Vec<_>>();
        for (key, previous_market_id) in ready {
            if let Some(previous_market_id) = previous_market_id {
                self.unsubscribe_market(previous_market_id).await?;
            }
            if let Some(state) = self.dynamic_subscriptions.get_mut(&key) {
                state.current_market_id = Some(market_id);
                state.pending_market_id = None;
            }
            if let Some(previous_market_id) = previous_market_id {
                self.remove_dynamic_market_if_unused(previous_market_id);
            }
        }
        Ok(())
    }
}
pub(super) async fn run_discovery_request(
    key: CryptoUpDownSelectorKey,
    version: u64,
    cancellation: CancellationToken,
    sender: mpsc::Sender<DiscoveryResult>,
    timeout: Duration,
    request: impl std::future::Future<
        Output = anyhow::Result<(SelectedCryptoUpDownMarket, Option<PredictMarket>)>,
    >,
) {
    let result = tokio::select! {
        () = cancellation.cancelled() => return,
        result = tokio::time::timeout(timeout, request) => result
            .unwrap_or_else(|_| Err(anyhow::anyhow!("Predict discovery deadline exceeded"))),
    };
    tokio::select! {
        () = cancellation.cancelled() => {},
        _ = sender.send(DiscoveryResult { key, version, result }) => {},
    }
}

pub(super) fn dynamic_selector_key(
    selector: &PredictCryptoUpDownSelector,
) -> anyhow::Result<CryptoUpDownSelectorKey> {
    CryptoUpDownSelectorKey::new(
        &selector.price_feed_symbol,
        &selector.title_asset,
        selector.interval_secs,
    )
}

fn next_dynamic_refresh_ns(
    window_end_ns: u64,
    now_ns: u64,
    retry_secs: u64,
    retry_failed: bool,
) -> u64 {
    if retry_failed {
        now_ns.saturating_add(retry_secs.saturating_mul(1_000_000_000))
    } else {
        window_end_ns.max(now_ns)
    }
}

fn inactive_window_deadline(
    start_ns: u64,
    end_ns: u64,
    now_ns: u64,
    retry_secs: u64,
) -> Option<u64> {
    if end_ns <= now_ns {
        Some(next_dynamic_refresh_ns(0, now_ns, retry_secs, true))
    } else if start_ns > now_ns {
        Some(start_ns)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ahash::AHashMap;
    use nautilus_live::task::TaskGroup;
    use tokio::sync::{mpsc, oneshot};
    use tokio_util::sync::CancellationToken;

    use super::{
        DiscoveryScheduler, DynamicSubscription, MAX_DISCOVERY_REQUESTS, dynamic_selector_key,
        inactive_window_deadline, next_dynamic_refresh_ns, run_discovery_request,
    };
    use crate::PredictCryptoUpDownSelector;

    fn subscriptions() -> AHashMap<crate::common::CryptoUpDownSelectorKey, DynamicSubscription> {
        let mut states = AHashMap::new();
        for (symbol, asset) in [
            ("BTC/USDT", "Bitcoin"),
            ("ETH/USDT", "Ethereum"),
            ("BNB/USDT", "BNB"),
        ] {
            for interval_secs in [300, 900] {
                let selector = PredictCryptoUpDownSelector {
                    price_feed_symbol: symbol.into(),
                    title_asset: asset.into(),
                    interval_secs,
                };
                let key = dynamic_selector_key(&selector).unwrap();
                states.insert(
                    key,
                    DynamicSubscription {
                        owners: ["test-owner".to_string()].into(),
                        selector,
                        version: 1,
                        current_market_id: None,
                        pending_market_id: None,
                        next_refresh_ns: 100,
                    },
                );
            }
        }
        states
    }

    #[test]
    fn six_selectors_are_bounded_deduplicated_and_fair_after_failure() {
        let mut scheduler = DiscoveryScheduler::default();
        let mut states = subscriptions();
        assert!(scheduler.due_keys(&states, 99).is_empty());
        let first = scheduler.due_keys(&states, 100);
        assert_eq!(first.len(), MAX_DISCOVERY_REQUESTS);
        for key in &first {
            scheduler
                .inflight
                .insert(key.clone(), (1, CancellationToken::new()));
        }
        assert!(scheduler.due_keys(&states, 100).is_empty());
        // Even an unsuccessful pass releases its slot, and waits from completion rather
        // than immediately replaying deadlines missed during slow REST work.
        for key in &first {
            assert!(scheduler.complete(key, 1));
            states.get_mut(key).unwrap().next_refresh_ns = next_dynamic_refresh_ns(0, 500, 1, true);
        }
        let remaining = scheduler.due_keys(&states, 500);
        assert_eq!(remaining.len(), 2);
        assert!(remaining.iter().all(|key| !first.contains(key)));
        assert_eq!(scheduler.due_keys(&states, 1_000_000_500).len(), 4);
    }

    #[test]
    fn cancelled_selector_result_cannot_complete_its_replacement() {
        let mut scheduler = DiscoveryScheduler::default();
        let key = subscriptions().into_keys().next().unwrap();
        let first = scheduler.version().unwrap();
        let cancelled = CancellationToken::new();
        scheduler
            .inflight
            .insert(key.clone(), (first, cancelled.clone()));
        scheduler.cancel(&key);
        assert!(cancelled.is_cancelled());
        let second = scheduler.version().unwrap();
        scheduler
            .inflight
            .insert(key.clone(), (second, CancellationToken::new()));
        assert!(!scheduler.complete(&key, first));
        assert_eq!(scheduler.inflight[&key].0, second);
        assert!(scheduler.complete(&key, second));
        assert!(!scheduler.complete(&key, second));
    }

    #[test]
    fn completed_rest_cannot_activate_future_or_expired_window() {
        assert_eq!(inactive_window_deadline(100, 200, 99, 1), Some(100));
        assert_eq!(inactive_window_deadline(100, 200, 100, 1), None);
        assert_eq!(inactive_window_deadline(100, 200, 199, 1), None);
        assert_eq!(
            inactive_window_deadline(100, 200, 200, 1),
            Some(1_000_000_200)
        );
        assert_eq!(
            inactive_window_deadline(100, 200, 500, 1),
            Some(1_000_000_500)
        );
    }

    #[tokio::test]
    async fn stalled_discovery_has_a_bounded_overall_deadline() {
        let (tx, mut rx) = mpsc::channel(1);
        let key = subscriptions().into_keys().next().unwrap();
        run_discovery_request(
            key,
            1,
            CancellationToken::new(),
            tx,
            Duration::ZERO,
            std::future::pending(),
        )
        .await;
        let result = rx.recv().await.unwrap();
        assert_eq!(
            result.result.unwrap_err().to_string(),
            "Predict discovery deadline exceeded"
        );
    }

    #[tokio::test]
    async fn blocked_discovery_yields_to_events_and_cancels_with_its_generation() {
        let tasks = TaskGroup::new();
        let (result_tx, mut result_rx) = mpsc::channel(4);
        let (started_tx, started_rx) = oneshot::channel();
        let key = subscriptions().into_keys().next().unwrap();
        tasks
            .spawner()
            .unwrap()
            .spawn(run_discovery_request(
                key,
                1,
                tasks.cancellation_token(),
                result_tx,
                Duration::from_secs(10),
                async move {
                    started_tx.send(()).unwrap();
                    std::future::pending().await
                },
            ))
            .unwrap();
        started_rx.await.unwrap();
        let (event_tx, mut event_rx) = mpsc::channel(3);
        for event in ["snapshot", "heartbeat", "unsubscribe"] {
            event_tx.send(event).await.unwrap();
        }
        // REST remains blocked while the receiver can observe all three non-empty events.
        for expected in ["snapshot", "heartbeat", "unsubscribe"] {
            assert_eq!(event_rx.recv().await, Some(expected));
        }
        tasks.begin_shutdown();
        tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(1))
            .await
            .unwrap();
        assert!(result_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn discovery_errors_are_delivered_without_failing_the_task_generation() {
        let tasks = TaskGroup::new();
        let (tx, mut rx) = mpsc::channel(1);
        let key = subscriptions().into_keys().next().unwrap();
        tasks
            .spawner()
            .unwrap()
            .spawn(run_discovery_request(
                key.clone(),
                7,
                tasks.cancellation_token(),
                tx,
                Duration::from_secs(1),
                async { Err(anyhow::anyhow!("venue unavailable")) },
            ))
            .unwrap();
        let result = rx.recv().await.unwrap();
        assert_eq!(result.key, key);
        assert_eq!(result.version, 7);
        assert_eq!(result.result.unwrap_err().to_string(), "venue unavailable");
        tasks.begin_shutdown();
        tasks
            .finish_shutdown(Duration::from_secs(1), Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[test]
    fn refreshes_at_window_boundary_and_retries_failed_discovery() {
        let now_ns = 100 * 1_000_000_000;
        let end_ns = 300 * 1_000_000_000;
        assert_eq!(next_dynamic_refresh_ns(end_ns, now_ns, 1, false), end_ns);
        assert_eq!(
            next_dynamic_refresh_ns(end_ns, now_ns, 1, true),
            now_ns + 1_000_000_000
        );
    }

    #[test]
    fn selector_key_deduplicates_title_casing_and_whitespace() {
        let canonical = PredictCryptoUpDownSelector {
            price_feed_symbol: "BTC/USDT".to_string(),
            title_asset: "Bitcoin".to_string(),
            interval_secs: 300,
        };
        let equivalent = PredictCryptoUpDownSelector {
            price_feed_symbol: " BTC/USDT ".to_string(),
            title_asset: " bitcoin ".to_string(),
            interval_secs: 300,
        };
        assert_eq!(
            dynamic_selector_key(&canonical).unwrap(),
            dynamic_selector_key(&equivalent).unwrap()
        );
    }

    #[test]
    fn selector_key_deduplicates_feed_separator_and_case_aliases() {
        let canonical = PredictCryptoUpDownSelector {
            price_feed_symbol: "BTC/USDT".to_string(),
            title_asset: "Bitcoin".to_string(),
            interval_secs: 300,
        };
        let equivalent = PredictCryptoUpDownSelector {
            price_feed_symbol: "btcusdt".to_string(),
            title_asset: "BITCOIN".to_string(),
            interval_secs: 300,
        };
        assert_eq!(
            dynamic_selector_key(&canonical).unwrap(),
            dynamic_selector_key(&equivalent).unwrap()
        );
    }
}
