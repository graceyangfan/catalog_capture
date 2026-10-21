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

//! Prepare the existing instrument-owned market slot outside normal order submission.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use alloy::primitives::U256;
use futures_util::{StreamExt, stream};
use nautilus_core::{AtomicMap, UnixNanos, time::AtomicTime};
use nautilus_model::{identifiers::InstrumentId, instruments::Instrument};
use nautilus_network::websocket::AuthTracker;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::types::{ExecutionInstrument, MarketMetadata};
use crate::{config::PredictExecutionClientConfig, http::market_models::PredictExecutionMarket};

type Definitions = Arc<AtomicMap<InstrumentId, ExecutionInstrument>>;

impl ExecutionInstrument {
    pub fn is_current(&self, definitions: &Definitions) -> bool {
        definitions
            .load()
            .get(&self.instrument.id())
            .is_some_and(|entry| Arc::ptr_eq(&entry.market, &self.market))
    }

    fn validate_market(&self, market: &PredictExecutionMarket) -> anyhow::Result<()> {
        let token: U256 = self.instrument.raw_symbol().as_str().parse()?;
        let outcome = if self.instrument.id().symbol.as_str().ends_with("-1") {
            1
        } else {
            2
        };
        anyhow::ensure!(
            market.id == super::parse::market_id_from_instrument(&self.instrument.id())?,
            "Predict market identity differs from the native instrument"
        );
        anyhow::ensure!(
            market.trading_status == "OPEN",
            "Predict market is not open"
        );
        anyhow::ensure!(
            market.fee_rate_bps <= 10_000,
            "Predict market fee exceeds supported range"
        );
        anyhow::ensure!(
            market.decimal_precision == self.instrument.price_precision()
                && market.outcomes.len() == 2
                && market
                    .outcomes
                    .iter()
                    .filter(|o| o.index_set == outcome && o.on_chain_id == token)
                    .count()
                    == 1,
            "Predict market outcome/token/precision differs from the native instrument"
        );
        Ok(())
    }

    pub fn cached_market(
        &self,
        epoch: u64,
        now: UnixNanos,
        max_age_ns: u64,
    ) -> anyhow::Result<Option<MarketMetadata>> {
        let cached = self.market.lock().expect("market metadata poisoned");
        if let Some(value) = cached.as_ref()
            && value.epoch == epoch
            && now
                .as_u64()
                .checked_sub(value.observed.as_u64())
                .is_some_and(|age| age < max_age_ns)
        {
            return Ok(Some(value.clone()));
        }
        Ok(None)
    }

    pub async fn prepare_market(
        &self,
        config: &PredictExecutionClientConfig,
        clock: &'static AtomicTime,
        epoch: &Arc<AtomicU64>,
        definitions: &Definitions,
        max_age_ns: u64,
    ) -> anyhow::Result<MarketMetadata> {
        let _loading = self.market_loading.lock().await;
        let expected_epoch = epoch.load(Ordering::Acquire);
        anyhow::ensure!(
            self.is_current(definitions),
            "Predict market definition was replaced before collection"
        );
        if let Some(value) = self.cached_market(expected_epoch, clock.get_time_ns(), max_age_ns)? {
            return Ok(value);
        }
        // Keep a still-valid value available while background refresh is in flight.
        // Observing the start bounds reuse even if HTTP is slow.
        let observed = clock.get_time_ns();
        let timer = config.latency.as_ref().map(|probe| {
            probe.count(crate::latency::LatencyCounter::MetadataRequests);
            probe.timer(crate::latency::LatencyMetric::MarketGet)
        });
        let market = tokio::time::timeout(
            Duration::from_secs(config.report_timeout_secs),
            config
                .session
                .request_execution_market(super::parse::market_id_from_instrument(
                    &self.instrument.id(),
                )?),
        )
        .await
        .map_err(|_| anyhow::anyhow!("Predict execution market collection timed out"))??;
        if let Some(timer) = timer {
            timer.finish();
        }
        anyhow::ensure!(
            epoch.load(Ordering::Acquire) == expected_epoch && self.is_current(definitions),
            "Predict market collection lost its instrument or wallet generation"
        );
        let validated = (|| -> anyhow::Result<()> {
            self.validate_market(&market)?;
            let definitions = definitions.load();
            for outcome in [1, 2] {
                let id = crate::common::instrument_id_from_outcome(market.id, outcome)?;
                if let Some(entry) = definitions.get(&id)
                    && entry.instrument.id() != self.instrument.id()
                    && Arc::ptr_eq(&entry.market, &self.market)
                {
                    entry.validate_market(&market)?;
                }
            }
            Ok(())
        })();
        if let Err(error) = validated {
            // A definitive new status/fee/identity supersedes the previous value.
            // Transport errors above can retain it only until its original hard TTL.
            *self.market.lock().expect("market metadata poisoned") = None;
            return Err(error);
        }
        anyhow::ensure!(
            clock
                .get_time_ns()
                .as_u64()
                .checked_sub(observed.as_u64())
                .is_some_and(|age| age < config.market_metadata_ttl_secs * 1_000_000_000),
            "Predict market metadata expired during collection"
        );
        let value = MarketMetadata {
            market: Arc::new(market),
            observed,
            epoch: expected_epoch,
        };
        *self.market.lock().expect("market metadata poisoned") = Some(value.clone());
        Ok(value)
    }
}

pub(super) async fn prepare(
    config: &PredictExecutionClientConfig,
    definitions: &Definitions,
    clock: &'static AtomicTime,
    epoch: &Arc<AtomicU64>,
) {
    if config.submission.is_none() {
        return;
    }
    let mut seen = HashSet::new();
    let entries = definitions
        .load()
        .values()
        .filter(|entry| {
            entry
                .instrument
                .expiration_ns()
                .is_some_and(|expiry| expiry > clock.get_time_ns())
                && super::parse::market_id_from_instrument(&entry.instrument.id())
                    .is_ok_and(|id| seen.insert(id))
        })
        .cloned()
        .collect::<Vec<_>>();
    let max_age = config.market_metadata_ttl_secs * 1_000_000_000 / 2;
    // Only independent markets overlap. YES/NO share the same background load lock.
    stream::iter(entries)
        .for_each_concurrent(8, |entry| async move {
            if let Err(error) = entry
                .prepare_market(config, clock, epoch, definitions, max_age)
                .await
            {
                log::warn!(
                    "Predict market {} preparation deferred: {error}",
                    entry.instrument.id()
                );
            }
        })
        .await;
}

pub(super) async fn run(
    config: PredictExecutionClientConfig,
    definitions: Definitions,
    clock: &'static AtomicTime,
    epoch: Arc<AtomicU64>,
    auth: AuthTracker,
    notify: Arc<Notify>,
    cancellation: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(
        (config.market_metadata_ttl_secs * 500).max(1),
    ));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await;
    loop {
        tokio::select! { biased;
            _ = cancellation.cancelled() => return,
            _ = notify.notified() => {},
            _ = interval.tick() => {},
        }
        if !auth.is_authenticated() {
            continue;
        }
        tokio::select! { biased;
            _ = cancellation.cancelled() => return,
            () = prepare(&config, &definitions, clock, &epoch) => {},
        }
    }
}
