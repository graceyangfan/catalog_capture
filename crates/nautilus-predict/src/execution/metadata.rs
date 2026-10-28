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

//! Prepared market definitions and account/contract signing nonces.

use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use ahash::AHashMap;
use alloy::primitives::{Address, U256};
use anyhow::Context;
use futures_util::{StreamExt, stream};
use nautilus_core::{AtomicMap, UnixNanos, time::AtomicTime};
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::websocket::AuthTracker;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::{
    blockchain::PredictChainClient,
    common::{contracts::PredictExchangeType, parse_market_id},
    config::PredictExecutionClientConfig,
    http::market_models::PredictExecutionMarket,
};

// Prepared signing inputs have one client account/environment owner. Cash reporting
// does not own or refresh these values. Instruments remain Native-derived views.
#[derive(Debug, Default)]
pub(super) struct ExecutionMetadata {
    pub instruments: Definitions,
    pub notify: Arc<Notify>,
    // Submission holds this guard through correlation registration, so an
    // invalidation cannot race between the final check and that registration.
    pub nonces: Mutex<[Option<U256>; 4]>,
    nonce_notify: Notify,
}

impl ExecutionMetadata {
    pub fn nonce(&self, exchange: PredictExchangeType) -> Option<U256> {
        self.nonces.lock().expect("signing nonces poisoned")[exchange.cache_index()]
    }

    pub fn invalidate_nonce(&self, exchange: PredictExchangeType, value: U256) {
        let mut nonces = self.nonces.lock().expect("signing nonces poisoned");
        if nonces[exchange.cache_index()] == Some(value) {
            nonces[exchange.cache_index()] = None;
            self.nonce_notify.notify_one();
        }
    }

    pub fn reset_nonces(&self) {
        *self.nonces.lock().expect("signing nonces poisoned") = [None; 4];
    }

    async fn refresh_nonces(&self, account: Address, chain: &PredictChainClient) {
        let prepared = *self.nonces.lock().expect("signing nonces poisoned");
        // Publish each verified contract independently within the metadata task.
        stream::iter(
            PredictExchangeType::ALL
                .into_iter()
                .filter(|exchange| prepared[exchange.cache_index()].is_none()),
        )
        .for_each_concurrent(4, |exchange| async move {
            match chain.request_signing_nonce(account, exchange).await {
                Ok(value) => {
                    let mut nonces = self.nonces.lock().expect("signing nonces poisoned");
                    let index = exchange.cache_index();
                    if nonces[index] == prepared[index] {
                        nonces[index] = Some(value);
                    }
                }
                Err(error) => {
                    log::warn!("Predict signing nonce refresh failed for {exchange:?}: {error}")
                }
            }
        })
        .await;
    }

    async fn run_nonces(
        &self,
        account: Address,
        chain: &PredictChainClient,
        cancellation: &CancellationToken,
    ) {
        // Retry only missing slots; valid contract nonces have no time-based expiry.
        let mut retry = 1;
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                () = self.refresh_nonces(account, chain) => {},
            }
            let missing = self
                .nonces
                .lock()
                .expect("signing nonces poisoned")
                .iter()
                .any(Option::is_none);
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                _ = self.nonce_notify.notified() => retry = 1,
                _ = tokio::time::sleep(Duration::from_secs(retry)), if missing => {
                    retry = (retry * 2).min(30);
                }
            }
        }
    }

    pub async fn run(
        self: Arc<Self>,
        config: PredictExecutionClientConfig,
        chain: Option<PredictChainClient>,
        clock: &'static AtomicTime,
        epoch: Arc<AtomicU64>,
        auth: AuthTracker,
        cancellation: CancellationToken,
    ) {
        tokio::join!(
            self.run_markets(&config, clock, &epoch, &auth, &cancellation),
            async {
                if let Some(chain) = chain.as_ref() {
                    self.run_nonces(config.session.account_address(), chain, &cancellation)
                        .await;
                }
            },
        );
    }
}

type Definitions = Arc<AtomicMap<InstrumentId, ExecutionInstrument>>;

// Market metadata follows the Native instrument definition through replacement and purge.
#[derive(Clone, Debug)]
pub(super) struct ExecutionInstrument {
    pub instrument: InstrumentAny,
    pub market: Arc<Mutex<Option<MarketMetadata>>>,
    pub market_loading: Arc<tokio::sync::Mutex<()>>,
}

impl From<InstrumentAny> for ExecutionInstrument {
    fn from(instrument: InstrumentAny) -> Self {
        Self {
            instrument,
            market: Default::default(),
            market_loading: Default::default(),
        }
    }
}

// YES and NO share one market response. Definition replacement detaches the old slot.
pub(super) fn upsert_instrument(
    map: &mut AHashMap<InstrumentId, ExecutionInstrument>,
    instrument: InstrumentAny,
) {
    let entry = ExecutionInstrument::from(instrument);
    if let Ok(market_id) = parse_market_id(&entry.instrument.id()) {
        for outcome in [1, 2] {
            if let Ok(id) = crate::common::instrument_id_from_outcome(market_id, outcome)
                && let Some(existing) = map.get_mut(&id)
            {
                existing.market = Arc::clone(&entry.market);
                existing.market_loading = Arc::clone(&entry.market_loading);
            }
        }
    }
    map.insert(entry.instrument.id(), entry);
}

#[derive(Clone, Debug)]
pub(super) struct MarketMetadata {
    pub market: Arc<PredictExecutionMarket>,
    pub observed: UnixNanos,
}

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
            market.id == parse_market_id(&self.instrument.id())?,
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
        now: UnixNanos,
        max_age_ns: u64,
    ) -> anyhow::Result<Option<MarketMetadata>> {
        let cached = self.market.lock().expect("market metadata poisoned");
        if let Some(value) = cached.as_ref()
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
        if let Some(value) = self.cached_market(clock.get_time_ns(), max_age_ns)? {
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
            Duration::from_secs(config.request_timeout_secs),
            config
                .session
                .request_execution_market(parse_market_id(&self.instrument.id())?),
        )
        .await
        .context("Predict execution market collection timed out")??;
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
    if config.submission_signer.is_none() {
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
                && parse_market_id(&entry.instrument.id()).is_ok_and(|id| seen.insert(id))
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

impl ExecutionMetadata {
    async fn run_markets(
        &self,
        config: &PredictExecutionClientConfig,
        clock: &'static AtomicTime,
        epoch: &Arc<AtomicU64>,
        auth: &AuthTracker,
        cancellation: &CancellationToken,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(
            (config.market_metadata_ttl_secs * 500).max(1),
        ));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                _ = self.notify.notified() => {},
                _ = interval.tick() => {},
            }
            if !auth.is_authenticated() {
                continue;
            }
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                () = prepare(config, &self.instruments, clock, epoch) => {},
            }
        }
    }
}
