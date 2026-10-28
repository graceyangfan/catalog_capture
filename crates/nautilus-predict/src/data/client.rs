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

//! Client construction, bootstrap, and command admission.

use std::sync::{Arc, RwLock, atomic::AtomicBool};

use anyhow::Context;
use nautilus_common::{
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::DataEvent,
    providers::InstrumentProvider,
};
use nautilus_core::time::{AtomicTime, get_atomic_clock_realtime};
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    data::DataType,
    identifiers::{ClientId, InstrumentId},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{instruments::PredictDynamicMarketRegistry, runtime::PredictCommand};
use crate::{
    PredictCryptoUpDownSelector, PredictDataClientConfig, PredictHttpClient,
    PredictInstrumentProvider, common::normalize_price_feed_symbol, predict_orderbook_data_type,
};

/// Nautilus data client which publishes Predict instruments and complete orderbook snapshots.
#[derive(Debug)]
pub struct PredictDataClient {
    pub(super) clock: &'static AtomicTime,
    pub(super) client_id: ClientId,
    pub(super) config: PredictDataClientConfig,
    pub(super) provider: PredictInstrumentProvider,
    pub(super) data_sender: EventSender<DataEvent>,
    pub(super) dynamic_registry: Arc<RwLock<PredictDynamicMarketRegistry>>,
    pub(super) command_tx: Option<mpsc::Sender<PredictCommand>>,
    pub(super) tasks: TaskGroup,
    pub(super) cancellation: CancellationToken,
    pub(super) connected: Arc<AtomicBool>,
}

impl PredictDataClient {
    pub fn new(client_id: ClientId, config: PredictDataClientConfig) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !config.api_key.expose_secret().trim().is_empty(),
            "Predict API key is required for REST market metadata"
        );
        anyhow::ensure!(
            config.market_ids.iter().all(|market_id| *market_id != 0),
            "Predict market IDs must be non-zero"
        );
        let mut unique_market_ids = config.market_ids.clone();
        unique_market_ids.sort_unstable();
        unique_market_ids.dedup();
        anyhow::ensure!(
            unique_market_ids.len() == config.market_ids.len(),
            "Predict market IDs must be unique"
        );

        let http_client = PredictHttpClient::new(
            config.api_key.expose_secret(),
            config.base_url_http.as_deref(),
            config.http_timeout_secs,
        )?;
        let tasks = TaskGroup::new();
        Ok(Self {
            clock: get_atomic_clock_realtime(),
            client_id,
            provider: PredictInstrumentProvider::new(http_client),
            data_sender: get_data_event_sender(),
            dynamic_registry: Arc::new(RwLock::new(PredictDynamicMarketRegistry::default())),
            command_tx: None,
            cancellation: tasks.cancellation_token(),
            tasks,
            connected: Arc::new(AtomicBool::new(false)),
            config,
        })
    }

    pub(super) fn instrument_context(
        &self,
        id: InstrumentId,
    ) -> anyhow::Result<crate::PredictMarketContext> {
        crate::common::parse_market_id(&id)
            .ok()
            .and_then(|market_id| {
                self.provider.context(market_id).cloned().or_else(|| {
                    self.dynamic_registry
                        .read()
                        .expect("Predict dynamic registry lock poisoned")
                        .context(market_id)
                })
            })
            .filter(|context| context.yes_instrument_id == id || context.no_instrument_id == id)
            .with_context(|| format!("Predict instrument {id} was not loaded"))
    }

    pub(super) async fn bootstrap_markets(&mut self) -> anyhow::Result<()> {
        self.provider.clear();
        for market_id in &self.config.market_ids {
            let (_, definitions) = self
                .provider
                .load_market_with_definitions(*market_id, self.clock.get_time_ns())
                .await
                .with_context(|| format!("failed to load Predict market {market_id}"))?;
            for instrument in definitions {
                self.data_sender.send(DataEvent::Instrument(instrument))?;
            }
        }
        Ok(())
    }

    pub(super) fn send_command(&self, command: PredictCommand) -> anyhow::Result<()> {
        let sender = self
            .command_tx
            .as_ref()
            .context("Predict WebSocket session is not connected")?;
        sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                anyhow::anyhow!("Predict subscription command queue is full")
            }
            mpsc::error::TrySendError::Closed(_) => {
                anyhow::anyhow!("Predict subscription command queue is closed")
            }
        })
    }

    pub(super) fn validate_snapshot_data_type(&self, data_type: &DataType) -> anyhow::Result<u64> {
        let market_id = Self::snapshot_market_id(data_type)?;
        anyhow::ensure!(
            self.provider.context(market_id).is_some()
                || self
                    .dynamic_registry
                    .read()
                    .expect("Predict dynamic registry lock poisoned")
                    .context(market_id)
                    .is_some(),
            "Predict market {market_id} was not bootstrapped; configure it before subscribing"
        );
        Ok(market_id)
    }

    pub(super) fn snapshot_market_id(data_type: &DataType) -> anyhow::Result<u64> {
        anyhow::ensure!(
            data_type.type_name() == "PredictOrderbookSnapshot",
            "Predict only supports PredictOrderbookSnapshot custom data"
        );
        anyhow::ensure!(
            data_type.metadata().is_none(),
            "PredictOrderbookSnapshot does not accept custom metadata"
        );
        let identifier = data_type.identifier().context(
            "PredictOrderbookSnapshot requires its decimal market ID as the data type identifier",
        )?;
        let market_id = identifier.parse::<u64>().with_context(|| {
            format!("invalid PredictOrderbookSnapshot market identifier `{identifier}`")
        })?;
        anyhow::ensure!(
            predict_orderbook_data_type(market_id).identifier() == Some(identifier),
            "PredictOrderbookSnapshot identifier must be the canonical decimal market ID"
        );
        Ok(market_id)
    }

    pub(super) fn dynamic_selector(
        data_type: &DataType,
    ) -> anyhow::Result<PredictCryptoUpDownSelector> {
        anyhow::ensure!(
            data_type.type_name() == "PredictCryptoUpDown",
            "Predict dynamic selector must be PredictCryptoUpDown"
        );
        anyhow::ensure!(
            data_type.identifier().is_none(),
            "PredictCryptoUpDown does not accept identifier; market identity is discovered at runtime"
        );
        let metadata = data_type.metadata().context(
            "PredictCryptoUpDown requires metadata.price_feed_symbol, metadata.title_asset, and metadata.interval_secs",
        )?;
        let string = |key: &str| -> anyhow::Result<String> {
            metadata
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .with_context(|| format!("PredictCryptoUpDown requires non-empty metadata.{key}"))
        };
        let interval_secs = string("interval_secs")?.parse::<u64>().with_context(
            || "PredictCryptoUpDown metadata.interval_secs must be a positive integer",
        )?;
        anyhow::ensure!(
            interval_secs > 0,
            "PredictCryptoUpDown metadata.interval_secs must be positive"
        );
        Ok(PredictCryptoUpDownSelector {
            price_feed_symbol: normalize_price_feed_symbol(&string("price_feed_symbol")?),
            title_asset: string("title_asset")?,
            interval_secs,
        })
    }

    pub(super) fn publish_instrument(&self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        let instrument = self
            .provider
            .store()
            .find(&instrument_id)
            .cloned()
            .or_else(|| {
                self.dynamic_registry
                    .read()
                    .expect("Predict dynamic registry lock poisoned")
                    .instrument(instrument_id)
            })
            .with_context(|| format!("Predict instrument {instrument_id} is not loaded"))?;
        self.data_sender.send(DataEvent::Instrument(instrument))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::Params;

    use super::*;

    #[test]
    fn control_feed_aliases_enter_the_session_as_one_canonical_feed() {
        for feed in ["BTCUSDT", "BTC/USDT", " btc/usdt "] {
            let metadata = Params::new(
                [
                    ("price_feed_symbol".into(), feed.into()),
                    ("title_asset".into(), "Bitcoin".into()),
                    ("interval_secs".into(), "300".into()),
                ]
                .into(),
            );
            let selector = PredictDataClient::dynamic_selector(&DataType::new(
                "PredictCryptoUpDown",
                Some(metadata),
                None,
            ))
            .unwrap();
            assert_eq!(selector.price_feed_symbol, "BTCUSDT");
            assert_eq!(selector.title_asset, "Bitcoin");
            assert_eq!(selector.interval_secs, 300);
        }
    }
}
