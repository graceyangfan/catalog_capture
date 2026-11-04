// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Current-window discovery for HIP-4; the Native adapter owns every market-data stream.
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::mpsc,
};

use anyhow::{Context, Result};
use catalog_capture_core::{
    BookDeltasCaptureSpec, CapturePlan, InstrumentCaptureSpec, TradeCaptureSpec,
    hip4::rolling::{Hip4RollingConfig, Hip4RollingMarket},
};
use nautilus_core::{Params, UnixNanos};
use nautilus_hyperliquid::{
    common::enums::HyperliquidEnvironment,
    http::{
        client::HyperliquidRawHttpClient,
        models::OutcomeMeta,
        parse::{create_instrument_from_def, parse_outcome_instruments},
        query::InfoRequest,
    },
};
use nautilus_model::{
    enums::BookType,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use serde_json::{Value, json};

#[derive(Debug)]
pub(crate) struct PendingDiscovery {
    pub receiver: mpsc::Receiver<std::result::Result<Value, String>>,
    cancel: Option<tokio::sync::oneshot::Sender<()>>,
}
impl Drop for PendingDiscovery {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}
/// Dropping the receiver cancels the request and isolates a stopped actor from late results.
pub(crate) fn discover() -> Result<PendingDiscovery> {
    let (sender, receiver) = mpsc::sync_channel(1);
    let (cancel, cancelled) = tokio::sync::oneshot::channel();
    std::thread::Builder::new().name("hip4-discovery".into()).spawn(move || {
        let result = (|| -> Result<Value> {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
            runtime.block_on(async move {
                let client = HyperliquidRawHttpClient::new(HyperliquidEnvironment::Mainnet, 10, None)?;
                let request = InfoRequest::outcome_meta();
                tokio::select! {
                    result = tokio::time::timeout(std::time::Duration::from_secs(10), client.send_info_request_raw(&request)) =>
                        result.context("HIP-4 discovery request timed out")?.map_err(Into::into),
                    _ = cancelled => anyhow::bail!("HIP-4 discovery cancelled"),
                }
            })
        })();
        let _ = sender.send(result.map_err(|e| format!("{e:#}")));
    }).context("failed to spawn HIP-4 discovery")?;
    Ok(PendingDiscovery {
        receiver,
        cancel: Some(cancel),
    })
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Series {
    pub current: Option<u32>,
    pub pending: Option<u32>,
    pub retry_after: u64,
}
#[derive(Debug)]
pub(crate) struct Recording {
    pub market: Hip4RollingMarket,
    pub definitions: Vec<InstrumentAny>,
}
impl Recording {
    pub fn ids(&self) -> impl Iterator<Item = InstrumentId> + '_ {
        self.definitions.iter().map(Instrument::id)
    }
}
#[derive(Debug, Default)]
pub(crate) struct Hip4RollingState {
    pub config: Option<Hip4RollingConfig>,
    pub series: BTreeMap<String, Series>,
    pub markets: BTreeMap<u32, Recording>,
    pub ready: BTreeSet<InstrumentId>,
    pub retiring: BTreeSet<InstrumentId>,
    pub definitions_recorded: BTreeSet<InstrumentId>,
    pub discovery: Option<PendingDiscovery>,
    pub next_discovery_ns: u64,
    pub instrument_request: Option<(nautilus_core::UUID4, u64)>,
}
impl Hip4RollingState {
    pub fn plan(&self) -> CapturePlan {
        let mut plan = CapturePlan::default();
        for market in self.markets.values() {
            for id in market.ids() {
                plan.instruments
                    .push(InstrumentCaptureSpec { instrument_id: id });
                plan.trades.push(TradeCaptureSpec { instrument_id: id });
                plan.book_deltas.push(BookDeltasCaptureSpec {
                    instrument_id: id,
                    book_type: BookType::L2_MBP,
                    depth: None,
                });
            }
        }
        plan
    }
    pub fn definition(&self, id: InstrumentId) -> Option<InstrumentAny> {
        self.markets
            .values()
            .flat_map(|m| &m.definitions)
            .find(|i| i.id() == id)
            .cloned()
    }
    pub fn recording(market: Hip4RollingMarket, now: UnixNanos) -> Result<Recording> {
        let meta: OutcomeMeta =
            serde_json::from_value(json!({"outcomes":[market.raw.clone()],"questions":[]}))?;
        let defs = parse_outcome_instruments(&meta).map_err(anyhow::Error::msg)?;
        let mut definitions = Vec::new();
        for def in defs {
            let mut instrument = create_instrument_from_def(&def, now)
                .context("HIP-4 Native instrument conversion failed")?;
            let InstrumentAny::BinaryOption(option) = &mut instrument else {
                anyhow::bail!("expected HIP-4 binary option");
            };
            // Local capture metadata enrichment compensates for the pinned adapter's expiry-only
            // parser without changing its identifiers, precision, quote token or wire registry.
            option.expiration_ns = market.end_ns.into();
            option.activation_ns = market
                .end_ns
                .saturating_sub(catalog_capture_core::hip4::rolling::ROLLING_INTERVAL_NS)
                .into();
            let mut info = option.info.as_deref().cloned().unwrap_or_default();
            info.insert("hip4_raw".into(), market.raw.clone());
            info.insert("capture_interval_secs".into(), json!(900));
            info.insert("capture_schedule_source".into(), json!("out_quarter_hour"));
            option.info = Some(Params::new(info));
            definitions.push(instrument);
        }
        anyhow::ensure!(
            definitions.len() == 2,
            "HIP-4 requires both outcome definitions"
        );
        Ok(Recording {
            market,
            definitions,
        })
    }
}
