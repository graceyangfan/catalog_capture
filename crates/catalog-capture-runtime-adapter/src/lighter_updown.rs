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

//! Capture policy for public Lighter 15m markets. Venue discovery uses Native instrument requests.
use catalog_capture_core::{
    BookDeltasCaptureSpec, CapturePlan, CustomDataCaptureSpec, IndexPriceCaptureSpec,
    InstrumentCaptureSpec, TradeCaptureSpec,
};
use nautilus_core::UUID4;
use nautilus_model::{
    data::DataType,
    enums::BookType,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LighterUpDownCaptureConfig {
    pub tag: String,
    #[serde(default = "client")]
    pub client_id: String,
    #[serde(default = "refresh")]
    pub refresh_secs: u64,
    #[serde(default = "prefetch")]
    pub prefetch_secs: u64,
}
fn client() -> String {
    "LIGHTER".into()
}
const fn refresh() -> u64 {
    10
}
const fn prefetch() -> u64 {
    30
}
impl LighterUpDownCaptureConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.client_id == "LIGHTER"
                && self.tag.ends_with("-updown-15m")
                && self
                    .tag
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
            "invalid Lighter 15m selector"
        );
        anyhow::ensure!(
            (1..=60).contains(&self.refresh_secs) && (5..=300).contains(&self.prefetch_secs),
            "invalid Lighter discovery cadence/prefetch"
        );
        Ok(())
    }
}
#[derive(Debug, Default)]
pub(crate) struct LighterCaptureState {
    pub configs: Vec<LighterUpDownCaptureConfig>,
    pub markets: BTreeMap<InstrumentId, InstrumentAny>,
    pub pending: Option<UUID4>,
    pub retiring: BTreeMap<InstrumentId, InstrumentAny>,
    pub next_refresh_ns: u64,
}
impl LighterCaptureState {
    pub fn enabled(&self) -> bool {
        !self.configs.is_empty()
    }
    pub fn plan(&self) -> CapturePlan {
        let mut plan = CapturePlan::default();
        for instrument in self.markets.values() {
            let id = instrument.id();
            plan.instruments
                .push(InstrumentCaptureSpec { instrument_id: id });
            plan.trades.push(TradeCaptureSpec { instrument_id: id });
            plan.book_deltas.push(BookDeltasCaptureSpec {
                instrument_id: id,
                book_type: BookType::L2_MBP,
                depth: None,
            });
            plan.index_prices
                .push(IndexPriceCaptureSpec { instrument_id: id });
            plan.custom_data.push(CustomDataCaptureSpec {
                data_type: oi_type(instrument),
            });
        }
        plan
    }
}
pub(crate) fn oi_type(instrument: &InstrumentAny) -> DataType {
    DataType::new(
        "LighterOpenInterest",
        None,
        Some(
            instrument.info().unwrap()["market_id"]
                .as_u64()
                .unwrap()
                .to_string(),
        ),
    )
}
