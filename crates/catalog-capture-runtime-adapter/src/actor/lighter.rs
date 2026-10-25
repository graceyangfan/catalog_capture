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

//! Expiry-driven recording: prepare the successor, seal expired partitions, release unused state.
use std::io::Write;

use anyhow::Result;
use catalog_capture_core::{PartitionKey, capture_plan_difference};
use nautilus_common::{
    actor::DataActor,
    messages::data::InstrumentsResponse,
    msgbus::{self, switchboard},
};
use nautilus_core::{DurationNanos, Params};
use nautilus_model::{
    data::{IndexPriceUpdate, OrderBookDelta, TradeTick},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::Instrument,
};
use serde_json::json;

use super::*;
use crate::lighter_updown::oi_type;

pub(super) const LIGHTER_TIMER: &str = "lighter_updown_refresh";
impl CatalogCaptureActor {
    pub(super) fn schedule_lighter(&mut self) -> Result<()> {
        if !self.lighter.enabled() {
            return Ok(());
        }
        let now = self.clock().timestamp_ns();
        let next = self
            .lighter
            .markets
            .values()
            .filter_map(Instrument::expiration_ns)
            .filter(|end| *end > now)
            .fold(now + DurationNanos::new(1_000_000_000), std::cmp::min);
        self.clock()
            .set_time_alert_ns(LIGHTER_TIMER, next, None, None)?;
        Ok(())
    }
    pub(super) fn refresh_lighter(&mut self) -> Result<()> {
        if !self.lighter.enabled() {
            return Ok(());
        }
        // A later timer turn runs after Native has applied queued unsubscribe commands.
        // Subscriber counts then describe other owners, rather than our departing handlers.
        for (id, instrument) in std::mem::take(&mut self.lighter.retiring) {
            let other = msgbus::subscriber_count_deltas(switchboard::get_book_deltas_topic(id)) > 0
                || msgbus::exact_subscriber_count_trades(switchboard::get_trades_topic(id)) > 0
                || msgbus::exact_subscriber_count_index_prices(switchboard::get_index_price_topic(
                    id,
                )) > 0
                || msgbus::subscriptions_count_any(
                    switchboard::get_custom_topic(&oi_type(&instrument)).as_str(),
                )? > 0
                || msgbus::subscriptions_count_any(switchboard::get_instrument_topic(id).as_str())?
                    > 0;
            if !other {
                self.purge_instrument_from_cache(id);
            }
            let kind = if other {
                "retained_other_owner"
            } else {
                "cache_purged"
            };
            self.lighter_audit(
                kind,
                &instrument,
                json!({
                    "cache_purged": !other,
                    "cache_contains_instrument": self.cache().instrument(&id).is_some(),
                }),
            )?;
        }
        let now = self.clock().timestamp_ns().as_u64();
        let previous = self.plan.clone();
        let expired: Vec<_> = self
            .lighter
            .markets
            .values()
            .filter(|i| i.expiration_ns().is_some_and(|end| end.as_u64() <= now))
            .cloned()
            .collect();
        for instrument in &expired {
            self.lighter.markets.remove(&instrument.id());
        }
        if !expired.is_empty() {
            self.sync_plan_state();
            let remove = capture_plan_difference(&previous, &self.plan);
            self.unsubscribe_plan(&remove);
            for instrument in expired {
                let id = instrument.id();
                self.seal_lighter_partitions(&remove, id)?;
                if !self.plan.planned_instrument_ids().contains(&id) {
                    self.forget_instrument_runtime_state(id);
                    self.lighter.retiring.insert(id, instrument.clone());
                    self.lighter_audit(
                        "expired",
                        &instrument,
                        json!({
                            "cache_purge_pending": true,
                            "active_market_count": self.lighter.markets.len(),
                        }),
                    )?;
                } else {
                    self.lighter_audit(
                        "expired_static_owner_retained",
                        &instrument,
                        json!({"cache_purged":false}),
                    )?;
                }
            }
            self.lighter.next_refresh_ns = 0;
        }
        if self.lighter.pending.is_none() && now >= self.lighter.next_refresh_ns {
            let params = Params::new(
                [
                    (
                        "updown_tags".into(),
                        json!(
                            self.lighter
                                .configs
                                .iter()
                                .map(|c| c.tag.clone())
                                .collect::<Vec<_>>()
                        ),
                    ),
                    (
                        "prefetch_secs".into(),
                        json!(
                            self.lighter
                                .configs
                                .iter()
                                .map(|c| c.prefetch_secs)
                                .max()
                                .unwrap()
                        ),
                    ),
                ]
                .into(),
            );
            let request = self.request_instruments(
                Some(Venue::new("LIGHTER")),
                None,
                None,
                Some(ClientId::new("LIGHTER")),
                Some(params),
            )?;
            self.lighter.pending = Some(request);
            self.lighter.next_refresh_ns = now
                + self
                    .lighter
                    .configs
                    .iter()
                    .map(|c| c.refresh_secs)
                    .min()
                    .unwrap()
                    * 1_000_000_000;
        }
        Ok(())
    }
    pub(super) fn apply_lighter_response(&mut self, response: &InstrumentsResponse) -> Result<()> {
        self.lighter.pending = None;
        if let Some(error) = response
            .params
            .as_ref()
            .and_then(|p| p.get("lighter_error"))
        {
            log::warn!("Lighter capture discovery retry: {error}");
            return Ok(());
        }
        let now = self.clock().timestamp_ns().as_u64();
        for instrument in &response.data {
            if instrument
                .expiration_ns()
                .is_none_or(|end| end.as_u64() <= now)
            {
                continue;
            }
            let Some(info) = instrument.info() else {
                continue;
            };
            let tag = info
                .get("event_tag")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            if instrument.id().venue.as_str() != "LIGHTER"
                || !self.lighter.configs.iter().any(|c| c.tag == tag)
            {
                continue;
            }
            let id = instrument.id();
            let market_id = info
                .get("market_id")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| anyhow::anyhow!("Lighter instrument missing numeric market_id"))?;
            anyhow::ensure!(
                id.symbol.as_str() == format!("{market_id}-BINARY"),
                "Lighter instrument identity does not match market_id"
            );
            let previous = self.lighter.markets.get(&id);
            if previous.is_some_and(|old| old.info() == instrument.info()) {
                continue;
            }
            let is_new = previous.is_none();
            let previous_plan = self.plan.clone();
            self.lighter.markets.insert(id, instrument.clone());
            self.sync_plan_state();
            self.submit_instrument(instrument.clone())?;
            crate::actor_runtime::optional_flush_all(&self.instrument_runtime)?;
            if is_new {
                let add = capture_plan_difference(&self.plan, &previous_plan);
                self.subscribe_plan(&add)?;
            }
            let kind = if is_new {
                "prepared"
            } else {
                "definition_updated"
            };
            self.lighter_audit(
                kind,
                instrument,
                json!({
                    "start_price": info.get("start_price"),
                    "active_market_count": self.lighter.markets.len(),
                }),
            )?;
        }
        Ok(())
    }
    fn lighter_audit(
        &self,
        kind: &str,
        instrument: &InstrumentAny,
        details: serde_json::Value,
    ) -> Result<()> {
        let record = json!({
            "kind": kind,
            "ts_ns": self.clock().timestamp_ns().as_u64(),
            "instrument_id": instrument.id().to_string(),
            "expiration_ns": instrument.expiration_ns().map(|time|time.as_u64()),
            "details": details,
        });
        std::fs::create_dir_all(&self.catalog_root)?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.catalog_root.join("lighter_universe_events.jsonl"))?;
        writeln!(file, "{}", serde_json::to_string(&record)?)?;
        log::info!("Lighter capture lifecycle: {record}");
        Ok(())
    }
    fn seal_lighter_partitions(&self, remove: &CapturePlan, id: InstrumentId) -> Result<()> {
        if remove.trades.iter().any(|s| s.instrument_id == id)
            && let Some(runtime) = &self.trade_runtime
        {
            runtime.seal_partition(&PartitionKey::catalog_data::<TradeTick>(id).stable_key())?;
        }
        if remove.book_deltas.iter().any(|s| s.instrument_id == id)
            && let Some(runtime) = &self.book_delta_runtime
        {
            runtime
                .seal_partition(&PartitionKey::catalog_data::<OrderBookDelta>(id).stable_key())?;
        }
        if remove.index_prices.iter().any(|s| s.instrument_id == id)
            && let Some(runtime) = &self.index_price_runtime
        {
            runtime
                .seal_partition(&PartitionKey::catalog_data::<IndexPriceUpdate>(id).stable_key())?;
        }
        for spec in &remove.custom_data {
            if spec.data_type.type_name() == "LighterOpenInterest"
                && let Some(runtime) = &self.custom_data_runtime
            {
                runtime.seal_partition(
                    &PartitionKey::custom_data(
                        spec.data_type.type_name(),
                        spec.data_type.identifier().map(str::to_string),
                        spec.data_type.topic(),
                    )
                    .stable_key(),
                )?;
            }
        }
        Ok(())
    }
}
