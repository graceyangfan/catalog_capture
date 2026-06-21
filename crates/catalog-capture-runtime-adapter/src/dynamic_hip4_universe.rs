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

use std::{sync::mpsc, thread};

use anyhow::{Context, Result};
use catalog_capture_core::{
    build_resolved_hip4_universe, capture_plan_difference, compute_hip4_refresh_rollover_reason,
    expand_hip4_universe, hip4_refresh_resolution_record, instrument_id_difference,
    merge_capture_plans, next_rotation_delay_secs, plan_instrument_ids, resolve_hip4_market,
    validate_hip4_refresh_resolution_record, CapturePlan, Hip4UniverseResolutionRecord,
    Hip4UniverseSpec, ResolveHip4MarketOptions, ResolvedHip4Universe,
};
use nautilus_core::UnixNanos;
use nautilus_hyperliquid::common::enums::HyperliquidEnvironment;
use nautilus_hyperliquid::http::{client::HyperliquidRawHttpClient, models::OutcomeMeta};
use nautilus_model::identifiers::InstrumentId;

#[derive(Debug, Clone)]
pub struct DynamicHip4UniverseConfig {
    pub idle_poll_secs: u64,
    pub active_poll_secs: u64,
    pub pre_expiry_window_secs: u64,
    pub http_timeout_secs: u64,
    pub static_plan: CapturePlan,
    pub initial_dynamic_plan: CapturePlan,
    pub universes: Vec<DynamicHip4UniverseEntryConfig>,
}

#[derive(Debug, Clone)]
pub struct DynamicHip4UniverseEntryConfig {
    pub environment: HyperliquidEnvironment,
    pub spec: Hip4UniverseSpec,
    pub initial_plan: CapturePlan,
    pub initial_resolved: ResolvedHip4Universe,
}

#[derive(Debug, Clone, Default)]
pub struct DynamicHip4UniverseDelta {
    pub add: CapturePlan,
    pub remove: CapturePlan,
    pub changes: Vec<DynamicHip4UniverseChange>,
    pub resolution_records: Vec<Hip4UniverseResolutionRecord>,
}

impl DynamicHip4UniverseDelta {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.add.is_empty() && self.remove.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicHip4UniverseChange {
    pub venue_id: String,
    pub underlying: String,
    pub period: String,
    pub market_class: String,
    pub question_id: u32,
    pub expiration_iso8601: String,
    pub perp_instrument_id: Option<InstrumentId>,
    pub outcome_instrument_ids: Vec<InstrumentId>,
    pub previous_count: usize,
    pub next_count: usize,
    pub added_instrument_ids: Vec<InstrumentId>,
    pub removed_instrument_ids: Vec<InstrumentId>,
}

#[derive(Debug, Clone)]
struct DynamicHip4UniverseState {
    environment: HyperliquidEnvironment,
    spec: Hip4UniverseSpec,
    current_plan: CapturePlan,
    applied_resolved: ResolvedHip4Universe,
    last_question_id: u32,
    last_expiration_ns: u64,
}

#[derive(Debug, Clone)]
pub struct DynamicHip4UniverseManager {
    idle_poll_secs: u64,
    active_poll_secs: u64,
    pre_expiry_window_secs: u64,
    http_timeout_secs: u64,
    static_plan: CapturePlan,
    current_dynamic_plan: CapturePlan,
    universes: Vec<DynamicHip4UniverseState>,
    last_refresh_failed: bool,
}

impl DynamicHip4UniverseManager {
    pub fn new(config: DynamicHip4UniverseConfig) -> Self {
        let universes = config
            .universes
            .into_iter()
            .map(|entry| DynamicHip4UniverseState {
                current_plan: entry.initial_plan,
                applied_resolved: entry.initial_resolved.clone(),
                environment: entry.environment,
                spec: entry.spec,
                last_question_id: entry.initial_resolved.market.question_id,
                last_expiration_ns: entry.initial_resolved.market.expiration_ns,
            })
            .collect();

        Self {
            idle_poll_secs: config.idle_poll_secs,
            active_poll_secs: config.active_poll_secs,
            pre_expiry_window_secs: config.pre_expiry_window_secs,
            http_timeout_secs: config.http_timeout_secs,
            static_plan: config.static_plan,
            current_dynamic_plan: config.initial_dynamic_plan,
            universes,
            last_refresh_failed: false,
        }
    }

    #[must_use]
    pub fn active_capture_plan(&self) -> CapturePlan {
        merge_capture_plans(&self.static_plan, &self.current_dynamic_plan)
    }

    /// Mirrors `hyperliquid_stale_quote.strategy.Hip4RecorderStrategy._schedule_next_rotation_check`.
    #[must_use]
    pub fn next_rotation_check_delay_secs(&self, now_ns: u64) -> u64 {
        if self.last_refresh_failed || self.universes.is_empty() {
            return self.rotation_delay_for_expiration(now_ns, None);
        }

        self.universes
            .iter()
            .map(|state| {
                self.rotation_delay_for_expiration(
                    now_ns,
                    Some(state.applied_resolved.market.expiration_ns),
                )
            })
            .min()
            .unwrap_or_else(|| self.rotation_delay_for_expiration(now_ns, None))
    }

    fn rotation_delay_for_expiration(&self, now_ns: u64, expiration_ns: Option<u64>) -> u64 {
        next_rotation_delay_secs(
            now_ns,
            expiration_ns,
            self.idle_poll_secs,
            self.active_poll_secs,
            self.pre_expiry_window_secs,
        )
    }

    pub fn refresh(&mut self, now_ns: u64) -> Result<DynamicHip4UniverseDelta> {
        let previous_dynamic_plan = self.current_dynamic_plan.clone();
        let mut next_dynamic_plan = CapturePlan::default();
        let mut changes = Vec::new();
        let mut resolution_records = Vec::new();
        let mut refresh_failed = false;

        for state in &mut self.universes {
            match resolve_runtime_hip4_universe(
                state.environment,
                &state.spec,
                now_ns,
                self.http_timeout_secs,
            ) {
                Ok(resolved) => {
                    let next_plan = expand_hip4_universe(&state.spec, &resolved);
                    let previous_ids = plan_instrument_ids(&state.current_plan);
                    let next_ids = plan_instrument_ids(&next_plan);
                    if next_ids != previous_ids {
                        let added_instrument_ids =
                            instrument_id_difference(&next_ids, &previous_ids);
                        let removed_instrument_ids =
                            instrument_id_difference(&previous_ids, &next_ids);
                        let rollover_reason = compute_hip4_refresh_rollover_reason(
                            Some(state.last_question_id),
                            Some(state.last_expiration_ns),
                            &resolved,
                            true,
                        );
                        changes.push(DynamicHip4UniverseChange {
                            venue_id: state.spec.venue_id.clone(),
                            underlying: state.spec.underlying.clone(),
                            period: state.spec.period.clone(),
                            market_class: state.spec.market_class.clone(),
                            question_id: resolved.market.question_id,
                            expiration_iso8601: nautilus_core::datetime::unix_nanos_to_iso8601(
                                UnixNanos::from(resolved.market.expiration_ns),
                            ),
                            perp_instrument_id: resolved.perp_instrument_id,
                            outcome_instrument_ids: resolved.outcome_instrument_ids.clone(),
                            previous_count: previous_ids.len(),
                            next_count: next_ids.len(),
                            added_instrument_ids: added_instrument_ids.clone(),
                            removed_instrument_ids: removed_instrument_ids.clone(),
                        });
                        let record = hip4_refresh_resolution_record(
                            &state.spec,
                            &resolved,
                            added_instrument_ids
                                .iter()
                                .map(ToString::to_string)
                                .collect(),
                            removed_instrument_ids
                                .iter()
                                .map(ToString::to_string)
                                .collect(),
                            rollover_reason,
                        );
                        validate_hip4_refresh_resolution_record(&record)?;
                        resolution_records.push(record);
                    }
                    state.applied_resolved = resolved.clone();
                    state.last_question_id = resolved.market.question_id;
                    state.last_expiration_ns = resolved.market.expiration_ns;
                    state.current_plan = next_plan.clone();
                    next_dynamic_plan = merge_capture_plans(&next_dynamic_plan, &next_plan);
                }
                Err(error) => {
                    refresh_failed = true;
                    log::warn!(
                        "HIP-4 universe refresh failed for venue_id={} underlying={}: {}",
                        state.spec.venue_id, state.spec.underlying, error,
                    );
                    next_dynamic_plan =
                        merge_capture_plans(&next_dynamic_plan, &state.current_plan);
                }
            }
        }

        let delta = DynamicHip4UniverseDelta {
            add: capture_plan_difference(&next_dynamic_plan, &previous_dynamic_plan),
            remove: capture_plan_difference(&previous_dynamic_plan, &next_dynamic_plan),
            changes,
            resolution_records,
        };
        self.current_dynamic_plan = next_dynamic_plan;
        self.last_refresh_failed = refresh_failed;
        Ok(delta)
    }
}

fn resolve_runtime_hip4_universe(
    environment: HyperliquidEnvironment,
    spec: &Hip4UniverseSpec,
    now_ns: u64,
    http_timeout_secs: u64,
) -> Result<ResolvedHip4Universe> {
    let outcome_meta = fetch_outcome_meta_blocking(environment, http_timeout_secs)
        .context("failed to fetch Hyperliquid outcomeMeta")?;
    let payload = serde_json::to_value(outcome_meta)
        .context("failed to serialize Hyperliquid outcomeMeta payload")?;
    let market = resolve_hip4_market(
        &payload,
        &ResolveHip4MarketOptions {
            underlying: &spec.underlying,
            period: &spec.period,
            market_class: &spec.market_class,
            include_fallback: spec.include_fallback,
            now_ns,
        },
    )?;
    Ok(build_resolved_hip4_universe(spec, market, now_ns))
}

fn fetch_outcome_meta_blocking(
    environment: HyperliquidEnvironment,
    http_timeout_secs: u64,
) -> Result<OutcomeMeta> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = (|| {
            let client = HyperliquidRawHttpClient::new(environment, http_timeout_secs, None)
                .map_err(anyhow::Error::from)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(anyhow::Error::from)?;
            runtime
                .block_on(client.get_outcome_meta())
                .map_err(anyhow::Error::from)
        })();
        let _ = tx.send(result);
    });
    rx.recv()
        .map_err(|_| anyhow::anyhow!("HIP-4 HTTP worker exited before returning outcomeMeta"))?
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use catalog_capture_core::{
        build_resolved_hip4_universe, capture_plan_difference,
        compute_hip4_refresh_rollover_reason, expand_hip4_universe, hip4_refresh_resolution_record,
        plan_instrument_ids, validate_hip4_refresh_resolution_record, CapturePlan,
        Hip4UniverseFamily, Hip4UniverseSpec, ResolvedHip4Market,
    };

    use super::{
        DynamicHip4UniverseConfig, DynamicHip4UniverseEntryConfig, DynamicHip4UniverseManager,
    };
    use nautilus_model::identifiers::InstrumentId;

    fn spec() -> Hip4UniverseSpec {
        Hip4UniverseSpec {
            venue_id: "hyperliquid_main".to_string(),
            underlying: "BTC".to_string(),
            period: "1d".to_string(),
            market_class: "priceBinary".to_string(),
            include_fallback: false,
            include_perp_mark: true,
            families: vec![
                Hip4UniverseFamily::Instruments,
                Hip4UniverseFamily::Quotes,
                Hip4UniverseFamily::MarkPrices,
            ],
        }
    }

    fn market(question_id: u32, outcome_id: u32, expiration_ns: u64) -> ResolvedHip4Market {
        ResolvedHip4Market {
            question_id,
            question_name: Some("Recurring".to_string()),
            market_class: Some("priceBinary".to_string()),
            underlying: Some("BTC".to_string()),
            period: Some("1d".to_string()),
            outcome_ids: vec![outcome_id],
            instrument_ids: vec![
                format!("{outcome_id}-YES-OUTCOME.HYPERLIQUID"),
                format!("{outcome_id}-NO-OUTCOME.HYPERLIQUID"),
            ],
            expiration_ns,
            start_price: None,
            price_thresholds: Vec::new(),
            description: "class:priceBinary|underlying:BTC|expiry:20260621-0600|period:1d"
                .to_string(),
        }
    }

    #[test]
    fn plan_delta_tracks_question_roll_instruments() {
        let spec = spec();
        let before = build_resolved_hip4_universe(
            &spec,
            market(55, 326, 1_781_416_800_000_000_000),
            1_781_410_000_000_000_000,
        );
        let after = build_resolved_hip4_universe(
            &spec,
            market(56, 330, 1_781_503_200_000_000_000),
            1_781_417_000_000_000_000,
        );
        let previous_plan = expand_hip4_universe(&spec, &before);
        let next_plan = expand_hip4_universe(&spec, &after);

        let add = capture_plan_difference(&next_plan, &previous_plan);
        let remove = capture_plan_difference(&previous_plan, &next_plan);
        let added = plan_instrument_ids(&add);
        let removed = plan_instrument_ids(&remove);

        assert_eq!(
            added,
            BTreeSet::from([
                InstrumentId::from("330-YES-OUTCOME.HYPERLIQUID"),
                InstrumentId::from("330-NO-OUTCOME.HYPERLIQUID"),
            ])
        );
        assert_eq!(
            removed,
            BTreeSet::from([
                InstrumentId::from("326-YES-OUTCOME.HYPERLIQUID"),
                InstrumentId::from("326-NO-OUTCOME.HYPERLIQUID"),
            ])
        );

        let rollover = compute_hip4_refresh_rollover_reason(
            Some(55),
            Some(before.market.expiration_ns),
            &after,
            true,
        );
        assert_eq!(rollover.as_deref(), Some("question_roll"));

        let record = hip4_refresh_resolution_record(
            &spec,
            &after,
            added.iter().map(ToString::to_string).collect(),
            removed.iter().map(ToString::to_string).collect(),
            rollover,
        );
        assert_eq!(
            record.event_kind,
            catalog_capture_core::Hip4UniverseResolutionEventKind::Refresh
        );
        assert_eq!(record.rollover_reason.as_deref(), Some("question_roll"));
        validate_hip4_refresh_resolution_record(&record).expect("refresh record should validate");
    }

    #[test]
    fn next_rotation_check_delay_matches_stale_quote_failure_and_idle_paths() {
        let spec = spec();
        let resolved = build_resolved_hip4_universe(
            &spec,
            market(55, 326, 1_781_416_800_000_000_000),
            1_781_410_000_000_000_000,
        );
        let mut manager = DynamicHip4UniverseManager::new(DynamicHip4UniverseConfig {
            idle_poll_secs: 1800,
            active_poll_secs: 10,
            pre_expiry_window_secs: 900,
            http_timeout_secs: 10,
            static_plan: CapturePlan::default(),
            initial_dynamic_plan: expand_hip4_universe(&spec, &resolved),
            universes: vec![DynamicHip4UniverseEntryConfig {
                environment: nautilus_hyperliquid::common::enums::HyperliquidEnvironment::Mainnet,
                spec: spec.clone(),
                initial_plan: expand_hip4_universe(&spec, &resolved),
                initial_resolved: resolved,
            }],
        });

        manager.last_refresh_failed = true;
        assert_eq!(
            manager.next_rotation_check_delay_secs(0),
            10,
            "failed refresh should schedule with expiration_ns=None -> active poll"
        );
        manager.last_refresh_failed = false;
        assert_eq!(
            manager.next_rotation_check_delay_secs(0),
            1800,
            "successful refresh far from expiry should use idle poll"
        );
    }
}
