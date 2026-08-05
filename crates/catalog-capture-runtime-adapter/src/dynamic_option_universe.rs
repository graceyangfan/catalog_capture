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

use anyhow::Result;
use catalog_capture_core::{
    compute_refresh_rollover_reason, expand_option_universe, instrument_id_difference,
    merge_capture_plans, plan_instrument_ids, refresh_resolution_record,
    should_apply_strike_change, CapturePlan, OptionUniverseResolutionRecord, OptionUniverseSpec,
    OptionUniverseVenueKind, ResolvedOptionUniverse, StrikeChangeSmoothingState, StrikePolicy,
};
use nautilus_common::cache::Cache;
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::{InstrumentId, Venue};

use crate::dynamic_option_universe_runtime::resolve_runtime_option_universe;
use crate::dynamic_plan::{build_dynamic_plan_delta, DynamicPlanDelta};

#[derive(Debug, Clone)]
pub struct DynamicOptionUniverseConfig {
    pub refresh_interval_secs: u64,
    pub strike_change_confirmations: u32,
    /// When true, rolled-off option instruments are purged from Nautilus Cache
    /// (definition + market-data maps). Catalog parquet on disk is never deleted.
    pub purge_removed_instruments: bool,
    pub static_plan: CapturePlan,
    pub initial_dynamic_plan: CapturePlan,
    pub universes: Vec<DynamicOptionUniverseEntryConfig>,
}

#[derive(Debug, Clone)]
pub struct DynamicOptionUniverseEntryConfig {
    pub venue: Venue,
    pub venue_kind: OptionUniverseVenueKind,
    pub spec: OptionUniverseSpec,
    pub initial_plan: CapturePlan,
    pub initial_resolved: ResolvedOptionUniverse,
}

pub type DynamicOptionUniverseDelta =
    DynamicPlanDelta<DynamicOptionUniverseChange, OptionUniverseResolutionRecord>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicOptionUniverseChange {
    pub venue_id: String,
    pub underlying: String,
    pub selected_expiry_iso8601: String,
    pub perp_instrument_id: Option<InstrumentId>,
    pub option_instrument_ids: Vec<InstrumentId>,
    pub previous_count: usize,
    pub next_count: usize,
    pub added_instrument_ids: Vec<InstrumentId>,
    pub removed_instrument_ids: Vec<InstrumentId>,
}

#[derive(Debug, Clone)]
struct DynamicOptionUniverseState {
    venue: Venue,
    venue_kind: OptionUniverseVenueKind,
    spec: OptionUniverseSpec,
    current_plan: CapturePlan,
    applied_resolved: ResolvedOptionUniverse,
    last_selected_expiry_ns: Option<u64>,
    last_atm_reference: Option<String>,
    strike_smoothing: StrikeChangeSmoothingState,
}

#[derive(Debug, Clone)]
pub struct DynamicOptionUniverseManager {
    refresh_interval_secs: u64,
    strike_change_confirmations: u32,
    purge_removed_instruments: bool,
    static_plan: CapturePlan,
    current_dynamic_plan: CapturePlan,
    universes: Vec<DynamicOptionUniverseState>,
}

impl DynamicOptionUniverseManager {
    pub fn new(config: DynamicOptionUniverseConfig) -> Self {
        let universes = config
            .universes
            .into_iter()
            .map(|entry| DynamicOptionUniverseState {
                current_plan: entry.initial_plan,
                applied_resolved: entry.initial_resolved.clone(),
                venue: entry.venue,
                venue_kind: entry.venue_kind,
                spec: entry.spec,
                last_selected_expiry_ns: Some(entry.initial_resolved.selected_expiry_ns.as_u64()),
                last_atm_reference: Some(entry.initial_resolved.atm_reference.to_string()),
                strike_smoothing: StrikeChangeSmoothingState::default(),
            })
            .collect();

        Self {
            refresh_interval_secs: config.refresh_interval_secs,
            strike_change_confirmations: config.strike_change_confirmations,
            purge_removed_instruments: config.purge_removed_instruments,
            static_plan: config.static_plan,
            current_dynamic_plan: config.initial_dynamic_plan,
            universes,
        }
    }

    #[must_use]
    pub fn refresh_interval_secs(&self) -> u64 {
        self.refresh_interval_secs
    }

    #[must_use]
    pub fn purge_removed_instruments_enabled(&self) -> bool {
        self.purge_removed_instruments
    }

    #[must_use]
    pub fn active_capture_plan(&self) -> CapturePlan {
        merge_capture_plans(&self.static_plan, &self.current_dynamic_plan)
    }

    pub fn refresh_from_cache(
        &mut self,
        cache: &Cache,
        now: UnixNanos,
    ) -> Result<DynamicOptionUniverseDelta> {
        let previous_dynamic_plan = self.current_dynamic_plan.clone();
        let mut next_dynamic_plan = CapturePlan::default();
        let mut changes = Vec::new();
        let mut resolution_records = Vec::new();

        for state in &mut self.universes {
            match resolve_runtime_option_universe(
                cache,
                now,
                &state.spec,
                state.venue,
                state.venue_kind,
            ) {
                Ok(resolved) => {
                    let expiry_changed = state
                        .last_selected_expiry_ns
                        .is_some_and(|expiry| expiry != resolved.selected_expiry_ns.as_u64());
                    let smoothing_enabled =
                        matches!(state.spec.strike_policy, StrikePolicy::OiRanked { .. })
                            && self.strike_change_confirmations > 0;
                    let apply_strike_change = if smoothing_enabled {
                        should_apply_strike_change(
                            &state.applied_resolved.selected_strikes,
                            &resolved.selected_strikes,
                            expiry_changed,
                            self.strike_change_confirmations,
                            &mut state.strike_smoothing,
                        )
                    } else {
                        true
                    };

                    let effective_resolved = if apply_strike_change {
                        resolved.clone()
                    } else {
                        state.applied_resolved.clone()
                    };
                    let next_plan = expand_option_universe(&state.spec, &effective_resolved);
                    let previous_ids = plan_instrument_ids(&state.current_plan);
                    let next_ids = plan_instrument_ids(&next_plan);
                    if next_ids != previous_ids {
                        let added_instrument_ids =
                            instrument_id_difference(&next_ids, &previous_ids);
                        let removed_instrument_ids =
                            instrument_id_difference(&previous_ids, &next_ids);
                        let rollover_reason = compute_refresh_rollover_reason(
                            state.last_selected_expiry_ns,
                            &effective_resolved,
                            state.last_atm_reference.as_deref(),
                            true,
                            state.spec.strike_policy.selection_mode(),
                        );
                        changes.push(DynamicOptionUniverseChange {
                            venue_id: state.spec.venue_id.clone(),
                            underlying: state.spec.underlying.clone(),
                            selected_expiry_iso8601: nautilus_core::datetime::unix_nanos_to_iso8601(
                                effective_resolved.selected_expiry_ns,
                            ),
                            perp_instrument_id: effective_resolved.perp_instrument_id,
                            option_instrument_ids: effective_resolved.option_instrument_ids.clone(),
                            previous_count: previous_ids.len(),
                            next_count: next_ids.len(),
                            added_instrument_ids: added_instrument_ids.clone(),
                            removed_instrument_ids: removed_instrument_ids.clone(),
                        });
                        resolution_records.push(refresh_resolution_record(
                            &state.spec,
                            &effective_resolved,
                            added_instrument_ids
                                .iter()
                                .map(ToString::to_string)
                                .collect(),
                            removed_instrument_ids
                                .iter()
                                .map(ToString::to_string)
                                .collect(),
                            rollover_reason,
                        ));
                    }
                    if apply_strike_change {
                        state.applied_resolved = effective_resolved;
                        state.last_selected_expiry_ns =
                            Some(state.applied_resolved.selected_expiry_ns.as_u64());
                        state.last_atm_reference =
                            Some(state.applied_resolved.atm_reference.to_string());
                    }
                    state.current_plan = next_plan.clone();
                    next_dynamic_plan = merge_capture_plans(&next_dynamic_plan, &next_plan);
                }
                Err(error) => {
                    log::warn!(
                        "Option universe refresh failed for venue_id={} underlying={}: {}",
                        state.spec.venue_id,
                        state.spec.underlying,
                        error,
                    );
                    next_dynamic_plan =
                        merge_capture_plans(&next_dynamic_plan, &state.current_plan);
                }
            }
        }

        let delta = build_dynamic_plan_delta(
            &previous_dynamic_plan,
            &next_dynamic_plan,
            changes,
            resolution_records,
        );
        self.current_dynamic_plan = next_dynamic_plan;
        Ok(delta)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use catalog_capture_core::{
        plan_instrument_ids, CapturePlan, ExpiryPolicy, IndexPriceCaptureSpec,
        MarkPriceCaptureSpec, OptionUniverseFamily, QuoteCaptureSpec, StrikePolicy,
    };
    use nautilus_common::cache::Cache;
    use nautilus_model::{
        data::{OptionGreekValues, OptionGreeks, QuoteTick},
        enums::{GreeksConvention, OptionKind},
        identifiers::Symbol,
        instruments::{CryptoOption, CryptoPerpetual, InstrumentAny},
        types::{Currency, Money, Price, Quantity},
    };

    use super::*;
    use crate::{plan_has_index_prices, plan_has_mark_prices, plan_has_quotes};

    fn spec() -> OptionUniverseSpec {
        OptionUniverseSpec {
            venue_id: "deribit_main".to_string(),
            underlying: "BTC".to_string(),
            settlement_currency: Some("BTC".to_string()),
            include_perp: true,
            families: vec![
                OptionUniverseFamily::Instruments,
                OptionUniverseFamily::Quotes,
                OptionUniverseFamily::OptionGreeks,
                OptionUniverseFamily::IndexPrices,
                OptionUniverseFamily::FundingRates,
            ],
            expiry_policy: ExpiryPolicy::Nearest { days_max: 45 },
            strike_policy: StrikePolicy::AtmRelative {
                strikes_above: 1,
                strikes_below: 1,
            },
        }
    }

    fn make_deribit_option(
        symbol: &str,
        strike: &str,
        kind: OptionKind,
        expiration_ns: u64,
    ) -> InstrumentAny {
        InstrumentAny::CryptoOption(CryptoOption::new(
            InstrumentId::from(format!("{symbol}.DERIBIT").as_str()),
            Symbol::from(symbol),
            Currency::from("BTC"),
            Currency::from("USD"),
            Currency::from("BTC"),
            false,
            kind,
            Price::from(strike),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            UnixNanos::from(expiration_ns),
            3,
            1,
            Price::from("0.001"),
            Quantity::from("0.1"),
            Some(Quantity::from(1)),
            Some(Quantity::from("0.1")),
            None,
            Some(Quantity::from("0.1")),
            None,
            Some(Money::new(10.0, Currency::from("USD"))),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            0.into(),
            0.into(),
        ))
    }

    fn make_btc_option_set() -> Vec<InstrumentAny> {
        vec![
            make_deribit_option(
                "BTC-26JUN26-64000-C",
                "64000",
                OptionKind::Call,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-64000-P",
                "64000",
                OptionKind::Put,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-65000-C",
                "65000",
                OptionKind::Call,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-65000-P",
                "65000",
                OptionKind::Put,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-66000-C",
                "66000",
                OptionKind::Call,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-66000-P",
                "66000",
                OptionKind::Put,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-67000-C",
                "67000",
                OptionKind::Call,
                1_782_432_000_000_000_000,
            ),
            make_deribit_option(
                "BTC-26JUN26-67000-P",
                "67000",
                OptionKind::Put,
                1_782_432_000_000_000_000,
            ),
        ]
    }

    fn make_deribit_perpetual() -> InstrumentAny {
        InstrumentAny::CryptoPerpetual(CryptoPerpetual::new(
            InstrumentId::from("BTC-PERPETUAL.DERIBIT"),
            Symbol::from("BTC-PERPETUAL"),
            Currency::from("BTC"),
            Currency::from("USD"),
            Currency::from("BTC"),
            false,
            1,
            1,
            Price::from("0.5"),
            Quantity::from("0.1"),
            None,
            None,
            None,
            None,
            Some(Money::from("10 USD")),
            Some(Money::from("1 USD")),
            Some(Price::from("1000000")),
            Some(Price::from("1")),
            None,
            None,
            None,
            None,
            None,
            None,
            UnixNanos::default(),
            UnixNanos::default(),
        ))
    }

    fn make_quote(instrument_id: InstrumentId, bid: &str, ask: &str) -> QuoteTick {
        QuoteTick::new(
            instrument_id,
            Price::from(bid),
            Price::from(ask),
            Quantity::from("1"),
            Quantity::from("1"),
            UnixNanos::default(),
            UnixNanos::default(),
        )
    }

    fn make_option_greeks(
        instrument_id: InstrumentId,
        underlying_price: f64,
        open_interest: Option<f64>,
    ) -> OptionGreeks {
        OptionGreeks {
            instrument_id,
            convention: GreeksConvention::PriceAdjusted,
            greeks: OptionGreekValues {
                delta: 0.5,
                gamma: 0.1,
                vega: 0.2,
                theta: -0.1,
                rho: 0.01,
            },
            bid_iv: None,
            ask_iv: None,
            mark_iv: Some(0.45),
            underlying_price: Some(underlying_price),
            open_interest,
            ts_event: UnixNanos::default(),
            ts_init: UnixNanos::default(),
        }
    }

    #[test]
    fn plan_helpers_detect_reference_families() {
        let perp = InstrumentId::from("BTC-PERPETUAL.DERIBIT");
        let plan = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: perp,
            }],
            mark_prices: vec![MarkPriceCaptureSpec {
                instrument_id: perp,
            }],
            index_prices: vec![IndexPriceCaptureSpec {
                instrument_id: perp,
            }],
            ..CapturePlan::default()
        };

        assert!(plan_has_quotes(&plan, perp));
        assert!(plan_has_mark_prices(&plan, perp));
        assert!(plan_has_index_prices(&plan, perp));
        assert_eq!(spec().underlying, "BTC");
    }

    #[test]
    fn refresh_from_cache_reports_atm_rotation_change() {
        let now = UnixNanos::from(1_781_740_800_000_000_000u64);
        let venue = Venue::from("DERIBIT");
        let mut cache = Cache::default();

        for instrument in make_btc_option_set() {
            cache.add_instrument(instrument).unwrap();
        }
        cache.add_instrument(make_deribit_perpetual()).unwrap();

        let missing_reference_error = resolve_runtime_option_universe(
            &cache,
            now,
            &spec(),
            venue,
            OptionUniverseVenueKind::Deribit,
        )
        .unwrap_err();
        assert!(missing_reference_error
            .to_string()
            .contains("failed to determine strike reference"));

        let reference_call = InstrumentId::from("BTC-26JUN26-64000-C.DERIBIT");
        cache.add_option_greeks(make_option_greeks(reference_call, 65_000.0, None));

        let initial_resolved = resolve_runtime_option_universe(
            &cache,
            now,
            &spec(),
            venue,
            OptionUniverseVenueKind::Deribit,
        )
        .unwrap();
        assert_eq!(
            initial_resolved.atm_reference_source.as_deref(),
            Some("cache_greeks_underlying_price")
        );
        let initial_plan = expand_option_universe(&spec(), &initial_resolved);
        let mut manager = DynamicOptionUniverseManager::new(DynamicOptionUniverseConfig {
            refresh_interval_secs: 60,
            strike_change_confirmations: 0,
            purge_removed_instruments: true,
            static_plan: CapturePlan::default(),
            initial_dynamic_plan: initial_plan.clone(),
            universes: vec![DynamicOptionUniverseEntryConfig {
                venue,
                venue_kind: OptionUniverseVenueKind::Deribit,
                spec: spec(),
                initial_plan,
                initial_resolved: initial_resolved.clone(),
            }],
        });

        cache.add_option_greeks(make_option_greeks(reference_call, 66_000.0, None));
        let delta = manager.refresh_from_cache(&cache, now).unwrap();

        assert_eq!(delta.changes.len(), 1);
        assert_eq!(delta.resolution_records.len(), 1);
        assert_eq!(
            delta.resolution_records[0].atm_reference_source,
            "cache_greeks_underlying_price"
        );
        assert_eq!(
            delta.resolution_records[0].rollover_reason.as_deref(),
            Some("atm_drift")
        );
        let change = &delta.changes[0];
        assert_eq!(change.venue_id, "deribit_main");
        assert_eq!(change.underlying, "BTC");
        assert_eq!(change.previous_count, 7);
        assert_eq!(change.next_count, 7);
        assert_eq!(
            change.added_instrument_ids,
            vec![
                InstrumentId::from("BTC-26JUN26-67000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-67000-P.DERIBIT"),
            ]
        );
        assert_eq!(
            change.removed_instrument_ids,
            vec![
                InstrumentId::from("BTC-26JUN26-64000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-64000-P.DERIBIT"),
            ]
        );
        assert_eq!(change.option_instrument_ids.len(), 6);
        assert_eq!(
            plan_instrument_ids(&delta.add),
            BTreeSet::from([
                InstrumentId::from("BTC-26JUN26-67000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-67000-P.DERIBIT"),
            ]),
        );
        assert_eq!(
            plan_instrument_ids(&delta.remove),
            BTreeSet::from([
                InstrumentId::from("BTC-26JUN26-64000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-64000-P.DERIBIT"),
            ]),
        );
    }

    #[test]
    fn refresh_from_cache_oi_ranked_selects_top_strikes_from_greeks() {
        let now = UnixNanos::from(1_781_740_800_000_000_000u64);
        let venue = Venue::from("DERIBIT");
        let mut cache = Cache::default();
        let mut oi_spec = spec();
        oi_spec.strike_policy = StrikePolicy::OiRanked { top_n: 2 };
        oi_spec.families = vec![
            OptionUniverseFamily::Instruments,
            OptionUniverseFamily::OptionGreeks,
        ];

        for instrument in make_btc_option_set() {
            cache.add_instrument(instrument).unwrap();
        }
        cache.add_instrument(make_deribit_perpetual()).unwrap();

        let greeks_by_id = [
            (InstrumentId::from("BTC-26JUN26-64000-C.DERIBIT"), 100.0),
            (InstrumentId::from("BTC-26JUN26-64000-P.DERIBIT"), 50.0),
            (InstrumentId::from("BTC-26JUN26-65000-C.DERIBIT"), 300.0),
            (InstrumentId::from("BTC-26JUN26-65000-P.DERIBIT"), 200.0),
            (InstrumentId::from("BTC-26JUN26-66000-C.DERIBIT"), 250.0),
            (InstrumentId::from("BTC-26JUN26-66000-P.DERIBIT"), 150.0),
            (InstrumentId::from("BTC-26JUN26-67000-C.DERIBIT"), 10.0),
            (InstrumentId::from("BTC-26JUN26-67000-P.DERIBIT"), 10.0),
        ];
        for (instrument_id, open_interest) in greeks_by_id {
            cache.add_option_greeks(make_option_greeks(
                instrument_id,
                65_000.0,
                Some(open_interest),
            ));
        }

        let resolved = resolve_runtime_option_universe(
            &cache,
            now,
            &oi_spec,
            venue,
            OptionUniverseVenueKind::Deribit,
        )
        .unwrap();

        assert_eq!(
            resolved.selected_strikes,
            vec![Price::from("65000"), Price::from("66000")]
        );
        assert_eq!(resolved.option_instrument_ids.len(), 4);
    }

    fn seed_oi_ranked_cache(cache: &mut Cache, oi_by_id: &[(InstrumentId, f64)]) {
        for (instrument_id, open_interest) in oi_by_id {
            cache.add_option_greeks(make_option_greeks(
                *instrument_id,
                65_000.0,
                Some(*open_interest),
            ));
        }
    }

    #[test]
    fn refresh_from_cache_oi_ranked_smoothing_defers_strike_shift() {
        let now = UnixNanos::from(1_781_740_800_000_000_000u64);
        let venue = Venue::from("DERIBIT");
        let mut cache = Cache::default();
        let mut oi_spec = spec();
        oi_spec.strike_policy = StrikePolicy::OiRanked { top_n: 2 };
        oi_spec.families = vec![
            OptionUniverseFamily::Instruments,
            OptionUniverseFamily::OptionGreeks,
        ];

        for instrument in make_btc_option_set() {
            cache.add_instrument(instrument).unwrap();
        }
        cache.add_instrument(make_deribit_perpetual()).unwrap();

        seed_oi_ranked_cache(
            &mut cache,
            &[
                (InstrumentId::from("BTC-26JUN26-64000-C.DERIBIT"), 100.0),
                (InstrumentId::from("BTC-26JUN26-64000-P.DERIBIT"), 50.0),
                (InstrumentId::from("BTC-26JUN26-65000-C.DERIBIT"), 300.0),
                (InstrumentId::from("BTC-26JUN26-65000-P.DERIBIT"), 200.0),
                (InstrumentId::from("BTC-26JUN26-66000-C.DERIBIT"), 250.0),
                (InstrumentId::from("BTC-26JUN26-66000-P.DERIBIT"), 150.0),
            ],
        );

        let initial_resolved = resolve_runtime_option_universe(
            &cache,
            now,
            &oi_spec,
            venue,
            OptionUniverseVenueKind::Deribit,
        )
        .unwrap();
        assert_eq!(
            initial_resolved.selected_strikes,
            vec![Price::from("65000"), Price::from("66000")]
        );
        let initial_plan = expand_option_universe(&oi_spec, &initial_resolved);
        let mut manager = DynamicOptionUniverseManager::new(DynamicOptionUniverseConfig {
            refresh_interval_secs: 60,
            strike_change_confirmations: 2,
            purge_removed_instruments: true,
            static_plan: CapturePlan::default(),
            initial_dynamic_plan: initial_plan.clone(),
            universes: vec![DynamicOptionUniverseEntryConfig {
                venue,
                venue_kind: OptionUniverseVenueKind::Deribit,
                spec: oi_spec.clone(),
                initial_plan,
                initial_resolved,
            }],
        });

        seed_oi_ranked_cache(
            &mut cache,
            &[
                (InstrumentId::from("BTC-26JUN26-64000-C.DERIBIT"), 500.0),
                (InstrumentId::from("BTC-26JUN26-64000-P.DERIBIT"), 400.0),
                (InstrumentId::from("BTC-26JUN26-65000-C.DERIBIT"), 300.0),
                (InstrumentId::from("BTC-26JUN26-65000-P.DERIBIT"), 200.0),
                (InstrumentId::from("BTC-26JUN26-66000-C.DERIBIT"), 50.0),
                (InstrumentId::from("BTC-26JUN26-66000-P.DERIBIT"), 25.0),
            ],
        );

        let first = manager.refresh_from_cache(&cache, now).unwrap();
        assert!(first.is_empty());

        let second = manager.refresh_from_cache(&cache, now).unwrap();
        assert_eq!(second.changes.len(), 1);
        assert_eq!(
            second.resolution_records[0].rollover_reason.as_deref(),
            Some("oi_rank_shift")
        );
        assert_eq!(
            second.changes[0].option_instrument_ids,
            vec![
                InstrumentId::from("BTC-26JUN26-64000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-64000-P.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-65000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-65000-P.DERIBIT"),
            ]
        );
    }

    #[test]
    fn refresh_from_cache_discovers_new_expiry_instruments_added_later() {
        let initial_now = UnixNanos::from(1_781_740_800_000_000_000u64);
        let later_now = UnixNanos::from(1_782_500_000_000_000_000u64);
        let venue = Venue::from("DERIBIT");
        let perp = InstrumentId::from("BTC-PERPETUAL.DERIBIT");
        let mut cache = Cache::default();

        for instrument in make_btc_option_set() {
            cache.add_instrument(instrument).unwrap();
        }
        cache.add_instrument(make_deribit_perpetual()).unwrap();
        cache.add_quote(make_quote(perp, "64990", "65010")).unwrap();

        let initial_resolved = resolve_runtime_option_universe(
            &cache,
            initial_now,
            &spec(),
            venue,
            OptionUniverseVenueKind::Deribit,
        )
        .unwrap();
        let initial_plan = expand_option_universe(&spec(), &initial_resolved);
        let mut manager = DynamicOptionUniverseManager::new(DynamicOptionUniverseConfig {
            refresh_interval_secs: 60,
            strike_change_confirmations: 0,
            purge_removed_instruments: true,
            static_plan: CapturePlan::default(),
            initial_dynamic_plan: initial_plan.clone(),
            universes: vec![DynamicOptionUniverseEntryConfig {
                venue,
                venue_kind: OptionUniverseVenueKind::Deribit,
                spec: spec(),
                initial_plan,
                initial_resolved: initial_resolved.clone(),
            }],
        });

        for instrument in [
            make_deribit_option(
                "BTC-03JUL26-64000-C",
                "64000",
                OptionKind::Call,
                1_783_036_800_000_000_000,
            ),
            make_deribit_option(
                "BTC-03JUL26-64000-P",
                "64000",
                OptionKind::Put,
                1_783_036_800_000_000_000,
            ),
            make_deribit_option(
                "BTC-03JUL26-65000-C",
                "65000",
                OptionKind::Call,
                1_783_036_800_000_000_000,
            ),
            make_deribit_option(
                "BTC-03JUL26-65000-P",
                "65000",
                OptionKind::Put,
                1_783_036_800_000_000_000,
            ),
            make_deribit_option(
                "BTC-03JUL26-66000-C",
                "66000",
                OptionKind::Call,
                1_783_036_800_000_000_000,
            ),
            make_deribit_option(
                "BTC-03JUL26-66000-P",
                "66000",
                OptionKind::Put,
                1_783_036_800_000_000_000,
            ),
        ] {
            cache.add_instrument(instrument).unwrap();
        }

        let delta = manager.refresh_from_cache(&cache, later_now).unwrap();

        assert_eq!(delta.changes.len(), 1);
        assert_eq!(
            delta.changes[0].added_instrument_ids,
            vec![
                InstrumentId::from("BTC-03JUL26-64000-C.DERIBIT"),
                InstrumentId::from("BTC-03JUL26-64000-P.DERIBIT"),
                InstrumentId::from("BTC-03JUL26-65000-C.DERIBIT"),
                InstrumentId::from("BTC-03JUL26-65000-P.DERIBIT"),
                InstrumentId::from("BTC-03JUL26-66000-C.DERIBIT"),
                InstrumentId::from("BTC-03JUL26-66000-P.DERIBIT"),
            ],
        );
        assert_eq!(
            delta.changes[0].removed_instrument_ids,
            vec![
                InstrumentId::from("BTC-26JUN26-64000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-64000-P.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-65000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-65000-P.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-66000-C.DERIBIT"),
                InstrumentId::from("BTC-26JUN26-66000-P.DERIBIT"),
            ],
        );
    }

    fn bybit_spec() -> OptionUniverseSpec {
        OptionUniverseSpec {
            venue_id: "bybit_main".to_string(),
            underlying: "BTC".to_string(),
            settlement_currency: Some("USDT".to_string()),
            include_perp: true,
            families: vec![
                OptionUniverseFamily::Instruments,
                OptionUniverseFamily::Quotes,
                OptionUniverseFamily::OptionGreeks,
                OptionUniverseFamily::IndexPrices,
                OptionUniverseFamily::FundingRates,
            ],
            expiry_policy: ExpiryPolicy::Nearest { days_max: 45 },
            strike_policy: StrikePolicy::AtmRelative {
                strikes_above: 1,
                strikes_below: 1,
            },
        }
    }

    fn make_bybit_option(
        symbol: &str,
        strike: &str,
        kind: OptionKind,
        expiration_ns: u64,
    ) -> InstrumentAny {
        InstrumentAny::CryptoOption(CryptoOption::new(
            InstrumentId::from(format!("{symbol}.BYBIT").as_str()),
            Symbol::from(symbol),
            Currency::from("BTC"),
            Currency::from("USDT"),
            Currency::from("USDT"),
            false,
            kind,
            Price::from(strike),
            UnixNanos::from(1_700_000_000_000_000_000u64),
            UnixNanos::from(expiration_ns),
            3,
            1,
            Price::from("0.001"),
            Quantity::from("0.1"),
            Some(Quantity::from(1)),
            Some(Quantity::from("0.1")),
            None,
            Some(Quantity::from("0.1")),
            None,
            Some(Money::new(10.0, Currency::from("USDT"))),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            0.into(),
            0.into(),
        ))
    }

    fn make_bybit_perpetual() -> InstrumentAny {
        InstrumentAny::CryptoPerpetual(CryptoPerpetual::new(
            InstrumentId::from("BTCUSDT-LINEAR.BYBIT"),
            Symbol::from("BTCUSDT"),
            Currency::from("BTC"),
            Currency::from("USDT"),
            Currency::from("USDT"),
            false,
            1,
            1,
            Price::from("0.5"),
            Quantity::from("0.1"),
            None,
            None,
            None,
            None,
            Some(Money::from("10 USDT")),
            Some(Money::from("1 USDT")),
            Some(Price::from("1000000")),
            Some(Price::from("1")),
            None,
            None,
            None,
            None,
            None,
            None,
            UnixNanos::default(),
            UnixNanos::default(),
        ))
    }

    #[test]
    fn refresh_from_cache_supports_bybit_runtime_resolve() {
        let now = UnixNanos::from(1_781_740_800_000_000_000u64);
        let venue = Venue::from("BYBIT");
        let mut cache = Cache::default();

        for instrument in [
            make_bybit_option(
                "BTC-26JUN26-64000-C",
                "64000",
                OptionKind::Call,
                1_782_432_000_000_000_000,
            ),
            make_bybit_option(
                "BTC-26JUN26-64000-P",
                "64000",
                OptionKind::Put,
                1_782_432_000_000_000_000,
            ),
            make_bybit_option(
                "BTC-26JUN26-65000-C",
                "65000",
                OptionKind::Call,
                1_782_432_000_000_000_000,
            ),
            make_bybit_option(
                "BTC-26JUN26-65000-P",
                "65000",
                OptionKind::Put,
                1_782_432_000_000_000_000,
            ),
            make_bybit_option(
                "BTC-26JUN26-66000-C",
                "66000",
                OptionKind::Call,
                1_782_432_000_000_000_000,
            ),
            make_bybit_option(
                "BTC-26JUN26-66000-P",
                "66000",
                OptionKind::Put,
                1_782_432_000_000_000_000,
            ),
            make_bybit_option(
                "BTC-26JUN26-67000-C",
                "67000",
                OptionKind::Call,
                1_782_432_000_000_000_000,
            ),
            make_bybit_option(
                "BTC-26JUN26-67000-P",
                "67000",
                OptionKind::Put,
                1_782_432_000_000_000_000,
            ),
        ] {
            cache.add_instrument(instrument).unwrap();
        }
        cache.add_instrument(make_bybit_perpetual()).unwrap();
        let reference_call = InstrumentId::from("BTC-26JUN26-64000-C.BYBIT");
        cache.add_option_greeks(make_option_greeks(reference_call, 65_000.0, None));

        let initial_resolved = resolve_runtime_option_universe(
            &cache,
            now,
            &bybit_spec(),
            venue,
            OptionUniverseVenueKind::Bybit,
        )
        .unwrap();
        let initial_plan = expand_option_universe(&bybit_spec(), &initial_resolved);
        let mut manager = DynamicOptionUniverseManager::new(DynamicOptionUniverseConfig {
            refresh_interval_secs: 60,
            strike_change_confirmations: 0,
            purge_removed_instruments: true,
            static_plan: CapturePlan::default(),
            initial_dynamic_plan: initial_plan.clone(),
            universes: vec![DynamicOptionUniverseEntryConfig {
                venue,
                venue_kind: OptionUniverseVenueKind::Bybit,
                spec: bybit_spec(),
                initial_plan,
                initial_resolved: initial_resolved.clone(),
            }],
        });

        cache.add_option_greeks(make_option_greeks(reference_call, 66_000.0, None));
        let delta = manager.refresh_from_cache(&cache, now).unwrap();

        assert_eq!(delta.changes.len(), 1);
        assert_eq!(
            delta.resolution_records[0].rollover_reason.as_deref(),
            Some("atm_drift")
        );
        assert_eq!(delta.changes[0].venue_id, "bybit_main");
        assert_eq!(
            delta.changes[0].added_instrument_ids,
            vec![
                InstrumentId::from("BTC-26JUN26-67000-C.BYBIT"),
                InstrumentId::from("BTC-26JUN26-67000-P.BYBIT"),
            ]
        );
        assert_eq!(
            delta.changes[0].removed_instrument_ids,
            vec![
                InstrumentId::from("BTC-26JUN26-64000-C.BYBIT"),
                InstrumentId::from("BTC-26JUN26-64000-P.BYBIT"),
            ]
        );
    }
}
