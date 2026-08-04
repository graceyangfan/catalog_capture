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

use catalog_capture_core::{capture_plan_difference, merge_capture_plans, CapturePlan};

use crate::dynamic_hip4_universe::{DynamicHip4UniverseConfig, DynamicHip4UniverseManager};
use crate::dynamic_option_universe::{DynamicOptionUniverseConfig, DynamicOptionUniverseManager};
use crate::dynamic_plan::merge_active_capture_plan;

pub fn supplemental_capture_plan(
    initial_materialized_plan: &CapturePlan,
    dynamic_option_universe: &Option<DynamicOptionUniverseConfig>,
    dynamic_hip4_universe: &Option<DynamicHip4UniverseConfig>,
) -> CapturePlan {
    let option_active = dynamic_option_universe
        .as_ref()
        .map(|config| merge_active_capture_plan(&config.static_plan, &config.initial_dynamic_plan));
    let hip4_active = dynamic_hip4_universe
        .as_ref()
        .map(|config| merge_active_capture_plan(&config.static_plan, &config.initial_dynamic_plan));

    match (&option_active, &hip4_active) {
        (Some(_), Some(_)) => CapturePlan::default(),
        (Some(option), None) => capture_plan_difference(initial_materialized_plan, option),
        (None, Some(hip4)) => capture_plan_difference(initial_materialized_plan, hip4),
        (None, None) => CapturePlan::default(),
    }
}

pub fn effective_capture_plan(
    initial_materialized_plan: &CapturePlan,
    supplemental_plan: &CapturePlan,
    dynamic_option_universe: Option<&DynamicOptionUniverseManager>,
    dynamic_hip4_universe: Option<&DynamicHip4UniverseManager>,
) -> CapturePlan {
    match (dynamic_option_universe, dynamic_hip4_universe) {
        (None, None) => initial_materialized_plan.clone(),
        (Some(option), Some(hip4)) => {
            merge_capture_plans(&option.active_capture_plan(), &hip4.active_capture_plan())
        }
        (Some(option), None) => {
            merge_capture_plans(&option.active_capture_plan(), supplemental_plan)
        }
        (None, Some(hip4)) => merge_capture_plans(&hip4.active_capture_plan(), supplemental_plan),
    }
}

#[must_use]
pub fn count_enabled_background_workers(enabled_families: [bool; 12]) -> usize {
    enabled_families
        .into_iter()
        .filter(|enabled| *enabled)
        .count()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use catalog_capture_core::{
        merge_capture_plans,
        plan::{CapturePlan, QuoteCaptureSpec},
    };
    use nautilus_model::identifiers::InstrumentId;

    use super::*;

    #[test]
    fn effective_capture_plan_merges_option_and_hip4_manager_views() {
        let static_plan = CapturePlan::default();
        let option_dynamic = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: InstrumentId::from_str("BTC-OPT.DERIBIT").unwrap(),
            }],
            ..CapturePlan::default()
        };
        let hip4_dynamic = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: InstrumentId::from_str("BTC-HIP4.HYPERLIQUID").unwrap(),
            }],
            ..CapturePlan::default()
        };
        let initial = merge_capture_plans(
            &merge_capture_plans(&static_plan, &option_dynamic),
            &hip4_dynamic,
        );

        let option_manager = DynamicOptionUniverseManager::new(DynamicOptionUniverseConfig {
            refresh_interval_secs: 60,
            strike_change_confirmations: 0,
            static_plan: static_plan.clone(),
            initial_dynamic_plan: option_dynamic,
            universes: vec![],
        });
        let hip4_manager = DynamicHip4UniverseManager::new(DynamicHip4UniverseConfig {
            idle_poll_secs: 1800,
            active_poll_secs: 10,
            pre_expiry_window_secs: 900,
            http_timeout_secs: 10,
            purge_removed_instruments: false,
            static_plan: static_plan.clone(),
            initial_dynamic_plan: hip4_dynamic,
            universes: vec![],
        });

        let merged = effective_capture_plan(
            &initial,
            &CapturePlan::default(),
            Some(&option_manager),
            Some(&hip4_manager),
        );

        assert_eq!(merged.quotes.len(), 2);
    }

    #[test]
    fn supplemental_plan_preserves_hip4_when_only_option_refresh_enabled() {
        let static_plan = CapturePlan::default();
        let option_dynamic = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: InstrumentId::from_str("BTC-OPT.DERIBIT").unwrap(),
            }],
            ..CapturePlan::default()
        };
        let hip4_only = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: InstrumentId::from_str("BTC-HIP4.HYPERLIQUID").unwrap(),
            }],
            ..CapturePlan::default()
        };
        let initial = merge_capture_plans(
            &merge_capture_plans(&static_plan, &option_dynamic),
            &hip4_only,
        );
        let option_config = DynamicOptionUniverseConfig {
            refresh_interval_secs: 60,
            strike_change_confirmations: 0,
            static_plan: static_plan.clone(),
            initial_dynamic_plan: option_dynamic,
            universes: vec![],
        };

        let supplemental = supplemental_capture_plan(&initial, &Some(option_config), &None);

        assert_eq!(supplemental.quotes.len(), 1);
        assert_eq!(
            supplemental.quotes[0].instrument_id,
            InstrumentId::from_str("BTC-HIP4.HYPERLIQUID").unwrap()
        );
    }

    #[test]
    fn count_enabled_background_workers_counts_true_flags() {
        assert_eq!(
            count_enabled_background_workers([
                true, true, false, false, false, false, false, false, false, false, false, false,
            ]),
            2
        );
    }
}
