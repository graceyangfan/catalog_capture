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

use std::path::Path;

use anyhow::{bail, Result};
use catalog_capture_core::{
    read_option_universe_resolution_records, summarize_option_universe_resolution_records,
    validate_option_universe_readback, OptionUniverseReadbackOptions, OptionUniverseReadbackReport,
    StrikePolicy, ALL_STRIKES_READBACK_SAMPLE_LIMIT,
};

use crate::config::EffectiveConfig;

use super::catalog_presets::validation_options_for_config;

pub fn readback_options_for_config(
    config: &EffectiveConfig,
    catalog_root: &Path,
    require_contract_state: bool,
) -> Result<OptionUniverseReadbackOptions> {
    let records = read_option_universe_resolution_records(catalog_root)?;
    let summaries = summarize_option_universe_resolution_records(&records);
    let summary = summaries
        .first()
        .ok_or_else(|| anyhow::anyhow!("no option universe resolution metadata found"))?;

    let perp_instrument_id = summary
        .perp_instrument_id
        .clone()
        .ok_or_else(|| anyhow::anyhow!("option universe resolution missing perp_instrument_id"))?;

    let mut option_instrument_ids = summary.option_instrument_ids.clone();
    if config
        .option_universes
        .iter()
        .any(|spec| matches!(spec.strike_policy, StrikePolicy::AllStrikes))
        && option_instrument_ids.len() > ALL_STRIKES_READBACK_SAMPLE_LIMIT
    {
        option_instrument_ids.truncate(ALL_STRIKES_READBACK_SAMPLE_LIMIT);
    }

    let catalog_options = validation_options_for_config(config);
    Ok(OptionUniverseReadbackOptions {
        perp_instrument_id,
        option_instrument_ids,
        min_rows: 1,
        min_perp_trade_rows: catalog_options.min_perp_trade_rows,
        require_contract_state,
        bar_types: catalog_options.bar_types,
    })
}

pub fn readback_options_from_cli(
    perp_instrument_id: Option<String>,
    option_instrument_ids: Vec<String>,
    min_rows: Option<i64>,
    min_perp_trade_rows: Option<i64>,
    require_contract_state: bool,
    bar_types: Vec<String>,
) -> Result<OptionUniverseReadbackOptions> {
    let perp_instrument_id =
        perp_instrument_id.ok_or_else(|| anyhow::anyhow!("--perp-id is required"))?;
    if option_instrument_ids.is_empty() {
        bail!("at least one --option-id is required");
    }

    Ok(OptionUniverseReadbackOptions {
        perp_instrument_id,
        option_instrument_ids,
        min_rows: min_rows.unwrap_or(1),
        min_perp_trade_rows: min_perp_trade_rows.unwrap_or(0),
        require_contract_state,
        bar_types,
    })
}

pub fn run_option_universe_readback_validation(
    catalog_root: &Path,
    options: &OptionUniverseReadbackOptions,
) -> Result<OptionUniverseReadbackReport> {
    validate_option_universe_readback(catalog_root, options)
}

pub fn render_option_universe_readback_json(
    report: &OptionUniverseReadbackReport,
) -> Result<String> {
    serde_json::to_string_pretty(report)
        .map_err(|err| anyhow::anyhow!("failed to render option universe readback: {err}"))
}

pub fn render_option_universe_readback_text(report: &OptionUniverseReadbackReport) -> String {
    let mut lines = vec![
        format!("Perp: {}", report.perp.instrument_id),
        format!(
            "Perp quotes={} trade_ticks={} mark_prices={} index_prices={} \
             instrument_statuses={} instrument_closes={}",
            report.perp.quotes,
            report.perp.trade_ticks,
            report.perp.mark_prices,
            report.perp.index_prices,
            report.perp.instrument_statuses,
            report.perp.instrument_closes,
        ),
        format!("Perp funding_rows={}", report.funding_rows),
    ];

    for bar in &report.bars {
        lines.push(format!("Bars: {} rows={}", bar.bar_type, bar.rows));
    }
    for option in &report.options {
        lines.push(format!(
            "Option: {} quotes={} mark_prices={} option_greeks={} \
             instrument_statuses={} instrument_closes={}",
            option.instrument_id,
            option.quotes,
            option.mark_prices,
            option.option_greeks,
            option.instrument_statuses,
            option.instrument_closes,
        ));
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readback_options_from_cli_requires_perp_and_option_ids() {
        let err = readback_options_from_cli(None, vec![], None, None, false, vec![])
            .expect_err("missing ids should fail");
        assert!(err.to_string().contains("--perp-id"));
    }

    #[test]
    fn readback_options_for_config_samples_all_strike_universe() {
        let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = crate::config::load_config(
            &repo_root.join("examples/capture.deribit-btc-universe-all.toml"),
        )
        .expect("example should load");
        let effective = crate::config::resolve_config(config).expect("example should resolve");

        let summary = catalog_capture_core::OptionUniverseResolutionSummary {
            venue_id: "deribit_main".to_string(),
            underlying: "BTC".to_string(),
            startup_resolved_at_iso8601: "2026-06-20T00:00:00Z".to_string(),
            latest_event_kind: catalog_capture_core::OptionUniverseResolutionEventKind::Startup,
            latest_resolved_at_iso8601: "2026-06-20T00:00:00Z".to_string(),
            latest_selected_expiry_iso8601: "2026-06-26T08:00:00Z".to_string(),
            strike_selection_mode: "all".to_string(),
            refresh_count: 0,
            latest_rollover_reason: None,
            perp_instrument_id: Some("BTC-PERPETUAL.DERIBIT".to_string()),
            option_count: 12,
            option_instrument_ids: (0..12)
                .map(|idx| format!("BTC-26JUN26-{}-C.DERIBIT", 64000 + idx))
                .collect(),
        };

        let temp = std::env::temp_dir().join(format!(
            "option-universe-readback-options-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp).unwrap();
        let record = catalog_capture_core::startup_resolution_record(
            &effective.option_universes[0],
            &catalog_capture_core::ResolvedOptionUniverse {
                resolved_at_ns: 1.into(),
                selected_expiry_ns: 2.into(),
                atm_reference: nautilus_model::types::Price::from("65000"),
                atm_reference_source: None,
                selected_strikes: vec![],
                perp_instrument_id: summary
                    .perp_instrument_id
                    .as_ref()
                    .map(|id| nautilus_model::identifiers::InstrumentId::from(id.as_str())),
                option_instrument_ids: summary
                    .option_instrument_ids
                    .iter()
                    .map(|id| nautilus_model::identifiers::InstrumentId::from(id.as_str()))
                    .collect(),
                all_instrument_ids: vec![],
            },
            vec![],
            vec![],
        );
        catalog_capture_core::append_option_universe_resolution_records(&temp, &[record]).unwrap();

        let options = readback_options_for_config(&effective, &temp, false).expect("options");
        assert_eq!(
            options.option_instrument_ids.len(),
            ALL_STRIKES_READBACK_SAMPLE_LIMIT
        );

        std::fs::remove_dir_all(temp).ok();
    }
}
