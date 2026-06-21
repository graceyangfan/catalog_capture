use catalog_capture_core::OptionUniverseFamily;

use crate::config::EffectiveConfig;

use super::catalog::OptionUniverseCatalogValidationOptions;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionUniverseCatalogValidationPreset {
    PostCapture,
    RollingAutorefresh,
    VenueTrades,
    Research,
}

pub fn validation_options_for_preset(
    preset: OptionUniverseCatalogValidationPreset,
) -> OptionUniverseCatalogValidationOptions {
    match preset {
        OptionUniverseCatalogValidationPreset::PostCapture => OptionUniverseCatalogValidationOptions {
            min_rows: 1,
            min_perp_trade_rows: 0,
            require_contract_state: false,
            require_refresh_change: false,
            bar_types: Vec::new(),
        },
        OptionUniverseCatalogValidationPreset::RollingAutorefresh => {
            OptionUniverseCatalogValidationOptions {
                min_rows: 1,
                min_perp_trade_rows: 0,
                require_contract_state: false,
                require_refresh_change: false,
                bar_types: Vec::new(),
            }
        }
        OptionUniverseCatalogValidationPreset::VenueTrades => OptionUniverseCatalogValidationOptions {
            min_rows: 1,
            min_perp_trade_rows: 1,
            require_contract_state: false,
            require_refresh_change: false,
            bar_types: Vec::new(),
        },
        OptionUniverseCatalogValidationPreset::Research => OptionUniverseCatalogValidationOptions {
            min_rows: 1,
            min_perp_trade_rows: 0,
            require_contract_state: true,
            require_refresh_change: false,
            bar_types: vec!["BTC-PERPETUAL.DERIBIT-1-MINUTE-LAST-EXTERNAL".to_string()],
        },
    }
}

pub fn validation_options_for_config(config: &EffectiveConfig) -> OptionUniverseCatalogValidationOptions {
    let preset = validation_preset_for_config(config);
    let mut options = validation_options_for_preset(preset);

    let bar_types = config
        .plan
        .bars
        .iter()
        .map(|spec| spec.bar_type.to_string())
        .collect::<Vec<_>>();
    if !bar_types.is_empty() {
        options.bar_types = bar_types;
    }

    if config
        .option_universes
        .iter()
        .any(|spec| option_universe_requires_contract_state(spec))
    {
        options.require_contract_state = true;
    }

    if config_uses_bybit_or_okx_option_universe(config) {
        options.min_perp_trade_rows = options.min_perp_trade_rows.max(1);
    }

    options
}

pub fn validation_preset_for_config(
    config: &EffectiveConfig,
) -> OptionUniverseCatalogValidationPreset {
    if config_has_research_baseline(config) {
        return OptionUniverseCatalogValidationPreset::Research;
    }
    if config.runtime.option_universe_refresh.enabled {
        return OptionUniverseCatalogValidationPreset::RollingAutorefresh;
    }
    if config_uses_bybit_or_okx_option_universe(config) {
        return OptionUniverseCatalogValidationPreset::VenueTrades;
    }
    OptionUniverseCatalogValidationPreset::PostCapture
}

fn config_has_research_baseline(config: &EffectiveConfig) -> bool {
    config.plan.bars.iter().any(|spec| {
        spec.bar_type
            .to_string()
            .contains("BTC-PERPETUAL.DERIBIT-1-MINUTE-LAST-EXTERNAL")
    }) || config.plan.custom_data.iter().any(|spec| {
        spec.data_type.type_name() == "DeribitVolatilityIndex"
    })
}

fn option_universe_requires_contract_state(
    spec: &catalog_capture_core::OptionUniverseSpec,
) -> bool {
    spec.families.iter().any(|family| {
        matches!(
            family,
            OptionUniverseFamily::InstrumentStatuses | OptionUniverseFamily::InstrumentCloses
        )
    })
}

fn config_uses_bybit_or_okx_option_universe(config: &EffectiveConfig) -> bool {
    config.option_universes.iter().any(|spec| {
        let venue = spec.venue_id.to_ascii_lowercase();
        venue.contains("bybit") || venue.contains("okx")
    })
}

pub fn merge_validation_options(
    mut base: OptionUniverseCatalogValidationOptions,
    overrides: &OptionUniverseCatalogValidationOverrides,
) -> OptionUniverseCatalogValidationOptions {
    if let Some(min_rows) = overrides.min_rows {
        base.min_rows = min_rows;
    }
    if let Some(min_perp_trade_rows) = overrides.min_perp_trade_rows {
        base.min_perp_trade_rows = min_perp_trade_rows;
    }
    if overrides.require_contract_state {
        base.require_contract_state = true;
    }
    if overrides.require_refresh_change {
        base.require_refresh_change = true;
    }
    if !overrides.bar_types.is_empty() {
        base.bar_types = overrides.bar_types.clone();
    }
    base
}

#[derive(Debug, Clone, Default)]
pub struct OptionUniverseCatalogValidationOverrides {
    pub min_rows: Option<i64>,
    pub min_perp_trade_rows: Option<i64>,
    pub require_contract_state: bool,
    pub require_refresh_change: bool,
    pub bar_types: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rolling_autorefresh_preset_defers_refresh_change_to_explicit_flag() {
        let options = validation_options_for_preset(
            OptionUniverseCatalogValidationPreset::RollingAutorefresh,
        );
        assert!(!options.require_refresh_change);
        let merged = merge_validation_options(
            options,
            &OptionUniverseCatalogValidationOverrides {
                require_refresh_change: true,
                ..OptionUniverseCatalogValidationOverrides::default()
            },
        );
        assert!(merged.require_refresh_change);
    }

    #[test]
    fn research_preset_requires_contract_state_and_bar_type() {
        let options =
            validation_options_for_preset(OptionUniverseCatalogValidationPreset::Research);
        assert!(options.require_contract_state);
        assert_eq!(
            options.bar_types,
            vec!["BTC-PERPETUAL.DERIBIT-1-MINUTE-LAST-EXTERNAL".to_string()]
        );
    }

    #[test]
    fn validation_preset_for_config_detects_autorefresh_profile() {
        let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = crate::config::load_config(
            &repo_root.join("examples/capture.deribit-btc-universe-autorefresh.toml"),
        )
        .expect("example should load");
        let effective = crate::config::resolve_config(config).expect("example should resolve");
        assert_eq!(
            validation_preset_for_config(&effective),
            OptionUniverseCatalogValidationPreset::RollingAutorefresh
        );
    }

    #[test]
    fn validation_preset_for_config_detects_research_profile() {
        let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = crate::config::load_config(
            &repo_root.join("examples/capture.deribit-btc-universe-research.toml"),
        )
        .expect("example should load");
        let effective = crate::config::resolve_config(config).expect("example should resolve");
        assert_eq!(
            validation_preset_for_config(&effective),
            OptionUniverseCatalogValidationPreset::Research
        );
        let options = validation_options_for_config(&effective);
        assert!(options.require_contract_state);
    }

    #[test]
    fn validation_options_for_config_adds_bybit_trade_requirement() {
        let repo_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = crate::config::load_config(
            &repo_root.join("examples/capture.bybit-btc-universe.toml"),
        )
        .expect("example should load");
        let effective = crate::config::resolve_config(config).expect("example should resolve");
        let options = validation_options_for_config(&effective);
        assert_eq!(options.min_perp_trade_rows, 1);
    }

    #[test]
    fn merge_validation_options_keeps_preset_unless_overridden() {
        let base = validation_options_for_preset(OptionUniverseCatalogValidationPreset::PostCapture);
        let merged = merge_validation_options(
            base,
            &OptionUniverseCatalogValidationOverrides {
                min_perp_trade_rows: Some(3),
                require_refresh_change: true,
                ..OptionUniverseCatalogValidationOverrides::default()
            },
        );
        assert_eq!(merged.min_rows, 1);
        assert_eq!(merged.min_perp_trade_rows, 3);
        assert!(merged.require_refresh_change);
    }
}