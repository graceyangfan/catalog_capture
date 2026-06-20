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
                require_refresh_change: true,
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
    fn rolling_autorefresh_preset_requires_refresh_change() {
        let options = validation_options_for_preset(
            OptionUniverseCatalogValidationPreset::RollingAutorefresh,
        );
        assert!(options.require_refresh_change);
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