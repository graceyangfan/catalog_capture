use anyhow::{bail, Context, Result};
use catalog_capture_core::{OptionUniverseFamily, OptionUniverseSpec, StrikePolicy};
use nautilus_bybit::common::enums::BybitProductType;
use nautilus_deribit::http::models::DeribitProductType;
use nautilus_okx::common::enums::OKXInstrumentType;

use crate::config::VenueRuntimeConfig;

pub fn validate_option_universes(
    specs: &[OptionUniverseSpec],
    venues: &[VenueRuntimeConfig],
) -> Result<()> {
    for spec in specs {
        validate_option_universe(spec, venues)?;
    }
    Ok(())
}

pub(crate) fn resolve_option_universe_venue<'a>(
    spec: &OptionUniverseSpec,
    venues: &'a [VenueRuntimeConfig],
) -> Result<&'a VenueRuntimeConfig> {
    venues
        .iter()
        .find(|venue| venue.id() == spec.venue_id)
        .with_context(|| {
            format!(
                "capture.option_universe references unknown venue_id `{}`",
                spec.venue_id
            )
        })
}

fn validate_option_universe(
    spec: &OptionUniverseSpec,
    venues: &[VenueRuntimeConfig],
) -> Result<()> {
    let venue = resolve_option_universe_venue(spec, venues)?;

    match venue {
        VenueRuntimeConfig::Deribit { product_types, .. } => {
            if !product_types.contains(&DeribitProductType::Option) {
                bail!(
                    "capture.option_universe venue_id `{}` requires deribit product_types to include \"option\"",
                    spec.venue_id
                );
            }
            if spec.include_perp && !product_types.contains(&DeribitProductType::Future) {
                bail!(
                    "capture.option_universe venue_id `{}` with include_perp = true requires deribit product_types to include \"future\"",
                    spec.venue_id
                );
            }
        }
        VenueRuntimeConfig::Bybit { product_types, .. } => {
            if !product_types.contains(&BybitProductType::Option) {
                bail!(
                    "capture.option_universe venue_id `{}` requires bybit product_types to include \"option\"",
                    spec.venue_id
                );
            }
            if spec.include_perp && !product_types.contains(&BybitProductType::Linear) {
                bail!(
                    "capture.option_universe venue_id `{}` with include_perp = true requires bybit product_types to include \"linear\"",
                    spec.venue_id
                );
            }
            if spec.settlement_currency.is_none() {
                bail!(
                    "capture.option_universe venue_id `{}` requires settlement_currency for Bybit option universe resolution",
                    spec.venue_id
                );
            }
        }
        VenueRuntimeConfig::Okx {
            instrument_types,
            instrument_families,
            ..
        } => {
            if !instrument_types.contains(&OKXInstrumentType::Option) {
                bail!(
                    "capture.option_universe venue_id `{}` requires okx instrument_types to include \"option\"",
                    spec.venue_id
                );
            }
            if spec.include_perp && !instrument_types.contains(&OKXInstrumentType::Swap) {
                bail!(
                    "capture.option_universe venue_id `{}` with include_perp = true requires okx instrument_types to include \"swap\"",
                    spec.venue_id
                );
            }
            let Some(settlement_currency) = spec.settlement_currency.as_deref() else {
                bail!(
                    "capture.option_universe venue_id `{}` requires settlement_currency for OKX option universe resolution",
                    spec.venue_id
                );
            };
            let expected_family = format!("{}-{settlement_currency}", spec.underlying);
            if instrument_families
                .as_ref()
                .is_none_or(|families| !families.iter().any(|family| family == &expected_family))
            {
                bail!(
                    "capture.option_universe venue_id `{}` requires okx instrument_families to include `{expected_family}`",
                    spec.venue_id
                );
            }
        }
        _ => bail!(
            "capture.option_universe currently only supports [[venues]] entries with kind = \"deribit\", \"bybit\", or \"okx\""
        ),
    }

    let needs_perp = spec.families.iter().any(|family| {
        matches!(
            family,
            OptionUniverseFamily::IndexPrices | OptionUniverseFamily::FundingRates
        )
    });
    if needs_perp && !spec.include_perp {
        bail!(
            "capture.option_universe families index_prices/funding_rates require include_perp = true"
        );
    }

    let wants_forward_prices = spec
        .families
        .iter()
        .any(|family| matches!(family, OptionUniverseFamily::ForwardPrices));
    let has_option_greeks = spec
        .families
        .iter()
        .any(|family| matches!(family, OptionUniverseFamily::OptionGreeks));
    if wants_forward_prices && !has_option_greeks {
        bail!(
            "capture.option_universe families forward_prices require option_greeks in the same families list"
        );
    }

    if matches!(spec.strike_policy, StrikePolicy::OiRanked { .. }) && !has_option_greeks {
        bail!(
            "capture.option_universe strike_policy oi_ranked requires option_greeks in families"
        );
    }

    Ok(())
}

