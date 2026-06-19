use std::str::FromStr;

use anyhow::{bail, Context, Result};
use catalog_capture_core::{
    expand_option_universe, merge_capture_plans, resolve_option_universe, CapturePlan,
    OptionUniverseFamily, OptionUniverseSpec, ResolvedOptionUniverse,
};
use nautilus_deribit::{
    common::enums::{DeribitCurrency, DeribitEnvironment},
    http::{client::DeribitHttpClient, models::DeribitBookSummary, models::DeribitProductType},
};
use nautilus_model::{identifiers::InstrumentId, types::Price};

use crate::config::{EffectiveConfig, VenueRuntimeConfig};

pub async fn materialize_capture_plan(config: &EffectiveConfig) -> Result<CapturePlan> {
    let mut plan = config.plan.clone();

    for spec in &config.option_universes {
        let venue = resolve_option_universe_venue(spec, &config.venues)?;
        let resolved = match venue {
            VenueRuntimeConfig::Deribit { environment, .. } => {
                resolve_deribit_option_universe(spec, *environment).await?
            }
            _ => bail!(
                "capture.option_universe currently only supports Deribit venues; got venue_id `{}`",
                spec.venue_id
            ),
        };

        log_resolved_option_universe(spec, &resolved);

        let expanded = expand_option_universe(spec, &resolved);
        plan = merge_capture_plans(&plan, &expanded);
    }

    Ok(plan)
}

pub fn validate_option_universes(
    specs: &[OptionUniverseSpec],
    venues: &[VenueRuntimeConfig],
) -> Result<()> {
    for spec in specs {
        validate_option_universe(spec, venues)?;
    }
    Ok(())
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
        _ => bail!(
            "capture.option_universe currently only supports [[venues]] entries with kind = \"deribit\""
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

    Ok(())
}

fn resolve_option_universe_venue<'a>(
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

async fn resolve_deribit_option_universe(
    spec: &OptionUniverseSpec,
    environment: DeribitEnvironment,
) -> Result<ResolvedOptionUniverse> {
    let currency = DeribitCurrency::from_str(&spec.underlying).with_context(|| {
        format!(
            "unsupported Deribit option underlying `{}`",
            spec.underlying
        )
    })?;
    let client = DeribitHttpClient::new(None, environment, 30, 3, 500, 5_000, None)
        .context("failed to create Deribit HTTP client for option universe resolution")?;

    let option_instruments = client
        .request_instruments(currency, Some(DeribitProductType::Option))
        .await
        .with_context(|| {
            format!(
                "failed to request Deribit option instruments for underlying {}",
                spec.underlying
            )
        })?;

    let atm_reference = request_deribit_atm_reference(&client, spec).await?;
    let perp_instrument_id = spec
        .include_perp
        .then(|| derive_deribit_perpetual_id(&spec.underlying))
        .transpose()?;

    resolve_option_universe(
        spec,
        &option_instruments,
        nautilus_core::time::get_atomic_clock_realtime().get_time_ns(),
        atm_reference,
        perp_instrument_id,
    )
    .map_err(anyhow::Error::from)
}

async fn request_deribit_atm_reference(
    client: &DeribitHttpClient,
    spec: &OptionUniverseSpec,
) -> Result<Price> {
    let summaries = client
        .request_book_summaries(&spec.underlying)
        .await
        .with_context(|| {
            format!(
                "failed to request Deribit option book summaries for underlying {}",
                spec.underlying
            )
        })?;

    select_deribit_atm_reference(&summaries, &spec.underlying)
}

fn select_deribit_atm_reference(
    summaries: &[DeribitBookSummary],
    underlying: &str,
) -> Result<Price> {
    let Some(summary) = summaries
        .iter()
        .find(|summary| summary.underlying_price.is_some())
    else {
        bail!(
            "no Deribit option book summary returned an underlying_price for {}",
            underlying
        );
    };

    Ok(Price::from(
        summary
            .underlying_price
            .expect("summary filtered to underlying_price")
            .to_string()
            .as_str(),
    ))
}

fn derive_deribit_perpetual_id(underlying: &str) -> Result<InstrumentId> {
    Ok(InstrumentId::from_str(
        format!("{underlying}-PERPETUAL.DERIBIT").as_str(),
    )?)
}

fn log_resolved_option_universe(spec: &OptionUniverseSpec, resolved: &ResolvedOptionUniverse) {
    println!(
        "Resolved option universe {} {} expiry={} atm={} strikes={:?} instruments={:?}",
        spec.venue_id,
        spec.underlying,
        resolved.selected_expiry_ns.as_u64(),
        resolved.atm_reference,
        resolved.selected_strikes,
        resolved.all_instrument_ids
    );
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    #[test]
    fn select_deribit_atm_reference_uses_first_non_empty_underlying_price() {
        let summaries = vec![
            DeribitBookSummary {
                instrument_name: "BTC-20JUN26-62000-C".to_string(),
                underlying_price: None,
                underlying_index: None,
                mark_price: None,
                creation_timestamp: 0,
            },
            DeribitBookSummary {
                instrument_name: "BTC-20JUN26-62500-C".to_string(),
                underlying_price: Some(Decimal::from_str("62393.29").unwrap()),
                underlying_index: Some("BTC-20JUN26".to_string()),
                mark_price: None,
                creation_timestamp: 0,
            },
        ];

        let price = select_deribit_atm_reference(&summaries, "BTC").expect("price should resolve");
        assert_eq!(price, Price::from("62393.29"));
    }

    #[test]
    fn select_deribit_atm_reference_fails_when_missing_forward_price() {
        let summaries = vec![DeribitBookSummary {
            instrument_name: "BTC-20JUN26-62000-C".to_string(),
            underlying_price: None,
            underlying_index: None,
            mark_price: None,
            creation_timestamp: 0,
        }];

        let err = select_deribit_atm_reference(&summaries, "BTC")
            .expect_err("missing underlying_price should fail");
        assert!(err.to_string().contains("underlying_price"));
    }

    #[test]
    fn derive_deribit_perpetual_id_builds_expected_symbol() {
        let instrument_id = derive_deribit_perpetual_id("BTC").expect("perpetual id should build");
        assert_eq!(instrument_id, InstrumentId::from("BTC-PERPETUAL.DERIBIT"));
    }
}
