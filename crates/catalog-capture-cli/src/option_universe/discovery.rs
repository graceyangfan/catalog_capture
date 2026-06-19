use std::str::FromStr;

use anyhow::{bail, Context, Result};
use catalog_capture_core::{
    derive_perp_instrument_id, okx_instrument_family, resolve_option_universe,
    select_http_perp_ticker_atm_reference, select_nearest_expiry_reference_instrument_id,
    AtmReferenceSource, OptionUniverseSpec, OptionUniverseVenueKind, ResolvedOptionUniverse,
};
use nautilus_bybit::{
    common::{enums::BybitEnvironment, urls::bybit_http_base_url},
    http::{client::BybitHttpClient, query::BybitTickersParams},
};
use nautilus_bybit::common::enums::BybitProductType;
use nautilus_deribit::{
    common::enums::{DeribitCurrency, DeribitEnvironment},
    http::{client::DeribitHttpClient, models::DeribitProductType},
};
use nautilus_model::instruments::InstrumentAny;
use nautilus_model::{identifiers::InstrumentId, types::Price};
use nautilus_okx::{
    common::enums::{OKXEnvironment, OKXInstrumentType},
    http::client::OKXHttpClient,
};
use ustr::Ustr;

use crate::config::VenueRuntimeConfig;

use super::validate::resolve_option_universe_venue;

pub async fn resolve_option_universe_spec(
    spec: &OptionUniverseSpec,
    venues: &[VenueRuntimeConfig],
) -> Result<ResolvedOptionUniverse> {
    let venue = resolve_option_universe_venue(spec, venues)?;
    dispatch_option_universe_resolution(spec, venue).await
}

async fn dispatch_option_universe_resolution(
    spec: &OptionUniverseSpec,
    venue: &VenueRuntimeConfig,
) -> Result<ResolvedOptionUniverse> {
    match venue {
        VenueRuntimeConfig::Deribit { environment, .. } => {
            resolve_deribit_option_universe(spec, *environment).await
        }
        VenueRuntimeConfig::Bybit { environment, .. } => {
            resolve_bybit_option_universe(spec, *environment).await
        }
        VenueRuntimeConfig::Okx { environment, .. } => {
            resolve_okx_option_universe(spec, *environment).await
        }
        _ => bail!(
            "capture.option_universe currently only supports Deribit/Bybit/OKX venues; got venue_id `{}`",
            spec.venue_id
        ),
    }
}

struct ResolvedVenueInputs {
    resolved_at_ns: nautilus_core::UnixNanos,
    spec_for_resolution: OptionUniverseSpec,
    option_instruments: Vec<InstrumentAny>,
    atm_reference: Price,
    atm_reference_source: String,
    perp_instrument_id: Option<InstrumentId>,
}

fn finalize_option_universe_resolution(
    inputs: ResolvedVenueInputs,
) -> Result<ResolvedOptionUniverse> {
    let mut resolved = resolve_option_universe(
        &inputs.spec_for_resolution,
        &inputs.option_instruments,
        inputs.resolved_at_ns,
        inputs.atm_reference,
        inputs.perp_instrument_id,
    )
    .map_err(anyhow::Error::from)?;
    resolved.atm_reference_source = Some(inputs.atm_reference_source);
    Ok(resolved)
}

fn normalized_resolution_spec(
    spec: &OptionUniverseSpec,
    clear_settlement_currency: bool,
) -> OptionUniverseSpec {
    let mut normalized = spec.clone();
    if clear_settlement_currency {
        normalized.settlement_currency = None;
    }
    normalized
}

async fn resolve_deribit_option_universe(
    spec: &OptionUniverseSpec,
    environment: DeribitEnvironment,
) -> Result<ResolvedOptionUniverse> {
    let resolved_at_ns = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
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

    let (atm_reference, atm_reference_source) = request_deribit_atm_reference(&client, spec).await?;
    let perp_instrument_id = spec
        .include_perp
        .then(|| derive_perp_instrument_id(spec, OptionUniverseVenueKind::Deribit).map_err(anyhow::Error::from))
        .transpose()?;

    finalize_option_universe_resolution(ResolvedVenueInputs {
        resolved_at_ns,
        spec_for_resolution: normalized_resolution_spec(spec, false),
        option_instruments,
        atm_reference,
        atm_reference_source,
        perp_instrument_id,
    })
}

async fn resolve_bybit_option_universe(
    spec: &OptionUniverseSpec,
    environment: BybitEnvironment,
) -> Result<ResolvedOptionUniverse> {
    let resolved_at_ns = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
    let client = BybitHttpClient::new(
        Some(bybit_http_base_url(environment).to_string()),
        30,
        3,
        500,
        5_000,
        5_000,
        None,
    )
    .context("failed to create Bybit HTTP client for option universe resolution")?;

    let option_instruments = client
        .request_instruments(
            nautilus_bybit::common::enums::BybitProductType::Option,
            None,
            Some(Ustr::from(spec.underlying.as_str())),
        )
        .await
        .with_context(|| {
            format!(
                "failed to request Bybit option instruments for underlying {}",
                spec.underlying
            )
        })?;

    let (atm_reference, atm_reference_source) = request_bybit_atm_reference(&client, spec).await?;
    let perp_instrument_id = spec
        .include_perp
        .then(|| derive_perp_instrument_id(spec, OptionUniverseVenueKind::Bybit).map_err(anyhow::Error::from))
        .transpose()?;

    finalize_option_universe_resolution(ResolvedVenueInputs {
        resolved_at_ns,
        spec_for_resolution: normalized_resolution_spec(spec, false),
        option_instruments,
        atm_reference,
        atm_reference_source,
        perp_instrument_id,
    })
}

async fn resolve_okx_option_universe(
    spec: &OptionUniverseSpec,
    environment: OKXEnvironment,
) -> Result<ResolvedOptionUniverse> {
    let resolved_at_ns = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
    let client = OKXHttpClient::new(None, 30, 3, 500, 5_000, environment, None)
        .context("failed to create OKX HTTP client for option universe resolution")?;
    let instrument_family = okx_instrument_family(spec).map_err(anyhow::Error::from)?;
    // OKX uses the family suffix (for example `BTC-USD`) in config, but the parsed
    // instrument settlement currency is the base asset (for example `BTC`), so the
    // core settlement filter must be cleared before strike selection.
    let normalized_spec = normalized_resolution_spec(spec, true);
    let (option_instruments, _) = client
        .request_instruments(OKXInstrumentType::Option, Some(instrument_family.clone()))
        .await
        .with_context(|| {
            format!(
                "failed to request OKX option instruments for family {}",
                instrument_family
            )
        })?;
    for instrument in &option_instruments {
        client.cache_instrument(instrument.clone());
    }

    let (atm_reference, atm_reference_source) = request_okx_atm_reference(
        &client,
        spec,
        &normalized_spec,
        &option_instruments,
        resolved_at_ns,
    )
    .await?;
    let perp_instrument_id = spec
        .include_perp
        .then(|| derive_perp_instrument_id(spec, OptionUniverseVenueKind::Okx).map_err(anyhow::Error::from))
        .transpose()?;

    finalize_option_universe_resolution(ResolvedVenueInputs {
        resolved_at_ns,
        spec_for_resolution: normalized_spec,
        option_instruments,
        atm_reference,
        atm_reference_source,
        perp_instrument_id,
    })
}

async fn request_deribit_atm_reference(
    client: &DeribitHttpClient,
    spec: &OptionUniverseSpec,
) -> Result<(Price, String)> {
    let perp_name = format!("{}-PERPETUAL", spec.underlying);
    let ticker = client
        .request_ticker(&perp_name)
        .await
        .with_context(|| format!("failed to request Deribit ticker for {perp_name}"))?;
    let mark = ticker.mark_price.as_ref().map(ToString::to_string);
    let index = ticker.index_price.as_ref().map(ToString::to_string);
    let bid = ticker.best_bid_price.as_ref().map(ToString::to_string);
    let ask = ticker.best_ask_price.as_ref().map(ToString::to_string);
    if let Some((price, source)) = select_http_perp_ticker_atm_reference(
        mark.as_deref(),
        index.as_deref(),
        bid.as_deref(),
        ask.as_deref(),
    ) {
        return Ok((price, source.as_str().to_string()));
    }

    bail!("no Deribit perpetual ticker reference available for {perp_name}")
}

async fn request_bybit_atm_reference(
    client: &BybitHttpClient,
    spec: &OptionUniverseSpec,
) -> Result<(Price, String)> {
    let perp_instrument_id =
        derive_perp_instrument_id(spec, OptionUniverseVenueKind::Bybit).map_err(anyhow::Error::from)?;
    let params = BybitTickersParams {
        category: BybitProductType::Linear,
        symbol: Some(perp_instrument_id.symbol.to_string()),
        base_coin: None,
        exp_date: None,
    };
    let tickers = client
        .request_tickers(&params)
        .await
        .with_context(|| {
            format!(
                "failed to request Bybit linear ticker for {}",
                perp_instrument_id.symbol
            )
        })?;
    let Some(ticker) = tickers.first() else {
        bail!(
            "no Bybit linear ticker returned for {}",
            perp_instrument_id.symbol
        );
    };

    if let Some((price, source)) = select_http_perp_ticker_atm_reference(
        ticker.mark_price.as_deref(),
        ticker.index_price.as_deref(),
        Some(ticker.bid1_price.as_str()),
        Some(ticker.ask1_price.as_str()),
    ) {
        return Ok((price, source.as_str().to_string()));
    }

    bail!(
        "no Bybit linear ticker reference available for {}",
        perp_instrument_id.symbol
    )
}

async fn request_okx_atm_reference(
    client: &OKXHttpClient,
    spec: &OptionUniverseSpec,
    normalized_spec: &OptionUniverseSpec,
    option_instruments: &[InstrumentAny],
    resolved_at_ns: nautilus_core::UnixNanos,
) -> Result<(Price, String)> {
    let reference_instrument_id = select_nearest_expiry_reference_instrument_id(
        normalized_spec,
        option_instruments,
        resolved_at_ns,
    )
    .map_err(anyhow::Error::from)?;
    let forward_prices = client
        .request_forward_prices(&spec.underlying, Some(reference_instrument_id))
        .await
        .with_context(|| {
            format!(
                "failed to request OKX option forward prices for underlying {}",
                spec.underlying
            )
        })?;

    let price = select_okx_atm_reference(&forward_prices, &spec.underlying)?;
    Ok((
        price,
        AtmReferenceSource::HttpPerpForwardPrice.as_str().to_string(),
    ))
}

fn select_okx_atm_reference(
    forward_prices: &[nautilus_model::data::ForwardPrice],
    underlying: &str,
) -> Result<Price> {
    let Some(forward_price) = forward_prices.first() else {
        bail!(
            "no OKX forward prices were returned for underlying {}",
            underlying
        );
    };

    Ok(Price::from(
        forward_price.forward_price.to_string().as_str(),
    ))
}