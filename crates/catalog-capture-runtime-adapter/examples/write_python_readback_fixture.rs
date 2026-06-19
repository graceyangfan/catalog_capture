use std::{env, fs, path::PathBuf, str::FromStr};

use anyhow::{Context, Result};
use catalog_capture_core::{
    config::CaptureConfig,
    plan::{
        CapturePlan, FundingRateCaptureSpec, IndexPriceCaptureSpec, InstrumentCaptureSpec,
        InstrumentCloseCaptureSpec, InstrumentStatusCaptureSpec, MarkPriceCaptureSpec,
        OptionGreeksCaptureSpec, QuoteCaptureSpec,
    },
};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use nautilus_common::actor::DataActor;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{
        FundingRateUpdate, IndexPriceUpdate, InstrumentStatus, MarkPriceUpdate, OptionGreekValues,
        OptionGreeks, QuoteTick, close::InstrumentClose,
    },
    enums::{GreeksConvention, InstrumentCloseType, MarketStatusAction},
    identifiers::{ActorId, InstrumentId},
    instruments::{InstrumentAny, stubs::crypto_perpetual_ethusdt},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

fn create_quote_ticks(instrument_id: InstrumentId, base_ts: u64, count: usize) -> Vec<QuoteTick> {
    (0..count)
        .map(|index| {
            let ts = base_ts + index as u64 * 1_000;
            QuoteTick::new(
                instrument_id,
                Price::from("1.0001"),
                Price::from("1.0002"),
                Quantity::from("100"),
                Quantity::from("100"),
                UnixNanos::from(ts),
                UnixNanos::from(ts),
            )
        })
        .collect()
}

fn create_mark_price_updates(
    instrument_id: InstrumentId,
    base_ts: u64,
    count: usize,
) -> Vec<MarkPriceUpdate> {
    (0..count)
        .map(|index| {
            let ts = base_ts + index as u64 * 1_000;
            MarkPriceUpdate::new(
                instrument_id,
                Price::from("1000.00"),
                UnixNanos::from(ts),
                UnixNanos::from(ts),
            )
        })
        .collect()
}

fn create_index_price_updates(
    instrument_id: InstrumentId,
    base_ts: u64,
    count: usize,
) -> Vec<IndexPriceUpdate> {
    (0..count)
        .map(|index| {
            let ts = base_ts + index as u64 * 1_000;
            IndexPriceUpdate::new(
                instrument_id,
                Price::from("1001.00"),
                UnixNanos::from(ts),
                UnixNanos::from(ts),
            )
        })
        .collect()
}

fn create_funding_rate_updates(
    instrument_id: InstrumentId,
    base_ts: u64,
    count: usize,
) -> Vec<FundingRateUpdate> {
    (0..count)
        .map(|index| {
            let ts = base_ts + index as u64 * 1_000;
            FundingRateUpdate::new(
                instrument_id,
                Decimal::from_str("0.0001").expect("valid decimal"),
                Some(480),
                Some(UnixNanos::from(ts + 1)),
                UnixNanos::from(ts),
                UnixNanos::from(ts),
            )
        })
        .collect()
}

fn create_instrument_statuses(
    instrument_id: InstrumentId,
    base_ts: u64,
    count: usize,
) -> Vec<InstrumentStatus> {
    (0..count)
        .map(|index| {
            let ts = base_ts + index as u64 * 1_000;
            InstrumentStatus::new(
                instrument_id,
                MarketStatusAction::Trading,
                UnixNanos::from(ts),
                UnixNanos::from(ts),
                None,
                None,
                Some(true),
                Some(true),
                Some(false),
            )
        })
        .collect()
}

fn create_instrument_closes(
    instrument_id: InstrumentId,
    base_ts: u64,
    count: usize,
) -> Vec<InstrumentClose> {
    (0..count)
        .map(|index| {
            let ts = base_ts + index as u64 * 1_000;
            InstrumentClose::new(
                instrument_id,
                Price::from("999.50"),
                InstrumentCloseType::EndOfSession,
                UnixNanos::from(ts),
                UnixNanos::from(ts),
            )
        })
        .collect()
}

fn create_option_greeks(
    instrument_id: InstrumentId,
    base_ts: u64,
    count: usize,
) -> Vec<OptionGreeks> {
    (0..count)
        .map(|index| {
            let ts = base_ts + index as u64 * 1_000;
            OptionGreeks {
                instrument_id,
                convention: GreeksConvention::PriceAdjusted,
                greeks: OptionGreekValues {
                    delta: 0.55 + index as f64 * 0.01,
                    gamma: 0.012,
                    vega: 3.4,
                    theta: -1.2,
                    rho: 0.01,
                },
                mark_iv: Some(0.64 + index as f64 * 0.01),
                bid_iv: None,
                ask_iv: Some(0.66 + index as f64 * 0.01),
                underlying_price: Some(100_000.0 + index as f64),
                open_interest: Some(10_000.0 + index as f64),
                ts_event: UnixNanos::from(ts),
                ts_init: UnixNanos::from(ts),
            }
        })
        .collect()
}

fn resolve_catalog_dir() -> Result<PathBuf> {
    if let Some(arg1) = env::args().nth(1) {
        return Ok(PathBuf::from(arg1));
    }

    if let Ok(dir) = env::var("CATALOG_DIR") {
        return Ok(PathBuf::from(dir));
    }

    anyhow::bail!("Provide catalog directory as first arg or CATALOG_DIR env var");
}

fn main() -> Result<()> {
    let catalog_dir = resolve_catalog_dir()?;
    fs::create_dir_all(&catalog_dir)
        .with_context(|| format!("failed to create catalog dir {}", catalog_dir.display()))?;

    let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
    let instrument_id = InstrumentId::from_str("ETHUSDT-PERP.BINANCE")?;

    let capture = CaptureConfig {
        catalog_uri: format!("file://{}", catalog_dir.display()),
        flush_rows: 3,
        flush_interval_ms: 1_000,
        max_buffer_bytes: 1024 * 1024,
        ..CaptureConfig::default()
    };

    let plan = CapturePlan {
        instruments: vec![InstrumentCaptureSpec { instrument_id }],
        quotes: vec![QuoteCaptureSpec { instrument_id }],
        mark_prices: vec![MarkPriceCaptureSpec { instrument_id }],
        index_prices: vec![IndexPriceCaptureSpec { instrument_id }],
        funding_rates: vec![FundingRateCaptureSpec { instrument_id }],
        instrument_statuses: vec![InstrumentStatusCaptureSpec { instrument_id }],
        instrument_closes: vec![InstrumentCloseCaptureSpec { instrument_id }],
        option_greeks: vec![OptionGreeksCaptureSpec { instrument_id }],
        ..CapturePlan::default()
    };

    let config = CatalogCaptureActorConfig {
        actor_id: Some(ActorId::from("CATALOG_CAPTURE-PYTHON_FIXTURE")),
        capture: capture.clone(),
        plan,
        online_option_metrics: None,
    };

    let mut actor = CatalogCaptureActor::new(config)?;

    DataActor::on_instrument(&mut actor, &instrument)?;

    let quotes = create_quote_ticks(instrument_id, 1_000_000, 5);
    for quote in &quotes {
        DataActor::on_quote(&mut actor, quote)?;
    }

    let mark_prices = create_mark_price_updates(instrument_id, 2_000_000, 2);
    for update in &mark_prices {
        DataActor::on_mark_price(&mut actor, update)?;
    }

    let index_prices = create_index_price_updates(instrument_id, 3_000_000, 2);
    for update in &index_prices {
        DataActor::on_index_price(&mut actor, update)?;
    }

    let funding_rates = create_funding_rate_updates(instrument_id, 4_000_000, 2);
    for update in &funding_rates {
        DataActor::on_funding_rate(&mut actor, update)?;
    }

    let statuses = create_instrument_statuses(instrument_id, 5_000_000, 2);
    for status in &statuses {
        DataActor::on_instrument_status(&mut actor, status)?;
    }

    let closes = create_instrument_closes(instrument_id, 6_000_000, 2);
    for close in &closes {
        DataActor::on_instrument_close(&mut actor, close)?;
    }

    let greeks = create_option_greeks(instrument_id, 7_000_000, 2);
    for item in &greeks {
        DataActor::on_option_greeks(&mut actor, item)?;
    }

    let _ = actor.flush_all()?;

    println!("Python readback fixture written");
    println!("Catalog dir: {}", catalog_dir.display());
    println!("Instrument id: {}", instrument_id);
    println!("Quote ticks: {}", quotes.len());
    println!("Mark price updates: {}", mark_prices.len());
    println!("Index price updates: {}", index_prices.len());
    println!("Funding rate updates: {}", funding_rates.len());
    println!("Instrument statuses: {}", statuses.len());
    println!("Instrument closes: {}", closes.len());
    println!("Option greeks: {}", greeks.len());

    Ok(())
}
