use std::str::FromStr;

use anyhow::Result;
use catalog_capture_core::{
    config::CaptureConfig,
    plan::{
        BookDeltasCaptureSpec, CapturePlan, FundingRateCaptureSpec, IndexPriceCaptureSpec,
        InstrumentCaptureSpec, InstrumentCloseCaptureSpec, InstrumentStatusCaptureSpec,
        MarkPriceCaptureSpec, OptionGreeksCaptureSpec, QuoteCaptureSpec, TradeCaptureSpec,
    },
};
use catalog_capture_runtime_adapter::{CatalogCaptureActor, CatalogCaptureActorConfig};
use nautilus_model::{
    enums::BookType,
    identifiers::{ActorId, InstrumentId},
};

fn main() -> Result<()> {
    let instrument_id = InstrumentId::from_str("ETHUSDT-PERP.BINANCE")?;

    let capture = CaptureConfig {
        catalog_uri: "file:///tmp/nautilus-catalog-capture-demo".to_string(),
        flush_rows: 1_000,
        flush_interval_ms: 1_000,
        max_buffer_bytes: 4 * 1024 * 1024,
        ..CaptureConfig::default()
    };

    let plan = CapturePlan {
        instruments: vec![InstrumentCaptureSpec { instrument_id }],
        quotes: vec![QuoteCaptureSpec { instrument_id }],
        trades: vec![TradeCaptureSpec { instrument_id }],
        bars: vec![],
        book_deltas: vec![BookDeltasCaptureSpec {
            instrument_id,
            book_type: BookType::L2_MBP,
        }],
        mark_prices: vec![MarkPriceCaptureSpec { instrument_id }],
        index_prices: vec![IndexPriceCaptureSpec { instrument_id }],
        funding_rates: vec![FundingRateCaptureSpec { instrument_id }],
        instrument_statuses: vec![InstrumentStatusCaptureSpec { instrument_id }],
        instrument_closes: vec![InstrumentCloseCaptureSpec { instrument_id }],
        option_greeks: vec![OptionGreeksCaptureSpec { instrument_id }],
        forward_prices: vec![],
        custom_data: vec![],
    };

    let config = CatalogCaptureActorConfig {
        actor_id: Some(ActorId::from("CATALOG_CAPTURE-DEMO")),
        capture,
        plan,
        online_option_metrics: None,
        dynamic_option_universe: None,
    };

    let actor = CatalogCaptureActor::new(config)?;
    println!("{actor:?}");

    Ok(())
}
