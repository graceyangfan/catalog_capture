use std::{fs, path::Path, str::FromStr};

use anyhow::{Context, Result, anyhow, bail};
use catalog_capture_core::{
    CaptureConfig, CapturePlan, CompressionKind, CustomDataCaptureSpec, FundingRateCaptureSpec,
    IndexPriceCaptureSpec, InstrumentCaptureSpec, InstrumentCloseCaptureSpec,
    InstrumentStatusCaptureSpec, LayoutCompatibility, MarkPriceCaptureSpec, OptionGreeksCaptureSpec,
    OverflowPolicy, QuoteCaptureSpec, TradeCaptureSpec,
    plan::{BarCaptureSpec, BookDeltasCaptureSpec},
};
use nautilus_binance::common::enums::{BinanceEnvironment, BinanceProductType};
use nautilus_deribit::{
    common::enums::DeribitEnvironment,
    http::models::DeribitProductType,
};
use nautilus_core::Params;
use nautilus_model::{
    data::{BarType, DataType},
    enums::BookType,
    identifiers::InstrumentId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CliConfigFile {
    #[serde(default)]
    pub runtime: RuntimeConfig,
    #[serde(default)]
    pub output: OutputConfig,
    #[serde(default)]
    pub capture: CaptureConfigFile,
    #[serde(default)]
    pub venues: Vec<VenueConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_capture_seconds")]
    pub capture_seconds: u64,
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    #[serde(default = "default_delay_post_stop_secs")]
    pub delay_post_stop_secs: u64,
    #[serde(default = "default_node_name")]
    pub node_name: String,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            capture_seconds: default_capture_seconds(),
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            delay_post_stop_secs: default_delay_post_stop_secs(),
            node_name: default_node_name(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    #[serde(default = "default_catalog_uri")]
    pub catalog_uri: String,
    #[serde(default = "default_compression")]
    pub compression: String,
    #[serde(default = "default_layout_compatibility")]
    pub layout_compatibility: String,
    #[serde(default = "default_flush_rows")]
    pub flush_rows: usize,
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    #[serde(default = "default_max_buffer_bytes")]
    pub max_buffer_bytes: usize,
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "default_overflow_policy")]
    pub overflow_policy: String,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            catalog_uri: default_catalog_uri(),
            compression: default_compression(),
            layout_compatibility: default_layout_compatibility(),
            flush_rows: default_flush_rows(),
            flush_interval_ms: default_flush_interval_ms(),
            max_buffer_bytes: default_max_buffer_bytes(),
            queue_capacity: default_queue_capacity(),
            overflow_policy: default_overflow_policy(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CaptureConfigFile {
    #[serde(default)]
    pub instruments: Vec<InstrumentSelector>,
    #[serde(default)]
    pub quotes: Vec<InstrumentSelector>,
    #[serde(default)]
    pub trades: Vec<InstrumentSelector>,
    #[serde(default)]
    pub mark_prices: Vec<InstrumentSelector>,
    #[serde(default)]
    pub instrument_statuses: Vec<InstrumentSelector>,
    #[serde(default)]
    pub instrument_closes: Vec<InstrumentSelector>,
    #[serde(default)]
    pub option_greeks: Vec<InstrumentSelector>,
    #[serde(default)]
    pub index_prices: Vec<InstrumentSelector>,
    #[serde(default)]
    pub funding_rates: Vec<InstrumentSelector>,
    #[serde(default)]
    pub bars: Vec<BarSelector>,
    #[serde(default)]
    pub book_deltas: Vec<BookDeltasSelector>,
    #[serde(default)]
    pub custom_data: Vec<CustomDataSelector>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstrumentSelector {
    pub instrument_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BarSelector {
    pub bar_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookDeltasSelector {
    pub instrument_id: String,
    pub book_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomDataSelector {
    pub type_name: String,
    #[serde(default)]
    pub identifier: Option<String>,
    #[serde(default)]
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueConfig {
    pub id: String,
    pub kind: String,
    #[serde(default = "default_binance_environment")]
    pub environment: String,
    #[serde(default = "default_binance_product_type")]
    pub product_type: String,
    /// Deribit-only: product types to load (e.g. `future`, `option`).
    #[serde(default)]
    pub product_types: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum VenueRuntimeConfig {
    BinanceFutures {
        id: String,
        environment: BinanceEnvironment,
        product_type: BinanceProductType,
    },
    Deribit {
        id: String,
        environment: DeribitEnvironment,
        product_types: Vec<DeribitProductType>,
    },
}

#[derive(Debug, Clone)]
pub struct EffectiveConfig {
    pub runtime: RuntimeConfig,
    pub capture: CaptureConfig,
    pub plan: CapturePlan,
    pub venues: Vec<VenueRuntimeConfig>,
}

pub fn load_config(path: &Path) -> Result<CliConfigFile> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("failed to parse TOML {}", path.display()))
}

pub fn resolve_config(config: CliConfigFile) -> Result<EffectiveConfig> {
    if config.venues.is_empty() {
        bail!("at least one [[venues]] entry is required");
    }

    let capture = CaptureConfig {
        enabled: true,
        catalog_uri: config.output.catalog_uri,
        queue_capacity: config.output.queue_capacity,
        flush_rows: config.output.flush_rows,
        flush_interval_ms: config.output.flush_interval_ms,
        max_buffer_bytes: config.output.max_buffer_bytes,
        compression: parse_compression(&config.output.compression)?,
        overflow_policy: parse_overflow_policy(&config.output.overflow_policy)?,
        layout_compatibility: parse_layout_compatibility(&config.output.layout_compatibility)?,
    };

    let plan = CapturePlan {
        instruments: parse_instrument_specs(&config.capture.instruments)?,
        quotes: parse_quote_specs(&config.capture.quotes)?,
        trades: parse_trade_specs(&config.capture.trades)?,
        bars: parse_bar_specs(&config.capture.bars)?,
        book_deltas: parse_book_delta_specs(&config.capture.book_deltas)?,
        mark_prices: parse_mark_price_specs(&config.capture.mark_prices)?,
        index_prices: parse_index_price_specs(&config.capture.index_prices)?,
        funding_rates: parse_funding_rate_specs(&config.capture.funding_rates)?,
        instrument_statuses: parse_instrument_status_specs(&config.capture.instrument_statuses)?,
        instrument_closes: parse_instrument_close_specs(&config.capture.instrument_closes)?,
        option_greeks: parse_option_greeks_specs(&config.capture.option_greeks)?,
        custom_data: parse_custom_data_specs(&config.capture.custom_data)?,
    };

    if plan.is_empty() {
        bail!("capture plan is empty; enable at least one capture family");
    }

    let venues = config
        .venues
        .into_iter()
        .map(parse_venue)
        .collect::<Result<Vec<_>>>()?;

    Ok(EffectiveConfig {
        runtime: config.runtime,
        capture,
        plan,
        venues,
    })
}

fn parse_instrument_id(value: &str) -> Result<InstrumentId> {
    InstrumentId::from_str(value).with_context(|| format!("invalid instrument_id {value}"))
}

fn parse_instrument_specs(items: &[InstrumentSelector]) -> Result<Vec<InstrumentCaptureSpec>> {
    items.iter()
        .map(|item| Ok(InstrumentCaptureSpec { instrument_id: parse_instrument_id(&item.instrument_id)? }))
        .collect()
}

fn parse_quote_specs(items: &[InstrumentSelector]) -> Result<Vec<QuoteCaptureSpec>> {
    items.iter()
        .map(|item| Ok(QuoteCaptureSpec { instrument_id: parse_instrument_id(&item.instrument_id)? }))
        .collect()
}

fn parse_trade_specs(items: &[InstrumentSelector]) -> Result<Vec<TradeCaptureSpec>> {
    items.iter()
        .map(|item| Ok(TradeCaptureSpec { instrument_id: parse_instrument_id(&item.instrument_id)? }))
        .collect()
}

fn parse_mark_price_specs(items: &[InstrumentSelector]) -> Result<Vec<MarkPriceCaptureSpec>> {
    items.iter()
        .map(|item| Ok(MarkPriceCaptureSpec { instrument_id: parse_instrument_id(&item.instrument_id)? }))
        .collect()
}

fn parse_index_price_specs(items: &[InstrumentSelector]) -> Result<Vec<IndexPriceCaptureSpec>> {
    items.iter()
        .map(|item| {
            Ok(IndexPriceCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

fn parse_funding_rate_specs(
    items: &[InstrumentSelector],
) -> Result<Vec<FundingRateCaptureSpec>> {
    items.iter()
        .map(|item| {
            Ok(FundingRateCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

fn parse_instrument_status_specs(
    items: &[InstrumentSelector],
) -> Result<Vec<InstrumentStatusCaptureSpec>> {
    items.iter()
        .map(|item| {
            Ok(InstrumentStatusCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

fn parse_instrument_close_specs(
    items: &[InstrumentSelector],
) -> Result<Vec<InstrumentCloseCaptureSpec>> {
    items.iter()
        .map(|item| {
            Ok(InstrumentCloseCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

fn parse_option_greeks_specs(items: &[InstrumentSelector]) -> Result<Vec<OptionGreeksCaptureSpec>> {
    items.iter()
        .map(|item| {
            Ok(OptionGreeksCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

fn parse_bar_specs(items: &[BarSelector]) -> Result<Vec<BarCaptureSpec>> {
    items.iter()
        .map(|item| {
            let bar_type =
                BarType::from_str(&item.bar_type).with_context(|| format!("invalid bar_type {}", item.bar_type))?;
            Ok(BarCaptureSpec { bar_type })
        })
        .collect()
}

fn parse_book_delta_specs(items: &[BookDeltasSelector]) -> Result<Vec<BookDeltasCaptureSpec>> {
    items.iter()
        .map(|item| {
            let book_type =
                BookType::from_str(&item.book_type).with_context(|| format!("invalid book_type {}", item.book_type))?;
            Ok(BookDeltasCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
                book_type,
            })
        })
        .collect()
}

fn parse_custom_data_specs(items: &[CustomDataSelector]) -> Result<Vec<CustomDataCaptureSpec>> {
    items.iter()
        .map(|item| {
            let metadata = if item.metadata.is_empty() {
                None
            } else {
                let mut params = Params::new();
                for (key, value) in &item.metadata {
                    params.insert(key.clone(), JsonValue::String(value.clone()));
                }
                Some(params)
            };
            Ok(CustomDataCaptureSpec {
                data_type: DataType::new(&item.type_name, metadata, item.identifier.clone()),
            })
        })
        .collect()
}

fn parse_venue(venue: VenueConfig) -> Result<VenueRuntimeConfig> {
    match venue.kind.to_ascii_lowercase().as_str() {
        "binance_futures" => Ok(VenueRuntimeConfig::BinanceFutures {
            id: venue.id,
            environment: parse_binance_environment(&venue.environment)?,
            product_type: parse_binance_product_type(&venue.product_type)?,
        }),
        "deribit" => Ok(VenueRuntimeConfig::Deribit {
            id: venue.id,
            environment: parse_deribit_environment(&venue.environment)?,
            product_types: parse_deribit_product_types(&venue.product_types)?,
        }),
        other => bail!(
            "unsupported venue kind {other}; currently supported: binance_futures, deribit"
        ),
    }
}

fn parse_compression(value: &str) -> Result<CompressionKind> {
    match value.to_ascii_lowercase().as_str() {
        "snappy" => Ok(CompressionKind::Snappy),
        "zstd" => Ok(CompressionKind::Zstd),
        other => bail!("unsupported compression {other}; expected snappy|zstd"),
    }
}

fn parse_overflow_policy(value: &str) -> Result<OverflowPolicy> {
    match value.to_ascii_lowercase().as_str() {
        "drop_newest" => Ok(OverflowPolicy::DropNewest),
        "drop_oldest" => Ok(OverflowPolicy::DropOldest),
        "fail_fast" => Ok(OverflowPolicy::FailFast),
        other => bail!("unsupported overflow_policy {other}; expected drop_newest|drop_oldest|fail_fast"),
    }
}

fn parse_layout_compatibility(value: &str) -> Result<LayoutCompatibility> {
    match value.to_ascii_lowercase().as_str() {
        "rust_canonical_only" => Ok(LayoutCompatibility::RustCanonicalOnly),
        "rust_canonical_with_python_legacy_mirror" => {
            Ok(LayoutCompatibility::RustCanonicalWithPythonLegacyMirror)
        }
        other => bail!(
            "unsupported layout_compatibility {other}; expected rust_canonical_only|rust_canonical_with_python_legacy_mirror"
        ),
    }
}

fn parse_binance_environment(value: &str) -> Result<BinanceEnvironment> {
    match value.to_ascii_lowercase().as_str() {
        "live" => Ok(BinanceEnvironment::Live),
        "testnet" => Ok(BinanceEnvironment::Testnet),
        "demo" => Ok(BinanceEnvironment::Demo),
        other => bail!("unsupported Binance environment {other}; expected live|testnet|demo"),
    }
}

fn parse_deribit_environment(value: &str) -> Result<DeribitEnvironment> {
    match value.to_ascii_lowercase().as_str() {
        "mainnet" | "live" => Ok(DeribitEnvironment::Mainnet),
        "testnet" => Ok(DeribitEnvironment::Testnet),
        other => bail!("unsupported Deribit environment {other}; expected mainnet|testnet"),
    }
}

fn parse_deribit_product_types(values: &[String]) -> Result<Vec<DeribitProductType>> {
    if values.is_empty() {
        return Ok(vec![DeribitProductType::Future, DeribitProductType::Option]);
    }

    values
        .iter()
        .map(|value| match value.to_ascii_lowercase().as_str() {
            "future" => Ok(DeribitProductType::Future),
            "option" => Ok(DeribitProductType::Option),
            "spot" => Ok(DeribitProductType::Spot),
            "future_combo" => Ok(DeribitProductType::FutureCombo),
            "option_combo" => Ok(DeribitProductType::OptionCombo),
            other => bail!(
                "unsupported Deribit product_type {other}; expected future|option|spot|future_combo|option_combo"
            ),
        })
        .collect()
}

fn parse_binance_product_type(value: &str) -> Result<BinanceProductType> {
    match value.to_ascii_lowercase().as_str() {
        "usd_m" => Ok(BinanceProductType::UsdM),
        "coin_m" => Ok(BinanceProductType::CoinM),
        "spot" => Ok(BinanceProductType::Spot),
        "margin" => Ok(BinanceProductType::Margin),
        "options" => Ok(BinanceProductType::Options),
        other => bail!(
            "unsupported Binance product_type {other}; expected usd_m|coin_m|spot|margin|options"
        ),
    }
}

pub fn render_effective_config(config: &CliConfigFile) -> Result<String> {
    toml::to_string_pretty(config).map_err(|err| anyhow!("failed to render effective TOML: {err}"))
}

const fn default_capture_seconds() -> u64 {
    30
}

const fn default_shutdown_timeout_secs() -> u64 {
    10
}

const fn default_delay_post_stop_secs() -> u64 {
    2
}

fn default_node_name() -> String {
    "CATALOG-CAPTURE-CLI-001".to_string()
}

fn default_catalog_uri() -> String {
    "file:///tmp/nautilus-catalog-capture".to_string()
}

fn default_compression() -> String {
    "snappy".to_string()
}

fn default_layout_compatibility() -> String {
    "rust_canonical_with_python_legacy_mirror".to_string()
}

const fn default_flush_rows() -> usize {
    5_000
}

const fn default_flush_interval_ms() -> u64 {
    1_000
}

const fn default_max_buffer_bytes() -> usize {
    32 * 1024 * 1024
}

const fn default_queue_capacity() -> usize {
    10_000
}

fn default_overflow_policy() -> String {
    "drop_oldest".to_string()
}

fn default_binance_environment() -> String {
    "testnet".to_string()
}

fn default_binance_product_type() -> String {
    "usd_m".to_string()
}
