use std::{fs, path::Path, str::FromStr};

use anyhow::{anyhow, bail, Context, Result};
use catalog_capture_core::{
    plan::{BarCaptureSpec, BookDeltasCaptureSpec},
    CaptureConfig, CapturePlan, CompressionKind, CustomDataCaptureSpec, ExpiryPolicy,
    FundingRateCaptureSpec, IndexPriceCaptureSpec, InstrumentCaptureSpec,
    InstrumentCloseCaptureSpec, InstrumentStatusCaptureSpec, LayoutCompatibility,
    ForwardPriceCaptureSpec, MarkPriceCaptureSpec, OptionGreeksCaptureSpec, OptionUniverseFamily,
    OptionUniverseSpec,
    OverflowPolicy, QuoteCaptureSpec, StrikePolicy, TradeCaptureSpec,
};
use nautilus_binance::common::enums::{BinanceEnvironment, BinanceProductType};
use nautilus_bybit::common::enums::{BybitEnvironment, BybitProductType};
use nautilus_core::Params;
use nautilus_deribit::{common::enums::DeribitEnvironment, http::models::DeribitProductType};
use nautilus_hyperliquid::common::enums::HyperliquidEnvironment;
use nautilus_model::{
    data::{BarType, DataType},
    enums::BookType,
    identifiers::InstrumentId,
};
use nautilus_okx::common::enums::{OKXEnvironment, OKXInstrumentType};
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
    #[serde(default)]
    pub online_option_metrics: OnlineOptionMetricsRuntimeConfig,
    #[serde(default)]
    pub option_universe_refresh: OptionUniverseRefreshRuntimeConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            capture_seconds: default_capture_seconds(),
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            delay_post_stop_secs: default_delay_post_stop_secs(),
            node_name: default_node_name(),
            online_option_metrics: OnlineOptionMetricsRuntimeConfig::default(),
            option_universe_refresh: OptionUniverseRefreshRuntimeConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineOptionMetricsRuntimeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_online_option_metrics_interval_secs")]
    pub snapshot_interval_secs: u64,
}

impl Default for OnlineOptionMetricsRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            snapshot_interval_secs: default_online_option_metrics_interval_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionUniverseRefreshRuntimeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_option_universe_refresh_interval_secs")]
    pub interval_secs: u64,
    /// Consecutive refresh ticks required before an `oi_ranked` strike set change
    /// is applied. Zero disables smoothing.
    #[serde(default = "default_option_universe_strike_change_confirmations")]
    pub strike_change_confirmations: u32,
}

impl Default for OptionUniverseRefreshRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: default_option_universe_refresh_interval_secs(),
            strike_change_confirmations: default_option_universe_strike_change_confirmations(),
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
    pub forward_prices: Vec<InstrumentSelector>,
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
    #[serde(default)]
    pub option_universe: Vec<OptionUniverseSelector>,
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
pub struct OptionUniverseSelector {
    pub venue_id: String,
    pub underlying: String,
    #[serde(default)]
    pub settlement_currency: Option<String>,
    #[serde(default)]
    pub include_perp: bool,
    #[serde(default)]
    pub families: Vec<String>,
    pub expiry_policy: ExpiryPolicySelector,
    pub strike_policy: StrikePolicySelector,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpiryPolicySelector {
    pub mode: String,
    pub days_max: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrikePolicySelector {
    pub mode: String,
    #[serde(default)]
    pub strikes_above: usize,
    #[serde(default)]
    pub strikes_below: usize,
    #[serde(default)]
    pub top_n: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueConfig {
    pub id: String,
    pub kind: String,
    #[serde(default = "default_binance_environment")]
    pub environment: String,
    #[serde(default = "default_binance_product_type")]
    pub product_type: String,
    /// Deribit / Bybit: product types to load (e.g. `future`, `option`, `linear`).
    #[serde(default)]
    pub product_types: Vec<String>,
    /// OKX-only: instrument types to load (e.g. `swap`, `option`).
    #[serde(default)]
    pub instrument_types: Vec<String>,
    /// OKX-only: instrument families for options (e.g. `BTC-USD`).
    #[serde(default)]
    pub instrument_families: Vec<String>,
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
    Bybit {
        id: String,
        environment: BybitEnvironment,
        product_types: Vec<BybitProductType>,
    },
    Hyperliquid {
        id: String,
        environment: HyperliquidEnvironment,
    },
    Okx {
        id: String,
        environment: OKXEnvironment,
        instrument_types: Vec<OKXInstrumentType>,
        instrument_families: Option<Vec<String>>,
    },
}

impl VenueRuntimeConfig {
    pub fn id(&self) -> &str {
        match self {
            Self::BinanceFutures { id, .. }
            | Self::Deribit { id, .. }
            | Self::Bybit { id, .. }
            | Self::Hyperliquid { id, .. }
            | Self::Okx { id, .. } => id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EffectiveConfig {
    pub runtime: RuntimeConfig,
    pub capture: CaptureConfig,
    pub plan: CapturePlan,
    pub option_universes: Vec<OptionUniverseSpec>,
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

    let venues = config
        .venues
        .into_iter()
        .map(parse_venue)
        .collect::<Result<Vec<_>>>()?;
    validate_unique_venue_ids(&venues)?;

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
        forward_prices: parse_forward_price_specs(&config.capture.forward_prices)?,
        custom_data: parse_custom_data_specs(&config.capture.custom_data)?,
    };
    let option_universes = parse_option_universe_specs(&config.capture.option_universe)?;

    if plan.is_empty() && option_universes.is_empty() {
        bail!("capture plan is empty; enable at least one capture family");
    }

    Ok(EffectiveConfig {
        runtime: config.runtime,
        capture,
        plan,
        option_universes,
        venues,
    })
}

fn parse_instrument_id(value: &str) -> Result<InstrumentId> {
    InstrumentId::from_str(value).with_context(|| format!("invalid instrument_id {value}"))
}

fn parse_instrument_specs(items: &[InstrumentSelector]) -> Result<Vec<InstrumentCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(InstrumentCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

fn parse_quote_specs(items: &[InstrumentSelector]) -> Result<Vec<QuoteCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(QuoteCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

fn parse_trade_specs(items: &[InstrumentSelector]) -> Result<Vec<TradeCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(TradeCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

fn parse_mark_price_specs(items: &[InstrumentSelector]) -> Result<Vec<MarkPriceCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(MarkPriceCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

fn parse_index_price_specs(items: &[InstrumentSelector]) -> Result<Vec<IndexPriceCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(IndexPriceCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

fn parse_funding_rate_specs(items: &[InstrumentSelector]) -> Result<Vec<FundingRateCaptureSpec>> {
    items
        .iter()
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
    items
        .iter()
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
    items
        .iter()
        .map(|item| {
            Ok(InstrumentCloseCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

fn parse_forward_price_specs(
    items: &[InstrumentSelector],
) -> Result<Vec<ForwardPriceCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(ForwardPriceCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

fn parse_option_greeks_specs(items: &[InstrumentSelector]) -> Result<Vec<OptionGreeksCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(OptionGreeksCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

fn parse_bar_specs(items: &[BarSelector]) -> Result<Vec<BarCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            let bar_type = BarType::from_str(&item.bar_type)
                .with_context(|| format!("invalid bar_type {}", item.bar_type))?;
            Ok(BarCaptureSpec { bar_type })
        })
        .collect()
}

fn parse_book_delta_specs(items: &[BookDeltasSelector]) -> Result<Vec<BookDeltasCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            let book_type = BookType::from_str(&item.book_type)
                .with_context(|| format!("invalid book_type {}", item.book_type))?;
            Ok(BookDeltasCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
                book_type,
            })
        })
        .collect()
}

fn parse_custom_data_specs(items: &[CustomDataSelector]) -> Result<Vec<CustomDataCaptureSpec>> {
    items
        .iter()
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

fn parse_option_universe_specs(
    items: &[OptionUniverseSelector],
) -> Result<Vec<OptionUniverseSpec>> {
    items.iter().map(parse_option_universe_spec).collect()
}

fn parse_option_universe_spec(item: &OptionUniverseSelector) -> Result<OptionUniverseSpec> {
    if item.venue_id.trim().is_empty() {
        bail!("capture.option_universe.venue_id must be non-empty");
    }
    if item.underlying.trim().is_empty() {
        bail!("capture.option_universe.underlying must be non-empty");
    }
    if item.families.is_empty() {
        bail!("capture.option_universe.families must be non-empty");
    }

    let families = item
        .families
        .iter()
        .map(|family| parse_option_universe_family(family))
        .collect::<Result<Vec<_>>>()?;

    let spec = OptionUniverseSpec {
        venue_id: item.venue_id.trim().to_string(),
        underlying: item.underlying.trim().to_ascii_uppercase(),
        settlement_currency: item
            .settlement_currency
            .as_ref()
            .map(|value| value.trim().to_ascii_uppercase()),
        include_perp: item.include_perp,
        families,
        expiry_policy: parse_expiry_policy(&item.expiry_policy)?,
        strike_policy: parse_strike_policy(&item.strike_policy)?,
    };

    validate_option_universe_family_shape(&spec)?;
    Ok(spec)
}

fn parse_option_universe_family(value: &str) -> Result<OptionUniverseFamily> {
    match value.to_ascii_lowercase().as_str() {
        "instruments" => Ok(OptionUniverseFamily::Instruments),
        "quotes" => Ok(OptionUniverseFamily::Quotes),
        "trades" => Ok(OptionUniverseFamily::Trades),
        "mark_prices" => Ok(OptionUniverseFamily::MarkPrices),
        "index_prices" => Ok(OptionUniverseFamily::IndexPrices),
        "funding_rates" => Ok(OptionUniverseFamily::FundingRates),
        "instrument_statuses" => Ok(OptionUniverseFamily::InstrumentStatuses),
        "instrument_closes" => Ok(OptionUniverseFamily::InstrumentCloses),
        "option_greeks" => Ok(OptionUniverseFamily::OptionGreeks),
        "forward_prices" => Ok(OptionUniverseFamily::ForwardPrices),
        other => bail!(
            "unsupported capture.option_universe family {other}; expected instruments|quotes|trades|mark_prices|index_prices|funding_rates|instrument_statuses|instrument_closes|option_greeks"
        ),
    }
}

fn parse_expiry_policy(policy: &ExpiryPolicySelector) -> Result<ExpiryPolicy> {
    match policy.mode.to_ascii_lowercase().as_str() {
        "nearest" => {
            if policy.days_max == 0 {
                bail!("capture.option_universe.expiry_policy.days_max must be > 0");
            }
            Ok(ExpiryPolicy::Nearest {
                days_max: policy.days_max,
            })
        }
        other => bail!(
            "unsupported capture.option_universe.expiry_policy.mode {other}; expected nearest"
        ),
    }
}

fn parse_strike_policy(policy: &StrikePolicySelector) -> Result<StrikePolicy> {
    match policy.mode.to_ascii_lowercase().as_str() {
        "atm_relative" => Ok(StrikePolicy::AtmRelative {
            strikes_above: policy.strikes_above,
            strikes_below: policy.strikes_below,
        }),
        "oi_ranked" => {
            let top_n = policy.top_n.filter(|value| *value > 0).ok_or_else(|| {
                anyhow::anyhow!(
                    "capture.option_universe.strike_policy.mode oi_ranked requires top_n > 0"
                )
            })?;
            Ok(StrikePolicy::OiRanked { top_n })
        }
        "volume_ranked" => {
            let top_n = policy.top_n.filter(|value| *value > 0).ok_or_else(|| {
                anyhow::anyhow!(
                    "capture.option_universe.strike_policy.mode volume_ranked requires top_n > 0"
                )
            })?;
            Ok(StrikePolicy::VolumeRanked { top_n })
        }
        "all" => Ok(StrikePolicy::AllStrikes),
        other => bail!(
            "unsupported capture.option_universe.strike_policy.mode {other}; expected atm_relative, oi_ranked, volume_ranked, or all"
        ),
    }
}

fn validate_option_universe_family_shape(spec: &OptionUniverseSpec) -> Result<()> {
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
        "bybit" => Ok(VenueRuntimeConfig::Bybit {
            id: venue.id,
            environment: parse_bybit_environment(&venue.environment)?,
            product_types: parse_bybit_product_types(&venue.product_types)?,
        }),
        "hyperliquid" => Ok(VenueRuntimeConfig::Hyperliquid {
            id: venue.id,
            environment: parse_hyperliquid_environment(&venue.environment)?,
        }),
        "okx" => {
            let instrument_types = parse_okx_instrument_types(&venue.instrument_types)?;
            let instrument_families = parse_okx_instrument_families(&venue.instrument_families)?;
            if instrument_types.contains(&OKXInstrumentType::Option)
                && instrument_families.as_ref().is_none_or(std::vec::Vec::is_empty)
            {
                bail!(
                    "okx venue requires non-empty instrument_families when instrument_types includes option"
                );
            }
            Ok(VenueRuntimeConfig::Okx {
                id: venue.id,
                environment: parse_okx_environment(&venue.environment)?,
                instrument_types,
                instrument_families,
            })
        }
        other => bail!(
            "unsupported venue kind {other}; currently supported: binance_futures, deribit, bybit, hyperliquid, okx"
        ),
    }
}

fn validate_unique_venue_ids(venues: &[VenueRuntimeConfig]) -> Result<()> {
    let mut ids = std::collections::BTreeSet::new();
    for venue in venues {
        let inserted = ids.insert(venue.id());
        if !inserted {
            bail!(
                "duplicate [[venues]] id {}; venue ids must be unique",
                venue.id()
            );
        }
    }
    Ok(())
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
        other => {
            bail!("unsupported overflow_policy {other}; expected drop_newest|drop_oldest|fail_fast")
        }
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

fn parse_bybit_environment(value: &str) -> Result<BybitEnvironment> {
    match value.to_ascii_lowercase().as_str() {
        "mainnet" | "live" => Ok(BybitEnvironment::Mainnet),
        "testnet" => Ok(BybitEnvironment::Testnet),
        "demo" => Ok(BybitEnvironment::Demo),
        other => bail!("unsupported Bybit environment {other}; expected mainnet|testnet|demo"),
    }
}

fn parse_hyperliquid_environment(value: &str) -> Result<HyperliquidEnvironment> {
    match value.to_ascii_lowercase().as_str() {
        "mainnet" | "live" => Ok(HyperliquidEnvironment::Mainnet),
        "testnet" => Ok(HyperliquidEnvironment::Testnet),
        other => bail!("unsupported Hyperliquid environment {other}; expected mainnet|testnet"),
    }
}

fn parse_bybit_product_types(values: &[String]) -> Result<Vec<BybitProductType>> {
    if values.is_empty() {
        return Ok(vec![BybitProductType::Linear, BybitProductType::Option]);
    }

    values
        .iter()
        .map(|value| match value.to_ascii_lowercase().as_str() {
            "linear" => Ok(BybitProductType::Linear),
            "inverse" => Ok(BybitProductType::Inverse),
            "spot" => Ok(BybitProductType::Spot),
            "option" => Ok(BybitProductType::Option),
            other => {
                bail!("unsupported Bybit product_type {other}; expected linear|inverse|spot|option")
            }
        })
        .collect()
}

fn parse_okx_environment(value: &str) -> Result<OKXEnvironment> {
    match value.to_ascii_lowercase().as_str() {
        "live" | "mainnet" => Ok(OKXEnvironment::Live),
        "demo" => Ok(OKXEnvironment::Demo),
        other => bail!("unsupported OKX environment {other}; expected live|demo"),
    }
}

fn parse_okx_instrument_types(values: &[String]) -> Result<Vec<OKXInstrumentType>> {
    if values.is_empty() {
        return Ok(vec![OKXInstrumentType::Swap]);
    }

    values
        .iter()
        .map(|value| match value.to_ascii_lowercase().as_str() {
            "swap" => Ok(OKXInstrumentType::Swap),
            "option" => Ok(OKXInstrumentType::Option),
            "spot" => Ok(OKXInstrumentType::Spot),
            "margin" => Ok(OKXInstrumentType::Margin),
            "futures" => Ok(OKXInstrumentType::Futures),
            "any" => Ok(OKXInstrumentType::Any),
            "events" => Ok(OKXInstrumentType::Events),
            other => bail!(
                "unsupported OKX instrument_type {other}; expected swap|option|spot|margin|futures|any|events"
            ),
        })
        .collect()
}

fn parse_okx_instrument_families(values: &[String]) -> Result<Option<Vec<String>>> {
    if values.is_empty() {
        Ok(None)
    } else {
        Ok(Some(values.to_vec()))
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

const fn default_online_option_metrics_interval_secs() -> u64 {
    5
}

const fn default_option_universe_refresh_interval_secs() -> u64 {
    300
}

const fn default_option_universe_strike_change_confirmations() -> u32 {
    2
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::validate_runtime;
    use std::path::{Path, PathBuf};

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("workspace root")
            .to_path_buf()
    }

    #[test]
    fn okx_default_instrument_types_is_swap_only() {
        let types = parse_okx_instrument_types(&[]).expect("defaults should parse");
        assert_eq!(types, vec![OKXInstrumentType::Swap]);
    }

    #[test]
    fn okx_option_without_families_is_rejected() {
        let venue = VenueConfig {
            id: "okx_main".to_string(),
            kind: "okx".to_string(),
            environment: "live".to_string(),
            product_type: default_binance_product_type(),
            product_types: Vec::new(),
            instrument_types: vec!["option".to_string()],
            instrument_families: Vec::new(),
        };

        let err = parse_venue(venue).expect_err("option without families should fail");
        assert!(
            err.to_string().contains("instrument_families"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn okx_option_with_families_is_accepted() {
        let venue = VenueConfig {
            id: "okx_main".to_string(),
            kind: "okx".to_string(),
            environment: "live".to_string(),
            product_type: default_binance_product_type(),
            product_types: Vec::new(),
            instrument_types: vec!["swap".to_string(), "option".to_string()],
            instrument_families: vec!["BTC-USD".to_string()],
        };

        let runtime = parse_venue(venue).expect("valid okx option venue");
        assert!(matches!(runtime, VenueRuntimeConfig::Okx { .. }));
    }

    #[test]
    fn validate_runtime_rejects_deribit_dvol_without_index_name() {
        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                custom_data: vec![CustomDataSelector {
                    type_name: "DeribitVolatilityIndex".to_string(),
                    identifier: None,
                    metadata: Default::default(),
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "deribit_main".to_string(),
                kind: "deribit".to_string(),
                environment: "live".to_string(),
                product_type: default_binance_product_type(),
                product_types: vec!["option".to_string()],
                instrument_types: Vec::new(),
                instrument_families: Vec::new(),
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        let err = validate_runtime(&effective).expect_err("missing index_name should fail");
        assert!(err.to_string().contains("metadata.index_name"));
    }

    #[test]
    fn validate_runtime_accepts_deribit_dvol_with_index_name() {
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert("index_name".to_string(), "btc_usd".to_string());

        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                custom_data: vec![CustomDataSelector {
                    type_name: "DeribitVolatilityIndex".to_string(),
                    identifier: None,
                    metadata,
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "deribit_main".to_string(),
                kind: "deribit".to_string(),
                environment: "live".to_string(),
                product_type: default_binance_product_type(),
                product_types: vec!["option".to_string()],
                instrument_types: Vec::new(),
                instrument_families: Vec::new(),
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        validate_runtime(&effective).expect("valid dvol config should pass");
    }

    #[test]
    fn validate_runtime_rejects_binance_liquidation_until_arrow_support_exists() {
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert(
            "instrument_id".to_string(),
            "ETHUSDT-PERP.BINANCE".to_string(),
        );

        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                custom_data: vec![CustomDataSelector {
                    type_name: "BinanceFuturesLiquidation".to_string(),
                    identifier: Some("ETHUSDT-PERP.BINANCE".to_string()),
                    metadata,
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "binance_main".to_string(),
                kind: "binance_futures".to_string(),
                environment: "testnet".to_string(),
                product_type: "usd_m".to_string(),
                product_types: Vec::new(),
                instrument_types: Vec::new(),
                instrument_families: Vec::new(),
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        let err = validate_runtime(&effective).expect_err("liquidation should fail early");
        assert!(err.to_string().contains("Arrow batch encoding"));
    }

    #[test]
    fn hyperliquid_environment_aliases_parse() {
        assert_eq!(
            parse_hyperliquid_environment("live").expect("live should parse"),
            HyperliquidEnvironment::Mainnet
        );
        assert_eq!(
            parse_hyperliquid_environment("testnet").expect("testnet should parse"),
            HyperliquidEnvironment::Testnet
        );
    }

    #[test]
    fn parse_hyperliquid_venue_is_supported() {
        let venue = VenueConfig {
            id: "hl_main".to_string(),
            kind: "hyperliquid".to_string(),
            environment: "testnet".to_string(),
            product_type: default_binance_product_type(),
            product_types: Vec::new(),
            instrument_types: Vec::new(),
            instrument_families: Vec::new(),
        };

        let runtime = parse_venue(venue).expect("valid hyperliquid venue");
        assert!(matches!(runtime, VenueRuntimeConfig::Hyperliquid { .. }));
    }

    #[test]
    fn validate_runtime_accepts_hyperliquid_open_interest() {
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert(
            "instrument_id".to_string(),
            "ETH-USD-PERP.HYPERLIQUID".to_string(),
        );

        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                custom_data: vec![CustomDataSelector {
                    type_name: "HyperliquidOpenInterest".to_string(),
                    identifier: Some("ETH-USD-PERP.HYPERLIQUID".to_string()),
                    metadata,
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "hl_main".to_string(),
                kind: "hyperliquid".to_string(),
                environment: "testnet".to_string(),
                product_type: default_binance_product_type(),
                product_types: Vec::new(),
                instrument_types: Vec::new(),
                instrument_families: Vec::new(),
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        validate_runtime(&effective).expect("valid hyperliquid OI config should pass");
    }

    #[test]
    fn validate_runtime_rejects_unknown_custom_type() {
        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                custom_data: vec![CustomDataSelector {
                    type_name: "HyperliquidOpenInterset".to_string(),
                    identifier: Some("ETH-USD-PERP.HYPERLIQUID".to_string()),
                    metadata: Default::default(),
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "hl_main".to_string(),
                kind: "hyperliquid".to_string(),
                environment: "testnet".to_string(),
                product_type: default_binance_product_type(),
                product_types: Vec::new(),
                instrument_types: Vec::new(),
                instrument_families: Vec::new(),
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        let err = validate_runtime(&effective).expect_err("unknown custom type should fail");
        assert!(err.to_string().contains("unknown custom_data type_name"));
    }

    #[test]
    fn validate_runtime_rejects_deribit_dvol_without_deribit_venue() {
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert("index_name".to_string(), "btc_usd".to_string());

        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                custom_data: vec![CustomDataSelector {
                    type_name: "DeribitVolatilityIndex".to_string(),
                    identifier: None,
                    metadata,
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "hl_main".to_string(),
                kind: "hyperliquid".to_string(),
                environment: "testnet".to_string(),
                product_type: default_binance_product_type(),
                product_types: Vec::new(),
                instrument_types: Vec::new(),
                instrument_families: Vec::new(),
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        let err = validate_runtime(&effective).expect_err("missing deribit venue should fail");
        assert!(err.to_string().contains("kind = \"deribit\""));
    }

    #[test]
    fn validate_runtime_rejects_hyperliquid_identifier_mismatch() {
        let mut metadata = std::collections::BTreeMap::new();
        metadata.insert(
            "instrument_id".to_string(),
            "ETH-USD-PERP.HYPERLIQUID".to_string(),
        );

        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                custom_data: vec![CustomDataSelector {
                    type_name: "HyperliquidOpenInterest".to_string(),
                    identifier: Some("BTC-USD-PERP.HYPERLIQUID".to_string()),
                    metadata,
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "hl_main".to_string(),
                kind: "hyperliquid".to_string(),
                environment: "testnet".to_string(),
                product_type: default_binance_product_type(),
                product_types: Vec::new(),
                instrument_types: Vec::new(),
                instrument_families: Vec::new(),
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        let err = validate_runtime(&effective).expect_err("identifier mismatch should fail");
        assert!(err
            .to_string()
            .contains("must match metadata.instrument_id"));
    }

    #[test]
    fn resolve_config_accepts_option_universe_without_explicit_capture_entries() {
        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                option_universe: vec![OptionUniverseSelector {
                    venue_id: "deribit_main".to_string(),
                    underlying: "BTC".to_string(),
                    settlement_currency: Some("BTC".to_string()),
                    include_perp: true,
                    families: vec![
                        "instruments".to_string(),
                        "quotes".to_string(),
                        "option_greeks".to_string(),
                        "index_prices".to_string(),
                        "funding_rates".to_string(),
                    ],
                    expiry_policy: ExpiryPolicySelector {
                        mode: "nearest".to_string(),
                        days_max: 45,
                    },
                    strike_policy: StrikePolicySelector {
                        mode: "atm_relative".to_string(),
                        strikes_above: 1,
                        strikes_below: 1,
                        top_n: None,
                    },
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "deribit_main".to_string(),
                kind: "deribit".to_string(),
                environment: "live".to_string(),
                product_type: default_binance_product_type(),
                product_types: vec!["future".to_string(), "option".to_string()],
                instrument_types: Vec::new(),
                instrument_families: Vec::new(),
            }],
            ..Default::default()
        })
        .expect("option universe config should resolve");

        assert!(effective.plan.is_empty());
        assert_eq!(effective.option_universes.len(), 1);
        validate_runtime(&effective).expect("runtime validation should pass");
    }

    #[test]
    fn resolve_config_rejects_unknown_option_universe_family() {
        let err = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                option_universe: vec![OptionUniverseSelector {
                    venue_id: "deribit_main".to_string(),
                    underlying: "BTC".to_string(),
                    settlement_currency: Some("BTC".to_string()),
                    include_perp: true,
                    families: vec!["books".to_string()],
                    expiry_policy: ExpiryPolicySelector {
                        mode: "nearest".to_string(),
                        days_max: 45,
                    },
                    strike_policy: StrikePolicySelector {
                        mode: "atm_relative".to_string(),
                        strikes_above: 1,
                        strikes_below: 1,
                        top_n: None,
                    },
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "deribit_main".to_string(),
                kind: "deribit".to_string(),
                environment: "live".to_string(),
                product_type: default_binance_product_type(),
                product_types: vec!["future".to_string(), "option".to_string()],
                instrument_types: Vec::new(),
                instrument_families: Vec::new(),
            }],
            ..Default::default()
        })
        .expect_err("unknown family should fail");

        assert!(err
            .to_string()
            .contains("unsupported capture.option_universe family"));
    }

    #[test]
    fn validate_runtime_rejects_option_universe_missing_future_product_type_for_perp() {
        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                option_universe: vec![OptionUniverseSelector {
                    venue_id: "deribit_main".to_string(),
                    underlying: "BTC".to_string(),
                    settlement_currency: Some("BTC".to_string()),
                    include_perp: true,
                    families: vec![
                        "instruments".to_string(),
                        "quotes".to_string(),
                        "option_greeks".to_string(),
                    ],
                    expiry_policy: ExpiryPolicySelector {
                        mode: "nearest".to_string(),
                        days_max: 45,
                    },
                    strike_policy: StrikePolicySelector {
                        mode: "atm_relative".to_string(),
                        strikes_above: 1,
                        strikes_below: 1,
                        top_n: None,
                    },
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "deribit_main".to_string(),
                kind: "deribit".to_string(),
                environment: "live".to_string(),
                product_type: default_binance_product_type(),
                product_types: vec!["option".to_string()],
                instrument_types: Vec::new(),
                instrument_families: Vec::new(),
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        let err = validate_runtime(&effective)
            .expect_err("include_perp without future product type should fail");
        assert!(err.to_string().contains("include \"future\""));
    }

    #[test]
    fn validate_runtime_rejects_option_universe_unknown_venue_id() {
        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                option_universe: vec![OptionUniverseSelector {
                    venue_id: "deribit_missing".to_string(),
                    underlying: "BTC".to_string(),
                    settlement_currency: Some("BTC".to_string()),
                    include_perp: true,
                    families: vec![
                        "instruments".to_string(),
                        "quotes".to_string(),
                        "option_greeks".to_string(),
                    ],
                    expiry_policy: ExpiryPolicySelector {
                        mode: "nearest".to_string(),
                        days_max: 45,
                    },
                    strike_policy: StrikePolicySelector {
                        mode: "atm_relative".to_string(),
                        strikes_above: 1,
                        strikes_below: 1,
                        top_n: None,
                    },
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "deribit_main".to_string(),
                kind: "deribit".to_string(),
                environment: "live".to_string(),
                product_type: default_binance_product_type(),
                product_types: vec!["future".to_string(), "option".to_string()],
                instrument_types: Vec::new(),
                instrument_families: Vec::new(),
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        let err = validate_runtime(&effective).expect_err("unknown venue id should fail");
        assert!(err.to_string().contains("unknown venue_id"));
    }

    #[test]
    fn validate_runtime_accepts_bybit_option_universe() {
        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                option_universe: vec![OptionUniverseSelector {
                    venue_id: "bybit_main".to_string(),
                    underlying: "BTC".to_string(),
                    settlement_currency: Some("USDT".to_string()),
                    include_perp: true,
                    families: vec![
                        "instruments".to_string(),
                        "quotes".to_string(),
                        "option_greeks".to_string(),
                        "index_prices".to_string(),
                        "funding_rates".to_string(),
                    ],
                    expiry_policy: ExpiryPolicySelector {
                        mode: "nearest".to_string(),
                        days_max: 45,
                    },
                    strike_policy: StrikePolicySelector {
                        mode: "atm_relative".to_string(),
                        strikes_above: 1,
                        strikes_below: 1,
                        top_n: None,
                    },
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "bybit_main".to_string(),
                kind: "bybit".to_string(),
                environment: "mainnet".to_string(),
                product_type: default_binance_product_type(),
                product_types: vec!["linear".to_string(), "option".to_string()],
                instrument_types: Vec::new(),
                instrument_families: Vec::new(),
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        validate_runtime(&effective).expect("valid bybit option universe should pass");
    }

    #[test]
    fn validate_runtime_rejects_bybit_option_universe_without_settlement_currency() {
        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                option_universe: vec![OptionUniverseSelector {
                    venue_id: "bybit_main".to_string(),
                    underlying: "BTC".to_string(),
                    settlement_currency: None,
                    include_perp: true,
                    families: vec![
                        "instruments".to_string(),
                        "quotes".to_string(),
                        "option_greeks".to_string(),
                    ],
                    expiry_policy: ExpiryPolicySelector {
                        mode: "nearest".to_string(),
                        days_max: 45,
                    },
                    strike_policy: StrikePolicySelector {
                        mode: "atm_relative".to_string(),
                        strikes_above: 1,
                        strikes_below: 1,
                        top_n: None,
                    },
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "bybit_main".to_string(),
                kind: "bybit".to_string(),
                environment: "mainnet".to_string(),
                product_type: default_binance_product_type(),
                product_types: vec!["linear".to_string(), "option".to_string()],
                instrument_types: Vec::new(),
                instrument_families: Vec::new(),
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        let err = validate_runtime(&effective)
            .expect_err("missing settlement_currency should fail for bybit");
        assert!(err.to_string().contains("requires settlement_currency"));
    }

    #[test]
    fn validate_runtime_accepts_okx_option_universe() {
        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                option_universe: vec![OptionUniverseSelector {
                    venue_id: "okx_main".to_string(),
                    underlying: "BTC".to_string(),
                    settlement_currency: Some("USD".to_string()),
                    include_perp: true,
                    families: vec![
                        "instruments".to_string(),
                        "quotes".to_string(),
                        "option_greeks".to_string(),
                        "index_prices".to_string(),
                        "funding_rates".to_string(),
                    ],
                    expiry_policy: ExpiryPolicySelector {
                        mode: "nearest".to_string(),
                        days_max: 45,
                    },
                    strike_policy: StrikePolicySelector {
                        mode: "atm_relative".to_string(),
                        strikes_above: 1,
                        strikes_below: 1,
                        top_n: None,
                    },
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "okx_main".to_string(),
                kind: "okx".to_string(),
                environment: "live".to_string(),
                product_type: default_binance_product_type(),
                product_types: Vec::new(),
                instrument_types: vec!["swap".to_string(), "option".to_string()],
                instrument_families: vec!["BTC-USD".to_string()],
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        validate_runtime(&effective).expect("valid okx option universe should pass");
    }

    #[test]
    fn validate_runtime_rejects_okx_option_universe_without_settlement_currency() {
        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                option_universe: vec![OptionUniverseSelector {
                    venue_id: "okx_main".to_string(),
                    underlying: "BTC".to_string(),
                    settlement_currency: None,
                    include_perp: true,
                    families: vec![
                        "instruments".to_string(),
                        "quotes".to_string(),
                        "option_greeks".to_string(),
                    ],
                    expiry_policy: ExpiryPolicySelector {
                        mode: "nearest".to_string(),
                        days_max: 45,
                    },
                    strike_policy: StrikePolicySelector {
                        mode: "atm_relative".to_string(),
                        strikes_above: 1,
                        strikes_below: 1,
                        top_n: None,
                    },
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "okx_main".to_string(),
                kind: "okx".to_string(),
                environment: "live".to_string(),
                product_type: default_binance_product_type(),
                product_types: Vec::new(),
                instrument_types: vec!["swap".to_string(), "option".to_string()],
                instrument_families: vec!["BTC-USD".to_string()],
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        let err = validate_runtime(&effective)
            .expect_err("missing settlement_currency should fail for okx");
        assert!(err.to_string().contains("requires settlement_currency"));
    }

    #[test]
    fn validate_runtime_rejects_okx_option_universe_without_matching_instrument_family() {
        let effective = resolve_config(CliConfigFile {
            capture: CaptureConfigFile {
                option_universe: vec![OptionUniverseSelector {
                    venue_id: "okx_main".to_string(),
                    underlying: "BTC".to_string(),
                    settlement_currency: Some("USD".to_string()),
                    include_perp: true,
                    families: vec![
                        "instruments".to_string(),
                        "quotes".to_string(),
                        "option_greeks".to_string(),
                    ],
                    expiry_policy: ExpiryPolicySelector {
                        mode: "nearest".to_string(),
                        days_max: 45,
                    },
                    strike_policy: StrikePolicySelector {
                        mode: "atm_relative".to_string(),
                        strikes_above: 1,
                        strikes_below: 1,
                        top_n: None,
                    },
                }],
                ..Default::default()
            },
            venues: vec![VenueConfig {
                id: "okx_main".to_string(),
                kind: "okx".to_string(),
                environment: "live".to_string(),
                product_type: default_binance_product_type(),
                product_types: Vec::new(),
                instrument_types: vec!["swap".to_string(), "option".to_string()],
                instrument_families: vec!["ETH-USD".to_string()],
            }],
            ..Default::default()
        })
        .expect("config should resolve");

        let err = validate_runtime(&effective)
            .expect_err("missing matching instrument_family should fail for okx");
        assert!(err.to_string().contains("instrument_families"));
    }

    #[test]
    fn example_deribit_dvol_config_loads_and_validates() {
        let path = repo_root().join("examples/capture.deribit-dvol.toml");
        let loaded = load_config(&path).expect("example should load");
        let effective = resolve_config(loaded).expect("example should resolve");
        validate_runtime(&effective).expect("example should validate");
    }

    #[test]
    fn example_hyperliquid_open_interest_config_loads_and_validates() {
        let path = repo_root().join("examples/capture.hyperliquid-open-interest.toml");
        let loaded = load_config(&path).expect("example should load");
        let effective = resolve_config(loaded).expect("example should resolve");
        validate_runtime(&effective).expect("example should validate");
    }

    #[test]
    fn example_deribit_option_universe_config_loads_and_validates() {
        let path = repo_root().join("examples/capture.deribit-btc-universe.toml");
        let loaded = load_config(&path).expect("example should load");
        let effective = resolve_config(loaded).expect("example should resolve");
        validate_runtime(&effective).expect("example should validate");
    }

    #[test]
    fn example_deribit_option_universe_oi_ranked_config_loads_and_validates() {
        let path = repo_root().join("examples/capture.deribit-btc-universe-oi-ranked.toml");
        let loaded = load_config(&path).expect("example should load");
        let effective = resolve_config(loaded).expect("example should resolve");
        validate_runtime(&effective).expect("example should validate");
        assert!(matches!(
            effective.option_universes[0].strike_policy,
            StrikePolicy::OiRanked { top_n: 3 }
        ));
    }

    #[test]
    fn example_deribit_option_universe_autorefresh_config_loads_and_validates() {
        let path = repo_root().join("examples/capture.deribit-btc-universe-autorefresh.toml");
        let loaded = load_config(&path).expect("example should load");
        let effective = resolve_config(loaded).expect("example should resolve");
        validate_runtime(&effective).expect("example should validate");
    }

    #[test]
    fn example_deribit_option_universe_oi_ranked_autorefresh_config_loads_and_validates() {
        let path =
            repo_root().join("examples/capture.deribit-btc-universe-oi-ranked-autorefresh.toml");
        let loaded = load_config(&path).expect("example should load");
        let effective = resolve_config(loaded).expect("example should resolve");
        validate_runtime(&effective).expect("example should validate");
        assert_eq!(
            effective.runtime.option_universe_refresh.strike_change_confirmations,
            2
        );
    }

    #[test]
    fn example_deribit_option_universe_research_config_loads_and_validates() {
        let path = repo_root().join("examples/capture.deribit-btc-universe-research.toml");
        let loaded = load_config(&path).expect("example should load");
        let effective = resolve_config(loaded).expect("example should resolve");
        validate_runtime(&effective).expect("example should validate");
    }

    #[test]
    fn example_bybit_option_universe_config_loads_and_validates() {
        let path = repo_root().join("examples/capture.bybit-btc-universe.toml");
        let loaded = load_config(&path).expect("example should load");
        let effective = resolve_config(loaded).expect("example should resolve");
        validate_runtime(&effective).expect("example should validate");
    }

    #[test]
    fn example_okx_option_universe_config_loads_and_validates() {
        let path = repo_root().join("examples/capture.okx-btc-universe.toml");
        let loaded = load_config(&path).expect("example should load");
        let effective = resolve_config(loaded).expect("example should resolve");
        validate_runtime(&effective).expect("example should validate");
    }

    #[test]
    fn example_bybit_option_universe_autorefresh_config_loads_and_validates() {
        let path = repo_root().join("examples/capture.bybit-btc-universe-autorefresh.toml");
        let loaded = load_config(&path).expect("example should load");
        let effective = resolve_config(loaded).expect("example should resolve");
        validate_runtime(&effective).expect("example should validate");
    }

    #[test]
    fn example_okx_option_universe_autorefresh_config_loads_and_validates() {
        let path = repo_root().join("examples/capture.okx-btc-universe-autorefresh.toml");
        let loaded = load_config(&path).expect("example should load");
        let effective = resolve_config(loaded).expect("example should resolve");
        validate_runtime(&effective).expect("example should validate");
    }

    #[test]
    fn example_bybit_option_universe_oi_ranked_config_loads_and_validates() {
        let path = repo_root().join("examples/capture.bybit-btc-universe-oi-ranked.toml");
        let loaded = load_config(&path).expect("example should load");
        let effective = resolve_config(loaded).expect("example should resolve");
        validate_runtime(&effective).expect("example should validate");
        assert!(matches!(
            effective.option_universes[0].strike_policy,
            StrikePolicy::OiRanked { top_n: 3 }
        ));
    }

    #[test]
    fn example_okx_option_universe_oi_ranked_config_loads_and_validates() {
        let path = repo_root().join("examples/capture.okx-btc-universe-oi-ranked.toml");
        let loaded = load_config(&path).expect("example should load");
        let effective = resolve_config(loaded).expect("example should resolve");
        validate_runtime(&effective).expect("example should validate");
        assert!(matches!(
            effective.option_universes[0].strike_policy,
            StrikePolicy::OiRanked { top_n: 3 }
        ));
    }
}
