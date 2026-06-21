use std::path::Path;

use anyhow::{bail, Context, Result};
use nautilus_model::{
    data::{
        close::InstrumentClose, IndexPriceUpdate,
        InstrumentStatus, MarkPriceUpdate, OptionGreeks, QuoteTick, TradeTick,
    },
    identifiers::InstrumentId,
    instruments::Instrument,
};
use nautilus_persistence::backend::catalog::ParquetDataCatalog;
use serde::{Deserialize, Serialize};

/// Default option sample size for `all` strike readback smoke validation.
pub const ALL_STRIKES_READBACK_SAMPLE_LIMIT: usize = 6;

#[derive(Debug, Clone)]
pub struct OptionUniverseReadbackOptions {
    pub perp_instrument_id: String,
    pub option_instrument_ids: Vec<String>,
    pub min_rows: i64,
    pub min_perp_trade_rows: i64,
    pub require_contract_state: bool,
    pub bar_types: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentReadbackCounts {
    pub instrument_id: String,
    pub quotes: usize,
    pub mark_prices: usize,
    pub index_prices: usize,
    pub trade_ticks: usize,
    pub option_greeks: usize,
    pub instrument_statuses: usize,
    pub instrument_closes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OptionUniverseReadbackReport {
    pub perp: InstrumentReadbackCounts,
    pub funding_rows: usize,
    pub bars: Vec<BarReadbackCount>,
    pub options: Vec<InstrumentReadbackCounts>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarReadbackCount {
    pub bar_type: String,
    pub rows: usize,
}

pub fn validate_option_universe_readback(
    catalog_root: &Path,
    options: &OptionUniverseReadbackOptions,
) -> Result<OptionUniverseReadbackReport> {
    // ParquetDataCatalog uses its own Tokio runtime via `get_runtime()`. Run readback
    // off the CLI/live capture runtime thread to avoid nested-runtime panics.
    let catalog_root = catalog_root.to_path_buf();
    let options = options.clone();
    std::thread::Builder::new()
        .name("option-universe-readback".into())
        .spawn(move || validate_option_universe_readback_inner(&catalog_root, &options))
        .context("failed to spawn option universe readback validation thread")?
        .join()
        .map_err(|_| anyhow::anyhow!("option universe readback validation thread panicked"))?
}

fn validate_option_universe_readback_inner(
    catalog_root: &Path,
    options: &OptionUniverseReadbackOptions,
) -> Result<OptionUniverseReadbackReport> {
    if options.min_rows <= 0 {
        bail!("min_rows must be positive");
    }
    if options.min_perp_trade_rows < 0 {
        bail!("min_perp_trade_rows must be >= 0");
    }
    if options.option_instrument_ids.is_empty() {
        bail!("at least one option instrument id is required");
    }

    let mut catalog = ParquetDataCatalog::new(catalog_root, None, None, None, None);

    for instrument_id in std::iter::once(options.perp_instrument_id.as_str()).chain(
        options
            .option_instrument_ids
            .iter()
            .map(String::as_str),
    ) {
        assert_instrument_metadata(&catalog, instrument_id)?;
    }

    let perp = validate_perp_readback(&mut catalog, options)?;
    let funding_rows = validate_funding_rates(&mut catalog, &options.perp_instrument_id)?;
    let bars = validate_bars(&mut catalog, &options.bar_types, options.min_rows)?;
    let mut option_reports = Vec::with_capacity(options.option_instrument_ids.len());
    for option_id in &options.option_instrument_ids {
        option_reports.push(validate_option_readback(
            &mut catalog,
            option_id,
            options,
        )?);
    }

    Ok(OptionUniverseReadbackReport {
        perp,
        funding_rows,
        bars,
        options: option_reports,
    })
}

fn validate_perp_readback(
    catalog: &mut ParquetDataCatalog,
    options: &OptionUniverseReadbackOptions,
) -> Result<InstrumentReadbackCounts> {
    let perp_id = &options.perp_instrument_id;
    let quotes = assert_quote_rows(catalog, perp_id, options.min_rows)?;
    let mark_prices = assert_mark_price_rows(catalog, perp_id, options.min_rows)?;
    let index_prices = assert_index_price_rows(catalog, perp_id, options.min_rows)?;
    let trade_ticks = if options.min_perp_trade_rows > 0 {
        assert_trade_rows(catalog, perp_id, options.min_perp_trade_rows)?
    } else {
        0
    };
    let (instrument_statuses, instrument_closes) =
        probe_contract_state(catalog, perp_id, options.require_contract_state)?;

    Ok(InstrumentReadbackCounts {
        instrument_id: perp_id.clone(),
        quotes,
        mark_prices,
        index_prices,
        trade_ticks,
        option_greeks: 0,
        instrument_statuses,
        instrument_closes,
    })
}

fn validate_option_readback(
    catalog: &mut ParquetDataCatalog,
    option_id: &str,
    options: &OptionUniverseReadbackOptions,
) -> Result<InstrumentReadbackCounts> {
    let quotes = assert_quote_rows(catalog, option_id, options.min_rows)?;
    let mark_prices = assert_mark_price_rows(catalog, option_id, options.min_rows)?;
    let option_greeks = assert_option_greeks_rows(catalog, option_id, options.min_rows)?;
    let (instrument_statuses, instrument_closes) =
        probe_contract_state(catalog, option_id, options.require_contract_state)?;

    Ok(InstrumentReadbackCounts {
        instrument_id: option_id.to_string(),
        quotes,
        mark_prices,
        index_prices: 0,
        trade_ticks: 0,
        option_greeks,
        instrument_statuses,
        instrument_closes,
    })
}

fn assert_instrument_metadata(
    catalog: &ParquetDataCatalog,
    instrument_id: &str,
) -> Result<()> {
    let instruments = catalog
        .instruments(Some(&[instrument_id.to_string()]), None, None)
        .with_context(|| format!("failed to query instrument metadata for {instrument_id}"))?;
    if instruments.is_empty() {
        bail!("expected instrument metadata for {instrument_id}");
    }
    if instruments[0].id().to_string() != instrument_id {
        bail!(
            "instrument metadata id mismatch for {instrument_id}: got {}",
            instruments[0].id()
        );
    }
    Ok(())
}

fn assert_quote_rows(
    catalog: &mut ParquetDataCatalog,
    instrument_id: &str,
    min_rows: i64,
) -> Result<usize> {
    let rows = catalog
        .quote_ticks(Some(vec![instrument_id.to_string()]), None, None)
        .with_context(|| format!("failed to read quotes for {instrument_id}"))?;
    assert_min_rows(&rows, min_rows, &format!("quotes[{instrument_id}]"))?;
    assert_matching_instrument_ids(&rows, instrument_id, &format!("quotes[{instrument_id}]"))?;
    ParquetDataCatalog::check_ascending_timestamps(&rows, &format!("quotes[{instrument_id}]"))?;
    Ok(rows.len())
}

fn assert_mark_price_rows(
    catalog: &mut ParquetDataCatalog,
    instrument_id: &str,
    min_rows: i64,
) -> Result<usize> {
    let rows = catalog
        .query_typed_data::<MarkPriceUpdate>(
            Some(vec![instrument_id.to_string()]),
            None,
            None,
            None,
            None,
            true,
        )
        .with_context(|| format!("failed to read mark prices for {instrument_id}"))?;
    assert_min_rows(
        &rows,
        min_rows,
        &format!("mark_prices[{instrument_id}]"),
    )?;
    assert_matching_instrument_ids(
        &rows,
        instrument_id,
        &format!("mark_prices[{instrument_id}]"),
    )?;
    ParquetDataCatalog::check_ascending_timestamps(
        &rows,
        &format!("mark_prices[{instrument_id}]"),
    )?;
    Ok(rows.len())
}

fn assert_index_price_rows(
    catalog: &mut ParquetDataCatalog,
    instrument_id: &str,
    min_rows: i64,
) -> Result<usize> {
    let rows = catalog
        .query_typed_data::<IndexPriceUpdate>(
            Some(vec![instrument_id.to_string()]),
            None,
            None,
            None,
            None,
            true,
        )
        .with_context(|| format!("failed to read index prices for {instrument_id}"))?;
    assert_min_rows(
        &rows,
        min_rows,
        &format!("index_prices[{instrument_id}]"),
    )?;
    assert_matching_instrument_ids(
        &rows,
        instrument_id,
        &format!("index_prices[{instrument_id}]"),
    )?;
    ParquetDataCatalog::check_ascending_timestamps(
        &rows,
        &format!("index_prices[{instrument_id}]"),
    )?;
    Ok(rows.len())
}

fn assert_trade_rows(
    catalog: &mut ParquetDataCatalog,
    instrument_id: &str,
    min_rows: i64,
) -> Result<usize> {
    let rows = catalog
        .trade_ticks(Some(vec![instrument_id.to_string()]), None, None)
        .with_context(|| format!("failed to read trade ticks for {instrument_id}"))?;
    assert_min_rows(
        &rows,
        min_rows,
        &format!("trade_ticks[{instrument_id}]"),
    )?;
    assert_matching_instrument_ids(
        &rows,
        instrument_id,
        &format!("trade_ticks[{instrument_id}]"),
    )?;
    ParquetDataCatalog::check_ascending_timestamps(
        &rows,
        &format!("trade_ticks[{instrument_id}]"),
    )?;
    Ok(rows.len())
}

fn assert_option_greeks_rows(
    catalog: &mut ParquetDataCatalog,
    instrument_id: &str,
    min_rows: i64,
) -> Result<usize> {
    let rows = catalog
        .option_greeks(Some(vec![instrument_id.to_string()]), None, None)
        .with_context(|| format!("failed to read option greeks for {instrument_id}"))?;
    assert_min_rows(
        &rows,
        min_rows,
        &format!("option_greeks[{instrument_id}]"),
    )?;
    assert_matching_instrument_ids(
        &rows,
        instrument_id,
        &format!("option_greeks[{instrument_id}]"),
    )?;
    ParquetDataCatalog::check_ascending_timestamps(
        &rows,
        &format!("option_greeks[{instrument_id}]"),
    )?;

    let sample = rows
        .last()
        .ok_or_else(|| anyhow::anyhow!("option_greeks[{instrument_id}] missing sample row"))?;
    if sample.mark_iv.is_none() {
        bail!("option_greeks[{instrument_id}] latest row missing mark_iv");
    }
    for (name, value) in [
        ("delta", sample.delta),
        ("gamma", sample.gamma),
        ("vega", sample.vega),
        ("theta", sample.theta),
    ] {
        if !value.is_finite() {
            bail!("option_greeks[{instrument_id}] latest row has invalid {name}");
        }
    }

    Ok(rows.len())
}

fn validate_funding_rates(
    catalog: &mut ParquetDataCatalog,
    instrument_id: &str,
) -> Result<usize> {
    let rows = catalog
        .funding_rates(Some(vec![instrument_id.to_string()]), None, None)
        .with_context(|| format!("failed to read funding rates for {instrument_id}"))?;
    if rows.is_empty() {
        bail!("expected funding parquet rows for {instrument_id}");
    }
    Ok(rows.len())
}

fn validate_bars(
    catalog: &mut ParquetDataCatalog,
    bar_types: &[String],
    min_rows: i64,
) -> Result<Vec<BarReadbackCount>> {
    let mut counts = Vec::with_capacity(bar_types.len());
    for bar_type in bar_types {
        let rows = catalog
            .bars(Some(vec![bar_type.clone()]), None, None)
            .with_context(|| format!("failed to read bars for {bar_type}"))?;
        assert_min_rows(&rows, min_rows, &format!("bars[{bar_type}]"))?;
        ParquetDataCatalog::check_ascending_timestamps(&rows, &format!("bars[{bar_type}]"))?;
        counts.push(BarReadbackCount {
            bar_type: bar_type.clone(),
            rows: rows.len(),
        });
    }
    Ok(counts)
}

fn probe_contract_state(
    catalog: &mut ParquetDataCatalog,
    instrument_id: &str,
    require: bool,
) -> Result<(usize, usize)> {
    let statuses = catalog
        .query_typed_data::<InstrumentStatus>(
            Some(vec![instrument_id.to_string()]),
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap_or_default();
    let closes = catalog
        .instrument_closes(Some(vec![instrument_id.to_string()]), None, None)
        .unwrap_or_default();

    if require {
        if statuses.is_empty() {
            bail!("expected instrument_status rows for {instrument_id}");
        }
        if closes.is_empty() {
            bail!("expected instrument_closes rows for {instrument_id}");
        }
    }

    if !statuses.is_empty() {
        assert_matching_instrument_ids(
            &statuses,
            instrument_id,
            &format!("instrument_status[{instrument_id}]"),
        )?;
        ParquetDataCatalog::check_ascending_timestamps(
            &statuses,
            &format!("instrument_status[{instrument_id}]"),
        )?;
    }
    if !closes.is_empty() {
        assert_matching_instrument_ids(
            &closes,
            instrument_id,
            &format!("instrument_closes[{instrument_id}]"),
        )?;
        ParquetDataCatalog::check_ascending_timestamps(
            &closes,
            &format!("instrument_closes[{instrument_id}]"),
        )?;
    }

    Ok((statuses.len(), closes.len()))
}

fn assert_min_rows<T>(rows: &[T], min_rows: i64, label: &str) -> Result<()> {
    if rows.len() < min_rows as usize {
        bail!(
            "{label} expected at least {min_rows} rows, got {}",
            rows.len()
        );
    }
    Ok(())
}

fn assert_matching_instrument_ids<T>(rows: &[T], instrument_id: &str, label: &str) -> Result<()>
where
    T: InstrumentIdRow,
{
    let expected = InstrumentId::from(instrument_id);
    if rows
        .iter()
        .any(|row| row.instrument_id() != expected)
    {
        bail!("{label} contained rows for unexpected instrument ids");
    }
    Ok(())
}

trait InstrumentIdRow {
    fn instrument_id(&self) -> InstrumentId;
}

impl InstrumentIdRow for QuoteTick {
    fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
}

impl InstrumentIdRow for TradeTick {
    fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
}

impl InstrumentIdRow for MarkPriceUpdate {
    fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
}

impl InstrumentIdRow for IndexPriceUpdate {
    fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
}

impl InstrumentIdRow for OptionGreeks {
    fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
}

impl InstrumentIdRow for InstrumentStatus {
    fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
}

impl InstrumentIdRow for InstrumentClose {
    fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::{FundingRateUpdate, OptionGreekValues},
        enums::GreeksConvention,
        instruments::{
            stubs::{crypto_option_btc_deribit, crypto_perpetual_ethusdt},
            InstrumentAny,
        },
        types::{Price, Quantity},
    };

    use super::*;

    fn write_minimal_option_universe_catalog(root: &Path) -> (String, String) {
        let perp_id = "ETHUSDT-PERP.BINANCE";
        let option_id = "BTC-13JAN23-16000-P.DERIBIT";
        let perp = InstrumentId::from(perp_id);
        let option = InstrumentId::from(option_id);

        let catalog = ParquetDataCatalog::new(root, None, None, None, None);
        catalog
            .write_instruments(vec![
                InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt()),
                InstrumentAny::CryptoOption(crypto_option_btc_deribit(
                    3,
                    1,
                    Price::from("0.001"),
                    Quantity::from("0.1"),
                )),
            ])
            .expect("write instruments");

        let quote = QuoteTick::new(
            perp,
            Price::from("1.0001"),
            Price::from("1.0002"),
            Quantity::from("100"),
            Quantity::from("100"),
            UnixNanos::from(1_000),
            UnixNanos::from(1_000),
        );
        catalog
            .write_to_parquet(vec![quote], None, None, None)
            .expect("write perp quote");

        let mark = MarkPriceUpdate::new(perp, Price::from("1000"), UnixNanos::from(1_000), UnixNanos::from(1_000));
        catalog
            .write_to_parquet(vec![mark], None, None, None)
            .expect("write perp mark");

        let index = IndexPriceUpdate::new(perp, Price::from("1001"), UnixNanos::from(1_000), UnixNanos::from(1_000));
        catalog
            .write_to_parquet(vec![index], None, None, None)
            .expect("write perp index");

        let funding = FundingRateUpdate::new(
            perp,
            rust_decimal::Decimal::new(1, 4),
            None,
            None,
            UnixNanos::from(1_000),
            UnixNanos::from(1_000),
        );
        catalog
            .write_to_parquet(vec![funding], None, None, None)
            .expect("write funding");

        let option_quote = QuoteTick::new(
            option,
            Price::from("10.1"),
            Price::from("10.2"),
            Quantity::from("1"),
            Quantity::from("1"),
            UnixNanos::from(2_000),
            UnixNanos::from(2_000),
        );
        catalog
            .write_to_parquet(vec![option_quote], None, None, None)
            .expect("write option quote");

        let option_mark = MarkPriceUpdate::new(option, Price::from("10"), UnixNanos::from(2_000), UnixNanos::from(2_000));
        catalog
            .write_to_parquet(vec![option_mark], None, None, None)
            .expect("write option mark");

        let greeks = OptionGreeks {
            instrument_id: option,
            convention: GreeksConvention::BlackScholes,
            greeks: OptionGreekValues {
                delta: 0.5,
                gamma: 0.01,
                vega: 0.2,
                theta: -0.1,
                rho: 0.0,
            },
            mark_iv: Some(0.55),
            bid_iv: None,
            ask_iv: None,
            underlying_price: Some(1000.0),
            open_interest: None,
            ts_event: UnixNanos::from(2_000),
            ts_init: UnixNanos::from(2_000),
        };
        catalog
            .write_to_parquet(vec![greeks], None, None, None)
            .expect("write option greeks");

        (perp_id.to_string(), option_id.to_string())
    }

    #[test]
    fn validate_option_universe_readback_accepts_minimal_catalog() {
        let root = std::env::temp_dir().join(format!(
            "option-universe-readback-{}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        let (perp_id, option_id) = write_minimal_option_universe_catalog(&root);

        let report = validate_option_universe_readback(
            &root,
            &OptionUniverseReadbackOptions {
                perp_instrument_id: perp_id,
                option_instrument_ids: vec![option_id],
                min_rows: 1,
                min_perp_trade_rows: 0,
                require_contract_state: false,
                bar_types: Vec::new(),
            },
        )
        .expect("minimal catalog should read back");

        assert_eq!(report.perp.quotes, 1);
        assert_eq!(report.funding_rows, 1);
        assert_eq!(report.options.len(), 1);
        assert_eq!(report.options[0].option_greeks, 1);

        fs::remove_dir_all(root).ok();
    }
}