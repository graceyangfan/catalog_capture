// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};
use catalog_capture_core::{
    forward_price_log_path, read_option_universe_resolution_records,
    summarize_option_universe_resolution_records, OptionUniverseResolutionSummary,
};
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct OptionUniverseCatalogValidationOptions {
    pub min_rows: i64,
    pub min_perp_trade_rows: i64,
    pub require_contract_state: bool,
    pub require_refresh_change: bool,
    pub bar_types: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OptionUniverseCatalogValidationReport {
    pub venue_id: String,
    pub underlying: String,
    pub perp_instrument_id: String,
    pub option_count: usize,
    pub refresh_count: usize,
    pub latest_rollover_reason: Option<String>,
}

pub fn validate_option_universe_catalog(
    catalog_root: &Path,
    options: &OptionUniverseCatalogValidationOptions,
) -> Result<Vec<OptionUniverseCatalogValidationReport>> {
    if options.min_rows <= 0 {
        bail!("min_rows must be positive");
    }
    if options.min_perp_trade_rows < 0 {
        bail!("min_perp_trade_rows must be >= 0");
    }

    let records = read_option_universe_resolution_records(catalog_root)?;
    let summaries = summarize_option_universe_resolution_records(&records);
    if summaries.is_empty() {
        bail!("no option universe resolution metadata found");
    }

    validate_forward_prices_metadata(catalog_root)?;
    for bar_type in &options.bar_types {
        assert_family_rows(
            catalog_root,
            &["bar"],
            bar_type,
            options.min_rows,
            &format!("bars[{bar_type}]"),
        )?;
    }

    let mut reports = Vec::with_capacity(summaries.len());
    for summary in summaries {
        reports.push(validate_summary(catalog_root, &summary, options)?);
    }

    Ok(reports)
}

pub fn render_option_universe_catalog_validation_json(
    reports: &[OptionUniverseCatalogValidationReport],
) -> Result<String> {
    serde_json::to_string_pretty(reports).map_err(|err| {
        anyhow::anyhow!("failed to render option universe catalog validation: {err}")
    })
}

pub fn render_option_universe_catalog_validation_text(
    reports: &[OptionUniverseCatalogValidationReport],
) -> String {
    if reports.is_empty() {
        return "No option universe catalog validation results.".to_string();
    }

    reports
        .iter()
        .map(|report| {
            format!(
                "venue={} underlying={}\n\
                 perp={}\n\
                 option_count={}\n\
                 refresh_count={}\n\
                 latest_rollover_reason={}",
                report.venue_id,
                report.underlying,
                report.perp_instrument_id,
                report.option_count,
                report.refresh_count,
                report.latest_rollover_reason.as_deref().unwrap_or("-"),
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn validate_summary(
    catalog_root: &Path,
    summary: &OptionUniverseResolutionSummary,
    options: &OptionUniverseCatalogValidationOptions,
) -> Result<OptionUniverseCatalogValidationReport> {
    if options.require_refresh_change && summary.refresh_count == 0 {
        bail!(
            "option universe {}:{} expected at least one refresh delta",
            summary.venue_id,
            summary.underlying
        );
    }

    let perp_id = summary.perp_instrument_id.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "option universe {}:{} missing perp_instrument_id",
            summary.venue_id,
            summary.underlying
        )
    })?;
    if summary.option_instrument_ids.is_empty() {
        bail!(
            "option universe {}:{} selected no option instruments",
            summary.venue_id,
            summary.underlying
        );
    }

    assert_family_rows(
        catalog_root,
        &["quotes", "quote_tick"],
        &perp_id,
        options.min_rows,
        &format!("perp quotes[{perp_id}]"),
    )?;
    assert_family_rows(
        catalog_root,
        &["mark_prices", "mark_price_update"],
        &perp_id,
        options.min_rows,
        &format!("perp mark_prices[{perp_id}]"),
    )?;
    assert_family_rows(
        catalog_root,
        &["index_prices", "index_price_updates"],
        &perp_id,
        options.min_rows,
        &format!("perp index_prices[{perp_id}]"),
    )?;
    assert_family_rows(
        catalog_root,
        &["funding_rate_update"],
        &perp_id,
        options.min_rows,
        &format!("perp funding[{perp_id}]"),
    )?;
    if options.min_perp_trade_rows > 0 {
        assert_family_rows(
            catalog_root,
            &["trades", "trade_tick"],
            &perp_id,
            options.min_perp_trade_rows,
            &format!("perp trades[{perp_id}]"),
        )?;
    }
    if options.require_contract_state {
        assert_family_rows(
            catalog_root,
            &["instrument_status"],
            &perp_id,
            1,
            &format!("perp instrument_status[{perp_id}]"),
        )?;
        assert_family_rows(
            catalog_root,
            &["instrument_closes"],
            &perp_id,
            1,
            &format!("perp instrument_closes[{perp_id}]"),
        )?;
    }

    for option_id in &summary.option_instrument_ids {
        assert_family_rows(
            catalog_root,
            &["quotes", "quote_tick"],
            option_id,
            options.min_rows,
            &format!("option quotes[{option_id}]"),
        )?;
        assert_family_rows(
            catalog_root,
            &["mark_prices", "mark_price_update"],
            option_id,
            options.min_rows,
            &format!("option mark_prices[{option_id}]"),
        )?;
        assert_family_rows(
            catalog_root,
            &["option_greeks"],
            option_id,
            options.min_rows,
            &format!("option greeks[{option_id}]"),
        )?;
        if options.require_contract_state {
            assert_family_rows(
                catalog_root,
                &["instrument_status"],
                option_id,
                1,
                &format!("option instrument_status[{option_id}]"),
            )?;
            assert_family_rows(
                catalog_root,
                &["instrument_closes"],
                option_id,
                1,
                &format!("option instrument_closes[{option_id}]"),
            )?;
        }
    }

    Ok(OptionUniverseCatalogValidationReport {
        venue_id: summary.venue_id.clone(),
        underlying: summary.underlying.clone(),
        perp_instrument_id: perp_id,
        option_count: summary.option_instrument_ids.len(),
        refresh_count: summary.refresh_count,
        latest_rollover_reason: summary.latest_rollover_reason.clone(),
    })
}

fn validate_forward_prices_metadata(catalog_root: &Path) -> Result<()> {
    let path = forward_price_log_path(catalog_root);
    let content = fs::read_to_string(&path)
        .with_context(|| format!("failed to read forward price metadata {}", path.display()))?;
    if !content.lines().any(|line| !line.trim().is_empty()) {
        bail!("forward price metadata is empty: {}", path.display());
    }
    Ok(())
}

fn assert_family_rows(
    catalog_root: &Path,
    family_aliases: &[&str],
    identifier: &str,
    min_rows: i64,
    label: &str,
) -> Result<i64> {
    for family in family_aliases {
        let path = catalog_root.join("data").join(family).join(identifier);
        if !path.exists() {
            continue;
        }
        let rows = sum_parquet_rows(&path)?;
        if rows >= min_rows {
            return Ok(rows);
        }
        bail!("{label} expected at least {min_rows} rows, got {rows}");
    }

    bail!(
        "{label} expected parquet data under one of [{}]",
        family_aliases.join(", ")
    )
}

fn sum_parquet_rows(path: &Path) -> Result<i64> {
    let mut rows = 0_i64;
    for file_path in collect_parquet_files(path)? {
        rows += parquet_rows(&file_path)?;
    }
    Ok(rows)
}

fn collect_parquet_files(path: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_parquet_files_recursive(path, &mut files)?;
    if files.is_empty() {
        bail!("no parquet files found under {}", path.display());
    }
    Ok(files)
}

fn collect_parquet_files_recursive(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(path)
        .with_context(|| format!("failed to read parquet directory {}", path.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_parquet_files_recursive(&path, files)?;
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            files.push(path);
        }
    }
    Ok(())
}

fn parquet_rows(path: &Path) -> Result<i64> {
    let file = File::open(path)
        .with_context(|| format!("failed to open parquet file {}", path.display()))?;
    let reader = SerializedFileReader::new(file)
        .with_context(|| format!("failed to read parquet metadata {}", path.display()))?;
    Ok(reader.metadata().file_metadata().num_rows())
}
