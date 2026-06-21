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

use anyhow::{bail, Result};

use crate::{config::CaptureConfig, plan::CapturePlan};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FamilyBufferEstimate {
    pub family: &'static str,
    pub partition_count: usize,
    pub naive_peak_bytes: u64,
    pub capped_peak_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferMemoryEstimate {
    pub families: Vec<FamilyBufferEstimate>,
    pub enabled_family_runtimes: usize,
    pub total_peak_buffered_bytes: u64,
}

/// Per-family partition counts used for startup memory estimation.
#[must_use]
pub fn family_partition_counts(plan: &CapturePlan) -> Vec<(&'static str, usize)> {
    let flags = plan.family_runtime_flags();
    let mut counts = Vec::new();
    push_family_count_if(
        &mut counts,
        flags.instruments,
        "instruments",
        plan.planned_instrument_ids().len().max(plan.instruments.len()),
    );
    push_family_count_if(&mut counts, flags.custom_data, "custom_data", plan.custom_data.len());
    push_family_count_if(&mut counts, flags.quotes, "quotes", plan.quotes.len());
    push_family_count_if(&mut counts, flags.trades, "trades", plan.trades.len());
    push_family_count_if(&mut counts, flags.bars, "bars", plan.bars.len());
    push_family_count_if(
        &mut counts,
        flags.book_deltas,
        "book_deltas",
        plan.book_deltas.len(),
    );
    push_family_count_if(
        &mut counts,
        flags.mark_prices,
        "mark_prices",
        plan.mark_prices.len(),
    );
    push_family_count_if(
        &mut counts,
        flags.index_prices,
        "index_prices",
        plan.index_prices.len(),
    );
    push_family_count_if(
        &mut counts,
        flags.funding_rates,
        "funding_rates",
        plan.funding_rates.len(),
    );
    push_family_count_if(
        &mut counts,
        flags.instrument_statuses,
        "instrument_statuses",
        plan.instrument_statuses.len(),
    );
    push_family_count_if(
        &mut counts,
        flags.instrument_closes,
        "instrument_closes",
        plan.instrument_closes.len(),
    );
    push_family_count_if(
        &mut counts,
        flags.option_greeks,
        "option_greeks",
        plan.option_greeks.len(),
    );
    counts
}

fn push_family_count_if(
    counts: &mut Vec<(&'static str, usize)>,
    enabled: bool,
    family: &'static str,
    n: usize,
) {
    if enabled && n > 0 {
        counts.push((family, n));
    }
}

/// Upper bound on in-process partition buffer bytes across all enabled family runtimes.
#[must_use]
pub fn estimate_peak_buffered_bytes(plan: &CapturePlan, config: &CaptureConfig) -> BufferMemoryEstimate {
    let per_partition = config.max_buffer_bytes as u64;
    let per_family_cap = config.max_total_buffer_bytes as u64;

    let families = family_partition_counts(plan)
        .into_iter()
        .map(|(family, partition_count)| {
            let naive_peak_bytes = partition_count as u64 * per_partition;
            let capped_peak_bytes = naive_peak_bytes.min(per_family_cap);
            FamilyBufferEstimate {
                family,
                partition_count,
                naive_peak_bytes,
                capped_peak_bytes,
            }
        })
        .collect::<Vec<_>>();

    let total_peak_buffered_bytes = families.iter().map(|f| f.capped_peak_bytes).sum();

    BufferMemoryEstimate {
        enabled_family_runtimes: plan.enabled_background_worker_count(),
        families,
        total_peak_buffered_bytes,
    }
}

#[must_use]
pub fn format_buffer_estimate(estimate: &BufferMemoryEstimate) -> String {
    let mut lines = vec![format!(
        "capture buffer estimate: {} MiB peak across {} family runtime(s) \
         (max_buffer_bytes per partition, max_total_buffer_bytes per family)",
        estimate.total_peak_buffered_bytes / (1024 * 1024),
        estimate.enabled_family_runtimes,
    )];

    for family in &estimate.families {
        lines.push(format!(
            "  - {}: {} partition(s), capped_peak={} MiB (naive={} MiB)",
            family.family,
            family.partition_count,
            family.capped_peak_bytes / (1024 * 1024),
            family.naive_peak_bytes / (1024 * 1024),
        ));
    }

    lines.join("\n")
}

#[must_use]
pub fn format_budget_warning(estimate: &BufferMemoryEstimate, budget_bytes: u64) -> String {
    format!(
        "WARNING: estimated peak capture buffers ({} bytes, ~{} MiB) exceed \
         runtime.resource_budget_bytes ({} bytes, ~{} MiB). \
         Reduce capture breadth, lower output.max_buffer_bytes / output.max_active_partitions, \
         or raise runtime.resource_budget_bytes.",
        estimate.total_peak_buffered_bytes,
        estimate.total_peak_buffered_bytes / (1024 * 1024),
        budget_bytes,
        budget_bytes / (1024 * 1024),
    )
}

pub fn validate_capture_config(config: &CaptureConfig) -> Result<()> {
    if config.max_buffer_bytes == 0 {
        bail!("output.max_buffer_bytes must be greater than zero");
    }
    if config.max_total_buffer_bytes == 0 {
        bail!("output.max_total_buffer_bytes must be greater than zero");
    }
    if config.max_active_partitions == 0 {
        bail!("output.max_active_partitions must be greater than zero");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use nautilus_model::identifiers::InstrumentId;

    use super::*;
    use crate::plan::{CapturePlan, QuoteCaptureSpec, TradeCaptureSpec};

    #[test]
    fn estimate_caps_per_family_total() {
        let plan = CapturePlan {
            quotes: vec![
                QuoteCaptureSpec {
                    instrument_id: InstrumentId::from("A.BINANCE"),
                },
                QuoteCaptureSpec {
                    instrument_id: InstrumentId::from("B.BINANCE"),
                },
            ],
            trades: vec![TradeCaptureSpec {
                instrument_id: InstrumentId::from("A.BINANCE"),
            }],
            ..CapturePlan::default()
        };

        let config = CaptureConfig {
            max_buffer_bytes: 32 * 1024 * 1024,
            max_total_buffer_bytes: 48 * 1024 * 1024,
            max_active_partitions: 64,
            ..CaptureConfig::default()
        };

        let estimate = estimate_peak_buffered_bytes(&plan, &config);
        assert_eq!(estimate.enabled_family_runtimes, 3);
        assert_eq!(
            estimate.total_peak_buffered_bytes,
            (48 + 48 + 32) * 1024 * 1024
        );
    }

    #[test]
    fn family_partition_counts_ignore_empty_families() {
        let plan = CapturePlan {
            quotes: vec![QuoteCaptureSpec {
                instrument_id: InstrumentId::from_str("ETHUSDT-PERP.BINANCE").unwrap(),
            }],
            ..CapturePlan::default()
        };

        let counts = family_partition_counts(&plan);
        assert_eq!(
            counts,
            vec![("instruments", 1), ("quotes", 1)]
        );
    }
}