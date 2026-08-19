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

//! Per-family thresholds for memory → sink flush and parquet row-group sizing.
//!
//! A single TOML `flush_rows` / `row_group_rows` cannot serve both Binance L2
//! (~tens of k rows/s) and sparse mark/quotes. Each background runtime clones
//! [`CaptureConfig`] and applies a family profile. Segment mode also interval-flushes
//! into the open `*.parquet.part` so sparse streams and BookSummary do not wait only
//! for row counts.
//!
//! **Memory flush** and **parquet row-group size** are intentionally separate:
//! BookSummary should leave RAM every ~1 poll, but each parquet row group must
//! hold many polls so a day-long part stays far below the 32 767 row-group limit.
//!
//! | Family | Memory flush | Parquet row group | Rationale |
//! |--------|--------------|-------------------|-----------|
//! | book_deltas | ~20 000 | same | High volume; amortize encode (profile C) |
//! | trades | ~2 000 | same | Medium |
//! | quotes | ~500 | same | Sparse HIP-4 outcomes |
//! | mark / status / … | ~100 | same | Very sparse |
//! | custom (BookSummary) | ~1 000 | ~50 000 | Poll often; pack many polls per RG |
//! | instruments | tiny | n/a | Chunked defs |

use crate::{
    config::CaptureConfig,
    lifecycle::row_group_capacity::{CUSTOM_MEMORY_FLUSH_ROWS, CUSTOM_ROW_GROUP_ROWS},
};

/// Capture family used only for flush-threshold selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureFlushFamily {
    BookDeltas,
    Trades,
    Quotes,
    MarkPrices,
    IndexPrices,
    FundingRates,
    InstrumentStatus,
    InstrumentClose,
    OptionGreeks,
    Bars,
    /// Request/subscribe custom (e.g. `DeribitBookSummary`).
    CustomData,
    Instruments,
}

/// Smoke / unit configs use very small `flush_rows`; never inflate those.
const SMOKE_ROW_CEILING: usize = 200;

/// Resolve the effective **memory → sink** row threshold for a family.
#[must_use]
pub fn family_row_threshold(base: &CaptureConfig, family: CaptureFlushFamily) -> usize {
    let configured = base.lifecycle.batch_row_threshold(base.flush_rows).max(1);
    if configured < SMOKE_ROW_CEILING {
        return configured;
    }

    match family {
        // Profile C: L2 — ~20k is reasonable; allow higher operator values up to 50k.
        CaptureFlushFamily::BookDeltas => configured.clamp(20_000, 50_000),
        CaptureFlushFamily::Trades => 2_000,
        CaptureFlushFamily::Quotes => 500,
        CaptureFlushFamily::MarkPrices
        | CaptureFlushFamily::IndexPrices
        | CaptureFlushFamily::FundingRates
        | CaptureFlushFamily::InstrumentStatus
        | CaptureFlushFamily::InstrumentClose => 100,
        CaptureFlushFamily::OptionGreeks => 2_000,
        CaptureFlushFamily::Bars => 500,
        // One Deribit BookSummary poll (~800–1000 rows); see row_group_capacity.
        CaptureFlushFamily::CustomData => CUSTOM_MEMORY_FLUSH_ROWS,
        CaptureFlushFamily::Instruments => 50,
    }
}

/// Resolve parquet **max rows per row group** for a family (segment mode).
///
/// May exceed the memory flush threshold so high-rate custom streams pack many
/// flushes into one row group and stay under the 32 767 RG/file limit.
/// Sizing derivation: [`crate::lifecycle::row_group_capacity`].
#[must_use]
pub fn family_row_group_rows(base: &CaptureConfig, family: CaptureFlushFamily) -> usize {
    let flush = family_row_threshold(base, family);
    if flush < SMOKE_ROW_CEILING {
        return flush;
    }
    match family {
        CaptureFlushFamily::CustomData => CUSTOM_ROW_GROUP_ROWS,
        // Already large enough for daily seal under normal L2 rates.
        other => family_row_threshold(base, other),
    }
}

/// Clone config with family-specific row thresholds applied.
#[must_use]
pub fn capture_config_for_family(base: &CaptureConfig, family: CaptureFlushFamily) -> CaptureConfig {
    let flush_rows = family_row_threshold(base, family);
    let row_group_rows = family_row_group_rows(base, family);
    let mut config = base.clone();
    config.flush_rows = flush_rows;
    if config.lifecycle.is_segment_mode() {
        config.lifecycle.segment.row_group_rows = row_group_rows;
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::{LifecycleConfig, LifecycleMode, SegmentLifecycleConfig};

    fn segment_base(row_group_rows: usize) -> CaptureConfig {
        CaptureConfig {
            flush_rows: 1_000,
            lifecycle: LifecycleConfig {
                mode: LifecycleMode::Segment,
                segment: SegmentLifecycleConfig { row_group_rows },
                ..LifecycleConfig::default()
            },
            ..CaptureConfig::default()
        }
    }

    #[test]
    fn book_deltas_use_20k_when_global_mid_range() {
        let base = segment_base(2_000);
        assert_eq!(
            family_row_threshold(&base, CaptureFlushFamily::BookDeltas),
            20_000
        );
    }

    #[test]
    fn book_deltas_respect_higher_operator_cap() {
        let base = segment_base(40_000);
        assert_eq!(
            family_row_threshold(&base, CaptureFlushFamily::BookDeltas),
            40_000
        );
    }

    #[test]
    fn custom_data_is_one_book_summary_poll_not_20k() {
        let base = segment_base(20_000);
        assert_eq!(
            family_row_threshold(&base, CaptureFlushFamily::CustomData),
            1_000
        );
    }

    #[test]
    fn custom_row_group_packs_many_polls_not_one() {
        let base = segment_base(20_000);
        assert_eq!(
            family_row_group_rows(&base, CaptureFlushFamily::CustomData),
            50_000
        );
        assert!(
            family_row_group_rows(&base, CaptureFlushFamily::CustomData)
                > family_row_threshold(&base, CaptureFlushFamily::CustomData)
        );
    }

    #[test]
    fn sparse_marks_do_not_inherit_20k() {
        let base = segment_base(20_000);
        assert_eq!(
            family_row_threshold(&base, CaptureFlushFamily::MarkPrices),
            100
        );
    }

    #[test]
    fn smoke_configs_are_not_inflated() {
        let base = segment_base(50);
        assert_eq!(
            family_row_threshold(&base, CaptureFlushFamily::BookDeltas),
            50
        );
        assert_eq!(
            family_row_threshold(&base, CaptureFlushFamily::CustomData),
            50
        );
        assert_eq!(
            family_row_group_rows(&base, CaptureFlushFamily::CustomData),
            50
        );
    }

    #[test]
    fn capture_config_for_family_decouples_custom_flush_and_row_group() {
        let base = segment_base(2_000);
        let cfg = capture_config_for_family(&base, CaptureFlushFamily::CustomData);
        assert_eq!(cfg.flush_rows, 1_000);
        assert_eq!(cfg.lifecycle.segment.row_group_rows, 50_000);
    }
}
