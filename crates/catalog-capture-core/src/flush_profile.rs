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

//! Per-family row thresholds for memory → sink flush.
//!
//! A single TOML `flush_rows` / `row_group_rows` cannot serve both Binance L2
//! (~tens of k rows/s) and sparse mark/quotes. Each background runtime clones
//! [`CaptureConfig`] and applies a family profile. Segment mode also interval-flushes
//! into the open `*.parquet.part` so sparse streams and BookSummary do not wait only
//! for row counts.
//!
//! | Family | Target rows | Rationale |
//! |--------|-------------|-----------|
//! | book_deltas | ~20 000 | High volume; amortize encode (profile C) |
//! | trades | ~2 000 | Medium |
//! | quotes | ~500 | Sparse HIP-4 outcomes |
//! | mark / status / … | ~100 | Very sparse |
//! | custom (BookSummary) | ~1 000 | ≈ one poll (~800 rows), not 20k |
//! | instruments | tiny | Chunked defs |

use crate::config::CaptureConfig;

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

/// Resolve the effective row threshold for a family.
#[must_use]
pub fn family_row_threshold(base: &CaptureConfig, family: CaptureFlushFamily) -> usize {
    let configured = base.lifecycle.batch_row_threshold(base.flush_rows).max(1);
    if configured < SMOKE_ROW_CEILING {
        return configured;
    }

    match family {
        // Profile C: L2 — ~20k is reasonable; allow higher operator values up to 50k.
        CaptureFlushFamily::BookDeltas => configured.max(20_000).min(50_000),
        CaptureFlushFamily::Trades => 2_000,
        CaptureFlushFamily::Quotes => 500,
        CaptureFlushFamily::MarkPrices
        | CaptureFlushFamily::IndexPrices
        | CaptureFlushFamily::FundingRates
        | CaptureFlushFamily::InstrumentStatus
        | CaptureFlushFamily::InstrumentClose => 100,
        CaptureFlushFamily::OptionGreeks => 2_000,
        CaptureFlushFamily::Bars => 500,
        // One Deribit BookSummary poll is typically ~800–1000 rows.
        CaptureFlushFamily::CustomData => 1_000,
        CaptureFlushFamily::Instruments => 50,
    }
}

/// Clone config with family-specific row thresholds applied.
#[must_use]
pub fn capture_config_for_family(base: &CaptureConfig, family: CaptureFlushFamily) -> CaptureConfig {
    let rows = family_row_threshold(base, family);
    let mut config = base.clone();
    config.flush_rows = rows;
    if config.lifecycle.is_segment_mode() {
        config.lifecycle.segment.row_group_rows = rows;
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
    }

    #[test]
    fn capture_config_for_family_writes_segment_row_group() {
        let base = segment_base(2_000);
        let cfg = capture_config_for_family(&base, CaptureFlushFamily::CustomData);
        assert_eq!(cfg.lifecycle.segment.row_group_rows, 1_000);
        assert_eq!(cfg.flush_rows, 1_000);
    }
}
