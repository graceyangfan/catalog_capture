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

//! Parquet row-group capacity model for long-lived segment parts.
//!
//! Constants here are **not** free-floating. They encode:
//! 1. The arrow-rs / parquet hard limit (`i16` row-group ordinal → 32 767).
//! 2. Cloud-observed Deribit BookSummary load that already hit that limit.
//! 3. Default daily seal (86 400 s) so a fixed part must survive one seal window.
//!
//! ## Cloud failure (observed)
//! Multi-venue mainnet capture with `DeribitBookSummary` @ `interval_secs = 1`:
//! - ~800–1000 rows per poll (BTC option book summary size)
//! - durability tick incorrectly called [`parquet::arrow::ArrowWriter::flush`] every
//!   ~1 s → **≈1 row group / second**
//! - writer error matched parquet exactly:
//!   `Parquet does not support more than 32767 row groups per file (currently: 32768)`
//! - at 1 RG/s the hard limit is reached in **32 767 s ≈ 9.1 h** (inside a 24 h seal day)
//!
//! Even after removing tick-flush, `row_group_rows = 1000` still auto-seals ~0.83 RG/s
//! at 830 rows/s → still exceeds 32 767 inside one day (~11 h). Hence custom uses a
//! **larger** parquet row group than its memory flush.

/// Parquet / arrow-rs hard limit: row-group ordinal is `i16` ([`i16::MAX`] = 32 767).
/// Cloud error text used this number (`currently: 32768` = first illegal ordinal).
pub const PARQUET_MAX_ROW_GROUPS: usize = i16::MAX as usize;

/// Soft seal+reopen threshold for an open `*.parquet.part`.
///
/// Chosen as a round **~91.5 %** of [`PARQUET_MAX_ROW_GROUPS`] so:
/// - headroom **2 767** RGs ≫ any single production flush as RGs
///   (custom flush 1 000 / RG 50 000 → ≤1 RG; book_deltas 50 k / 20 k → ≤3 RGs);
/// - under the **old 1 RG/s bug**, soft roll fires at ~8.3 h and resets the file
///   instead of dying at 9.1 h;
/// - not a free “round 30k”: `MAX - 2767` keeps integer headroom tied to the hard cap.
pub const ROW_GROUP_ROLL_THRESHOLD: usize = PARQUET_MAX_ROW_GROUPS - 2_767;

const _: () = assert!(PARQUET_MAX_ROW_GROUPS == 32_767);
const _: () = assert!(ROW_GROUP_ROLL_THRESHOLD == 30_000);
const _: () = assert!(ROW_GROUP_ROLL_THRESHOLD < PARQUET_MAX_ROW_GROUPS);

// --- Cloud-observed BookSummary load (multi-venue mainnet, 1s poll) ---

/// Mid estimate of rows/s from ~800–1000 rows/poll × 1 poll/s (cloud BookSummary).
pub const CLOUD_BOOK_SUMMARY_ROWS_PER_SEC: u64 = 830;

/// Upper end of one BookSummary poll (memory flush target for custom).
pub const CLOUD_BOOK_SUMMARY_POLL_ROWS: usize = 1_000;

/// Default wall-clock seal interval (UTC day boundary schedule).
pub const DEFAULT_SEAL_INTERVAL_SECS: u64 = 86_400;

/// Custom/BookSummary parquet max rows per row group.
///
/// Minimum to keep **one seal day** under 1/10 of the soft roll cap at cloud rate:
/// `ceil(86400 * 830 * 10 / 30000) = 23_904`. We use **50 000** (~2× min) so:
/// - ~60 s/RG at 830 rows/s → **~1 435 RGs / 24 h** (≪ 30 000);
/// - ~5× rate spike still ~7 k RGs/day;
/// - ~20× spike approaches soft roll (backstop still works).
pub const CUSTOM_ROW_GROUP_ROWS: usize = 50_000;

/// Memory flush for custom: one poll, not the parquet RG size.
pub const CUSTOM_MEMORY_FLUSH_ROWS: usize = CLOUD_BOOK_SUMMARY_POLL_ROWS;

/// Estimated row groups written over `duration_secs` at a constant row rate.
///
/// Assumes row groups fill to `row_group_rows` (no extra tick-flush RGs).
#[must_use]
pub fn estimated_row_groups(rows_per_sec: u64, row_group_rows: usize, duration_secs: u64) -> u64 {
    let rg = row_group_rows.max(1) as u64;
    // ceil(rows / rg) with integer math: (rows + rg - 1) / rg
    let rows = rows_per_sec.saturating_mul(duration_secs);
    rows.saturating_add(rg - 1) / rg
}

/// Seconds until [`PARQUET_MAX_ROW_GROUPS`] at a constant RG creation rate (RG/s).
#[must_use]
pub fn seconds_to_hard_limit(row_groups_per_sec: f64) -> f64 {
    if row_groups_per_sec <= 0.0 {
        return f64::INFINITY;
    }
    PARQUET_MAX_ROW_GROUPS as f64 / row_groups_per_sec
}

/// Minimum `row_group_rows` so a seal window stays ≤ `max_day_row_groups` at rate.
#[must_use]
pub fn min_row_group_rows_for_day(
    rows_per_sec: u64,
    seal_interval_secs: u64,
    max_day_row_groups: u64,
) -> u64 {
    let max_rg = max_day_row_groups.max(1);
    let rows = rows_per_sec.saturating_mul(seal_interval_secs);
    // ceil(rows / max_rg)
    rows.saturating_add(max_rg - 1) / max_rg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_limit_is_parquet_i16_max_matching_cloud_error() {
        assert_eq!(PARQUET_MAX_ROW_GROUPS, 32_767);
        assert_eq!(PARQUET_MAX_ROW_GROUPS, i16::MAX as usize);
        // Cloud log: "... more than 32767 row groups ... (currently: 32768)"
        assert_eq!(PARQUET_MAX_ROW_GROUPS + 1, 32_768);
    }

    #[test]
    fn soft_roll_is_hard_limit_minus_fixed_headroom() {
        assert_eq!(ROW_GROUP_ROLL_THRESHOLD, 30_000);
        assert_eq!(PARQUET_MAX_ROW_GROUPS - ROW_GROUP_ROLL_THRESHOLD, 2_767);
        // Headroom covers ≫ production single-write RG bursts (≤3 for L2 profile).
        assert!(PARQUET_MAX_ROW_GROUPS - ROW_GROUP_ROLL_THRESHOLD > 100);
    }

    #[test]
    fn old_tick_flush_bug_exceeds_hard_limit_inside_one_seal_day() {
        // Observed bug path: ~1 RG finalized per durability second.
        let secs = seconds_to_hard_limit(1.0);
        assert!((secs - 32_767.0).abs() < 0.5);
        assert!(
            secs < DEFAULT_SEAL_INTERVAL_SECS as f64,
            "1 RG/s hits hard limit in {secs}s, inside default seal day"
        );
        // Soft roll would have fired earlier and reset the part.
        let secs_to_soft = ROW_GROUP_ROLL_THRESHOLD as f64 / 1.0;
        assert!(secs_to_soft < secs);
        assert!(secs_to_soft < DEFAULT_SEAL_INTERVAL_SECS as f64);
    }

    #[test]
    fn small_custom_row_group_still_blows_day_without_capacity_roll() {
        // A1-only fix (no tick flush) but RG size = memory flush = 1000:
        // auto-fill ~0.83 RG/s at cloud rate → still > hard limit in one day.
        let day_rgs = estimated_row_groups(
            CLOUD_BOOK_SUMMARY_ROWS_PER_SEC,
            CUSTOM_MEMORY_FLUSH_ROWS,
            DEFAULT_SEAL_INTERVAL_SECS,
        );
        assert!(
            day_rgs > PARQUET_MAX_ROW_GROUPS as u64,
            "day_rgs={day_rgs} must exceed hard max so A2/A3 remain necessary"
        );
    }

    #[test]
    fn custom_row_group_rows_meets_cloud_day_budget_with_slack() {
        // Floor: keep day RGs ≤ roll/10 at cloud rate → min RG size.
        let max_day_rgs = (ROW_GROUP_ROLL_THRESHOLD as u64) / 10;
        let min_rg = min_row_group_rows_for_day(
            CLOUD_BOOK_SUMMARY_ROWS_PER_SEC,
            DEFAULT_SEAL_INTERVAL_SECS,
            max_day_rgs,
        );
        assert!(
            CUSTOM_ROW_GROUP_ROWS as u64 >= min_rg,
            "CUSTOM_ROW_GROUP_ROWS={} < derived minimum {} for 10× slack",
            CUSTOM_ROW_GROUP_ROWS,
            min_rg
        );

        let day_rgs = estimated_row_groups(
            CLOUD_BOOK_SUMMARY_ROWS_PER_SEC,
            CUSTOM_ROW_GROUP_ROWS,
            DEFAULT_SEAL_INTERVAL_SECS,
        );
        // 86400 * 830 / 50000 ≈ 1434.24 → ceil 1435
        assert!(
            (1_400..=1_500).contains(&day_rgs),
            "unexpected day_rgs={day_rgs} for cloud rate + 50k RG"
        );
        assert!(day_rgs * 10 < ROW_GROUP_ROLL_THRESHOLD as u64);
        assert!(day_rgs < PARQUET_MAX_ROW_GROUPS as u64);
    }

    #[test]
    fn custom_profile_survives_rate_spikes_before_soft_roll() {
        // 5× cloud rate still under soft roll for one seal day.
        let day_5x = estimated_row_groups(
            CLOUD_BOOK_SUMMARY_ROWS_PER_SEC * 5,
            CUSTOM_ROW_GROUP_ROWS,
            DEFAULT_SEAL_INTERVAL_SECS,
        );
        assert!(
            day_5x < ROW_GROUP_ROLL_THRESHOLD as u64,
            "5× rate day_rgs={day_5x} should stay under soft roll"
        );

        // ~20× approaches soft roll — capacity roll is the backstop, not a no-op.
        let day_20x = estimated_row_groups(
            CLOUD_BOOK_SUMMARY_ROWS_PER_SEC * 20,
            CUSTOM_ROW_GROUP_ROWS,
            DEFAULT_SEAL_INTERVAL_SECS,
        );
        assert!(
            day_20x > ROW_GROUP_ROLL_THRESHOLD as u64 / 2,
            "20× rate should stress the soft cap so A3 stays meaningful"
        );
    }

    #[test]
    fn soft_roll_headroom_covers_max_production_flush_as_tiny_rgs() {
        // Worst realistic production memory flush (book_deltas profile cap).
        let max_flush_rows = 50_000_u64;
        // Even if misconfigured to 1 row/RG, headroom must exceed one such flush
        // when we are already at the soft threshold (pre-write roll resets first;
        // post-write path still needs headroom if pre-check raced).
        let headroom = (PARQUET_MAX_ROW_GROUPS - ROW_GROUP_ROLL_THRESHOLD) as u64;
        // We do not require headroom ≥ 50k (that would force roll at ~0); we require
        // headroom ≥ max RGs from one custom poll written as size-1 groups, and
        // production L2 flush split by min production RG (20k) → 3 groups.
        assert!(headroom >= CLOUD_BOOK_SUMMARY_POLL_ROWS as u64);
        let l2_rgs_per_flush = (max_flush_rows + 20_000 - 1) / 20_000;
        assert!(headroom >= l2_rgs_per_flush);
    }

    #[test]
    fn memory_flush_stays_one_poll_row_group_packs_many() {
        assert_eq!(CUSTOM_MEMORY_FLUSH_ROWS, 1_000);
        assert!(CUSTOM_ROW_GROUP_ROWS > CUSTOM_MEMORY_FLUSH_ROWS);
        assert_eq!(CUSTOM_ROW_GROUP_ROWS / CUSTOM_MEMORY_FLUSH_ROWS, 50);
    }
}
