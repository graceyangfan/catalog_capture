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

//! Request-style custom data poll jobs.
//!
//! This path is deliberately separate from subscribe-style `[[capture.custom_data]]`:
//!
//! | Mode | Nautilus API | Actor callback |
//! |------|--------------|----------------|
//! | subscribe | `subscribe_data` | `on_data` |
//! | request | `request_data` | `handle_data_response` / `on_historical_data` |
//!
//! Capture only schedules timers and calls `request_data`. Venue HTTP, retries,
//! and rate limits stay in the Nautilus adapter client.

use catalog_capture_core::{
    CustomDataRequestCaptureSpec, RequestOverlapPolicy, DEFAULT_CUSTOM_DATA_REQUEST_TIMEOUT_SECS,
};
use nautilus_model::data::DataType;

pub const CUSTOM_DATA_REQUEST_TIMER_PREFIX: &str = "CUSTOM_DATA_REQUEST:";

#[derive(Debug, Clone)]
pub struct CustomDataRequestJob {
    pub index: usize,
    pub spec: CustomDataRequestCaptureSpec,
    pub in_flight: bool,
    pub last_fire_ns: u64,
    pub polls: u64,
    pub rows: u64,
    pub skipped_inflight: u64,
    pub timeouts: u64,
}

impl CustomDataRequestJob {
    #[must_use]
    pub fn new(index: usize, spec: CustomDataRequestCaptureSpec) -> Self {
        Self {
            index,
            spec,
            in_flight: false,
            last_fire_ns: 0,
            polls: 0,
            rows: 0,
            skipped_inflight: 0,
            timeouts: 0,
        }
    }

    #[must_use]
    pub fn timer_name(&self) -> String {
        format!("{CUSTOM_DATA_REQUEST_TIMER_PREFIX}{}", self.index)
    }

    #[must_use]
    pub fn data_type(&self) -> &DataType {
        &self.spec.data_type
    }

    #[must_use]
    pub fn client_id_str(&self) -> &str {
        self.spec.client_id.as_deref().unwrap_or("DERIBIT")
    }

    #[must_use]
    pub fn interval_ns(&self) -> u64 {
        self.spec.interval_secs.saturating_mul(1_000_000_000)
    }

    #[must_use]
    pub fn request_timeout_ns(&self) -> u64 {
        let secs = if self.spec.request_timeout_secs == 0 {
            DEFAULT_CUSTOM_DATA_REQUEST_TIMEOUT_SECS
        } else {
            self.spec.request_timeout_secs
        };
        secs.saturating_mul(1_000_000_000)
    }

    /// Returns true if this tick should fire a new `request_data`.
    pub fn prepare_fire(&mut self, now_ns: u64) -> bool {
        if self.in_flight {
            if now_ns.saturating_sub(self.last_fire_ns) >= self.request_timeout_ns() {
                self.in_flight = false;
                self.timeouts = self.timeouts.saturating_add(1);
                log::warn!(
                    "custom_data_request timeout type={} id={:?} after {}s; allowing re-fire",
                    self.spec.data_type.type_name(),
                    self.spec.data_type.identifier(),
                    self.spec.request_timeout_secs
                );
            } else {
                match self.spec.overlap_policy {
                    RequestOverlapPolicy::Skip => {
                        self.skipped_inflight = self.skipped_inflight.saturating_add(1);
                        return false;
                    }
                }
            }
        }

        self.in_flight = true;
        self.last_fire_ns = now_ns;
        self.polls = self.polls.saturating_add(1);
        true
    }

    pub fn complete_response(&mut self, rows: u64) {
        self.in_flight = false;
        self.rows = self.rows.saturating_add(rows);
    }

    #[must_use]
    pub fn matches_data_type(&self, data_type: &DataType) -> bool {
        self.spec.data_type.type_name() == data_type.type_name()
            && self.spec.data_type.identifier() == data_type.identifier()
    }
}

#[must_use]
pub fn parse_request_timer_index(timer_name: &str) -> Option<usize> {
    timer_name
        .strip_prefix(CUSTOM_DATA_REQUEST_TIMER_PREFIX)?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_model::data::DataType;

    fn sample_job() -> CustomDataRequestJob {
        CustomDataRequestJob::new(
            0,
            CustomDataRequestCaptureSpec {
                data_type: DataType::new(
                    "DeribitBookSummary",
                    None,
                    Some("BTC:option".to_string()),
                ),
                interval_secs: 5,
                fire_immediately: true,
                overlap_policy: RequestOverlapPolicy::Skip,
                request_timeout_secs: 10,
                client_id: Some("DERIBIT".to_string()),
            },
        )
    }

    #[test]
    fn skip_overlap_blocks_second_fire_until_complete_or_timeout() {
        let mut job = sample_job();
        assert!(job.prepare_fire(1_000));
        assert!(!job.prepare_fire(2_000));
        assert_eq!(job.skipped_inflight, 1);
        job.complete_response(10);
        assert!(job.prepare_fire(3_000));
    }

    #[test]
    fn timeout_clears_inflight() {
        let mut job = sample_job();
        assert!(job.prepare_fire(0));
        // 10s timeout
        assert!(job.prepare_fire(10_000_000_000));
        assert_eq!(job.timeouts, 1);
    }

    #[test]
    fn timer_name_roundtrip() {
        let job = sample_job();
        assert_eq!(parse_request_timer_index(&job.timer_name()), Some(0));
        assert_eq!(parse_request_timer_index("OPTION_UNIVERSE_REFRESH"), None);
    }
}
