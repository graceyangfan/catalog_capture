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

//! Operator-facing advisories (warnings that do not fail validation).

use crate::{config::CaptureConfig, plan::CapturePlan};

/// Multi-line advisory when custom data runs under **chunked** lifecycle.
///
/// Production default is `mode = "segment"`. Chunked is opt-in for short smoke only.
pub const CHUNKED_CUSTOM_DATA_ADVISORY: &str = "\
chunked custom data is for smoke / short validation only — not production capture.\n\
  Custom streams (subscribe or request, e.g. DeribitBookSummary) with \
output.lifecycle.mode = \"chunked\" write a new catalog parquet on each flush \
and can explode file counts under 1s polling.\n\
  Production default is segment (append *.parquet.part + seal). To keep using chunked \
intentionally for smoke, leave mode = \"chunked\"; otherwise remove the override or set:\n\
    [output.lifecycle]\n\
    mode = \"segment\"\n\
    # seal defaults: enabled, 06:00 UTC daily — see docs/concepts/segment_lifecycle.md";

/// Returns advisories for a capture config + plan (non-fatal).
#[must_use]
pub fn capture_advisories(config: &CaptureConfig, plan: &CapturePlan) -> Vec<String> {
    let mut out = Vec::new();
    if !config.lifecycle.is_segment_mode() && plan.family_runtime_flags().needs_custom_data_writer()
    {
        out.push(CHUNKED_CUSTOM_DATA_ADVISORY.to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        lifecycle::{LifecycleConfig, LifecycleMode},
        plan::CustomDataRequestCaptureSpec,
    };

    #[test]
    fn no_advisory_when_segment_and_custom() {
        let config = CaptureConfig {
            lifecycle: LifecycleConfig {
                mode: LifecycleMode::Segment,
                ..LifecycleConfig::default()
            },
            ..CaptureConfig::default()
        };
        let plan = CapturePlan {
            custom_data_requests: vec![sample_book_summary_request()],
            ..CapturePlan::default()
        };
        assert!(capture_advisories(&config, &plan).is_empty());
    }

    fn sample_book_summary_request() -> CustomDataRequestCaptureSpec {
        CustomDataRequestCaptureSpec {
            data_type: nautilus_model::data::DataType::new("DeribitBookSummary", None, None),
            interval_secs: 1,
            fire_immediately: true,
            overlap_policy: crate::plan::RequestOverlapPolicy::Skip,
            request_timeout_secs: 5,
            client_id: None,
        }
    }

    #[test]
    fn advisory_when_chunked_and_custom_request() {
        let config = CaptureConfig {
            lifecycle: LifecycleConfig {
                mode: LifecycleMode::Chunked,
                ..LifecycleConfig::default()
            },
            ..CaptureConfig::default()
        };
        let plan = CapturePlan {
            custom_data_requests: vec![sample_book_summary_request()],
            ..CapturePlan::default()
        };
        let advisories = capture_advisories(&config, &plan);
        assert_eq!(advisories.len(), 1);
        assert!(advisories[0].contains("smoke"));
        assert!(advisories[0].contains("segment"));
    }

    #[test]
    fn no_advisory_on_production_defaults_with_custom() {
        // Default lifecycle is segment — no smoke advisory.
        let config = CaptureConfig::default();
        let plan = CapturePlan {
            custom_data_requests: vec![sample_book_summary_request()],
            ..CapturePlan::default()
        };
        assert!(capture_advisories(&config, &plan).is_empty());
    }

    #[test]
    fn no_advisory_when_chunked_but_no_custom() {
        let config = CaptureConfig {
            lifecycle: LifecycleConfig {
                mode: LifecycleMode::Chunked,
                ..LifecycleConfig::default()
            },
            ..CaptureConfig::default()
        };
        let plan = CapturePlan::default();
        assert!(capture_advisories(&config, &plan).is_empty());
    }
}
