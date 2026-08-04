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

//! TOML selectors for custom data; type knowledge lives in [`crate::custom_data`].

use anyhow::{bail, Result};
use catalog_capture_core::{
    CustomDataCaptureSpec, CustomDataRequestCaptureSpec, RequestOverlapPolicy,
    DEFAULT_CUSTOM_DATA_REQUEST_INTERVAL_SECS, DEFAULT_CUSTOM_DATA_REQUEST_TIMEOUT_SECS,
    DEFAULT_MAX_AGGREGATE_CUSTOM_DATA_REQUEST_RPS, MIN_CUSTOM_DATA_REQUEST_INTERVAL_SECS,
};
use nautilus_core::Params;
use nautilus_model::data::DataType;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::custom_data::build_request_data_type;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomDataSelector {
    pub type_name: String,
    #[serde(default)]
    pub identifier: Option<String>,
    #[serde(default)]
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomDataRequestSelector {
    pub type_name: String,
    #[serde(default)]
    pub identifier: Option<String>,
    #[serde(default)]
    pub metadata: std::collections::BTreeMap<String, String>,
    /// Poll interval in seconds (min 1; recommended 5 for Deribit book summary).
    #[serde(default = "default_custom_data_request_interval_secs")]
    pub interval_secs: u64,
    #[serde(default = "default_true")]
    pub fire_immediately: bool,
    /// Currently only `skip` is supported.
    #[serde(default = "default_overlap_policy")]
    pub overlap_policy: String,
    #[serde(default = "default_custom_data_request_timeout_secs")]
    pub request_timeout_secs: u64,
    /// Optional ClientId override (defaults by type, e.g. DERIBIT).
    #[serde(default)]
    pub client_id: Option<String>,
}

fn default_custom_data_request_interval_secs() -> u64 {
    DEFAULT_CUSTOM_DATA_REQUEST_INTERVAL_SECS
}

fn default_custom_data_request_timeout_secs() -> u64 {
    DEFAULT_CUSTOM_DATA_REQUEST_TIMEOUT_SECS
}

fn default_overlap_policy() -> String {
    "skip".to_string()
}

fn default_true() -> bool {
    true
}

pub(crate) fn parse_custom_data_specs(
    items: &[CustomDataSelector],
) -> Result<Vec<CustomDataCaptureSpec>> {
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

pub(crate) fn parse_custom_data_request_specs(
    items: &[CustomDataRequestSelector],
) -> Result<Vec<CustomDataRequestCaptureSpec>> {
    let mut specs = Vec::with_capacity(items.len());
    for item in items {
        specs.push(parse_custom_data_request_spec(item)?);
    }
    validate_custom_data_request_aggregate_budget(&specs)?;
    Ok(specs)
}

fn parse_custom_data_request_spec(
    item: &CustomDataRequestSelector,
) -> Result<CustomDataRequestCaptureSpec> {
    if item.type_name.trim().is_empty() {
        bail!("capture.custom_data_requests.type_name must be non-empty");
    }
    if item.interval_secs < MIN_CUSTOM_DATA_REQUEST_INTERVAL_SECS {
        bail!(
            "capture.custom_data_requests.interval_secs must be >= {MIN_CUSTOM_DATA_REQUEST_INTERVAL_SECS} \
             (got {})",
            item.interval_secs
        );
    }
    if item.request_timeout_secs == 0 {
        bail!("capture.custom_data_requests.request_timeout_secs must be > 0");
    }

    let overlap_policy = parse_overlap_policy(&item.overlap_policy)?;
    let (data_type, default_client_id) =
        build_request_data_type(&item.type_name, &item.metadata, item.identifier.as_deref())?;

    let client_id = item
        .client_id
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or(default_client_id);

    Ok(CustomDataRequestCaptureSpec {
        data_type,
        interval_secs: item.interval_secs,
        fire_immediately: item.fire_immediately,
        overlap_policy,
        request_timeout_secs: item.request_timeout_secs,
        client_id,
    })
}

fn parse_overlap_policy(value: &str) -> Result<RequestOverlapPolicy> {
    match value.trim().to_ascii_lowercase().as_str() {
        "skip" => Ok(RequestOverlapPolicy::Skip),
        other => bail!(
            "unsupported capture.custom_data_requests.overlap_policy `{other}`; supported: skip"
        ),
    }
}

fn validate_custom_data_request_aggregate_budget(
    specs: &[CustomDataRequestCaptureSpec],
) -> Result<()> {
    if specs.is_empty() {
        return Ok(());
    }
    let aggregate_rps: f64 = specs
        .iter()
        .map(|spec| 1.0 / spec.interval_secs as f64)
        .sum();
    if aggregate_rps > DEFAULT_MAX_AGGREGATE_CUSTOM_DATA_REQUEST_RPS + f64::EPSILON {
        bail!(
            "capture.custom_data_requests aggregate rate {aggregate_rps:.3} rps exceeds \
             budget {DEFAULT_MAX_AGGREGATE_CUSTOM_DATA_REQUEST_RPS} rps \
             (~10% of Deribit non-matching REST capacity); increase interval_secs or reduce jobs"
        );
    }
    Ok(())
}
