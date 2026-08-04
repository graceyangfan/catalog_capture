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

//! Build `DataType` values for request-style custom data from TOML metadata.

use anyhow::{anyhow, bail, Result};
use nautilus_core::Params;
use nautilus_model::data::DataType;
use serde_json::Value as JsonValue;

use super::{supported_request_csv, KnownCustomDataType};

/// Builds a request `DataType` and optional default client id for config parse.
///
/// Returns an error if `type_name` is subscribe-only or unknown.
pub fn build_request_data_type(
    type_name: &str,
    metadata: &std::collections::BTreeMap<String, String>,
    identifier: Option<&str>,
) -> Result<(DataType, Option<String>)> {
    let Some(entry) = KnownCustomDataType::from_type_name(type_name) else {
        bail!(
            "unsupported capture.custom_data_requests.type_name `{type_name}`; \
             supported: {}",
            supported_request_csv()
        );
    };
    if !entry.is_request() {
        bail!(
            "custom_data_requests type_name `{type_name}` is subscribe-only; use [[capture.custom_data]] \
             (Nautilus subscribe_data), not [[capture.custom_data_requests]] (request_data)"
        );
    }

    match entry {
        #[cfg(feature = "venue-deribit")]
        KnownCustomDataType::DeribitBookSummary => build_deribit_book_summary(metadata, identifier),
        #[allow(unreachable_patterns)]
        other => bail!(
            "internal error: {} marked request but has no request builder",
            other.type_name()
        ),
    }
}

#[cfg(feature = "venue-deribit")]
fn build_deribit_book_summary(
    metadata: &std::collections::BTreeMap<String, String>,
    identifier: Option<&str>,
) -> Result<(DataType, Option<String>)> {
    let currency = metadata
        .get("currency")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "capture.custom_data_requests DeribitBookSummary requires metadata.currency \
                 (for example `BTC`)"
            )
        })?
        .to_ascii_uppercase();
    let kind = metadata
        .get("kind")
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or("option")
        .to_ascii_lowercase();
    let expected_id = format!("{currency}:{kind}");
    if let Some(identifier) = identifier {
        let identifier = identifier.trim();
        if !identifier.is_empty() && identifier != expected_id {
            bail!(
                "capture.custom_data_requests DeribitBookSummary identifier `{identifier}` \
                 must match `{expected_id}` (or be omitted)"
            );
        }
    }
    let mut params = Params::new();
    params.insert("currency".to_string(), JsonValue::String(currency));
    params.insert("kind".to_string(), JsonValue::String(kind));
    Ok((
        DataType::new("DeribitBookSummary", Some(params), Some(expected_id)),
        Some("DERIBIT".to_string()),
    ))
}
