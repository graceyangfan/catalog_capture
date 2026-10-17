// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/LICENSE-3.0.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Typed private REST query parameters, serialized by the Nautilus HTTP transport.

use alloy::primitives::{Address, U256};
use serde::Serialize;

use super::error::{Error, Result};

/// The order-list endpoint supports fewer filters than the returned order statuses.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PredictOrderStatusFilter {
    Open,
    Filled,
}

/// A single page of the authenticated wallet's orders.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictOrdersQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<PredictOrderStatusFilter>,
}

/// Venue position sort order.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PredictPositionSort {
    AmountDesc,
    EventBlockAsc,
    EventBlockDesc,
    SharesValueDesc,
    ReturnDesc,
}

/// A single page of the authenticated wallet's ERC-1155 holdings.
/// `include_lost` defaults to the venue default; a filtered page is not a complete account snapshot.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictPositionsQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_resolved: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_lost: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<PredictPositionSort>,
}

/// A single page of match events scoped by the client to its authenticated signer.
/// `is_signer_maker` selects maker/taker participation, or both when omitted.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PredictMatchesQuery<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_id: Option<u64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_optional_decimal"
    )]
    pub min_value_usdt_wei: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_signer_maker: Option<bool>,
}

/// Private wire projection: decimal wei and a mandatory signer filter.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PredictAccountMatchesQuery<'a, 'q> {
    #[serde(flatten)]
    pub query: &'q PredictMatchesQuery<'a>,
    pub signer_address: Address,
}

pub(crate) fn validate_first(first: Option<u32>) -> Result<()> {
    if first.is_some_and(|value| value == 0 || value > i32::MAX as u32) {
        return Err(Error::BadRequest("page size must be a positive int32"));
    }
    Ok(())
}

fn serialize_optional_decimal<S: serde::Serializer>(
    value: &Option<U256>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match value {
        Some(value) => serializer.serialize_str(&value.to_string()),
        None => serializer.serialize_none(),
    }
}
