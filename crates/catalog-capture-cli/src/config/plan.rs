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

use std::str::FromStr;

use anyhow::{Context, Result};
use catalog_capture_core::{
    plan::{BarCaptureSpec, BookDeltasCaptureSpec},
    ForwardPriceCaptureSpec, FundingRateCaptureSpec, IndexPriceCaptureSpec, InstrumentCaptureSpec,
    InstrumentCloseCaptureSpec, InstrumentStatusCaptureSpec, MarkPriceCaptureSpec,
    OptionGreeksCaptureSpec, QuoteCaptureSpec, TradeCaptureSpec,
};
use nautilus_model::{data::BarType, enums::BookType, identifiers::InstrumentId};

use super::capture::{BarSelector, BookDeltasSelector, InstrumentSelector};

pub(crate) fn parse_instrument_id(value: &str) -> Result<InstrumentId> {
    InstrumentId::from_str(value).with_context(|| format!("invalid instrument_id {value}"))
}

pub(crate) fn parse_instrument_specs(
    items: &[InstrumentSelector],
) -> Result<Vec<InstrumentCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(InstrumentCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

pub(crate) fn parse_quote_specs(items: &[InstrumentSelector]) -> Result<Vec<QuoteCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(QuoteCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

pub(crate) fn parse_trade_specs(items: &[InstrumentSelector]) -> Result<Vec<TradeCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(TradeCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

pub(crate) fn parse_mark_price_specs(
    items: &[InstrumentSelector],
) -> Result<Vec<MarkPriceCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(MarkPriceCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

pub(crate) fn parse_index_price_specs(
    items: &[InstrumentSelector],
) -> Result<Vec<IndexPriceCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(IndexPriceCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

pub(crate) fn parse_funding_rate_specs(
    items: &[InstrumentSelector],
) -> Result<Vec<FundingRateCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(FundingRateCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

pub(crate) fn parse_instrument_status_specs(
    items: &[InstrumentSelector],
) -> Result<Vec<InstrumentStatusCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(InstrumentStatusCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

pub(crate) fn parse_instrument_close_specs(
    items: &[InstrumentSelector],
) -> Result<Vec<InstrumentCloseCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(InstrumentCloseCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

pub(crate) fn parse_forward_price_specs(
    items: &[InstrumentSelector],
) -> Result<Vec<ForwardPriceCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(ForwardPriceCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

pub(crate) fn parse_option_greeks_specs(
    items: &[InstrumentSelector],
) -> Result<Vec<OptionGreeksCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            Ok(OptionGreeksCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
            })
        })
        .collect()
}

pub(crate) fn parse_bar_specs(items: &[BarSelector]) -> Result<Vec<BarCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            let bar_type = BarType::from_str(&item.bar_type)
                .with_context(|| format!("invalid bar_type {}", item.bar_type))?;
            Ok(BarCaptureSpec { bar_type })
        })
        .collect()
}

pub(crate) fn parse_book_delta_specs(
    items: &[BookDeltasSelector],
) -> Result<Vec<BookDeltasCaptureSpec>> {
    items
        .iter()
        .map(|item| {
            let book_type = BookType::from_str(&item.book_type)
                .with_context(|| format!("invalid book_type {}", item.book_type))?;
            Ok(BookDeltasCaptureSpec {
                instrument_id: parse_instrument_id(&item.instrument_id)?,
                book_type,
            })
        })
        .collect()
}
