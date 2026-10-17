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

//! Private envelope dispatch and invariant checks. Raw JWT topics never leave this module.

use alloy::primitives::Address;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::{
    error::{PredictWalletResponseErrorCode, PredictWalletWsError, Result},
    private_messages::{PredictWalletEvent, PredictWalletEventKind, PredictWalletOutcome},
};

pub(super) enum PredictWalletFrame<'a> {
    Response {
        request_id: i64,
        success: bool,
        code: PredictWalletResponseErrorCode,
    },
    Heartbeat(&'a RawValue),
    Wallet(Box<PredictWalletEvent>),
    Ignored,
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(rename = "type", borrow)]
    frame_type: &'a str,
    #[serde(rename = "requestId")]
    request_id: Option<i64>,
    success: Option<bool>,
    error: Option<ResponseError>,
    #[serde(borrow)]
    topic: Option<&'a str>,
    // Borrow directly from JSON; internally tagged serde enums buffer data and cannot
    // deserialize a borrowed RawValue. Preserve null and arbitrary heartbeat values.
    #[serde(borrow, default, deserialize_with = "raw_data")]
    data: Option<&'a RawValue>,
}

fn raw_data<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<&'de RawValue>, D::Error> {
    <&RawValue>::deserialize(deserializer).map(Some)
}

#[derive(Deserialize)]
struct ResponseError {
    code: PredictWalletResponseErrorCode,
}

pub(super) fn parse_frame<'a>(
    text: &'a str,
    topic: &str,
    account: Address,
) -> Result<PredictWalletFrame<'a>> {
    let envelope: Envelope<'a> =
        serde_json::from_str(text).map_err(|_| PredictWalletWsError::Decode)?;
    match envelope.frame_type {
        "R" => Ok(PredictWalletFrame::Response {
            request_id: envelope.request_id.ok_or(PredictWalletWsError::Decode)?,
            success: envelope.success.ok_or(PredictWalletWsError::Decode)?,
            code: envelope
                .error
                .map_or(PredictWalletResponseErrorCode::Unknown, |error| error.code),
        }),
        "M" => {
            let returned = envelope.topic.ok_or(PredictWalletWsError::Decode)?;
            if returned != topic && returned != "heartbeat" {
                return Ok(PredictWalletFrame::Ignored);
            }
            let data = envelope.data.ok_or(PredictWalletWsError::Decode)?;
            if returned == "heartbeat" {
                return Ok(PredictWalletFrame::Heartbeat(data));
            }
            let event: PredictWalletEvent =
                serde_json::from_str(data.get()).map_err(|_| PredictWalletWsError::Decode)?;
            validate_event(&event, account)?;
            Ok(PredictWalletFrame::Wallet(Box::new(event)))
        }
        _ => Ok(PredictWalletFrame::Ignored),
    }
}

fn validate_event(event: &PredictWalletEvent, account: Address) -> Result<()> {
    if event.order.wallet_address != account {
        return Err(PredictWalletWsError::AccountMismatch);
    }
    event.order.timestamp_ns()?;
    let details = &event.order.details;
    let index = match details.outcome {
        PredictWalletOutcome::Yes => 1,
        PredictWalletOutcome::No => 2,
    };
    if event.order.order_id.is_empty()
        || details.outcome_index != index
        || details.quantity_filled > details.quantity
        || details.price > Decimal::ONE
    {
        return Err(PredictWalletWsError::Protocol);
    }
    match &event.kind {
        PredictWalletEventKind::Unsupported => Err(PredictWalletWsError::UnsupportedEvent),
        PredictWalletEventKind::OrderMarketMatchPlan {
            amount,
            amount_filled,
            planned_fill,
            remainder_cancelled,
            settlement_ids,
        } => {
            if details.strategy_type != crate::http::account_models::PredictOrderStrategy::Market
                || amount.is_zero()
                || amount_filled > amount
                || planned_fill > amount
                || remainder_cancelled > amount
                || settlement_ids.iter().any(String::is_empty)
                || (!planned_fill.is_zero() && settlement_ids.is_empty())
            {
                Err(PredictWalletWsError::Protocol)
            } else {
                Ok(())
            }
        }
        PredictWalletEventKind::OrderTransactionSubmitted { settlement_id, .. }
        | PredictWalletEventKind::OrderTransactionSuccess { settlement_id, .. }
        | PredictWalletEventKind::OrderTransactionFailed { settlement_id, .. }
            if settlement_id.is_empty() =>
        {
            Err(PredictWalletWsError::Protocol)
        }
        _ => Ok(()),
    }
}
