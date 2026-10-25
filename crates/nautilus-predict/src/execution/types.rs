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

//! Shared worker order views and Nautilus command failure classification.

use std::sync::{Arc, Mutex};

use ahash::AHashMap;
use nautilus_core::UnixNanos;
use nautilus_live::execution::failure::CommandFailure;
use nautilus_model::instruments::InstrumentAny;

use crate::http::{error::Error, market_models::PredictExecutionMarket};

// Worker views of Native orders, not an OMS. Frequent per-order writes must not clone
// the entire retained map. Clone Arc values only when a collector needs a stable snapshot.
pub(super) type OrderSnapshots<K> =
    Arc<Mutex<indexmap::IndexMap<K, Arc<super::context::WorkerOrder>, ahash::RandomState>>>;

// Protocol metadata lives with the existing Native instrument worker view, so purge,
// definition replacement and generation setup release it without another market index.
#[derive(Clone, Debug)]
pub(super) struct ExecutionInstrument {
    pub instrument: InstrumentAny,
    pub market: Arc<Mutex<Option<MarketMetadata>>>,
    // Coalesce only this market's cold/expired GET; never block wallet delivery.
    pub market_loading: Arc<tokio::sync::Mutex<()>>,
}
impl From<InstrumentAny> for ExecutionInstrument {
    fn from(instrument: InstrumentAny) -> Self {
        Self {
            instrument,
            market: Default::default(),
            market_loading: Default::default(),
        }
    }
}
// Both outcome views share one market response. Replacing either definition detaches
// the previous slot for both, so a delayed worker cannot reattach old metadata.
pub(super) fn upsert_instrument(
    map: &mut AHashMap<nautilus_model::identifiers::InstrumentId, ExecutionInstrument>,
    instrument: InstrumentAny,
) {
    use nautilus_model::instruments::Instrument;
    let entry = ExecutionInstrument::from(instrument);
    if let Ok(market_id) = super::parse::market_id_from_instrument(&entry.instrument.id()) {
        for outcome in [1, 2] {
            if let Ok(id) = crate::common::instrument_id_from_outcome(market_id, outcome)
                && let Some(existing) = map.get_mut(&id)
            {
                existing.market = Arc::clone(&entry.market);
                existing.market_loading = Arc::clone(&entry.market_loading);
            }
        }
    }
    map.insert(entry.instrument.id(), entry);
}

#[derive(Clone, Debug)]
pub(super) struct MarketMetadata {
    pub market: Arc<PredictExecutionMarket>,
    pub observed: UnixNanos,
}

pub(super) fn classify_http_command_failure(error: &Error) -> CommandFailure {
    let reason = error.to_string();
    match error {
        Error::BadRequest(_) | Error::Configuration(_) | Error::Signing => {
            CommandFailure::NotSent(reason)
        }
        Error::Authentication { .. }
        | Error::RateLimit { .. }
        | Error::Venue { .. }
        | Error::Http {
            status: 400 | 404 | 410 | 422,
            ..
        } => CommandFailure::VenueRejected(reason),
        Error::Api {
            status: 400 | 404 | 410 | 422,
            ..
        }
        | Error::Api {
            status: 200..=299,
            code: Some(400..=499),
            ..
        } => CommandFailure::VenueRejected(reason),
        // Transport failures carry no proof that the venue did not act. In particular, a
        // server error, redirect, malformed 2xx, mismatched hash or missing ID is ambiguous.
        _ => CommandFailure::Ambiguous(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::error::{PredictDecodeErrorKind, PredictTransportErrorKind};

    #[test]
    fn only_definitive_evidence_can_reject_a_write() {
        assert!(matches!(
            classify_http_command_failure(&Error::BadRequest("invalid price")),
            CommandFailure::NotSent(_)
        ));
        for error in [
            Error::Authentication {
                endpoint: "/v1/orders",
                status: 401,
            },
            Error::RateLimit {
                endpoint: "/v1/orders",
                retry_after_secs: Some(2),
            },
            Error::Venue {
                endpoint: "/v1/orders",
            },
            Error::Http {
                endpoint: "/v1/orders",
                status: 400,
            },
        ] {
            assert!(matches!(
                classify_http_command_failure(&error),
                CommandFailure::VenueRejected(_)
            ));
        }
        for error in [
            Error::Timeout {
                endpoint: "/v1/orders",
            },
            Error::Transport {
                endpoint: "/v1/orders",
                kind: PredictTransportErrorKind::Network,
            },
            Error::Http {
                endpoint: "/v1/orders",
                status: 500,
            },
            Error::Http {
                endpoint: "/v1/orders",
                status: 307,
            },
            Error::Http {
                endpoint: "/v1/orders",
                status: 409,
            },
            Error::decode("/v1/orders", PredictDecodeErrorKind::InvalidAcknowledgement),
            Error::AccountMismatch,
        ] {
            assert!(matches!(
                classify_http_command_failure(&error),
                CommandFailure::Ambiguous(_)
            ));
        }
    }

    #[test]
    fn error_envelopes_never_turn_server_failure_into_definitive_rejection() {
        use crate::http::error::{PredictApiErrorKind, PredictApiErrorReason};
        for (status, rejected) in [(400, true), (422, true), (500, false), (503, false)] {
            let failure = classify_http_command_failure(&Error::Api {
                endpoint: "/v1/orders",
                status,
                code: Some(i64::from(status)),
                kind: PredictApiErrorKind::InternalServerError,
                reason: PredictApiErrorReason::OrderCreationFailed,
            });
            assert_eq!(
                matches!(failure, CommandFailure::VenueRejected(_)),
                rejected
            );
            assert_eq!(matches!(failure, CommandFailure::Ambiguous(_)), !rejected);
        }
    }
}
