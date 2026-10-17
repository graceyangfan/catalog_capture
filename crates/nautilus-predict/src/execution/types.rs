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

//! Predict wire failures mapped onto the shared Nautilus command outcome vocabulary.

use nautilus_live::execution::failure::CommandFailure;

use crate::http::error::Error;

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
