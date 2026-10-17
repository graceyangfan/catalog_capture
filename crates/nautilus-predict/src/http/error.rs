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

//! Typed errors for private requests. Venue bodies and transport details can echo credentials,
//! so errors retain endpoint/status classification without retaining those raw strings.

use nautilus_network::http::{HttpClientError, HttpResponse};
use serde::{Deserialize, de::DeserializeOwned};
use thiserror::Error;

use super::{account_models::PredictPage, execution_models::PredictPrivateResponse};

/// Result of a private API or wallet-signing operation.
pub type Result<T> = std::result::Result<T, Error>;

/// Recoverable private API failures with safe diagnostic metadata.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid configuration: {0}")]
    Configuration(&'static str),
    #[error("invalid request: {0}")]
    BadRequest(&'static str),
    #[error("invalid report conversion: {0}")]
    Report(&'static str),
    #[error("wallet signing failed")]
    Signing,
    #[error("HTTP {kind:?} error at {endpoint}")]
    Transport {
        endpoint: &'static str,
        kind: PredictTransportErrorKind,
    },
    #[error("HTTP request timed out at {endpoint}")]
    Timeout { endpoint: &'static str },
    #[error("authentication rejected at {endpoint}: HTTP {status}")]
    Authentication { endpoint: &'static str, status: u16 },
    #[error("rate limited at {endpoint}, retry after seconds: {retry_after_secs:?}")]
    RateLimit {
        endpoint: &'static str,
        retry_after_secs: Option<u64>,
    },
    #[error("HTTP {status} at {endpoint}")]
    Http { endpoint: &'static str, status: u16 },
    #[error("HTTP {status} at {endpoint}: {kind:?}, API code {code:?}, reason {reason:?}")]
    Api {
        endpoint: &'static str,
        status: u16,
        code: Option<i64>,
        kind: PredictApiErrorKind,
        reason: PredictApiErrorReason,
    },
    #[error(
        "invalid response at {endpoint}: {kind:?}, HTTP status {status:?}, line {line}, column {column}"
    )]
    Decode {
        endpoint: &'static str,
        kind: PredictDecodeErrorKind,
        /// Known response status, absent only for locally constructed decoding errors.
        status: Option<u16>,
        line: usize,
        column: usize,
    },
    #[error("venue reported success=false at {endpoint}")]
    Venue { endpoint: &'static str },
    #[error("returned account differs from the authenticated wallet")]
    AccountMismatch,
}

/// Recognized machine error names; unknown wire text is never retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredictApiErrorKind {
    Unauthorized,
    Forbidden,
    InvalidRequest,
    NotFound,
    Conflict,
    InternalServerError,
    Unknown,
}

/// Safe business diagnostics projected from known venue messages, never raw text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredictApiErrorReason {
    OrderCreationFailed,
    InvalidSignature,
    InsufficientBalance,
    InsufficientAllowance,
    InvalidNonce,
    InvalidSalt,
    MinimumOrderValue,
    PostOnlyWouldCross,
    MarketClosed,
    ExpiryTooSoon,
    Unknown,
}

fn decode_api_error(endpoint: &'static str, response: &HttpResponse) -> Option<Error> {
    #[derive(Deserialize)]
    struct Envelope<'a> {
        success: bool,
        code: Option<i64>,
        error: &'a str,
        message: Option<&'a str>,
    }
    // Error text may echo secrets. Only fixed enum values and the numeric code escape.
    if response.body.len() > 65_536 {
        return None;
    }
    let wire: Envelope<'_> = serde_json::from_slice(&response.body).ok()?;
    if wire.success {
        return None;
    }
    let kind = match wire.error {
        "unauthorized" => PredictApiErrorKind::Unauthorized,
        "forbidden" => PredictApiErrorKind::Forbidden,
        "bad_request" | "invalid_request" | "validation_error" | "create_order_expiry_too_soon" => {
            PredictApiErrorKind::InvalidRequest
        }
        "not_found" => PredictApiErrorKind::NotFound,
        "conflict" => PredictApiErrorKind::Conflict,
        "internal_server_error" => PredictApiErrorKind::InternalServerError,
        _ => PredictApiErrorKind::Unknown,
    };
    let reason = if wire.error == "create_order_expiry_too_soon" {
        PredictApiErrorReason::ExpiryTooSoon
    } else {
        match wire
            .message
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "failed to create order" => PredictApiErrorReason::OrderCreationFailed,
            "invalid signature" => PredictApiErrorReason::InvalidSignature,
            "insufficient balance" => PredictApiErrorReason::InsufficientBalance,
            "insufficient allowance" => PredictApiErrorReason::InsufficientAllowance,
            "invalid nonce" => PredictApiErrorReason::InvalidNonce,
            "invalid salt" => PredictApiErrorReason::InvalidSalt,
            "order value below minimum" => PredictApiErrorReason::MinimumOrderValue,
            "post-only order would cross" => PredictApiErrorReason::PostOnlyWouldCross,
            "market is not open" => PredictApiErrorReason::MarketClosed,
            _ => PredictApiErrorReason::Unknown,
        }
    };
    Some(Error::Api {
        endpoint,
        status: response.status.as_u16(),
        code: wire.code,
        kind,
        reason,
    })
}

impl Error {
    pub(crate) fn from_http_client(endpoint: &'static str, e: HttpClientError) -> Self {
        let kind = match e {
            HttpClientError::TimeoutError(_) => return Self::Timeout { endpoint },
            HttpClientError::TransportError(_) => PredictTransportErrorKind::Network,
            HttpClientError::InvalidProxy(_) => PredictTransportErrorKind::InvalidProxy,
            HttpClientError::ClientBuildError(_) => PredictTransportErrorKind::ClientBuild,
            HttpClientError::Error(_) => PredictTransportErrorKind::HttpClient,
        };
        Self::Transport { endpoint, kind }
    }

    pub(crate) fn decode(endpoint: &'static str, kind: PredictDecodeErrorKind) -> Self {
        Self::Decode {
            endpoint,
            kind,
            status: None,
            line: 0,
            column: 0,
        }
    }

    pub(crate) fn with_response_status(mut self, status: u16) -> Self {
        if let Self::Decode {
            status: response_status,
            ..
        } = &mut self
        {
            *response_status = Some(status);
        }
        self
    }

    pub(crate) fn from_json(endpoint: &'static str, e: &serde_json::Error) -> Self {
        let kind = match e.classify() {
            serde_json::error::Category::Io => PredictDecodeErrorKind::Io,
            serde_json::error::Category::Syntax => PredictDecodeErrorKind::Syntax,
            serde_json::error::Category::Data => PredictDecodeErrorKind::Data,
            serde_json::error::Category::Eof => PredictDecodeErrorKind::Eof,
        };
        Self::Decode {
            endpoint,
            kind,
            status: None,
            line: e.line(),
            column: e.column(),
        }
    }
}

/// Transport classification supplied by Nautilus, without retaining raw error strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredictTransportErrorKind {
    /// Connection, DNS, or TLS failure reported by the transport.
    Network,
    /// Invalid proxy configuration.
    InvalidProxy,
    /// HTTP client initialization failed.
    ClientBuild,
    /// Other HTTP client failure.
    HttpClient,
}

/// Safe parser diagnostics. Semantic failures have line/column zero; JSON failures locate
/// the JSON value being decoded, which may be the nested `data` value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PredictDecodeErrorKind {
    /// Malformed JSON syntax.
    Syntax,
    /// Value does not match the response schema.
    Data,
    /// Truncated JSON input.
    Eof,
    /// JSON reader failure.
    Io,
    /// Success envelope has no usable data value.
    MissingData,
    /// A page omitted its required cursor, so completion is unknown.
    MissingCursor,
    /// Single-order lookup returned a different hash.
    InvalidOrderHash,
    /// A write acknowledgement does not identify the requested operation completely.
    InvalidAcknowledgement,
    /// Empty authentication challenge.
    InvalidChallenge,
    /// Token cannot be sent as a bearer header.
    InvalidToken,
}

pub(crate) fn decode_response<T: DeserializeOwned>(
    endpoint: &'static str,
    response: &HttpResponse,
) -> Result<T> {
    let result = (|| {
        let envelope = decode_envelope(endpoint, response)?;
        decode_data(endpoint, &envelope)
    })();
    result.map_err(|error: Error| error.with_response_status(response.status.as_u16()))
}

pub(crate) fn decode_page<T: DeserializeOwned>(
    endpoint: &'static str,
    response: &HttpResponse,
    require_cursor: bool,
) -> Result<PredictPage<T>> {
    let result = (|| {
        let envelope = decode_envelope(endpoint, response)?;
        let cursor = match envelope.cursor {
            Some(cursor) => {
                serde_json::from_str(cursor.get()).map_err(|e| Error::from_json(endpoint, &e))?
            }
            None if require_cursor => {
                return Err(Error::decode(
                    endpoint,
                    PredictDecodeErrorKind::MissingCursor,
                ));
            }
            None => None,
        };
        Ok(PredictPage {
            data: decode_data(endpoint, &envelope)?,
            cursor,
        })
    })();
    result.map_err(|error: Error| error.with_response_status(response.status.as_u16()))
}

fn decode_envelope<'a>(
    endpoint: &'static str,
    response: &'a HttpResponse,
) -> Result<PredictPrivateResponse<'a>> {
    check_response_status(endpoint, response)?;
    // Borrow data until success is known: failed payloads need not match the success schema.
    // Only the envelope is decoded here; the selected data schema is decoded once below.
    let envelope: PredictPrivateResponse<'_> =
        serde_json::from_slice(&response.body).map_err(|e| Error::from_json(endpoint, &e))?;
    if !envelope.success {
        return Err(decode_api_error(endpoint, response).unwrap_or(Error::Venue { endpoint }));
    }
    Ok(envelope)
}

pub(crate) fn check_response_status(endpoint: &'static str, response: &HttpResponse) -> Result<()> {
    let status = response.status.as_u16();
    match status {
        401 | 403 => return Err(Error::Authentication { endpoint, status }),
        429 => {
            let retry_after_secs = response
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
                .and_then(|(_, value)| value.parse().ok());
            return Err(Error::RateLimit {
                endpoint,
                retry_after_secs,
            });
        }
        200..=299 => {}
        _ => {
            return Err(
                decode_api_error(endpoint, response).unwrap_or(Error::Http { endpoint, status })
            );
        }
    }
    Ok(())
}

fn decode_data<T: DeserializeOwned>(
    endpoint: &'static str,
    envelope: &PredictPrivateResponse<'_>,
) -> Result<T> {
    let data = envelope
        .data
        .ok_or_else(|| Error::decode(endpoint, PredictDecodeErrorKind::MissingData))?;
    serde_json::from_str(data.get()).map_err(|e| Error::from_json(endpoint, &e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_diagnostics_classify_without_retaining_echoed_secrets() {
        for (e, expected) in [
            (
                HttpClientError::TransportError("api-key jwt-secret signature-secret".into()),
                PredictTransportErrorKind::Network,
            ),
            (
                HttpClientError::InvalidProxy("api-key jwt-secret signature-secret".into()),
                PredictTransportErrorKind::InvalidProxy,
            ),
            (
                HttpClientError::ClientBuildError("api-key jwt-secret signature-secret".into()),
                PredictTransportErrorKind::ClientBuild,
            ),
            (
                HttpClientError::Error("api-key jwt-secret signature-secret".into()),
                PredictTransportErrorKind::HttpClient,
            ),
        ] {
            let e = Error::from_http_client("/v1/account", e);
            assert!(
                matches!(e, Error::Transport { endpoint: "/v1/account", kind } if kind == expected)
            );
            let diagnostic = format!("{e:?} {e}");
            for secret in ["api-key", "jwt-secret", "signature-secret"] {
                assert!(!diagnostic.contains(secret));
            }
        }
        let e = Error::from_http_client(
            "/v1/auth",
            HttpClientError::TimeoutError("jwt-secret".into()),
        );
        assert!(matches!(
            e,
            Error::Timeout {
                endpoint: "/v1/auth"
            }
        ));
        assert!(!format!("{e:?} {e}").contains("jwt-secret"));
    }
}
