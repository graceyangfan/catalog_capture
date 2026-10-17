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

//! Secret ownership for private Predict sessions.

use nautilus_core::string::secret::SecretString;

use crate::http::error::{Error, Result};

/// Opaque bearer token returned by Predict. Debug output is redacted by `SecretString`.
///
/// Local construction does not verify JWT claims or expiry. The venue authenticates requests;
/// session renewal is explicit and never silently retries a private operation.
#[derive(Clone, Debug)]
pub struct PredictAuthToken(SecretString);

impl PredictAuthToken {
    /// Creates an opaque token whose bytes can be sent in a bearer header.
    ///
    /// # Errors
    ///
    /// Returns an error if the token is empty or contains whitespace or non-ASCII bytes.
    pub fn new(token: SecretString) -> Result<Self> {
        validate_header_secret(&token)?;
        Ok(Self(token))
    }

    /// Unverified server-issued timing hint for scheduling only; never grants access locally.
    ///
    /// # Errors
    /// Returns a redacted error for malformed JWT payloads or expiration values.
    pub fn expires_at(&self) -> Result<Option<u64>> {
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
        let parts = self.expose_secret().split('.').collect::<Vec<_>>();
        if parts.len() != 3 {
            return Ok(None);
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(parts[1])
            .map_err(|_| Error::BadRequest("invalid JWT timing payload"))?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|_| Error::BadRequest("invalid JWT timing payload"))?;
        match value.get("exp") {
            None => Ok(None),
            Some(exp) => exp
                .as_u64()
                .map(Some)
                .ok_or(Error::BadRequest("invalid JWT expiration")),
        }
    }

    /// Exposes the JWT for the authenticated HTTP and future wallet WebSocket transports.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

pub(crate) fn validate_header_secret(secret: &SecretString) -> Result<()> {
    let value = secret.expose_secret();
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::BadRequest(
            "credential must be nonempty printable ASCII without spaces",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

    use super::*;
    #[test]
    fn expiry_is_a_timing_hint_and_never_exposes_the_payload_on_failure() {
        assert_eq!(
            PredictAuthToken::new("opaque".into())
                .unwrap()
                .expires_at()
                .unwrap(),
            None
        );
        for (payload, expected) in [
            (r#"{"exp":1234,"other":"private"}"#, Some(1234)),
            (r#"{"other":"private"}"#, None),
        ] {
            let token = PredictAuthToken::new(
                format!("e30.{}.signature", URL_SAFE_NO_PAD.encode(payload)).into(),
            )
            .unwrap();
            assert_eq!(token.expires_at().unwrap(), expected);
        }
        for payload in [
            r#"{"exp":"private"}"#,
            r#"{"exp":-1}"#,
            "private-invalid-json",
        ] {
            let token = PredictAuthToken::new(
                format!("e30.{}.signature", URL_SAFE_NO_PAD.encode(payload)).into(),
            )
            .unwrap();
            let error = token.expires_at().unwrap_err().to_string();
            assert!(!error.contains("private"));
        }
    }
}
