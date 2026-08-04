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

//! Optional venue credentials from environment variables (Track O8).
//!
//! Default capture is **public market data** (no keys). When set, credentials are
//! read only from the process environment / `.env` (via dotenvy in `main`) — never
//! from TOML configs.

use std::env;

/// Generic API key + secret pair used by most REST/WS venues.
#[derive(Debug, Clone, Default)]
pub struct ApiKeySecret {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
}

/// OKX also requires a passphrase.
#[derive(Debug, Clone, Default)]
pub struct OkxCredentials {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub api_passphrase: Option<String>,
}

fn first_non_empty(names: &[&str]) -> Option<String> {
    for name in names {
        if let Ok(value) = env::var(name) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

/// Prefer venue-id scoped vars, then venue-wide defaults.
///
/// Examples for venue id `deribit_main`:
/// - `CAPTURE_VENUE_DERIBIT_MAIN_API_KEY`
/// - else `DERIBIT_API_KEY`
fn scoped_or_global(venue_id: &str, kind: &str, suffix: &str) -> Option<String> {
    let id_upper = venue_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    let kind_upper = kind.to_ascii_uppercase().replace('-', "_");
    let scoped = format!("CAPTURE_VENUE_{id_upper}_{suffix}");
    let global = format!("{kind_upper}_{suffix}");
    first_non_empty(&[&scoped, &global])
}

pub fn binance_credentials(venue_id: &str) -> ApiKeySecret {
    ApiKeySecret {
        api_key: scoped_or_global(venue_id, "BINANCE", "API_KEY"),
        api_secret: scoped_or_global(venue_id, "BINANCE", "API_SECRET"),
    }
}

pub fn deribit_credentials(venue_id: &str) -> ApiKeySecret {
    ApiKeySecret {
        api_key: scoped_or_global(venue_id, "DERIBIT", "API_KEY"),
        api_secret: scoped_or_global(venue_id, "DERIBIT", "API_SECRET"),
    }
}

pub fn bybit_credentials(venue_id: &str) -> ApiKeySecret {
    ApiKeySecret {
        api_key: scoped_or_global(venue_id, "BYBIT", "API_KEY"),
        api_secret: scoped_or_global(venue_id, "BYBIT", "API_SECRET"),
    }
}

pub fn okx_credentials(venue_id: &str) -> OkxCredentials {
    OkxCredentials {
        api_key: scoped_or_global(venue_id, "OKX", "API_KEY"),
        api_secret: scoped_or_global(venue_id, "OKX", "API_SECRET"),
        api_passphrase: scoped_or_global(venue_id, "OKX", "API_PASSPHRASE")
            .or_else(|| scoped_or_global(venue_id, "OKX", "PASSPHRASE")),
    }
}

pub fn hyperliquid_private_key(venue_id: &str) -> Option<String> {
    scoped_or_global(venue_id, "HYPERLIQUID", "PRIVATE_KEY")
        .or_else(|| first_non_empty(&["HYPERLIQUID_PRIVATE_KEY", "HL_PRIVATE_KEY"]))
}

/// True if any credential field is present (for logging without leaking secrets).
pub fn api_key_secret_present(creds: &ApiKeySecret) -> bool {
    creds.api_key.is_some() || creds.api_secret.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn scoped_env_beats_global() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        env::set_var("DERIBIT_API_KEY", "global-key");
        env::set_var("CAPTURE_VENUE_DERIBIT_MAIN_API_KEY", "scoped-key");
        let creds = deribit_credentials("deribit_main");
        assert_eq!(creds.api_key.as_deref(), Some("scoped-key"));
        env::remove_var("DERIBIT_API_KEY");
        env::remove_var("CAPTURE_VENUE_DERIBIT_MAIN_API_KEY");
    }

    #[test]
    fn missing_env_yields_none() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        env::remove_var("BINANCE_API_KEY");
        env::remove_var("BINANCE_API_SECRET");
        env::remove_var("CAPTURE_VENUE_BINANCE_MAIN_API_KEY");
        env::remove_var("CAPTURE_VENUE_BINANCE_MAIN_API_SECRET");
        let creds = binance_credentials("binance_main");
        assert!(creds.api_key.is_none());
        assert!(creds.api_secret.is_none());
    }
}
