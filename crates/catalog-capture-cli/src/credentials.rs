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

//! Venue credentials for the capture CLI — **public market data by default**.
//!
//! Most capture traffic is public. API keys are optional and **opt-in**:
//!
//! - Default: pass `None` into data clients and scrub known credential env vars so
//!   Nautilus adapters cannot pick up placeholder / fake keys from the environment
//!   (`get_or_env_var_opt` falls back to env when config is `None`).
//! - Authenticated mode: set `CAPTURE_USE_VENUE_CREDENTIALS=1` **and** provide a
//!   complete non-placeholder key pair (never store secrets in TOML).

use std::env;
use std::sync::atomic::{AtomicBool, Ordering};

/// Generic API key + secret pair used by most REST/WS venues.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApiKeySecret {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
}

/// OKX also requires a passphrase.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OkxCredentials {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub api_passphrase: Option<String>,
}

/// Env var that enables injecting real venue credentials into data clients.
pub const USE_VENUE_CREDENTIALS_ENV: &str = "CAPTURE_USE_VENUE_CREDENTIALS";

/// Credential-related env vars that Nautilus adapters may also read.
const VENUE_CREDENTIAL_ENV_VARS: &[&str] = &[
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "BINANCE_TESTNET_API_KEY",
    "BINANCE_TESTNET_API_SECRET",
    "DERIBIT_API_KEY",
    "DERIBIT_API_SECRET",
    "DERIBIT_CLIENT_ID",
    "DERIBIT_CLIENT_SECRET",
    "BYBIT_API_KEY",
    "BYBIT_API_SECRET",
    "OKX_API_KEY",
    "OKX_API_SECRET",
    "OKX_API_PASSPHRASE",
    "OKX_PASSPHRASE",
    "HYPERLIQUID_PRIVATE_KEY",
    "HL_PRIVATE_KEY",
];

static PUBLIC_MODE_LOGGED: AtomicBool = AtomicBool::new(false);

/// `true` only when the operator explicitly opts into authenticated clients.
#[must_use]
pub fn venue_credentials_enabled() -> bool {
    matches!(
        env::var(USE_VENUE_CREDENTIALS_ENV)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Prepare process env for **public** capture so fake/placeholder keys cannot
/// leak into Nautilus adapter clients via env fallback.
///
/// Call once before building data clients when credentials are not enabled.
pub fn prepare_public_capture_env() {
    if venue_credentials_enabled() {
        return;
    }

    let mut cleared = Vec::new();
    for name in VENUE_CREDENTIAL_ENV_VARS {
        if env::var_os(name).is_some() {
            env::remove_var(name);
            cleared.push(*name);
        }
    }
    // Also clear scoped CAPTURE_VENUE_* keys so we never inject them accidentally.
    let scoped: Vec<String> = env::vars()
        .map(|(k, _)| k)
        .filter(|k| k.starts_with("CAPTURE_VENUE_") && is_credential_suffix(k))
        .collect();
    for name in &scoped {
        env::remove_var(name);
        cleared.push(name.as_str());
    }

    if !cleared.is_empty()
        && PUBLIC_MODE_LOGGED
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    {
        log::info!(
            "public capture mode: ignoring venue credential env vars ({}); \
             set {USE_VENUE_CREDENTIALS_ENV}=1 to use real keys",
            cleared.join(", ")
        );
    }
}

fn is_credential_suffix(name: &str) -> bool {
    name.ends_with("_API_KEY")
        || name.ends_with("_API_SECRET")
        || name.ends_with("_API_PASSPHRASE")
        || name.ends_with("_PASSPHRASE")
        || name.ends_with("_PRIVATE_KEY")
        || name.ends_with("_CLIENT_ID")
        || name.ends_with("_CLIENT_SECRET")
}

/// Values that must never be treated as real credentials.
#[must_use]
pub fn is_placeholder_credential(value: &str) -> bool {
    let v = value.trim();
    if v.is_empty() {
        return true;
    }
    let lower = v.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "none"
            | "null"
            | "nil"
            | "n/a"
            | "na"
            | "undefined"
            | "false"
            | "0"
            | "test"
            | "testing"
            | "dummy"
            | "fake"
            | "placeholder"
            | "changeme"
            | "password"
            | "secret"
            | "xxx"
            | "xxxx"
            | "your_api_key"
            | "your_api_secret"
            | "your-api-key"
            | "your-api-secret"
            | "<api_key>"
            | "<api_secret>"
            | "apikey"
            | "apisecret"
    ) || lower.starts_with("your_")
        || lower.starts_with("your-")
        || lower.starts_with("xxx")
        || lower.contains("replace_me")
        || lower.contains("placeholder")
}

fn sanitize_opt(value: Option<String>) -> Option<String> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        if is_placeholder_credential(trimmed) {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
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
    sanitize_opt(first_non_empty(&[&scoped, &global]))
}

/// Resolve key/secret for injection. Incomplete or placeholder pairs become empty.
fn resolve_key_secret(venue_id: &str, kind: &str) -> ApiKeySecret {
    if !venue_credentials_enabled() {
        return ApiKeySecret::default();
    }
    let api_key = scoped_or_global(venue_id, kind, "API_KEY");
    let api_secret = scoped_or_global(venue_id, kind, "API_SECRET");
    match (api_key, api_secret) {
        (Some(k), Some(s)) => ApiKeySecret {
            api_key: Some(k),
            api_secret: Some(s),
        },
        (k, s) => {
            if k.is_some() || s.is_some() {
                log::warn!(
                    "incomplete {kind} credentials for venue_id=`{venue_id}` \
                     (need both API_KEY and API_SECRET); using public client"
                );
            }
            ApiKeySecret::default()
        }
    }
}

pub fn binance_credentials(venue_id: &str) -> ApiKeySecret {
    resolve_key_secret(venue_id, "BINANCE")
}

pub fn deribit_credentials(venue_id: &str) -> ApiKeySecret {
    // Deribit also accepts CLIENT_ID / CLIENT_SECRET aliases in some setups.
    if !venue_credentials_enabled() {
        return ApiKeySecret::default();
    }
    let api_key = scoped_or_global(venue_id, "DERIBIT", "API_KEY")
        .or_else(|| scoped_or_global(venue_id, "DERIBIT", "CLIENT_ID"));
    let api_secret = scoped_or_global(venue_id, "DERIBIT", "API_SECRET")
        .or_else(|| scoped_or_global(venue_id, "DERIBIT", "CLIENT_SECRET"));
    match (api_key, api_secret) {
        (Some(k), Some(s)) => ApiKeySecret {
            api_key: Some(k),
            api_secret: Some(s),
        },
        (k, s) => {
            if k.is_some() || s.is_some() {
                log::warn!(
                    "incomplete DERIBIT credentials for venue_id=`{venue_id}`; using public client"
                );
            }
            ApiKeySecret::default()
        }
    }
}

pub fn bybit_credentials(venue_id: &str) -> ApiKeySecret {
    resolve_key_secret(venue_id, "BYBIT")
}

pub fn okx_credentials(venue_id: &str) -> OkxCredentials {
    if !venue_credentials_enabled() {
        return OkxCredentials::default();
    }
    let api_key = scoped_or_global(venue_id, "OKX", "API_KEY");
    let api_secret = scoped_or_global(venue_id, "OKX", "API_SECRET");
    let api_passphrase = scoped_or_global(venue_id, "OKX", "API_PASSPHRASE")
        .or_else(|| scoped_or_global(venue_id, "OKX", "PASSPHRASE"));
    match (api_key, api_secret, api_passphrase) {
        (Some(k), Some(s), Some(p)) => OkxCredentials {
            api_key: Some(k),
            api_secret: Some(s),
            api_passphrase: Some(p),
        },
        (k, s, p) => {
            if k.is_some() || s.is_some() || p.is_some() {
                log::warn!(
                    "incomplete OKX credentials for venue_id=`{venue_id}` \
                     (need API_KEY, API_SECRET, API_PASSPHRASE); using public client"
                );
            }
            OkxCredentials::default()
        }
    }
}

pub fn hyperliquid_private_key(venue_id: &str) -> Option<String> {
    if !venue_credentials_enabled() {
        return None;
    }
    scoped_or_global(venue_id, "HYPERLIQUID", "PRIVATE_KEY").or_else(|| {
        sanitize_opt(first_non_empty(&[
            "HYPERLIQUID_PRIVATE_KEY",
            "HL_PRIVATE_KEY",
        ]))
    })
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

    fn clear_related_env() {
        for name in VENUE_CREDENTIAL_ENV_VARS {
            env::remove_var(name);
        }
        env::remove_var(USE_VENUE_CREDENTIALS_ENV);
        env::remove_var("CAPTURE_VENUE_DERIBIT_MAIN_API_KEY");
        env::remove_var("CAPTURE_VENUE_DERIBIT_MAIN_API_SECRET");
        env::remove_var("CAPTURE_VENUE_BINANCE_MAIN_API_KEY");
        env::remove_var("CAPTURE_VENUE_BINANCE_MAIN_API_SECRET");
        PUBLIC_MODE_LOGGED.store(false, Ordering::SeqCst);
    }

    #[test]
    fn missing_env_yields_none() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_related_env();
        let creds = binance_credentials("binance_main");
        assert_eq!(creds, ApiKeySecret::default());
        assert!(!api_key_secret_present(&creds));
    }

    #[test]
    fn default_public_mode_ignores_env_keys() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_related_env();
        env::set_var("DERIBIT_API_KEY", "fake-key");
        env::set_var("DERIBIT_API_SECRET", "fake-secret");
        // Opt-in not set → public
        let creds = deribit_credentials("deribit_main");
        assert_eq!(creds, ApiKeySecret::default());
        prepare_public_capture_env();
        assert!(env::var("DERIBIT_API_KEY").is_err());
        assert!(env::var("DERIBIT_API_SECRET").is_err());
        clear_related_env();
    }

    #[test]
    fn placeholders_never_injected_even_when_opted_in() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_related_env();
        env::set_var(USE_VENUE_CREDENTIALS_ENV, "1");
        env::set_var("BINANCE_API_KEY", "none");
        env::set_var("BINANCE_API_SECRET", "none");
        let creds = binance_credentials("binance_main");
        assert_eq!(creds, ApiKeySecret::default());
        clear_related_env();
    }

    #[test]
    fn incomplete_pair_not_injected() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_related_env();
        env::set_var(USE_VENUE_CREDENTIALS_ENV, "1");
        env::set_var("BYBIT_API_KEY", "only-key-no-secret");
        let creds = bybit_credentials("bybit_main");
        assert_eq!(creds, ApiKeySecret::default());
        clear_related_env();
    }

    #[test]
    fn complete_real_pair_injected_when_opted_in() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_related_env();
        env::set_var(USE_VENUE_CREDENTIALS_ENV, "1");
        env::set_var("DERIBIT_API_KEY", "real-looking-client-id-abc");
        env::set_var("DERIBIT_API_SECRET", "real-looking-secret-xyz");
        let creds = deribit_credentials("deribit_main");
        assert_eq!(creds.api_key.as_deref(), Some("real-looking-client-id-abc"));
        assert_eq!(creds.api_secret.as_deref(), Some("real-looking-secret-xyz"));
        clear_related_env();
    }

    #[test]
    fn scoped_env_beats_global_when_opted_in() {
        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_related_env();
        env::set_var(USE_VENUE_CREDENTIALS_ENV, "true");
        env::set_var("DERIBIT_API_KEY", "global-key");
        env::set_var("DERIBIT_API_SECRET", "global-secret");
        env::set_var("CAPTURE_VENUE_DERIBIT_MAIN_API_KEY", "scoped-key");
        env::set_var("CAPTURE_VENUE_DERIBIT_MAIN_API_SECRET", "scoped-secret");
        let creds = deribit_credentials("deribit_main");
        assert_eq!(creds.api_key.as_deref(), Some("scoped-key"));
        assert_eq!(creds.api_secret.as_deref(), Some("scoped-secret"));
        clear_related_env();
    }

    #[test]
    fn placeholder_detection() {
        assert!(is_placeholder_credential(""));
        assert!(is_placeholder_credential("  none  "));
        assert!(is_placeholder_credential("None"));
        assert!(is_placeholder_credential("YOUR_API_KEY"));
        assert!(is_placeholder_credential("xxx"));
        assert!(!is_placeholder_credential("AbCdEfGh123456"));
    }

    /// Live check: public Deribit HTTP still works after scrubbing fake env keys.
    #[cfg(feature = "venue-deribit")]
    #[tokio::test]
    async fn public_deribit_http_ignores_fake_env_keys() {
        use nautilus_deribit::{
            common::enums::{DeribitCurrency, DeribitEnvironment},
            http::{client::DeribitHttpClient, models::DeribitProductType},
        };

        {
            let _guard = ENV_LOCK.lock().expect("env lock");
            clear_related_env();
            env::set_var("DERIBIT_API_KEY", "fake");
            env::set_var("DERIBIT_API_SECRET", "fake");
            env::set_var("DERIBIT_CLIENT_ID", "none");
            env::set_var("DERIBIT_CLIENT_SECRET", "none");

            // Same path as runner: scrub then resolve credentials for public capture.
            prepare_public_capture_env();
            let creds = deribit_credentials("deribit_main");
            assert_eq!(creds, ApiKeySecret::default());
            assert!(env::var("DERIBIT_API_KEY").is_err());
            assert!(env::var("DERIBIT_API_SECRET").is_err());
            // Drop lock before await (clippy::await_holding_lock).
        }

        let client =
            DeribitHttpClient::new(None, DeribitEnvironment::Mainnet, 30, 3, 500, 5_000, None)
                .expect("public Deribit HTTP client");
        let instruments = client
            .request_instruments(DeribitCurrency::BTC, Some(DeribitProductType::Future))
            .await
            .expect("public instruments request must succeed without API keys");
        assert!(
            !instruments.is_empty(),
            "expected at least one BTC future from public API"
        );

        let _guard = ENV_LOCK.lock().expect("env lock");
        clear_related_env();
    }
}
