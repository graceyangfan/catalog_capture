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

//! Venue credentials — two modes only:
//!
//! 1. **Public** (default): `api_key` / `api_secret` = `None`
//! 2. **Authenticated**: both key and secret set from env (complete pair)
//!
//! General venue configuration never carries secrets. Predict and Binance Spot
//! SBE accept dedicated ignored TOML credential files; incomplete env pairs stay
//! public for venues that permit unauthenticated data.

use std::{env, fs, path::Path};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApiKeySecret {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
}

#[cfg(feature = "venue-okx")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OkxCredentials {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub api_passphrase: Option<String>,
}

fn env_nonempty(name: &str) -> Option<String> {
    env::var(name).ok().and_then(|v| {
        let t = v.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    })
}

/// Venue-id scoped name, e.g. `deribit_main` + `API_KEY` → `CAPTURE_VENUE_DERIBIT_MAIN_API_KEY`.
fn scoped(venue_id: &str, suffix: &str) -> String {
    let id = venue_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("CAPTURE_VENUE_{id}_{suffix}")
}

fn read_pair(venue_id: &str, kind: &str) -> ApiKeySecret {
    read_pair_with_value_name(venue_id, kind, "API_SECRET")
}

fn read_pair_with_value_name(
    venue_id: &str,
    kind: &str,
    value_name: &str,
) -> ApiKeySecret {
    let kind = kind.to_ascii_uppercase();
    let key = env_nonempty(&scoped(venue_id, "API_KEY"))
        .or_else(|| env_nonempty(&format!("{kind}_API_KEY")));
    let value = env_nonempty(&scoped(venue_id, value_name))
        .or_else(|| env_nonempty(&format!("{kind}_{value_name}")));
    match (key, value) {
        (Some(api_key), Some(api_secret)) => ApiKeySecret {
            api_key: Some(api_key),
            api_secret: Some(api_secret),
        },
        _ => ApiKeySecret::default(), // public
    }
}

pub fn binance_credentials(venue_id: &str) -> ApiKeySecret {
    read_pair(venue_id, "BINANCE")
}

/// Reads Binance Spot SBE credentials from an operator-owned TOML file, falling
/// back to an Ed25519 `API_KEY`/`PRIVATE_KEY` environment pair for container
/// deployments. The private-key naming is deliberate: SBE must not accept a
/// Binance HMAC `API_SECRET` by accident.
///
/// The file boundary deliberately mirrors Predict: callers supply data, never
/// executable shell. This keeps private keys out of capture TOML and avoids a
/// launcher sourcing arbitrary code.
#[cfg(feature = "venue-binance")]
pub fn binance_spot_sbe_credentials(venue_id: &str) -> Result<ApiKeySecret> {
    let configured_path = env_nonempty("BINANCE_SBE_CREDENTIALS_FILE");
    let default_path = Path::new("env/binance-spot-sbe.toml");
    if let Some(path) = configured_path.as_deref() {
        return read_api_key_private_key_file(Path::new(path), "Binance Spot SBE");
    }
    if default_path.exists() {
        return read_api_key_private_key_file(default_path, "Binance Spot SBE");
    }
    Ok(binance_spot_sbe_env_credentials(venue_id))
}

fn binance_spot_sbe_env_credentials(venue_id: &str) -> ApiKeySecret {
    read_pair_with_value_name(venue_id, "BINANCE", "PRIVATE_KEY")
}

#[cfg(any(feature = "venue-deribit", test))]
pub fn deribit_credentials(venue_id: &str) -> ApiKeySecret {
    read_pair(venue_id, "DERIBIT")
}

#[cfg(feature = "venue-bybit")]
pub fn bybit_credentials(venue_id: &str) -> ApiKeySecret {
    read_pair(venue_id, "BYBIT")
}

#[cfg(feature = "venue-okx")]
pub fn okx_credentials(venue_id: &str) -> OkxCredentials {
    let key = env_nonempty(&scoped(venue_id, "API_KEY")).or_else(|| env_nonempty("OKX_API_KEY"));
    let secret =
        env_nonempty(&scoped(venue_id, "API_SECRET")).or_else(|| env_nonempty("OKX_API_SECRET"));
    let passphrase = env_nonempty(&scoped(venue_id, "API_PASSPHRASE"))
        .or_else(|| env_nonempty("OKX_API_PASSPHRASE"))
        .or_else(|| env_nonempty("OKX_PASSPHRASE"));
    match (key, secret, passphrase) {
        (Some(api_key), Some(api_secret), Some(api_passphrase)) => OkxCredentials {
            api_key: Some(api_key),
            api_secret: Some(api_secret),
            api_passphrase: Some(api_passphrase),
        },
        _ => OkxCredentials::default(), // public
    }
}

#[cfg(any(feature = "venue-hyperliquid", test))]
pub fn hyperliquid_private_key(venue_id: &str) -> Option<String> {
    env_nonempty(&scoped(venue_id, "PRIVATE_KEY"))
        .or_else(|| env_nonempty("HYPERLIQUID_PRIVATE_KEY"))
        .or_else(|| env_nonempty("HL_PRIVATE_KEY"))
}

/// Predict REST market metadata uses one API key, not an API key/secret pair.
#[cfg(feature = "venue-predict")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PredictCredentialFile {
    api_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApiKeyPrivateKeyFile {
    api_key: String,
    private_key: String,
}

fn read_api_key_private_key_file(path: &Path, label: &str) -> Result<ApiKeySecret> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("failed to stat {label} credential file {}", path.display()))?;
    anyhow::ensure!(
        metadata.is_file(),
        "{label} credential path {} is not a regular file",
        path.display()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        anyhow::ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "{label} credential file {} must not be group/world readable (chmod 600)",
            path.display()
        );
    }
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read {label} credential file {}", path.display()))?;
    let credentials: ApiKeyPrivateKeyFile = toml::from_str(&content)
        .with_context(|| format!("failed to parse {label} credential file {}", path.display()))?;
    let api_key = credentials.api_key.trim();
    let private_key = credentials.private_key.trim();
    anyhow::ensure!(
        !api_key.is_empty(),
        "{label} credential api_key must be non-empty"
    );
    anyhow::ensure!(
        !private_key.is_empty(),
        "{label} credential private_key must be non-empty"
    );
    Ok(ApiKeySecret {
        api_key: Some(api_key.to_string()),
        // NautilusTrader's generic Binance config calls this field `api_secret`,
        // but for SBE it carries the Ed25519 private key, not an HMAC secret.
        api_secret: Some(private_key.to_string()),
    })
}

/// Reads a Predict API key from the local ignored TOML file.
///
/// `env/predictfun.toml` is the default. `PREDICT_CREDENTIALS_FILE` can point to another local
/// file. Environment credentials remain an operator fallback for container deployments.
#[cfg(feature = "venue-predict")]
pub fn predict_api_key(venue_id: &str) -> Result<String> {
    let configured_path = env_nonempty("PREDICT_CREDENTIALS_FILE");
    let default_path = Path::new("env/predictfun.toml");
    let legacy_path = Path::new("env/predictfun.env");
    let credentials_file = configured_path
        .as_deref()
        .map(Path::new)
        .filter(|path| path.exists())
        .or_else(|| default_path.exists().then_some(default_path));
    let Some(credentials_file) = credentials_file else {
        if legacy_path.exists() {
            return read_legacy_predict_api_key(legacy_path);
        }
        return env_nonempty(&scoped(venue_id, "API_KEY"))
            .or_else(|| env_nonempty("PREDICT_API_KEY"))
            .context("Predict API key is not configured (create env/predictfun.toml or set PREDICT_CREDENTIALS_FILE)");
    };
    let metadata = fs::metadata(credentials_file).with_context(|| {
        format!(
            "failed to stat Predict credential file {}",
            credentials_file.display()
        )
    })?;
    anyhow::ensure!(
        metadata.is_file(),
        "Predict credential path {} is not a regular file",
        credentials_file.display()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        anyhow::ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "Predict credential file {} must not be group/world readable (chmod 600)",
            credentials_file.display()
        );
    }
    let content = fs::read_to_string(credentials_file).with_context(|| {
        format!(
            "failed to read Predict credential file {}",
            credentials_file.display()
        )
    })?;
    let credentials: PredictCredentialFile = toml::from_str(&content).with_context(|| {
        format!(
            "failed to parse Predict credential file {}",
            credentials_file.display()
        )
    })?;
    let api_key = credentials.api_key.trim();
    anyhow::ensure!(
        !api_key.is_empty(),
        "Predict credential api_key must be non-empty"
    );
    Ok(api_key.to_string())
}

#[cfg(feature = "venue-predict")]
fn read_legacy_predict_api_key(path: &Path) -> Result<String> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("failed to stat legacy Predict key file {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        anyhow::ensure!(
            metadata.permissions().mode() & 0o077 == 0,
            "legacy Predict key file {} must not be group/world readable (chmod 600)",
            path.display()
        );
    }
    let key = fs::read_to_string(path)
        .with_context(|| format!("failed to read legacy Predict key file {}", path.display()))?;
    let key = key.trim();
    anyhow::ensure!(
        !key.is_empty(),
        "legacy Predict key file {} is empty",
        path.display()
    );
    Ok(key.to_string())
}

pub fn api_key_secret_present(creds: &ApiKeySecret) -> bool {
    creds.api_key.is_some() && creds.api_secret.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, sync::Mutex};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear() {
        for k in [
            "BINANCE_API_KEY",
            "BINANCE_API_SECRET",
            "BINANCE_PRIVATE_KEY",
            "DERIBIT_API_KEY",
            "DERIBIT_API_SECRET",
            "BYBIT_API_KEY",
            "BYBIT_API_SECRET",
            "OKX_API_KEY",
            "OKX_API_SECRET",
            "OKX_API_PASSPHRASE",
            "HYPERLIQUID_PRIVATE_KEY",
            "CAPTURE_VENUE_DERIBIT_MAIN_API_KEY",
            "CAPTURE_VENUE_DERIBIT_MAIN_API_SECRET",
            "CAPTURE_VENUE_BINANCE_SPOT_SBE_API_KEY",
            "CAPTURE_VENUE_BINANCE_SPOT_SBE_PRIVATE_KEY",
        ] {
            // Tests serialize environment access with `ENV_LOCK`.
            unsafe { env::remove_var(k) };
        }
    }

    fn set(key: &str, value: &str) {
        // Tests serialize environment access with `ENV_LOCK`.
        unsafe { env::set_var(key, value) };
    }

    #[test]
    fn no_env_is_public() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        assert_eq!(binance_credentials("x"), ApiKeySecret::default());
        assert!(!api_key_secret_present(&deribit_credentials(
            "deribit_main"
        )));
    }

    #[test]
    fn only_key_is_public() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        set("DERIBIT_API_KEY", "only-key");
        assert_eq!(deribit_credentials("deribit_main"), ApiKeySecret::default());
        clear();
    }

    #[test]
    fn both_key_and_secret_is_authenticated() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        set("DERIBIT_API_KEY", "k");
        set("DERIBIT_API_SECRET", "s");
        let c = deribit_credentials("deribit_main");
        assert_eq!(c.api_key.as_deref(), Some("k"));
        assert_eq!(c.api_secret.as_deref(), Some("s"));
        assert!(api_key_secret_present(&c));
        clear();
    }

    #[test]
    fn scoped_overrides_global() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        set("DERIBIT_API_KEY", "global-k");
        set("DERIBIT_API_SECRET", "global-s");
        set("CAPTURE_VENUE_DERIBIT_MAIN_API_KEY", "scoped-k");
        set("CAPTURE_VENUE_DERIBIT_MAIN_API_SECRET", "scoped-s");
        let c = deribit_credentials("deribit_main");
        assert_eq!(c.api_key.as_deref(), Some("scoped-k"));
        assert_eq!(c.api_secret.as_deref(), Some("scoped-s"));
        clear();
    }

    #[cfg(feature = "venue-binance")]
    #[test]
    fn binance_spot_sbe_toml_is_data_not_shell() {
        let path = std::env::temp_dir().join(format!(
            "catalog-capture-binance-sbe-credentials-{}.toml",
            std::process::id()
        ));
        fs::write(
            &path,
            "api_key = \"key\"\nprivate_key = \"private-key\"\n",
        )
            .expect("credential fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("private fixture permissions");
        }

        assert_eq!(
            read_api_key_private_key_file(&path, "Binance Spot SBE").expect("credential parse"),
            ApiKeySecret {
                api_key: Some("key".to_string()),
                api_secret: Some("private-key".to_string()),
            }
        );
        let _ = fs::remove_file(path);
    }

    #[cfg(feature = "venue-binance")]
    #[test]
    fn binance_spot_sbe_rejects_api_secret_file_field() {
        let path = std::env::temp_dir().join(format!(
            "catalog-capture-binance-sbe-legacy-credentials-{}.toml",
            std::process::id()
        ));
        fs::write(&path, "api_key = \"key\"\napi_secret = \"hmac-secret\"\n")
            .expect("credential fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("private fixture permissions");
        }

        assert!(read_api_key_private_key_file(&path, "Binance Spot SBE").is_err());
        let _ = fs::remove_file(path);
    }

    #[cfg(feature = "venue-binance")]
    #[test]
    fn binance_spot_sbe_env_uses_private_key_not_api_secret() {
        let _g = ENV_LOCK.lock().unwrap();
        clear();
        set("BINANCE_API_KEY", "key");
        set("BINANCE_API_SECRET", "hmac-secret");
        assert_eq!(
            binance_spot_sbe_env_credentials("binance_spot_sbe"),
            ApiKeySecret::default()
        );
        set("BINANCE_PRIVATE_KEY", "private-key");
        assert_eq!(
            binance_spot_sbe_env_credentials("binance_spot_sbe"),
            ApiKeySecret {
                api_key: Some("key".to_string()),
                api_secret: Some("private-key".to_string()),
            }
        );
        clear();
    }
}
