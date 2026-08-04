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

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// Set to `0` to run until Ctrl+C or SIGTERM (unattended daemon mode).
    #[serde(default = "default_capture_seconds")]
    pub capture_seconds: u64,
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,
    #[serde(default = "default_delay_post_stop_secs")]
    pub delay_post_stop_secs: u64,
    #[serde(default = "default_node_name")]
    pub node_name: String,
    #[serde(default)]
    pub online_option_metrics: OnlineOptionMetricsRuntimeConfig,
    #[serde(default)]
    pub option_universe_refresh: OptionUniverseRefreshRuntimeConfig,
    #[serde(default)]
    pub hip4_universe_refresh: Hip4UniverseRefreshRuntimeConfig,
    #[serde(default)]
    pub metrics: MetricsExportRuntimeConfig,
    /// Optional process memory budget for startup warnings (capture buffers only).
    #[serde(default)]
    pub resource_budget_bytes: Option<u64>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            capture_seconds: default_capture_seconds(),
            shutdown_timeout_secs: default_shutdown_timeout_secs(),
            delay_post_stop_secs: default_delay_post_stop_secs(),
            node_name: default_node_name(),
            online_option_metrics: OnlineOptionMetricsRuntimeConfig::default(),
            option_universe_refresh: OptionUniverseRefreshRuntimeConfig::default(),
            hip4_universe_refresh: Hip4UniverseRefreshRuntimeConfig::default(),
            metrics: MetricsExportRuntimeConfig::default(),
            resource_budget_bytes: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsExportRuntimeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_metrics_bind_addr")]
    pub bind_addr: String,
    #[serde(default = "default_metrics_port")]
    pub port: u16,
    #[serde(default = "default_metrics_refresh_interval_secs")]
    pub refresh_interval_secs: u64,
}

impl Default for MetricsExportRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_addr: default_metrics_bind_addr(),
            port: default_metrics_port(),
            refresh_interval_secs: default_metrics_refresh_interval_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hip4UniverseRefreshRuntimeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_hip4_idle_poll_secs")]
    pub idle_poll_secs: u64,
    #[serde(default = "default_hip4_active_poll_secs")]
    pub active_poll_secs: u64,
    #[serde(default = "default_hip4_pre_expiry_window_secs")]
    pub pre_expiry_window_secs: u64,
    #[serde(default = "default_hip4_http_timeout_secs")]
    pub http_timeout_secs: u64,
    #[serde(default)]
    pub purge_removed_instruments: bool,
}

impl Default for Hip4UniverseRefreshRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            idle_poll_secs: default_hip4_idle_poll_secs(),
            active_poll_secs: default_hip4_active_poll_secs(),
            pre_expiry_window_secs: default_hip4_pre_expiry_window_secs(),
            http_timeout_secs: default_hip4_http_timeout_secs(),
            purge_removed_instruments: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineOptionMetricsRuntimeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_online_option_metrics_interval_secs")]
    pub snapshot_interval_secs: u64,
}

impl Default for OnlineOptionMetricsRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            snapshot_interval_secs: default_online_option_metrics_interval_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionUniverseRefreshRuntimeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_option_universe_refresh_interval_secs")]
    pub interval_secs: u64,
    /// Consecutive refresh ticks required before an `oi_ranked` strike set change
    /// is applied. Zero disables smoothing.
    #[serde(default = "default_option_universe_strike_change_confirmations")]
    pub strike_change_confirmations: u32,
}

impl Default for OptionUniverseRefreshRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: default_option_universe_refresh_interval_secs(),
            strike_change_confirmations: default_option_universe_strike_change_confirmations(),
        }
    }
}

const fn default_capture_seconds() -> u64 {
    30
}

const fn default_shutdown_timeout_secs() -> u64 {
    10
}

const fn default_delay_post_stop_secs() -> u64 {
    2
}

fn default_node_name() -> String {
    "CATALOG-CAPTURE-CLI-001".to_string()
}

fn default_metrics_bind_addr() -> String {
    "127.0.0.1".to_string()
}

const fn default_metrics_port() -> u16 {
    9898
}

const fn default_metrics_refresh_interval_secs() -> u64 {
    5
}

const fn default_online_option_metrics_interval_secs() -> u64 {
    5
}

const fn default_option_universe_refresh_interval_secs() -> u64 {
    300
}

const fn default_hip4_idle_poll_secs() -> u64 {
    1800
}

const fn default_hip4_active_poll_secs() -> u64 {
    10
}

const fn default_hip4_pre_expiry_window_secs() -> u64 {
    900
}

const fn default_hip4_http_timeout_secs() -> u64 {
    10
}

const fn default_option_universe_strike_change_confirmations() -> u32 {
    2
}
