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

//! Runtime-only configuration for incremental private execution integration.

use std::sync::Arc;

use nautilus_model::identifiers::AccountId;

use crate::signing::PredictAuthSigner;

use super::{PredictChainClientConfig, PredictWalletWebSocketConfig};
use crate::http::PredictAuthenticatedHttpClient;

/// Normal execution initializes reported cash; disabled mode is for transport diagnostics only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PredictAccountStateMode {
    #[default]
    Reported,
    Disabled,
}

/// A pre-authenticated session is injected; no environment credentials are read implicitly.
/// Strategy submission and REST removal require explicit opt-in.
#[derive(Clone, Debug)]
pub struct PredictExecutionClientConfig {
    /// None disables SubmitOrder. No credentials are implicitly acquired by the client.
    pub submission_signer: Option<Arc<PredictAuthSigner>>,
    /// Optional bounded telemetry. The host owns/drains the collector away from trading callbacks.
    pub latency: Option<crate::latency::PredictLatency>,
    /// Optional authentication authority, independent of permission to submit orders.
    pub auth_signer: Option<Arc<PredictAuthSigner>>,
    pub account_id: AccountId,
    pub session: PredictAuthenticatedHttpClient,
    pub websocket: PredictWalletWebSocketConfig,
    /// With a source configured, connect initializes a reported cash account through Portfolio.
    pub chain: Option<PredictChainClientConfig>,
    pub account_state_mode: PredictAccountStateMode,
    /// Explicit opt-in to REST orderbook removal, which does not invalidate a signature on-chain.
    /// Commands verify cached GTC/GTD limit orders; `BatchCancelOrders` groups eligible IDs
    /// in removal requests and resolves each result separately. No chain transactions are sent.
    pub orderbook_removal_enabled: bool,
    /// Periodic refresh also covers deposits and withdrawals outside the wallet event stream.
    /// Order events and connected `QueryAccount` commands wake the same refresh loop;
    /// they do not create additional periodic timers. Missing signing nonces are prepared
    /// independently at initialization or after nonce rejection; the same periodic cadence
    /// retries missing entries. Ordinary order events only reproject local reservations.
    pub account_refresh_interval_secs: u64,
    /// Account refresh cadence after wallet transport changes (default one second).
    /// Native owns order, fill and position recovery. This timer also retires purged
    /// worker contexts at the account-refresh cadence; it never scans historical holdings.
    /// None requires explicit host QueryAccount after a transport change; context cleanup
    /// then runs at instrument, report and account-query boundaries.
    pub reconnect_reconciliation_interval_secs: Option<u64>,
    /// Maximum age of strict execution-market metadata in the Native instrument worker view.
    /// Preparation runs at connection, instrument updates and before expiry, sharing YES/NO.
    /// Valid range 1..=300 seconds. Zero is invalid. Cold/expired commands fail promptly;
    /// only background preparation queries market metadata.
    /// Definition changes and TTL expiry invalidate it; wallet resubscription preserves it.
    pub market_metadata_ttl_secs: u64,
    pub report_page_size: u32,
    pub max_report_pages: usize,
    pub report_timeout_secs: u64,
}

impl PredictExecutionClientConfig {
    /// Creates private integration with strategy submission and orderbook removal disabled.
    #[must_use]
    pub fn new(
        account_id: AccountId,
        session: PredictAuthenticatedHttpClient,
        websocket: PredictWalletWebSocketConfig,
    ) -> Self {
        Self {
            submission_signer: None,
            latency: None,
            auth_signer: None,
            account_id,
            session,
            websocket,
            chain: None,
            account_state_mode: PredictAccountStateMode::Reported,
            orderbook_removal_enabled: false,
            account_refresh_interval_secs: 30,
            reconnect_reconciliation_interval_secs: Some(1),
            market_metadata_ttl_secs: 30,
            report_page_size: 100,
            max_report_pages: 100,
            report_timeout_secs: 30,
        }
    }

    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        if let Some(signer) = &self.auth_signer {
            anyhow::ensure!(
                signer.environment() == self.session.environment()
                    && signer.account_address() == self.session.account_address(),
                "Predict authentication signer differs from session"
            );
        }
        if let Some(signer) = &self.submission_signer {
            anyhow::ensure!(
                signer.environment() == self.session.environment()
                    && signer.account_address() == self.session.account_address(),
                "Predict submission signer differs from the authenticated account/environment"
            );
        }
        anyhow::ensure!(
            self.account_id.get_issuer() == *crate::common::PREDICT_VENUE,
            "Predict account ID must use the PREDICT issuer"
        );
        anyhow::ensure!(
            self.session.environment() == self.websocket.environment,
            "Predict execution transport environments differ"
        );
        self.websocket.validated_ws_url()?;
        if let Some(chain) = &self.chain {
            chain.validate()?;
            anyhow::ensure!(
                chain.environment == self.session.environment(),
                "Predict RPC and REST environments differ"
            );
        }
        anyhow::ensure!(
            (1..=300).contains(&self.market_metadata_ttl_secs),
            "Predict market metadata TTL must be 1..=300 seconds; zero is unsupported"
        );
        anyhow::ensure!(
            self.reconnect_reconciliation_interval_secs
                .is_none_or(|secs| (1..=300).contains(&secs))
                && (1..=300).contains(&self.account_refresh_interval_secs)
                && self.report_page_size > 0
                && self.report_page_size <= i32::MAX as u32
                && self.max_report_pages > 0
                && self.max_report_pages <= 10000
                && self.report_timeout_secs > 0
                && self.report_timeout_secs <= 300,
            "invalid Predict report collection bounds"
        );
        Ok(())
    }
}
