// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Account-scoped authentication lifecycle; Native owns all trading recovery.
use std::{sync::atomic::Ordering, time::Duration};

use super::PredictExecutionClient;

pub(super) fn renewal_delay(expiry: u64, now: u64) -> Duration {
    // Like Deribit, renew after 80% of the remaining token lifetime.
    Duration::from_secs(expiry.saturating_sub(now).saturating_mul(4) / 5)
}

impl PredictExecutionClient {
    pub(super) fn start_authentication_refresh(&self) -> anyhow::Result<()> {
        let Some(signer) = self.config.auth_signer.clone() else {
            return Ok(());
        };
        let session = self.config.session.clone();
        if session.token().expires_at()?.is_none() {
            log::warn!(
                "Predict external token has no expiry; automatic authentication renewal unavailable"
            );
            return Ok(());
        }
        let cancellation = self.tasks.cancellation_token();
        let auth = self
            .wallet_auth
            .clone()
            .ok_or_else(|| anyhow::anyhow!("wallet auth unavailable"))?;
        let required = self.reconciliation_required.clone();
        let clock = self.clock;
        self.tasks.spawn_named("predict-authentication", async move {
            loop {
                let now = clock.get_time_ns().as_u64() / 1_000_000_000;
                let expiry = match session.token().expires_at() { Ok(Some(value)) => value, _ => break };
                tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => break,
                    _ = tokio::time::sleep(renewal_delay(expiry, now)) => {},
                }
                // Pause submissions before publishing a replacement bearer. The wallet ACK
                // and native reconciliation establish readiness for the new session.
                auth.invalidate();
                required.store(true, Ordering::Release);
                let mut retry = 1;
                loop {
                    let result = tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => return,
                        result = session.renew(&signer) => result,
                    };
                    match result {
                        Ok(()) => {
                            // The wallet token watcher requests Native connection replacement and ACKs.
                            break;
                        }
                        Err(_) => {
                            log::warn!("Predict authentication renewal failed; no trading operation retried");
                        }
                    }
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(retry)) => {},
                    }
                    retry = (retry * 2).min(30);
                }
            }
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn renewal_is_before_expiration_and_expired_tokens_refresh_immediately() {
        assert_eq!(renewal_delay(86400, 0), Duration::from_secs(69120));
        assert_eq!(renewal_delay(101, 100), Duration::ZERO);
        assert_eq!(renewal_delay(90, 100), Duration::ZERO);
    }
}
