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

//! Native command admission and event dispatch; ExecutionEngine remains the only OMS.

pub(in crate::execution) mod cancel;
mod routing;
pub(in crate::execution) mod submit;
pub(in crate::execution) use routing::{RestOrderIds, observe, parse_id, register, register_many};

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use alloy::primitives::U256;
use anyhow::Context;
use nautilus_common::{
    clients::ExecutionClient,
    factories::OrderEventFactory,
    messages::execution::{ModifyOrder, SubmitOrder},
};
use nautilus_live::execution::failure::CommandFailure;
use nautilus_model::{
    identifiers::{ClientOrderId, VenueOrderId},
    instruments::{Instrument, InstrumentAny},
    orders::Order,
};
use rust_decimal::Decimal;

use self::submit::{SubmitWorker, terms};
use crate::execution::{metadata, parse};
use crate::{
    config::PredictAccountStateMode, execution::PredictExecutionClient, http::error::Error,
};

// A write can be rejected only with venue proof; transport uncertainty stays ambiguous.
pub(in crate::execution) fn classify_http_command_failure(error: &Error) -> CommandFailure {
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
        _ => CommandFailure::Ambiguous(reason),
    }
}

#[derive(Debug, Default)]
pub(in crate::execution) struct OrderRequests {
    // One-shot Native query intents also cover restored orders without a local submission.
    pub order_status_requests: HashSet<ClientOrderId>,
    /// Active submissions by this client; Native terminal events remove entries.
    pub entries: HashMap<ClientOrderId, Submission>,
}
#[derive(Clone, Copy, Debug)]
pub(in crate::execution) struct Submission {
    pub venue_id: Option<VenueOrderId>,
    // Direct live feedback is scoped to this transport epoch. Setup invalidates old entries.
    pub wallet_epoch: Option<u64>,
    // Immutable signed budget, not a balance/reservation ledger. Retired with this entry.
    pub market_buy_collateral: Option<Decimal>,
}
pub(in crate::execution) type PendingSubmissions = Arc<Mutex<OrderRequests>>;

pub(in crate::execution) struct SubmissionGuard {
    pub registry: PendingSubmissions,
    pub id: ClientOrderId,
    pub retain: bool,
    pub transmitted: bool,
    pub venue_id: Option<nautilus_model::identifiers::VenueOrderId>,
}
impl Drop for SubmissionGuard {
    fn drop(&mut self) {
        if !self.retain {
            self.registry
                .lock()
                .expect("submission registry poisoned")
                .entries
                .remove(&self.id);
        }
    }
}

impl PredictExecutionClient {
    /// Predict's public API and official SDK expose no atomic amend operation.
    /// No modification request is sent to the venue. Publish a native rejection so callers
    /// can resolve PendingUpdate before explicitly cancelling and replacing an order.
    pub(in crate::execution) fn modify_order_command(
        &self,
        cmd: &ModifyOrder,
    ) -> anyhow::Result<()> {
        let order = self
            .core
            .get_order(&cmd.client_order_id)
            .context("Predict modify requires a cached native order")?;
        anyhow::ensure!(
            cmd.trader_id == self.core.trader_id
                && order.trader_id() == self.core.trader_id
                && cmd.strategy_id == order.strategy_id()
                && cmd.instrument_id == order.instrument_id()
                && cmd.client_id.is_none_or(|id| id == self.core.client_id)
                && cmd
                    .venue_order_id
                    .is_none_or(|id| Some(id) == order.venue_order_id())
                && order.account_id() == Some(self.core.account_id)
                && self
                    .core
                    .cache()
                    .client_id(&cmd.client_order_id)
                    .is_none_or(|id| *id == self.core.client_id),
            "Predict modify identity mismatch"
        );
        let factory = OrderEventFactory::new(
            self.core.trader_id,
            self.core.account_id,
            self.core.account_type,
            self.core.base_currency,
        );
        let now = self.clock.get_time_ns();
        let event = factory.generate_order_modify_rejected(
            &order,
            order.venue_order_id(),
            "Predict atomic order modification is unsupported; cancel then replace explicitly",
            now,
            now,
        );
        self.emitter.try_send_order_event(event)?;
        Ok(())
    }

    pub(in crate::execution) fn submit_order_command(
        &self,
        cmd: &SubmitOrder,
    ) -> anyhow::Result<()> {
        let command_started = self
            .config
            .latency
            .as_ref()
            .map(|_| std::time::Instant::now());
        let signer = self
            .order_signer
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Predict order submission is not enabled"))?;
        anyhow::ensure!(
            self.config.account_state_mode == PredictAccountStateMode::Reported
                && self.is_connected(),
            "Predict reported execution session is not connected"
        );
        let cache = self.core.cache();
        let order = cache
            .order_owned(&cmd.client_order_id)
            .ok_or_else(|| anyhow::anyhow!("Predict submit requires a cached native order"))?;
        anyhow::ensure!(
            cmd.trader_id == self.core.trader_id
                && order.trader_id() == self.core.trader_id
                && cmd.strategy_id == order.strategy_id()
                && cmd.instrument_id == order.instrument_id()
                && cmd.client_id.is_none_or(|id| id == self.core.client_id)
                && order
                    .account_id()
                    .is_none_or(|id| id == self.core.account_id),
            "Predict submit identity mismatch"
        );
        let Some(InstrumentAny::BinaryOption(instrument)) =
            cache.instrument(&order.instrument_id())
        else {
            anyhow::bail!("Predict native binary instrument is missing");
        };
        anyhow::ensure!(
            order.status() == nautilus_model::enums::OrderStatus::Initialized,
            "Predict cannot resubmit an active or terminal native order"
        );
        let instrument = instrument.clone();
        let validated = (|| -> anyhow::Result<_> {
            let market_price = submit::market_price(cmd.params.as_ref(), &order)?;
            let slippage_bps = submit::market_order_slippage_bps(cmd.params.as_ref(), &order)?;
            let values = terms(
                &order,
                &instrument,
                slippage_bps,
                self.clock.get_time_ns().as_u64(),
                market_price,
            )?;
            let options = submit::limit_options(cmd.params.as_ref(), &order)?;
            Ok((values, options))
        })();
        let (terms, limit_options) = match validated {
            Ok(terms) => terms,
            Err(reason) => {
                let factory = OrderEventFactory::new(
                    self.core.trader_id,
                    self.core.account_id,
                    self.core.account_type,
                    self.core.base_currency,
                );
                self.emitter
                    .try_send_order_event(factory.generate_order_denied(
                        &order,
                        &reason.to_string(),
                        self.clock.get_time_ns(),
                    ))?;
                return Ok(());
            }
        };
        drop(cache);
        // As in Polymarket, normalize quote sizing through a Native event before
        // any base-share wallet/fill evidence can arrive. Preserve the quote intent
        // in OrderInitialized; no separate mutable denomination registry is needed.
        let mut order = order;
        let normalization = if order.is_quote_quantity() {
            let now = self.clock.get_time_ns();
            let quantity = parse::quantity(
                crate::common::parse::exact_wei(terms.shares)?,
                instrument.size_precision(),
            )?;
            let event = nautilus_model::events::OrderEventAny::Updated(
                nautilus_model::events::OrderUpdated::new(
                    order.trader_id(),
                    order.strategy_id(),
                    order.instrument_id(),
                    order.client_order_id(),
                    quantity,
                    nautilus_core::UUID4::new(),
                    now,
                    now,
                    false,
                    None,
                    None,
                    None,
                    None,
                    None,
                    false,
                ),
            );
            order.apply(event.clone())?;
            Some(event)
        } else {
            None
        };
        let mut registry = self
            .pending_submissions
            .lock()
            .expect("submission registry poisoned");
        anyhow::ensure!(
            !registry.entries.contains_key(&cmd.client_order_id),
            "Predict submission for this client order is already active or unresolved"
        );
        registry.entries.insert(
            cmd.client_order_id,
            Submission {
                venue_id: None,
                wallet_epoch: Some(
                    self.wallet_epoch
                        .as_ref()
                        .expect("connected wallet")
                        .load(std::sync::atomic::Ordering::Acquire),
                ),
                market_buy_collateral: (terms.market.is_some()
                    && order.order_side() == nautilus_model::enums::OrderSide::Buy)
                    .then_some(terms.cost),
            },
        );
        drop(registry);
        let mut guard = SubmissionGuard {
            registry: Arc::clone(&self.pending_submissions),
            id: cmd.client_order_id,
            retain: false,
            transmitted: false,
            venue_id: None,
        };
        if let Some(event) = normalization {
            self.emitter.try_send_order_event(event)?;
        }
        self.emitter.try_emit_order_submitted(&order)?;
        guard.retain = true; // A queued native Submitted event must retain ownership through shutdown.
        // Native callbacks replace this view. Avoid rebuilding the read-heavy map and
        // discarding fresh protocol metadata on every command for the same definition.
        let current = self.metadata.instruments.get_cloned(&instrument.id());
        if !current.as_ref().is_some_and(|entry| {
            matches!(&entry.instrument, InstrumentAny::BinaryOption(old)
                if old.event_id == instrument.event_id && old.ts_init == instrument.ts_init
                    && old.raw_symbol == instrument.raw_symbol
                    && old.price_precision == instrument.price_precision
                    && old.expiration_ns == instrument.expiration_ns)
        }) {
            self.metadata.instruments.rcu(|map| {
                metadata::upsert_instrument(map, InstrumentAny::BinaryOption(instrument.clone()))
            });
        }
        let metadata = self
            .metadata
            .instruments
            .get_cloned(&instrument.id())
            .expect("instrument installed");
        let worker = SubmitWorker {
            command_started,
            signer: Arc::clone(signer),
            order: Arc::new(order),
            definition: metadata,
            terms,
            limit_options,
            data_sender: self.data_sender.clone(),
            // Match the official SDK's default salt range while retaining the complete
            // native command UUID for ownership and duplicate-command protection.
            salt: U256::from(u128::from_be_bytes(cmd.command_id.as_bytes()) % 2_147_483_648),
            pending: Arc::clone(&self.pending_submissions),
            orders: Arc::clone(&self.orders),
            rest_ids: Arc::clone(&self.rest_order_ids),
            account_refresh_required: Arc::clone(&self.account_refresh_required),
            account_sync: Arc::clone(&self.account_sync),
            metadata: Arc::clone(&self.metadata),
            healthy: Arc::clone(&self.healthy),
            authentication: self
                .wallet_auth
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Predict wallet auth is missing"))?,
            admitted_epoch: self
                .wallet_epoch
                .as_ref()
                .expect("connected wallet")
                .load(std::sync::atomic::Ordering::Acquire),
            epoch: self
                .wallet_epoch
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Predict wallet epoch is missing"))?,
            emitter: self.emitter.clone(),
            clock: self.clock,
            config: self.config.clone(),
        };
        self.tasks.spawn_named(
            "predict-submit-order",
            worker.run(guard, self.tasks.cancellation_token()),
        )?;
        Ok(())
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
