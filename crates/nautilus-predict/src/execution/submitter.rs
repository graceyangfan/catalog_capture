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

//! Local preparation, signing and one-shot private submission over owned worker snapshots.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use alloy::primitives::{Address, U256};
use nautilus_common::factories::OrderEventFactory;
use nautilus_core::{AtomicMap, time::AtomicTime};
use nautilus_live::{ExecutionEventEmitter, execution::failure::CommandFailure};
use nautilus_model::{
    enums::{OrderSide, TimeInForce},
    identifiers::{InstrumentId, VenueOrderId},
    instruments::Instrument,
    orders::{Order, OrderAny},
};
use nautilus_network::websocket::AuthTracker;

use super::{
    order_builder::decimal_wei,
    orders::{PendingSubmissions, SubmissionGuard},
    types::classify_http_command_failure,
};
use crate::{
    common::enums::{PredictOrderSide, PredictSignatureType},
    config::PredictExecutionClientConfig,
    http::order_models::PredictOrder,
    signing::PredictOrderSigner,
};

pub(super) struct SubmitWorker {
    pub signer: Arc<PredictOrderSigner>,
    pub command_started: Option<std::time::Instant>,
    pub config: PredictExecutionClientConfig,
    pub order: Arc<OrderAny>,
    pub definition: super::types::ExecutionInstrument,
    pub terms: super::order_builder::OrderTerms,
    pub limit_options: crate::http::command_models::PredictLimitOrderOptions,
    pub data_sender:
        Option<nautilus_common::live::sender::EventSender<nautilus_common::messages::DataEvent>>,
    pub salt: U256,
    pub pending: PendingSubmissions,
    pub orders: super::types::OrderSnapshots<VenueOrderId>,
    pub rest_ids: super::routing::RestOrderIds,
    pub account_refresh_required: Arc<AtomicBool>,
    pub account_sync: Arc<super::account::AccountSync>,
    pub instruments: Arc<AtomicMap<InstrumentId, super::types::ExecutionInstrument>>,
    pub healthy: Arc<AtomicBool>,
    pub authentication: AuthTracker,
    pub epoch: Arc<AtomicU64>,
    pub admitted_epoch: u64,
    pub emitter: ExecutionEventEmitter,
    pub factory: OrderEventFactory,
    pub clock: &'static AtomicTime,
    pub metadata_notify: Arc<tokio::sync::Notify>,
}

impl SubmitWorker {
    pub(super) fn available(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
            && self.authentication.is_authenticated()
            && self.epoch.load(Ordering::Acquire) == self.admitted_epoch
    }

    fn current_definition(&self) -> bool {
        self.definition.is_current(&self.instruments)
    }

    fn execution_market(&self) -> anyhow::Result<super::types::MarketMetadata> {
        if let Some(value) = self.definition.cached_market(
            self.clock.get_time_ns(),
            self.config
                .market_metadata_ttl_secs
                .saturating_mul(1_000_000_000),
        )? {
            if let Some(probe) = &self.config.latency {
                probe.count(crate::latency::LatencyCounter::MetadataHit);
            }
            return Ok(value);
        }
        if let Some(probe) = &self.config.latency {
            probe.count(crate::latency::LatencyCounter::MetadataMiss);
        }
        self.metadata_notify.notify_one();
        anyhow::bail!("Predict market metadata is not ready; wait for background preparation")
    }

    fn prepare_order(
        &self,
        market: &crate::http::market_models::PredictExecutionMarket,
        nonce: U256,
    ) -> anyhow::Result<(
        crate::http::order_models::PredictSignedOrder,
        Option<crate::common::market::PredictMarketOrderAmounts>,
    )> {
        let token: U256 = self
            .definition
            .instrument
            .raw_symbol()
            .as_str()
            .parse()
            .map_err(|_| anyhow::anyhow!("Predict token ID is invalid"))?;
        let side = if self.order.order_side() == OrderSide::Sell {
            PredictOrderSide::Sell
        } else {
            PredictOrderSide::Buy
        };
        let amounts = self.terms.market;
        let now = self.clock.get_time_ns().as_u64();
        anyhow::ensure!(
            self.definition
                .instrument
                .expiration_ns()
                .is_some_and(|expiry| now < expiry.as_u64())
                && self.terms.expiry > U256::from(now / 1_000_000_000),
            "Predict order expired during admission"
        );
        let (maker_amount, taker_amount) = match amounts {
            Some(amounts) => (amounts.maker_amount, amounts.taker_amount),
            None => {
                let collateral = decimal_wei(self.terms.cost)?;
                if side == PredictOrderSide::Sell {
                    (self.terms.shares, collateral)
                } else {
                    (collateral, self.terms.shares)
                }
            }
        };
        let signed = self.signer.sign_order(
            &PredictOrder {
                salt: self.salt,
                maker: self.config.session.account_address(),
                signer: self.config.session.account_address(),
                taker: Address::ZERO,
                token_id: token,
                maker_amount,
                taker_amount,
                expiration: self.terms.expiry,
                nonce,
                fee_rate_bps: U256::from(market.fee_rate_bps),
                side,
                signature_type: PredictSignatureType::Eoa,
            },
            market.exchange(),
        )?;
        Ok((signed, amounts))
    }

    pub(super) async fn run(&self, guard: &mut SubmissionGuard) -> Result<(), CommandFailure> {
        let preparation = self
            .config
            .latency
            .as_ref()
            .map(|probe| probe.timer(crate::latency::LatencyMetric::Preparation));
        if !self.available() || !self.current_definition() {
            return Err(CommandFailure::not_sent(
                "Predict admission lost wallet or instrument continuity",
            ));
        }
        let metadata = self
            .execution_market()
            .map_err(|error| CommandFailure::NotSent(error.to_string()))?;
        let nonce = self
            .account_sync
            .state
            .lock()
            .expect("account sync poisoned")
            .signing_nonces[metadata.market.exchange().cache_index()]
        .ok_or_else(|| CommandFailure::not_sent("Predict account signing nonce is not prepared"))?;
        if !self.available() || !self.current_definition() {
            return Err(CommandFailure::not_sent(
                "Predict admission lost wallet or instrument continuity",
            ));
        }
        let (signed, amounts) = self
            .prepare_order(&metadata.market, nonce)
            .map_err(|error| CommandFailure::NotSent(error.to_string()))?;
        if let Some(preparation) = preparation {
            preparation.finish();
        }
        let id = {
            // Fence the prepared nonce, generation and definition with registration.
            // This short correlation lock never covers signing or network IO.
            let mut sync = self
                .account_sync
                .state
                .lock()
                .expect("account sync poisoned");
            if sync.signing_nonces[metadata.market.exchange().cache_index()] != Some(nonce)
                || !self.available()
                || !self.current_definition()
                || !self
                    .clock
                    .get_time_ns()
                    .as_u64()
                    .checked_sub(metadata.observed.as_u64())
                    .is_some_and(|age| age < self.config.market_metadata_ttl_secs * 1_000_000_000)
            {
                return Err(CommandFailure::NotSent(
                    "Predict wallet, instrument or market metadata changed before transmission"
                        .into(),
                ));
            }
            let id = VenueOrderId::new(format!("{:#x}", signed.hash()));
            if self
                .orders
                .lock()
                .expect("order snapshots poisoned")
                .get(&id)
                .is_some_and(|existing| existing.client_order_id() != self.order.client_order_id())
            {
                return Err(CommandFailure::NotSent(
                    "Predict signed order hash already belongs to another native order".into(),
                ));
            }
            {
                let mut registry = self.pending.lock().expect("submission registry poisoned");
                registry
                    .entries
                    .get_mut(&self.order.client_order_id())
                    .ok_or_else(|| {
                        CommandFailure::NotSent("Predict submission lost command ownership".into())
                    })?
                    .venue_id = Some(id);
                // Install correlation before POST. Only immutable snapshots are stored here.
                self.orders
                    .lock()
                    .expect("order snapshots poisoned")
                    .insert(
                        id,
                        Arc::new(super::context::WorkerOrder::capture(&self.order)),
                    );
                sync.active_orders
                    .insert(self.order.client_order_id(), Some(id));
                guard.venue_id = Some(id);
                guard.transmitted = true;
            }
            id
        };
        // Correlation is installed before transmission; no funds ledger or admission
        // mutex is needed. Native risk and the venue enforce trading capacity.
        // Public correlation metadata survives an ambiguous reply without exposing signed
        // payloads. This is transmission intent, never a venue acceptance acknowledgement.
        log::info!(
            "Predict transmitting order: client_order_id={} order_hash={:#x}",
            self.order.client_order_id(),
            signed.hash(),
        );
        log::debug!(
            "Predict prepared order: client_order_id={} order_hash={:#x} instrument_id={} native_order_type={:?} native_tif={:?} initial_quote_quantity={} initial_quantity={} prepared_quantity={} wire_strategy={} is_post_only={:?} is_fill_or_kill={:?} slippage_bps={:?} price_per_share_wei={} maker_amount_wei={} taker_amount_wei={}",
            self.order.client_order_id(),
            signed.hash(),
            self.order.instrument_id(),
            self.order.order_type(),
            self.order.time_in_force(),
            self.order.init_event().quote_quantity,
            self.order.init_event().quantity,
            self.order.quantity(),
            if amounts.is_some() { "MARKET" } else { "LIMIT" },
            amounts.is_none().then_some(self.limit_options.is_post_only),
            (amounts.is_some() && self.order.time_in_force() == TimeInForce::Fok).then_some(true),
            amounts.map(|amounts| amounts.slippage_bps),
            amounts.map_or(self.terms.price, |amounts| amounts.price_per_share),
            signed.order().maker_amount,
            signed.order().taker_amount,
        );
        let timing = self.config.latency.as_ref().map(|probe| {
            if let Some(started) = self.command_started {
                probe.post(self.order.client_order_id(), started);
            }
            probe.count(crate::latency::LatencyCounter::CreateCalls);
            probe.timer(crate::latency::LatencyMetric::CreateRequest)
        });
        let receipt = if let Some(amounts) = amounts {
            if self.order.time_in_force() == TimeInForce::Fok {
                self.config
                    .session
                    .request_create_market_fok_order(&signed, &amounts)
                    .await
            } else {
                self.config
                    .session
                    .request_create_market_order(&signed, &amounts)
                    .await
            }
        } else {
            self.config
                .session
                .request_create_limit_order_with_options(
                    &signed,
                    self.terms.price,
                    self.limit_options,
                )
                .await
        };
        if let Some(timing) = timing {
            timing.finish();
        }
        let receipt = match receipt {
            Ok(receipt) => receipt,
            Err(error) => {
                // Definitive venue rejection is published without account IO or retry.
                // A nonce failure invalidates prepared signing context, waking the existing
                // background collector. Ordinary funding failures do not evict metadata.
                if matches!(
                    &error,
                    crate::http::error::Error::Api {
                        reason: crate::http::error::PredictApiErrorReason::InvalidNonce,
                        ..
                    }
                ) {
                    let mut sync = self
                        .account_sync
                        .state
                        .lock()
                        .expect("account sync poisoned");
                    let value = &mut sync.signing_nonces[metadata.market.exchange().cache_index()];
                    if *value == Some(nonce) {
                        *value = None;
                        self.account_sync.nonce_notify.notify_one();
                    }
                }
                return Err(classify_http_command_failure(&error));
            }
        };
        super::routing::register(&self.rest_ids, id, receipt.order_id).map_err(|error| {
            self.account_refresh_required.store(true, Ordering::Release);
            CommandFailure::Ambiguous(error.to_string())
        })?;
        if self.terms.market.is_none() {
            let removal_locked_until = receipt.removal_lock_deadline().map_err(|_| {
                CommandFailure::ambiguous(
                    "Predict create receipt has invalid removal lock timestamp",
                )
            })?;
            // Receipt metadata uses the existing Native data channel, with no retained OMS.
            // An old transport generation cannot publish into a renewed session.
            if self.available()
                && let Some(sender) = &self.data_sender
            {
                let now = self.clock.get_time_ns();
                let update = super::receipt::PredictLimitOrderReceiptUpdate {
                    account_id: self.factory.account_id(),
                    strategy_id: self.order.strategy_id(),
                    instrument_id: self.order.instrument_id(),
                    client_order_id: self.order.client_order_id(),
                    venue_order_id: id,
                    is_post_only: self.limit_options.is_post_only,
                    self_trade_prevention: self.limit_options.self_trade_prevention,
                    removal_locked_until,
                    connection_epoch: self.admitted_epoch,
                    ts_event: now,
                    ts_init: now,
                };
                if sender
                    .send(nautilus_common::messages::DataEvent::Data(
                        nautilus_model::data::Data::Custom(update.into_custom_data()),
                    ))
                    .is_err()
                {
                    // Optional metadata delivery does not change the remote order outcome.
                    log::warn!("Predict LIMIT receipt metadata channel closed");
                }
            }
        }
        // The receipt's opaque code is never interpreted as native acceptance.
        Ok(())
    }
}
