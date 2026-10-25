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

//! Venue facts mapped to Nautilus reports using immutable snapshots of cached native orders.

use alloy::primitives::{Address, B256, Keccak256, U256};
use nautilus_common::messages::ExecutionReport;
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{LiquiditySide, OrderSide, OrderStatus, OrderType, TimeInForce},
    identifiers::{AccountId, InstrumentId, TradeId, VenueOrderId},
    instruments::{BinaryOption, Instrument},
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

use crate::{
    common::{enums::PredictOrderSide, instrument_id_from_outcome, parse::exact_wei},
    http::account_models::{
        PredictFeeType, PredictOrderData, PredictOrderStatus, PredictOrderStrategy,
    },
    websocket::private_messages::{
        PredictWalletCancellationReason, PredictWalletEvent, PredictWalletEventKind,
        PredictWalletOutcome, PredictWalletQuoteType, PredictWalletRejectionReason,
    },
};

/// Deterministic order-scoped settlement ID, using 128 digest bits within the native
/// 36-character TradeId limit. No timestamp or random event ID is substituted.
#[must_use]
pub fn settlement_trade_id(settlement_id: &str, order_hash: B256) -> TradeId {
    let mut hasher = Keccak256::new();
    hasher.update(b"predict.fill\0");
    hasher.update((settlement_id.len() as u64).to_be_bytes());
    hasher.update(settlement_id.as_bytes());
    hasher.update(order_hash.as_slice());
    let digest = hasher.finalize();
    // Nautilus TradeId is a 36-character stack string. Keep a domain-prefixed 128-bit
    // digest; full order hash and venue settlement facts are retained separately.
    TradeId::new(format!(
        "PF-{:x}",
        alloy::primitives::B128::from_slice(&digest.as_slice()[..16])
    ))
}

pub(super) fn quantity(value: Decimal, precision: u8) -> anyhow::Result<Quantity> {
    anyhow::ensure!(
        value.round_dp(u32::from(precision)) == value,
        "Predict quantity requires rounding"
    );
    Quantity::from_decimal_dp(value, precision)
        .map_err(|_| anyhow::anyhow!("Predict quantity exceeds Nautilus range"))
}

pub(super) fn price(value: Decimal, precision: u8) -> anyhow::Result<Price> {
    anyhow::ensure!(
        (Decimal::ZERO..=Decimal::ONE).contains(&value)
            && value.round_dp(u32::from(precision)) == value,
        "Predict price is outside the outcome range or requires rounding"
    );
    Price::from_decimal_dp(value, precision)
        .map_err(|_| anyhow::anyhow!("Predict price exceeds Nautilus range"))
}

pub(super) fn execution_price(value: Decimal, instrument_precision: u8) -> anyhow::Result<Price> {
    // Quote increments constrain orders, not the representable precision of a venue execution.
    let precision = instrument_precision.max(value.normalize().scale().try_into()?);
    price(value, precision)
}

/// Parses lifecycle or confirmed fill evidence. Submitted settlements emit no fill report.
/// Unknown fees, nonzero share fees, missing acceptance evidence and contradictory rejections
/// require reconciliation; they are not approximated. Lifecycle acknowledgements do not wait
/// for missing fills; their reports retain only independently confirmed quantity.
/// Raw venue IDs and hashes stay distinct.
///
/// # Errors
/// Returns an error when context, quantities, timestamps or fee semantics are insufficient.
pub fn parse_wallet_report(
    event: &PredictWalletEvent,
    order: &OrderAny,
    instrument: &BinaryOption,
    account_id: AccountId,
    wallet: Address,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<ExecutionReport>> {
    parse_wallet_report_with_confirmed_qty(
        event,
        order,
        instrument,
        account_id,
        wallet,
        ts_init,
        order.filled_qty(),
    )
}

pub(super) fn parse_wallet_report_with_confirmed_qty(
    event: &PredictWalletEvent,
    order: &impl super::context::OrderView,
    instrument: &BinaryOption,
    account_id: AccountId,
    wallet: Address,
    ts_init: UnixNanos,
    confirmed_qty: Quantity,
) -> anyhow::Result<Option<ExecutionReport>> {
    parse_wallet_report_with_registration(
        event,
        order,
        instrument,
        account_id,
        wallet,
        ts_init,
        WalletReportEvidence {
            venue_id: VenueOrderId::new(format!("{:#x}", event.order.order_hash)),
            instrument_id: instrument_id_from_outcome(
                event.order.details.market_id,
                match event.order.details.outcome {
                    PredictWalletOutcome::Yes => 1,
                    PredictWalletOutcome::No => 2,
                },
            )?,
            confirmed_qty,
            pending_settlement: false,
            registered: None,
            queued_acceptance: None,
            normalize: false,
            allow_share_fees: false,
        },
    )
}

/// Message-derived identity and queued Native facts, without retained execution state.
pub(super) struct WalletReportEvidence {
    pub venue_id: VenueOrderId,
    pub instrument_id: InstrumentId,
    pub confirmed_qty: Quantity,
    pub pending_settlement: bool,
    pub registered: Option<VenueOrderId>,
    pub queued_acceptance: Option<UnixNanos>,
    pub normalize: bool,
    pub allow_share_fees: bool,
}

pub(super) fn parse_wallet_report_with_registration(
    event: &PredictWalletEvent,
    order: &impl super::context::OrderView,
    instrument: &BinaryOption,
    account_id: AccountId,
    wallet: Address,
    ts_init: UnixNanos,
    evidence: WalletReportEvidence,
) -> anyhow::Result<Option<ExecutionReport>> {
    let WalletReportEvidence {
        venue_id,
        instrument_id: id,
        confirmed_qty,
        pending_settlement,
        registered,
        queued_acceptance,
        normalize,
        allow_share_fees,
    } = evidence;
    anyhow::ensure!(
        event.order.wallet_address == wallet
            && (order.account_id() == Some(account_id)
                || (registered.is_some() && order.account_id().is_none())),
        "Predict wallet/account context mismatch"
    );
    anyhow::ensure!(
        order.venue_order_id() == Some(venue_id)
            || (registered == Some(venue_id) && order.venue_order_id().is_none()),
        "Predict wallet order hash differs from cached venue order ID"
    );
    let index = match event.order.details.outcome {
        PredictWalletOutcome::Yes => 1,
        PredictWalletOutcome::No => 2,
    };
    let side = match event.order.details.quote_type {
        PredictWalletQuoteType::Bid => OrderSide::Buy,
        PredictWalletQuoteType::Ask => OrderSide::Sell,
    };
    anyhow::ensure!(
        order.instrument_id() == id
            && instrument.id() == id
            && order.order_side() == side
            && !order.is_quote_quantity(),
        "Predict wallet order/instrument context mismatch"
    );
    let details = &event.order.details;
    anyhow::ensure!(
        matches!(
            (details.strategy_type, order.order_type()),
            (PredictOrderStrategy::Limit, OrderType::Limit)
                | (PredictOrderStrategy::Market, OrderType::Market)
        ),
        "Predict wallet strategy differs from native order context"
    );
    anyhow::ensure!(
        order
            .price()
            .is_none_or(|price| price.as_decimal() == details.price),
        "Predict wallet limit price differs from native order"
    );
    anyhow::ensure!(
        details.outcome_index == index
            && details.quantity_filled >= Decimal::ZERO
            && details.quantity_filled <= details.quantity
            && confirmed_qty <= order.submitted_quantity(),
        "Predict order quantities or outcome index are inconsistent"
    );
    anyhow::ensure!(
        details.quantity == order.submitted_quantity().as_decimal(),
        "Predict wallet quantity differs from cached order"
    );
    let ts_event = event.order.timestamp_ns()?;
    // An already accepted native order can receive its original ACK after REST fill recovery.
    // Identity and immutable terms above must still match; a historical ACK cannot reopen it.
    if matches!(event.kind, PredictWalletEventKind::OrderAccepted)
        && order.ts_accepted().or(queued_acceptance).is_some()
        && ts_event <= order.ts_last().max(queued_acceptance.unwrap_or_default())
        && (details.quantity_filled < confirmed_qty.as_decimal()
            || order.is_closed()
                && order.status() != OrderStatus::Filled
                && details.quantity_filled <= confirmed_qty.as_decimal())
    {
        return Ok(None);
    }
    match &event.kind {
        PredictWalletEventKind::OrderNotAccepted { reason }
            if pending_settlement
                || !confirmed_qty.is_zero()
                || details.quantity_filled > Decimal::ZERO
                || !matches!(
                    reason,
                    PredictWalletRejectionReason::NoMarketMatch
                        | PredictWalletRejectionReason::RejectedPostOnly
                ) =>
        {
            // The documented rejection envelope does not establish whether a
            // duplicate attempt or a partially executed original order ended.
            // Preserve existing execution and require independent lifecycle evidence.
            Ok(None)
        }
        PredictWalletEventKind::OrderTransactionSubmitted { .. }
        | PredictWalletEventKind::OrderMarketMatchPlan { .. } => Ok(None),
        PredictWalletEventKind::OrderTransactionFailed { .. } => Ok(None),
        PredictWalletEventKind::OrderTransactionSuccess {
            settlement_id,
            fill,
            is_maker,
            fee,
        } => {
            anyhow::ensure!(
                !settlement_id.is_empty(),
                "Predict fill lacks settlement identity"
            );
            if order.is_post_only() && *is_maker == Some(false) {
                log::warn!(
                    "Predict confirmed execution reports taker liquidity for post-only {}",
                    order.client_order_id()
                );
            }
            let (fee_amount, fee_type) = match fee {
                Some(fee) => (fee.amount_wei, fee.fee_type),
                None if *is_maker == Some(true) => (U256::ZERO, PredictFeeType::Collateral),
                None => anyhow::bail!("Predict fill lacks fee evidence"),
            };
            let liquidity = match is_maker {
                Some(true) => LiquiditySide::Maker,
                Some(false) => LiquiditySide::Taker,
                None => LiquiditySide::NoLiquiditySide,
            };
            anyhow::ensure!(
                instrument.quote_currency() == Currency::USDT(),
                "Predict collateral currency must be USDT"
            );
            if !normalize && fee_type == PredictFeeType::Collateral {
                let value = exact_wei(fee_amount)?;
                Money::from_decimal_exact(value, Currency::USDT()).map_err(|_| {
                    anyhow::anyhow!("Predict commission requires rounding at native precision")
                })?;
            }
            let commission = fill_commission(
                fee_amount,
                fee_type,
                fill.executed_size_wei,
                side,
                liquidity,
                allow_share_fees,
            )?;
            let qty = crate::common::parse::exact_wei_quantity(fill.executed_size_wei)?;
            anyhow::ensure!(
                !fill.executed_size_wei.is_zero(),
                "Predict confirmed fill has zero size"
            );
            let px = execution_price(
                exact_wei(fill.executed_price_wei)?,
                instrument.price_precision(),
            )?;
            let product = fill
                .executed_price_wei
                .checked_mul(fill.executed_size_wei)
                .ok_or_else(|| anyhow::anyhow!("Predict confirmed fill value overflows U256"))?;
            let scale = U256::from(1_000_000_000_000_000_000u64);
            anyhow::ensure!(
                product / scale == fill.executed_value_wei,
                "Predict confirmed fill price, size and value are inconsistent"
            );
            if let Some(limit) = order.price()
                && match side {
                    OrderSide::Buy => px > limit,
                    OrderSide::Sell => px < limit,
                }
            {
                log::warn!(
                    "Predict confirmed execution price differs from limit intent for {}",
                    order.client_order_id()
                );
            }
            let mut report = FillReport::new(
                account_id,
                id,
                venue_id,
                settlement_trade_id(settlement_id, event.order.order_hash),
                side,
                qty,
                px,
                commission,
                liquidity,
                Some(order.client_order_id()),
                None,
                ts_event,
                ts_init,
                None,
            );
            if fee_type == PredictFeeType::Shares && !fee_amount.is_zero() {
                report.quantity_commission =
                    Some(crate::common::parse::exact_wei_quantity(fee_amount)?);
            }
            Ok(Some(ExecutionReport::Fill(Box::new(report))))
        }
        kind => {
            // Acceptance/cancellation/expiry is separate from settlement. Native can
            // apply late fills independently; never manufacture them here.
            let (status, accepted) = match kind {
                PredictWalletEventKind::OrderAccepted => (
                    if confirmed_qty == order.quantity() {
                        OrderStatus::Filled
                    } else if confirmed_qty.is_zero() {
                        OrderStatus::Accepted
                    } else {
                        OrderStatus::PartiallyFilled
                    },
                    order
                        .ts_accepted()
                        .or(queued_acceptance)
                        .unwrap_or(ts_event),
                ),
                PredictWalletEventKind::OrderNotAccepted { .. } => (
                    OrderStatus::Rejected,
                    order.ts_accepted().unwrap_or_default(),
                ),
                PredictWalletEventKind::OrderCancelled { .. } => (
                    OrderStatus::Canceled,
                    order
                        .ts_accepted()
                        .or(queued_acceptance)
                        .unwrap_or(ts_event),
                ),
                PredictWalletEventKind::OrderExpired => (
                    OrderStatus::Expired,
                    order
                        .ts_accepted()
                        .or(queued_acceptance)
                        .unwrap_or(ts_event),
                ),
                _ => anyhow::bail!("unsupported Predict wallet lifecycle event"),
            };
            let mut report = OrderStatusReport::new(
                account_id,
                id,
                Some(order.client_order_id()),
                venue_id,
                Some(side),
                order.order_type(),
                order.time_in_force(),
                status,
                order.quantity(),
                confirmed_qty,
                accepted,
                ts_event,
                ts_init,
                None,
            )
            .with_post_only(order.is_post_only())
            .with_reduce_only(order.is_reduce_only());
            // Native reconciliation uses cancel_reason for both rejection and cancellation,
            // including its post-only rejection flag. Do not drop typed wallet reasons.
            let reason = match kind {
                PredictWalletEventKind::OrderNotAccepted {
                    reason: PredictWalletRejectionReason::NoMarketMatch,
                } => Some("Predict no market match"),
                PredictWalletEventKind::OrderNotAccepted {
                    reason: PredictWalletRejectionReason::RejectedPostOnly,
                } => Some("Predict post-only order rejected"),
                PredictWalletEventKind::OrderCancelled {
                    reason: Some(reason),
                } => Some(match reason {
                    PredictWalletCancellationReason::SelfTradePrevention => {
                        "Predict self-trade prevention"
                    }
                    PredictWalletCancellationReason::Unknown => {
                        "Predict unknown cancellation reason"
                    }
                }),
                _ => None,
            };
            if let Some(reason) = reason {
                report = report.with_cancel_reason(reason.to_string());
            }
            if let Some(value) = order.price() {
                report = report.with_price(value);
            }
            if let Some(expire_time) = order.expire_time() {
                report.expire_time = Some(expire_time);
            }
            Ok(Some(ExecutionReport::Order(Box::new(report))))
        }
    }
}

pub(crate) fn market_id_from_instrument(id: &InstrumentId) -> anyhow::Result<u64> {
    let (market, outcome) = id
        .symbol
        .as_str()
        .split_once('-')
        .ok_or_else(|| anyhow::anyhow!("invalid Predict outcome symbol"))?;
    let market: u64 = market
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid Predict market ID"))?;
    let outcome: u8 = outcome
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid Predict outcome index"))?;
    anyhow::ensure!(
        instrument_id_from_outcome(market, outcome)? == *id,
        "noncanonical Predict instrument ID"
    );
    Ok(market)
}

/// Maps a REST order observation for a known Native order. Signed
/// maker/taker amounts establish base quantity and the LIMIT price; a recovered MARKET
/// retains its native missing price instead of inventing a limit. Nonzero `amountFilled` is not
/// scaled or substituted for confirmed settlements. `ts_last` is observation time, as for other
/// snapshot reports; native TIF/flags are retained. A canceled/expired observation reports
/// only the known Native filled quantity; production recovery pairs it with actual fills.
///
/// # Errors
/// Missing context, changed order terms, unresolved fills or invalidation semantics return errors.
pub fn parse_rest_order_report(
    row: &PredictOrderData,
    order: &OrderAny,
    instrument: &BinaryOption,
    account_id: AccountId,
    wallet: Address,
    observed: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    rest_order_report_from_view(row, order, instrument, account_id, wallet, observed)
}
pub(super) fn rest_order_report_from_view(
    row: &PredictOrderData,
    order: &impl super::context::OrderView,
    instrument: &BinaryOption,
    account_id: AccountId,
    wallet: Address,
    observed: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    let venue_id = validate_rest_order_terms(row, order, instrument, account_id, wallet)?;
    rest_order_report(
        row, order, instrument, account_id, venue_id, observed, false,
    )
}

/// A locally transmitted hash plus authenticated venue evidence can recover a missing
/// wallet acknowledgement. Zero-fill terminal MARKET FOK evidence also resolves this
/// path. The endpoint has no timestamps: acceptance uses observation time only on this
/// path; nonzero fills require settlement recovery.
pub(super) fn parse_submitted_order_report(
    row: &PredictOrderData,
    order: &impl super::context::OrderView,
    instrument: &BinaryOption,
    hasher: &crate::signing::PredictOrderHasher,
    venue_id: VenueOrderId,
    observed: UnixNanos,
) -> anyhow::Result<OrderStatusReport> {
    anyhow::ensure!(
        matches!(
            order.status(),
            OrderStatus::Submitted | OrderStatus::PendingCancel
        ) && order.ts_submitted().is_some()
            && order.venue_order_id().is_none()
            && order.ts_accepted().is_none()
            && matches!(
                row.status,
                PredictOrderStatus::Open
                    | PredictOrderStatus::Cancelled
                    | PredictOrderStatus::Expired
            ),
        "Predict missing acknowledgement recovery requires OPEN or terminal evidence"
    );
    let account_id = order
        .account_id()
        .ok_or_else(|| anyhow::anyhow!("Predict submitted order lacks its account"))?;
    let exchange = crate::common::contracts::PredictExchangeType::from_market_flags(
        row.is_neg_risk,
        row.is_yield_bearing,
    );
    let hash = hasher.order_hash(&row.order.fields, exchange);
    anyhow::ensure!(
        venue_id == VenueOrderId::new(format!("{hash:#x}")),
        "Predict queried contract fields do not reproduce the transmitted hash"
    );
    validate_rest_order_terms_for_hash(
        row,
        order,
        instrument,
        account_id,
        row.order.fields.maker,
        venue_id,
    )?;
    rest_order_report(row, order, instrument, account_id, venue_id, observed, true)
}

fn rest_order_report(
    row: &PredictOrderData,
    order: &impl super::context::OrderView,
    instrument: &BinaryOption,
    account_id: AccountId,
    venue_id: VenueOrderId,
    observed: UnixNanos,
    recovering: bool,
) -> anyhow::Result<OrderStatusReport> {
    let side = order.order_side();
    let filled = Decimal::from_str_exact(&row.amount_filled)
        .map_err(|_| anyhow::anyhow!("Predict REST filled amount is invalid"))?;
    anyhow::ensure!(
        (matches!(
            row.status,
            PredictOrderStatus::Cancelled | PredictOrderStatus::Expired
        ) && order.status() != OrderStatus::Rejected
            && filled >= Decimal::ZERO)
            || filled.is_zero() && order.filled_qty().is_zero(),
        "Predict nonzero REST fills require verified units and settlement reconciliation"
    );
    let status = match row.status {
        PredictOrderStatus::Open => {
            anyhow::ensure!(
                !order.is_closed(),
                "Predict REST open snapshot conflicts with terminal cached order"
            );
            OrderStatus::Accepted
        }
        // Wallet noMarketMatch is Rejected, while a zero-fill MARKET can
        // appear as CANCELLED in REST. Preserve the applied Native terminal fact;
        // without wallet evidence retain REST cancellation rather than invent a rejection.
        PredictOrderStatus::Cancelled
            if order.order_type() == OrderType::Market
                && order.status() == OrderStatus::Rejected =>
        {
            OrderStatus::Rejected
        }
        PredictOrderStatus::Cancelled => OrderStatus::Canceled,
        PredictOrderStatus::Expired => OrderStatus::Expired,
        PredictOrderStatus::Filled | PredictOrderStatus::Invalidated => {
            anyhow::bail!("Predict REST status requires settlement or invalidation evidence")
        }
    };
    let accepted = if recovering {
        anyhow::ensure!(
            observed >= order.ts_last(),
            "Predict recovery observation precedes submission"
        );
        observed
    } else if status == OrderStatus::Rejected {
        order.ts_accepted().unwrap_or_default()
    } else {
        order.ts_accepted().unwrap_or(observed)
    };
    anyhow::ensure!(
        !order.is_closed() || order.status() == status,
        "Predict REST terminal snapshot conflicts with terminal cached order"
    );
    anyhow::ensure!(
        observed >= accepted,
        "Predict REST observation precedes acceptance"
    );
    let mut report = OrderStatusReport::new(
        account_id,
        instrument.id(),
        Some(order.client_order_id()),
        venue_id,
        Some(side),
        order.order_type(),
        order.time_in_force(),
        status,
        order.quantity(),
        order.filled_qty(),
        accepted,
        observed,
        observed,
        None,
    )
    .with_post_only(order.is_post_only())
    .with_reduce_only(order.is_reduce_only());
    if let Some(px) = order.price() {
        report = report.with_price(px);
    }
    report.expire_time = order.expire_time();
    Ok(report)
}

/// Validates identity/base quantity and original LIMIT price without interpreting frontend
/// cumulative fills. A recovered MARKET has no native fixed price or original signed budget;
/// authenticated signed collateral is bounded to the binary range, never inferred from VWAP.
pub(super) fn validate_rest_order_terms(
    row: &PredictOrderData,
    order: &impl super::context::OrderView,
    instrument: &BinaryOption,
    account_id: AccountId,
    wallet: Address,
) -> anyhow::Result<VenueOrderId> {
    let venue_id = order
        .venue_order_id()
        .ok_or_else(|| anyhow::anyhow!("Predict REST report requires a cached venue hash"))?;
    validate_rest_order_terms_for_hash(row, order, instrument, account_id, wallet, venue_id)
}

pub(super) fn validate_rest_order_terms_for_hash(
    row: &PredictOrderData,
    order: &impl super::context::OrderView,
    instrument: &BinaryOption,
    account_id: AccountId,
    wallet: Address,
    venue_id: VenueOrderId,
) -> anyhow::Result<VenueOrderId> {
    let hash: B256 = venue_id
        .as_str()
        .parse()
        .map_err(|_| anyhow::anyhow!("Predict venue order ID is not an exchange hash"))?;
    let fields = &row.order.fields;
    anyhow::ensure!(
        row.order.hash == Some(hash)
            && venue_id == VenueOrderId::new(format!("{hash:#x}"))
            && fields.maker == wallet
            && fields.signer == wallet
            && fields.taker.is_zero()
            && order.account_id() == Some(account_id)
            && order.instrument_id() == instrument.id()
            && market_id_from_instrument(&instrument.id())? == row.market_id
            && instrument.raw_symbol().as_str().parse::<U256>().ok() == Some(fields.token_id)
            && row.currency == "USDT"
            && instrument.quote_currency() == Currency::USDT(),
        "Predict REST order identity or collateral context differs from native context"
    );
    anyhow::ensure!(
        (matches!(
            (row.strategy, order.order_type()),
            (PredictOrderStrategy::Limit, OrderType::Limit)
        ) && matches!(order.time_in_force(), TimeInForce::Gtc | TimeInForce::Gtd)
            || matches!(
                (row.strategy, order.order_type()),
                (PredictOrderStrategy::Market, OrderType::Market)
            ) && matches!(order.time_in_force(), TimeInForce::Gtc | TimeInForce::Fok))
            && !order.is_quote_quantity(),
        "Predict REST reporting requires matching cached base-quantity order strategy"
    );
    if let Some(expiry) = order.expire_time() {
        let seconds: u64 = fields
            .expiration
            .try_into()
            .map_err(|_| anyhow::anyhow!("Predict REST expiry exceeds u64"))?;
        anyhow::ensure!(
            seconds.checked_mul(1_000_000_000) == Some(expiry.as_u64()),
            "Predict REST expiration differs from cached order"
        );
    }
    let side = match fields.side {
        PredictOrderSide::Buy => OrderSide::Buy,
        PredictOrderSide::Sell => OrderSide::Sell,
    };
    anyhow::ensure!(
        side == order.order_side(),
        "Predict REST order side differs from cached order"
    );
    let (shares, collateral) = if side == OrderSide::Buy {
        (fields.taker_amount, fields.maker_amount)
    } else {
        (fields.maker_amount, fields.taker_amount)
    };
    let shares = crate::common::parse::exact_wei_quantity(shares)?;
    anyhow::ensure!(
        !shares.is_zero()
            && shares == order.submitted_quantity()
            && !collateral.is_zero()
            && collateral
                <= match side {
                    OrderSide::Buy => fields.taker_amount,
                    _ => fields.maker_amount,
                },
        "Predict REST signed quantity or price differs from cached order"
    );
    if let Some(budget) = order.quote_buy_budget() {
        anyhow::ensure!(
            exact_wei(collateral)? <= budget,
            "Predict REST signed collateral exceeds original Native quote budget"
        );
    }
    if let Some(px) = order.price() {
        anyhow::ensure!(
            (Decimal::ZERO..=Decimal::ONE).contains(&px.as_decimal())
                && shares.as_decimal().checked_mul(px.as_decimal()) == Some(exact_wei(collateral)?),
            "Predict REST signed quantity or price differs from cached order"
        );
    }
    Ok(venue_id)
}

/// Confirms a cross-transport replay against native applied events. REST execution time
/// and wallet notification time can differ; identity and economic facts must agree.
pub(super) fn validate_applied_fill(
    report: &FillReport,
    order: &impl super::context::OrderView,
) -> anyhow::Result<bool> {
    order.with_applied_fill(report.trade_id, |fill| {
        if let Some(fill) = fill {
            validate_native_fill(report, fill)?;
            return Ok(true);
        }
        Ok(false)
    })
}

pub(super) fn validate_native_fill(
    report: &FillReport,
    fill: &nautilus_model::events::OrderFilled,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        fill.account_id == report.account_id
            && fill.instrument_id == report.instrument_id
            && fill.venue_order_id == report.venue_order_id
            && fill.last_qty == report.last_qty
            && fill.last_px == report.last_px
            && fill.commission == Some(report.commission)
            && fill.quantity_commission == report.quantity_commission
            && fill.liquidity_side == report.liquidity_side
            && fill.order_side == report.order_side,
        "Predict match contradicts an already applied native fill"
    );
    Ok(())
}

pub(super) fn fill_commission(
    fee: U256,
    asset: PredictFeeType,
    gross: U256,
    side: OrderSide,
    liquidity: LiquiditySide,
    allow_share_fees: bool,
) -> anyhow::Result<Money> {
    anyhow::ensure!(
        liquidity != LiquiditySide::Maker || fee.is_zero(),
        "Predict maker fill carries an unexpected nonzero fee"
    );
    if asset == PredictFeeType::Shares && !fee.is_zero() {
        anyhow::ensure!(
            side == OrderSide::Buy && liquidity == LiquiditySide::Taker && fee < gross,
            "Predict share fee is incompatible with the fill direction, liquidity or size"
        );
        anyhow::ensure!(
            allow_share_fees,
            "Predict share fee requires a reported cash account without calculated balances"
        );
        return Ok(Money::zero(Currency::USDT()));
    }
    let amount = exact_wei(fee)?.round_dp(u32::from(nautilus_model::types::fixed::FIXED_PRECISION));
    Ok(Money::from_decimal_exact(amount, Currency::USDT())?)
}
