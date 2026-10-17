// -------------------------------------------------------------------------------------------------
// Copyright (C) 2026 yfclark and contributors. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// See https://www.gnu.org/licenses/lgpl-3.0.html for license details.
// -------------------------------------------------------------------------------------------------

//! Predict fee-equivalent estimates through Nautilus' existing FeeModel interface.
//! Reported settlement fees remain authoritative. This model does not adjust share inventory.

use nautilus_execution::models::fee::FeeModel;
use nautilus_model::{
    enums::LiquiditySide,
    fees::{MakerTakerFeeRates, MakerTakerFeeSchedule},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    types::{Money, Price, Quantity},
};
use rust_decimal::Decimal;

/// Maker-zero, probability-dependent taker commission for binary-option replay.
/// Rates are explicit inputs from market discovery; account discounts are never assumed.
#[derive(Clone, Debug)]
pub struct PredictFeeModel {
    schedule: MakerTakerFeeSchedule,
    discount: Decimal,
}

impl PredictFeeModel {
    /// Creates a model with a default market base fee in basis points.
    ///
    /// # Errors
    /// A rate greater than 10,000 bps is rejected.
    pub fn new(fee_rate_bps: u32) -> anyhow::Result<Self> {
        Ok(Self {
            schedule: MakerTakerFeeSchedule::new(Decimal::ZERO, rate(fee_rate_bps)?),
            discount: Decimal::ONE,
        })
    }

    /// Configures an independently verified discount (1,000 bps means 10% off).
    ///
    /// # Errors
    /// A discount greater than 10,000 bps is rejected.
    pub fn with_discount(mut self, discount_bps: u32) -> anyhow::Result<Self> {
        self.discount = Decimal::ONE - rate(discount_bps)?;
        Ok(self)
    }

    /// Sets the discovered base fee for an exact instrument identity.
    ///
    /// # Errors
    /// A rate greater than 10,000 bps is rejected without changing the schedule.
    pub fn set_override(&mut self, id: InstrumentId, fee_rate_bps: u32) -> anyhow::Result<()> {
        let rates = MakerTakerFeeRates::new(Decimal::ZERO, rate(fee_rate_bps)?);
        self.schedule.set_override(id, rates);
        Ok(())
    }

    /// Calculates a quote-currency fee equivalent without changing fill quantity.
    ///
    /// # Errors
    /// Non-binary instruments, unknown liquidity, invalid prices or overflow are rejected.
    pub fn calculate_commission(
        &self,
        instrument: &InstrumentAny,
        quantity: Quantity,
        price: Price,
        liquidity_side: LiquiditySide,
    ) -> anyhow::Result<Money> {
        anyhow::ensure!(
            matches!(instrument, InstrumentAny::BinaryOption(_)),
            "PredictFeeModel requires a binary option instrument"
        );
        let p = price.as_decimal();
        anyhow::ensure!(
            (Decimal::ZERO..=Decimal::ONE).contains(&p),
            "Predict fee price must be in [0, 1]"
        );
        let fee_rate = self.schedule.rate_for(instrument.id(), liquidity_side)?;
        let amount = quantity
            .as_decimal()
            .checked_mul(p.min(Decimal::ONE - p))
            .and_then(|v| v.checked_mul(fee_rate))
            .and_then(|v| v.checked_mul(self.discount))
            .ok_or_else(|| anyhow::anyhow!("Predict fee-equivalent calculation overflow"))?;
        let currency = instrument.quote_currency();
        // Monetary estimates use the native currency precision, not venue settlement rounding.
        Money::from_decimal(amount.round_dp(u32::from(currency.precision)), currency)
            .map_err(Into::into)
    }
}

impl FeeModel for PredictFeeModel {
    fn get_commission(
        &self,
        order: &OrderAny,
        fill_quantity: Quantity,
        fill_px: Price,
        instrument: &InstrumentAny,
    ) -> anyhow::Result<Money> {
        anyhow::ensure!(
            order.instrument_id() == instrument.id(),
            "Predict fee order/instrument identity mismatch"
        );
        let side = order
            .liquidity_side()
            .ok_or_else(|| anyhow::anyhow!("Predict fee liquidity side is missing"))?;
        self.calculate_commission(instrument, fill_quantity, fill_px, side)
    }
}

fn rate(bps: u32) -> anyhow::Result<Decimal> {
    anyhow::ensure!(bps <= 10_000, "Predict fee basis points exceed 10,000");
    Ok(Decimal::from(bps) / Decimal::from(10_000))
}
