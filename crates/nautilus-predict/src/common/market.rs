// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Exact SDK MARKET amount preparation. This module neither selects markets nor submits orders.

use alloy::primitives::U256;

use super::enums::PredictOrderSide;

/// Prepared base-quantity MARKET terms, before signing or collateral/inventory admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PredictMarketOrderAmounts {
    pub side: PredictOrderSide,
    pub price_per_share: U256,
    pub last_price: U256,
    pub maker_amount: U256,
    pub taker_amount: U256,
    pub amount: U256,
    pub slippage_bps: u32,
}

impl PredictMarketOrderAmounts {
    /// Checks that public metadata still describes the prepared signed worst-price bounds.
    ///
    /// # Errors
    /// Rejects inconsistent sides, amount ratios, slippage and average/worst prices.
    pub fn validate(&self) -> anyhow::Result<()> {
        let scale = U256::from(1_000_000_000_000_000_000u64);
        anyhow::ensure!(
            self.slippage_bps < 10_000
                && self.amount.to_string().trim_end_matches('0').len() <= 5
                && self.amount >= scale / U256::from(100)
                && self.price_per_share > U256::ZERO
                && self.price_per_share <= scale
                && self.last_price > U256::ZERO
                && self.last_price <= scale,
            "MARKET metadata outside supported ranges"
        );
        let bps = U256::from(10_000);
        let base = self
            .last_price
            .checked_mul(self.amount)
            .ok_or_else(|| anyhow::anyhow!("MARKET metadata amount overflow"))?
            / scale;
        let factor = match self.side {
            PredictOrderSide::Buy => bps + U256::from(self.slippage_bps),
            PredictOrderSide::Sell => bps - U256::from(self.slippage_bps),
        };
        let collateral = base
            .checked_mul(factor)
            .ok_or_else(|| anyhow::anyhow!("MARKET metadata slippage overflow"))?
            / bps;
        let valid = match self.side {
            PredictOrderSide::Buy => {
                self.taker_amount == self.amount
                    && self.maker_amount == collateral.min(self.amount)
                    && self.price_per_share <= self.last_price
            }
            PredictOrderSide::Sell => {
                self.maker_amount == self.amount
                    && self.taker_amount == collateral
                    && self.price_per_share >= self.last_price
            }
        };
        anyhow::ensure!(
            valid && !self.maker_amount.is_zero() && !self.taker_amount.is_zero(),
            "MARKET signed amounts differ from SDK worst-price/slippage bounds"
        );
        Ok(())
    }

    /// Prepares quantity-based MARKET terms from a strategy-selected reference price.
    /// This is the SDK amount calculation for a single level covering the quantity;
    /// it does not assert that such liquidity exists or promise an execution price.
    ///
    /// # Errors
    /// Rejects the same quantity, price, slippage and overflow boundaries as `from_levels`.
    pub fn from_price(
        side: PredictOrderSide,
        quantity: U256,
        price: U256,
        slippage_bps: u32,
    ) -> anyhow::Result<Self> {
        Self::from_levels(side, quantity, &[(price, quantity)], slippage_bps)
    }

    /// Matches SDK quantity-based default slippage, preserving the requested base quantity.
    /// Levels must be sorted asks for BUY or sorted bids for SELL, already converted to
    /// the selected outcome. Insufficient depth and SDK quantity truncation fail explicitly.
    ///
    /// # Errors
    /// Invalid/unsorted levels, quantities below 0.01 shares, more than five significant
    /// quantity digits, excessive slippage, insufficient depth and U256 overflow are rejected.
    pub fn from_levels(
        side: PredictOrderSide,
        quantity: U256,
        levels: &[(U256, U256)],
        slippage_bps: u32,
    ) -> anyhow::Result<Self> {
        let scale = U256::from(1_000_000_000_000_000_000u64);
        anyhow::ensure!(
            quantity >= scale / U256::from(100),
            "MARKET quantity minimum is 0.01 shares"
        );
        anyhow::ensure!(
            quantity.to_string().trim_end_matches('0').len() <= 5,
            "MARKET SDK quantity preparation would change native quantity"
        );
        anyhow::ensure!(
            slippage_bps < 10_000,
            "MARKET slippage must be below 10000 bps"
        );
        let mut previous = None;
        let mut remaining = quantity;
        let mut weighted = U256::ZERO;
        let mut last_price = U256::ZERO;
        for &(price, available) in levels {
            anyhow::ensure!(
                price > U256::ZERO && price <= scale && available > U256::ZERO,
                "MARKET depth contains invalid price or quantity"
            );
            if let Some(previous) = previous {
                anyhow::ensure!(
                    match side {
                        PredictOrderSide::Buy => price > previous,
                        PredictOrderSide::Sell => price < previous,
                    },
                    "MARKET depth is unsorted or contains duplicate prices"
                );
            }
            previous = Some(price);
            if remaining.is_zero() {
                continue;
            }
            let take = remaining.min(available);
            weighted = weighted
                .checked_add(
                    price
                        .checked_mul(take)
                        .ok_or_else(|| anyhow::anyhow!("MARKET weighted amount overflow"))?,
                )
                .ok_or_else(|| anyhow::anyhow!("MARKET aggregate amount overflow"))?;
            remaining -= take;
            last_price = price;
        }
        anyhow::ensure!(
            remaining.is_zero(),
            "MARKET depth cannot fill original native quantity"
        );
        let base = last_price
            .checked_mul(quantity)
            .ok_or_else(|| anyhow::anyhow!("MARKET worst-price amount overflow"))?
            / scale;
        let bps = U256::from(10_000);
        let (maker_amount, taker_amount) = match side {
            PredictOrderSide::Buy => (
                (base
                    .checked_mul(bps + U256::from(slippage_bps))
                    .ok_or_else(|| anyhow::anyhow!("MARKET slippage amount overflow"))?
                    / bps)
                    .min(quantity),
                quantity,
            ),
            PredictOrderSide::Sell => (
                quantity,
                base.checked_mul(bps - U256::from(slippage_bps))
                    .ok_or_else(|| anyhow::anyhow!("MARKET slippage amount overflow"))?
                    / bps,
            ),
        };
        anyhow::ensure!(
            !maker_amount.is_zero() && !taker_amount.is_zero(),
            "MARKET signed amount is zero"
        );
        Ok(Self {
            side,
            price_per_share: weighted / quantity,
            last_price,
            maker_amount,
            taker_amount,
            amount: quantity,
            slippage_bps,
        })
    }
}
