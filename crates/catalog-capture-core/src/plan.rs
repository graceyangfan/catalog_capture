use std::collections::BTreeSet;

use nautilus_model::{
    data::{BarType, DataType},
    enums::BookType,
    identifiers::InstrumentId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuoteCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarCaptureSpec {
    pub bar_type: BarType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookDeltasCaptureSpec {
    pub instrument_id: InstrumentId,
    pub book_type: BookType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkPriceCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPriceCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FundingRateCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentStatusCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentCloseCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionGreeksCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardPriceCaptureSpec {
    pub instrument_id: InstrumentId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomDataCaptureSpec {
    pub data_type: DataType,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CapturePlan {
    pub instruments: Vec<InstrumentCaptureSpec>,
    pub quotes: Vec<QuoteCaptureSpec>,
    pub trades: Vec<TradeCaptureSpec>,
    pub bars: Vec<BarCaptureSpec>,
    pub book_deltas: Vec<BookDeltasCaptureSpec>,
    pub mark_prices: Vec<MarkPriceCaptureSpec>,
    pub index_prices: Vec<IndexPriceCaptureSpec>,
    pub funding_rates: Vec<FundingRateCaptureSpec>,
    pub instrument_statuses: Vec<InstrumentStatusCaptureSpec>,
    pub instrument_closes: Vec<InstrumentCloseCaptureSpec>,
    pub option_greeks: Vec<OptionGreeksCaptureSpec>,
    pub forward_prices: Vec<ForwardPriceCaptureSpec>,
    pub custom_data: Vec<CustomDataCaptureSpec>,
}

impl CapturePlan {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.instruments.is_empty()
            && self.quotes.is_empty()
            && self.trades.is_empty()
            && self.bars.is_empty()
            && self.book_deltas.is_empty()
            && self.mark_prices.is_empty()
            && self.index_prices.is_empty()
            && self.funding_rates.is_empty()
            && self.instrument_statuses.is_empty()
            && self.instrument_closes.is_empty()
            && self.option_greeks.is_empty()
            && self.forward_prices.is_empty()
            && self.custom_data.is_empty()
    }

    /// Union of every `instrument_id` referenced by any instrument-scoped family.
    ///
    /// `bars` and `custom_data` are excluded because they are not keyed by
    /// `InstrumentId` alone.
    #[must_use]
    pub fn planned_instrument_ids(&self) -> Vec<InstrumentId> {
        let mut ids = BTreeSet::new();
        self.extend_planned_instrument_ids(&mut ids);
        ids.into_iter().collect()
    }

    fn extend_planned_instrument_ids(&self, ids: &mut BTreeSet<InstrumentId>) {
        ids.extend(self.instruments.iter().map(|spec| spec.instrument_id));
        ids.extend(self.quotes.iter().map(|spec| spec.instrument_id));
        ids.extend(self.trades.iter().map(|spec| spec.instrument_id));
        ids.extend(self.book_deltas.iter().map(|spec| spec.instrument_id));
        ids.extend(self.mark_prices.iter().map(|spec| spec.instrument_id));
        ids.extend(self.index_prices.iter().map(|spec| spec.instrument_id));
        ids.extend(self.funding_rates.iter().map(|spec| spec.instrument_id));
        ids.extend(
            self.instrument_statuses
                .iter()
                .map(|spec| spec.instrument_id),
        );
        ids.extend(self.instrument_closes.iter().map(|spec| spec.instrument_id));
        ids.extend(self.option_greeks.iter().map(|spec| spec.instrument_id));
        ids.extend(self.forward_prices.iter().map(|spec| spec.instrument_id));
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    fn eth_perp() -> InstrumentId {
        InstrumentId::from_str("ETHUSDT-PERP.BINANCE").expect("valid instrument id")
    }

    fn btc_perp() -> InstrumentId {
        InstrumentId::from_str("BTCUSDT-PERP.BINANCE").expect("valid instrument id")
    }

    #[test]
    fn planned_instrument_ids_dedupes_across_families() {
        let eth = eth_perp();
        let plan = CapturePlan {
            instruments: vec![InstrumentCaptureSpec { instrument_id: eth }],
            quotes: vec![QuoteCaptureSpec { instrument_id: eth }],
            mark_prices: vec![MarkPriceCaptureSpec { instrument_id: eth }],
            funding_rates: vec![FundingRateCaptureSpec { instrument_id: eth }],
            ..CapturePlan::default()
        };

        assert_eq!(plan.planned_instrument_ids(), vec![eth]);
    }

    #[test]
    fn planned_instrument_ids_returns_sorted_unique_ids() {
        let eth = eth_perp();
        let btc = btc_perp();
        let plan = CapturePlan {
            quotes: vec![QuoteCaptureSpec { instrument_id: eth }],
            trades: vec![TradeCaptureSpec { instrument_id: btc }],
            instrument_statuses: vec![InstrumentStatusCaptureSpec { instrument_id: eth }],
            ..CapturePlan::default()
        };

        assert_eq!(plan.planned_instrument_ids(), vec![btc, eth]);
    }

    #[test]
    fn planned_instrument_ids_ignores_bars_and_custom_data() {
        let eth = eth_perp();
        let plan = CapturePlan {
            bars: vec![BarCaptureSpec {
                bar_type: BarType::from_str("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL").unwrap(),
            }],
            custom_data: vec![CustomDataCaptureSpec {
                data_type: DataType::new("SomeCustom", None, None),
            }],
            ..CapturePlan::default()
        };

        assert!(plan.planned_instrument_ids().is_empty());

        let plan = CapturePlan {
            quotes: vec![QuoteCaptureSpec { instrument_id: eth }],
            bars: vec![BarCaptureSpec {
                bar_type: BarType::from_str("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL").unwrap(),
            }],
            ..CapturePlan::default()
        };

        assert_eq!(plan.planned_instrument_ids(), vec![eth]);
    }
}
