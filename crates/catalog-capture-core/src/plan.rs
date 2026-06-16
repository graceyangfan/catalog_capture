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
            && self.custom_data.is_empty()
    }
}
