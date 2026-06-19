use nautilus_model::types::Price;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtmReferenceSource {
    HttpPerpTickerMark,
    HttpPerpTickerIndex,
    HttpPerpTickerMid,
    HttpPerpForwardPrice,
    CacheMark,
    CacheQuoteMid,
    CacheIndex,
}

impl AtmReferenceSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HttpPerpTickerMark => "http_perp_ticker_mark",
            Self::HttpPerpTickerIndex => "http_perp_ticker_index",
            Self::HttpPerpTickerMid => "http_perp_ticker_mid",
            Self::HttpPerpForwardPrice => "http_forward_price",
            Self::CacheMark => "cache_mark",
            Self::CacheQuoteMid => "cache_quote_mid",
            Self::CacheIndex => "cache_index",
        }
    }
}

pub fn select_http_perp_ticker_atm_reference(
    mark: Option<&str>,
    index: Option<&str>,
    bid: Option<&str>,
    ask: Option<&str>,
) -> Option<(Price, AtmReferenceSource)> {
    if let Some(value) = non_empty_decimal(mark) {
        return Some((Price::from(value), AtmReferenceSource::HttpPerpTickerMark));
    }
    if let Some(value) = non_empty_decimal(index) {
        return Some((Price::from(value), AtmReferenceSource::HttpPerpTickerIndex));
    }
    if let (Some(bid), Some(ask)) = (non_empty_decimal(bid), non_empty_decimal(ask)) {
        let bid = bid.parse::<f64>().ok()?;
        let ask = ask.parse::<f64>().ok()?;
        let mid = ((bid + ask) / 2.0).to_string();
        return Some((Price::from(mid.as_str()), AtmReferenceSource::HttpPerpTickerMid));
    }
    None
}

pub fn select_cache_atm_reference(
    mark: Option<Price>,
    quote_mid: Option<Price>,
    index: Option<Price>,
) -> Option<(Price, AtmReferenceSource)> {
    if let Some(price) = mark {
        return Some((price, AtmReferenceSource::CacheMark));
    }
    if let Some(price) = quote_mid {
        return Some((price, AtmReferenceSource::CacheQuoteMid));
    }
    if let Some(price) = index {
        return Some((price, AtmReferenceSource::CacheIndex));
    }
    None
}

fn non_empty_decimal(value: Option<&str>) -> Option<&str> {
    value.filter(|entry| !entry.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_preflight_prefers_mark_then_index_then_mid() {
        assert_eq!(
            select_http_perp_ticker_atm_reference(
                Some("65000"),
                Some("64900"),
                Some("64990"),
                Some("65010"),
            )
            .map(|(price, source)| (price.to_string(), source)),
            Some(("65000".to_string(), AtmReferenceSource::HttpPerpTickerMark))
        );
        assert_eq!(
            select_http_perp_ticker_atm_reference(None, Some("64900"), Some("64990"), Some("65010"))
                .map(|(_, source)| source),
            Some(AtmReferenceSource::HttpPerpTickerIndex)
        );
        assert_eq!(
            select_http_perp_ticker_atm_reference(None, None, Some("64990"), Some("65010"))
                .map(|(price, source)| (price.to_string(), source)),
            Some(("65000".to_string(), AtmReferenceSource::HttpPerpTickerMid))
        );
    }

    #[test]
    fn cache_preflight_prefers_mark_then_quote_then_index() {
        let mark = Price::from("65100");
        let quote = Price::from("65050");
        let index = Price::from("65000");
        assert_eq!(
            select_cache_atm_reference(Some(mark), Some(quote), Some(index))
                .map(|(_, source)| source),
            Some(AtmReferenceSource::CacheMark)
        );
        assert_eq!(
            select_cache_atm_reference(None, Some(quote), Some(index))
                .map(|(_, source)| source),
            Some(AtmReferenceSource::CacheQuoteMid)
        );
    }
}