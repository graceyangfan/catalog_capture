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

use std::collections::HashMap;

use ahash::AHashMap;
use async_trait::async_trait;
use nautilus_common::providers::{InstrumentProvider, InstrumentStore};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::AssetClass,
    identifiers::{InstrumentId, Symbol},
    instruments::{BinaryOption, InstrumentAny},
    types::{Currency, Price, Quantity, fixed::FIXED_PRECISION},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::{
    common::{PredictMarketContext, instrument_id_from_outcome},
    http::PredictHttpClient,
    models::PredictMarket,
};

// Predict's execution SDK represents on-chain quantities in 18-decimal wei, while its
// public orderbook sends human-readable share quantities. Nautilus `FIXED_PRECISION`
// supports 16 decimal places; the data adapter preserves prices exactly and deterministically
// rounds only excess quantity precision at that representation boundary.
const SNAPSHOT_SIZE_PRECISION: u8 = FIXED_PRECISION;

/// Loads and caches Predict's two outcome instruments for each market.
#[derive(Debug)]
pub struct PredictInstrumentProvider {
    store: InstrumentStore,
    http_client: PredictHttpClient,
    contexts: AHashMap<u64, PredictMarketContext>,
}

impl PredictInstrumentProvider {
    #[must_use]
    pub fn new(http_client: PredictHttpClient) -> Self {
        Self {
            store: InstrumentStore::new(),
            http_client,
            contexts: AHashMap::new(),
        }
    }

    pub fn context(&self, market_id: u64) -> Option<&PredictMarketContext> {
        self.contexts.get(&market_id)
    }

    pub async fn load_market(
        &mut self,
        market_id: u64,
        ts_init: UnixNanos,
    ) -> anyhow::Result<PredictMarketContext> {
        let (context, _) = self
            .load_market_with_definitions(market_id, ts_init)
            .await?;
        Ok(context)
    }

    /// Loads, caches, and returns one binary market's context and outcome definitions.
    pub async fn load_market_with_definitions(
        &mut self,
        market_id: u64,
        ts_init: UnixNanos,
    ) -> anyhow::Result<(PredictMarketContext, Vec<InstrumentAny>)> {
        let (context, instruments) = self.load_market_instruments(market_id, ts_init).await?;
        let definitions = instruments.clone();
        self.store.add_bulk(instruments);
        self.contexts.insert(market_id, context.clone());
        Ok((context, definitions))
    }

    /// Loads one binary market and returns the context plus both outcome definitions.
    ///
    /// The caller owns publication ordering: instruments must enter the Nautilus data engine
    /// before its market-scoped custom data subscription can emit snapshots.
    pub async fn load_market_instruments(
        &self,
        market_id: u64,
        ts_init: UnixNanos,
    ) -> anyhow::Result<(PredictMarketContext, Vec<InstrumentAny>)> {
        let market = self.http_client.request_market(market_id).await?;
        instruments_from_market(&market, ts_init)
    }
}

#[async_trait(?Send)]
impl InstrumentProvider for PredictInstrumentProvider {
    fn store(&self) -> &InstrumentStore {
        &self.store
    }
    fn store_mut(&mut self) -> &mut InstrumentStore {
        &mut self.store
    }

    async fn load_all(&mut self, _filters: Option<&HashMap<String, String>>) -> anyhow::Result<()> {
        anyhow::bail!(
            "PredictInstrumentProvider does not bulk-load markets; load explicit market IDs"
        )
    }

    async fn load_ids(
        &mut self,
        instrument_ids: &[InstrumentId],
        _filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        let mut market_ids = instrument_ids
            .iter()
            .map(parse_market_id)
            .collect::<anyhow::Result<Vec<_>>>()?;
        market_ids.sort_unstable();
        market_ids.dedup();
        for market_id in market_ids {
            if !self.contexts.contains_key(&market_id) {
                self.load_market(market_id, UnixNanos::default()).await?;
            }
        }
        for instrument_id in instrument_ids {
            anyhow::ensure!(
                self.store.contains(instrument_id),
                "Predict market metadata did not contain requested instrument {instrument_id}"
            );
        }
        Ok(())
    }

    async fn load(
        &mut self,
        instrument_id: &InstrumentId,
        _filters: Option<&HashMap<String, String>>,
    ) -> anyhow::Result<()> {
        self.load_ids(std::slice::from_ref(instrument_id), None)
            .await
    }
}

pub(crate) fn instruments_from_market(
    market: &PredictMarket,
    ts_init: UnixNanos,
) -> anyhow::Result<(PredictMarketContext, Vec<InstrumentAny>)> {
    anyhow::ensure!(
        market.decimal_precision <= FIXED_PRECISION,
        "Predict decimalPrecision {} exceeds Nautilus precision",
        market.decimal_precision
    );
    anyhow::ensure!(
        market.outcomes.len() == 2,
        "Predict market {} is not binary",
        market.id
    );
    let yes = market
        .outcomes
        .iter()
        .find(|outcome| outcome.index_set == 1)
        .ok_or_else(|| anyhow::anyhow!("Predict market {} lacks indexSet=1 outcome", market.id))?;
    let no = market
        .outcomes
        .iter()
        .find(|outcome| outcome.index_set == 2)
        .ok_or_else(|| anyhow::anyhow!("Predict market {} lacks indexSet=2 outcome", market.id))?;
    let yes_id = instrument_id_from_outcome(market.id, yes.index_set)?;
    let no_id = instrument_id_from_outcome(market.id, no.index_set)?;
    let price_increment = Price::from_decimal_dp(
        Decimal::new(1, u32::from(market.decimal_precision)),
        market.decimal_precision,
    )?;
    let min_price = Price::zero(market.decimal_precision);
    let max_price = Price::from_decimal_dp(Decimal::ONE, market.decimal_precision)?;
    let size_increment = Quantity::from_decimal_dp(
        Decimal::new(1, u32::from(SNAPSHOT_SIZE_PRECISION)),
        SNAPSHOT_SIZE_PRECISION,
    )?;
    let currency = Currency::get_or_create_crypto("USDT");
    let instruments = [yes, no]
        .into_iter()
        .map(|outcome| {
            let instrument_id = instrument_id_from_outcome(market.id, outcome.index_set)?;
            let binary = BinaryOption::builder()
                .instrument_id(instrument_id)
                .raw_symbol(Symbol::new(outcome.on_chain_id.as_str()))
                .asset_class(AssetClass::Alternative)
                .currency(currency)
                .activation_ns(UnixNanos::default())
                // REST market metadata does not expose an expiry. Keep the definition
                // Arrow-serializable; the Crypto Up/Down universe manager owns actual rotation.
                .expiration_ns(UnixNanos::from(i64::MAX as u64))
                .price_precision(market.decimal_precision)
                .size_precision(SNAPSHOT_SIZE_PRECISION)
                .price_increment(price_increment)
                .size_increment(size_increment)
                .min_price(min_price)
                .max_price(max_price)
                .outcome(Ustr::from(outcome.name.as_str()))
                .description(Ustr::from(market.title.as_str()))
                .ts_event(ts_init)
                .ts_init(ts_init)
                .build()?;
            Ok(InstrumentAny::BinaryOption(binary))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok((
        PredictMarketContext {
            market_id: market.id,
            yes_instrument_id: yes_id,
            no_instrument_id: no_id,
            price_precision: market.decimal_precision,
            size_precision: SNAPSHOT_SIZE_PRECISION,
        },
        instruments,
    ))
}

fn parse_market_id(instrument_id: &InstrumentId) -> anyhow::Result<u64> {
    anyhow::ensure!(
        instrument_id.venue.as_str() == "PREDICT",
        "instrument {instrument_id} is not a Predict instrument"
    );
    let mut fields = instrument_id.symbol.as_str().split('-');
    let market_id = fields
        .next()
        .ok_or_else(|| {
            anyhow::anyhow!("invalid Predict instrument symbol {}", instrument_id.symbol)
        })?
        .parse()
        .map_err(|error| {
            anyhow::anyhow!("invalid Predict market ID in {instrument_id}: {error}")
        })?;
    let outcome_index: u8 = fields
        .next()
        .ok_or_else(|| {
            anyhow::anyhow!("Predict instrument {instrument_id} lacks an outcome index")
        })?
        .parse()
        .map_err(|error| {
            anyhow::anyhow!("invalid Predict outcome index in {instrument_id}: {error}")
        })?;
    anyhow::ensure!(
        fields.next().is_none() && matches!(outcome_index, 1 | 2),
        "invalid Predict instrument symbol {instrument_id}; expected {{market_id}}-{{1|2}}.PREDICT"
    );
    Ok(market_id)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use catalog_capture_core::{CaptureConfig, CryptoUpDownSelectorKey, NautilusCatalogSink};
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::Data,
        instruments::Instrument,
        types::{Quantity, fixed::FIXED_PRECISION},
    };
    use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
    use rust_decimal::Decimal;

    use super::{instruments_from_market, parse_market_id};
    use crate::{
        PredictOrderbookSnapshot, PredictOrderbookWire, predict_crypto_up_down_orderbook_data_type,
        models::{PredictMarket, PredictResponse},
        register_predict_custom_data,
    };

    #[test]
    fn parses_official_btc_up_down_fixture_into_two_instruments() {
        let response: PredictResponse<PredictMarket> =
            serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
        let (context, instruments) = instruments_from_market(&response.data, 1.into()).unwrap();
        assert_eq!(context.market_id, 2_859_346);
        assert_eq!(context.yes_instrument_id.to_string(), "2859346-1.PREDICT");
        assert_eq!(context.no_instrument_id.to_string(), "2859346-2.PREDICT");
        assert_eq!(instruments.len(), 2);
        assert_eq!(instruments[0].price_precision(), 2);
        assert_eq!(instruments[0].size_precision(), FIXED_PRECISION);
        assert_eq!(
            instruments[0].raw_symbol().as_str(),
            response.data.outcomes[0].on_chain_id
        );
    }

    #[test]
    fn market_instruments_and_yes_snapshot_round_trip_through_parquet_catalog() {
        register_predict_custom_data();
        let response: PredictResponse<PredictMarket> =
            serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
        let (context, instruments) =
            instruments_from_market(&response.data, UnixNanos::from(1_000)).unwrap();
        let wire: PredictOrderbookWire =
            serde_json::from_str(include_str!("../test_data/orderbook_2859346.json")).unwrap();
        let snapshot = PredictOrderbookSnapshot::from_wire(
            &context,
            wire,
            UnixNanos::from(1_700_000_000_123_000_000),
        )
        .unwrap();

        let selector = CryptoUpDownSelectorKey::new("BTC/USDT", "Bitcoin", 300).unwrap();
        let data_type = predict_crypto_up_down_orderbook_data_type(context.market_id, &selector);
        assert_eq!(data_type.identifier(), Some("2859346"));
        assert_eq!(snapshot.instrument_id, context.yes_instrument_id);
        assert_eq!(snapshot.bids.len(), 2, "non-empty source depth is required");
        assert_eq!(snapshot.asks.len(), 2, "non-empty source depth is required");
        assert_eq!(snapshot.bids[0].price.to_string(), "0.62");
        assert_eq!(
            snapshot.bids[0].quantity,
            Quantity::from_decimal_dp(Decimal::new(150_025, 2), context.size_precision).unwrap()
        );

        let root = unique_temp_catalog("predict-market-instrument-roundtrip");
        let mut config = CaptureConfig::default();
        config.catalog_uri = format!("file://{}", root.display());
        let sink = NautilusCatalogSink::from_config(&config).unwrap();
        sink.write_instruments(instruments).unwrap();
        sink.write_custom_data_batch(vec![snapshot.into_custom_data_with_type(data_type)])
            .unwrap();

        let mut catalog = ParquetDataCatalog::new(&root, None, None, None, None);
        let instrument_ids = vec![
            context.yes_instrument_id.to_string(),
            context.no_instrument_id.to_string(),
        ];
        let restored_instruments = catalog
            .instruments(Some(&instrument_ids), None, None)
            .unwrap();
        assert_eq!(
            restored_instruments.len(),
            2,
            "both market outcomes must persist"
        );
        assert!(
            restored_instruments
                .iter()
                .any(|item| item.id() == context.yes_instrument_id)
        );
        assert!(
            restored_instruments
                .iter()
                .any(|item| item.id() == context.no_instrument_id)
        );

        let market_ids = vec![context.market_id.to_string()];
        let restored = catalog
            .query_custom_data_dynamic(
                "PredictOrderbookSnapshot",
                Some(&market_ids),
                None,
                None,
                None,
                None,
                true,
            )
            .unwrap();
        assert_eq!(
            restored.len(),
            1,
            "exactly one market-scoped snapshot must load"
        );
        let Data::Custom(custom) = &restored[0] else {
            panic!("expected Predict custom data");
        };
        assert_eq!(custom.data_type.identifier(), Some("2859346"));
        assert_eq!(
            custom
                .data_type
                .metadata()
                .and_then(|metadata| metadata.get("predict_price_feed_symbol"))
                .and_then(serde_json::Value::as_str),
            Some("BTCUSDT")
        );
        let restored_snapshot = custom
            .data
            .as_any()
            .downcast_ref::<PredictOrderbookSnapshot>()
            .expect("registered PredictOrderbookSnapshot");
        assert_eq!(restored_snapshot.market_id, context.market_id);
        assert_eq!(restored_snapshot.instrument_id, context.yes_instrument_id);
        assert_eq!(restored_snapshot.bids.len(), 2);
        assert_eq!(restored_snapshot.asks.len(), 2);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn snapshot_rejects_a_wrong_market_or_lossy_level_precision() {
        let response: PredictResponse<PredictMarket> =
            serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
        let (context, _) = instruments_from_market(&response.data, UnixNanos::from(1_000)).unwrap();

        let mut wrong_market: PredictOrderbookWire =
            serde_json::from_str(include_str!("../test_data/orderbook_2859346.json")).unwrap();
        wrong_market.market_id = context.market_id + 1;
        let error =
            PredictOrderbookSnapshot::from_wire(&context, wrong_market, UnixNanos::from(2_000))
                .unwrap_err();
        assert!(error.to_string().contains("does not match context"));

        let mut excess_precision: PredictOrderbookWire =
            serde_json::from_str(include_str!("../test_data/orderbook_2859346.json")).unwrap();
        excess_precision.bids[0].0 = Decimal::new(6_201, 4);
        let error =
            PredictOrderbookSnapshot::from_wire(&context, excess_precision, UnixNanos::from(2_000))
                .unwrap_err();
        assert!(error.to_string().contains("exceeds declared precision 2"));
    }

    #[test]
    fn instrument_id_parser_rejects_unknown_or_malformed_outcomes() {
        assert_eq!(
            parse_market_id(&"2859346-1.PREDICT".parse().unwrap()).unwrap(),
            2_859_346
        );
        for invalid in [
            "2859346-3.PREDICT",
            "2859346.PREDICT",
            "2859346-1-extra.PREDICT",
        ] {
            assert!(
                parse_market_id(&invalid.parse().unwrap()).is_err(),
                "expected {invalid} to be rejected"
            );
        }
    }

    fn unique_temp_catalog(label: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }
}
