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
use chrono::{DateTime, Datelike, Utc};
use nautilus_common::providers::{InstrumentProvider, InstrumentStore};
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    enums::AssetClass,
    identifiers::{InstrumentId, Symbol},
    instruments::{BinaryOption, InstrumentAny},
    types::{Currency, Money, Price, Quantity, fixed::FIXED_PRECISION},
};
use rust_decimal::Decimal;
use ustr::Ustr;

use crate::{
    common::{PredictMarketContext, instrument_id_from_outcome},
    http::{PredictHttpClient, models::PredictMarket},
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

    pub(crate) fn contexts(&self) -> &AHashMap<u64, PredictMarketContext> {
        &self.contexts
    }

    /// Release bootstrap definitions when their data-client generation stops.
    pub fn clear(&mut self) {
        self.store.clear();
        self.contexts.clear();
    }

    pub(crate) fn http_client(&self) -> &PredictHttpClient {
        &self.http_client
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
                self.load_market(
                    market_id,
                    nautilus_core::time::get_atomic_clock_realtime().get_time_ns(),
                )
                .await?;
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

/// Trading window shared by instrument construction and execution report scope.
pub(crate) fn market_window(
    market: &PredictMarket,
    ts_init: UnixNanos,
) -> anyhow::Result<Option<crate::common::CryptoUpDownMarketWindow>> {
    if market
        .variant_data
        .as_ref()
        .is_some_and(|v| v.variant_type == "CRYPTO_UP_DOWN")
    {
        // Titles omit the year. Creation metadata anchors historical loads too;
        // neighbouring years resolve markets advertised across New Year.
        let anchor = market
            .created_at
            .as_deref()
            .map(DateTime::parse_from_rfc3339)
            .transpose()?
            .map(|date| date.with_timezone(&Utc))
            .unwrap_or(DateTime::<Utc>::from_timestamp_nanos(i64::try_from(
                ts_init.as_u64(),
            )?));
        let anchor_ns = anchor
            .timestamp_nanos_opt()
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| anyhow::anyhow!("Predict market creation timestamp is out of range"))?;
        // Do not reinterpret an invalid/ambiguous local time as another year's market.
        crate::common::parse_crypto_up_down_title(&market.title, anchor.year())?;
        Ok(Some(
            [anchor.year() - 1, anchor.year(), anchor.year() + 1]
                .into_iter()
                .filter_map(|year| {
                    crate::common::parse_crypto_up_down_title(&market.title, year).ok()
                })
                .min_by_key(|window| window.start_ns.abs_diff(anchor_ns))
                .ok_or_else(|| {
                    anyhow::anyhow!("Predict crypto market lacks a valid trading window")
                })?,
        ))
    } else {
        Ok(None)
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
    let info = market
        .variant_data
        .as_ref()
        .filter(|variant| variant.variant_type == "CRYPTO_UP_DOWN")
        .and_then(|variant| variant.start_price)
        .map(|price| Params::new([("start_price".into(), price.to_string().into())].into()));
    let window = market_window(market, ts_init)?;
    let instruments = [yes, no]
        .into_iter()
        .map(|outcome| {
            let instrument_id = instrument_id_from_outcome(market.id, outcome.index_set)?;
            let binary = BinaryOption::builder()
                .instrument_id(instrument_id)
                .raw_symbol(Symbol::new(outcome.on_chain_id.as_str()))
                .asset_class(AssetClass::Alternative)
                .currency(currency)
                .activation_ns(UnixNanos::from(window.as_ref().map_or(0, |w| w.start_ns)))
                // Unknown expiry is retained only for non-crypto event markets.
                .expiration_ns(UnixNanos::from(
                    window.as_ref().map_or(i64::MAX as u64, |w| w.end_ns),
                ))
                .price_precision(market.decimal_precision)
                .size_precision(SNAPSHOT_SIZE_PRECISION)
                .price_increment(price_increment)
                .size_increment(size_increment)
                .min_notional(Money::from_decimal(Decimal::ONE, currency)?)
                .min_price(min_price)
                .max_price(max_price)
                .outcome(Ustr::from(outcome.name.as_str()))
                .description(Ustr::from(market.title.as_str()))
                .maybe_info(info.clone())
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

pub(crate) fn parse_market_id(instrument_id: &InstrumentId) -> anyhow::Result<u64> {
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
    anyhow::ensure!(
        instrument_id_from_outcome(market_id, outcome_index)? == *instrument_id,
        "Predict instrument ID must be canonical"
    );
    Ok(market_id)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use catalog_capture_core::{CaptureConfig, NautilusCatalogSink};
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
        PredictOrderbookSnapshot, PredictOrderbookWire,
        common::CryptoUpDownSelectorKey,
        http::models::{PredictMarket, PredictResponse},
        predict_crypto_up_down_orderbook_data_type, register_predict_custom_data,
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
    fn crypto_definitions_preserve_price_bounds_notional_and_historical_window() {
        use chrono::{TimeZone, Utc};
        use nautilus_model::types::{Money, Price};
        let response: PredictResponse<PredictMarket> =
            serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
        let (_, definitions) = instruments_from_market(&response.data, 1.into()).unwrap();
        for instrument in definitions {
            assert_eq!(instrument.min_price(), Some(Price::from("0.00")));
            assert_eq!(instrument.max_price(), Some(Price::from("1.00")));
            assert_eq!(instrument.min_notional(), Some(Money::from("1 USDT")));
            let nautilus_model::instruments::InstrumentAny::BinaryOption(binary) = instrument
            else {
                unreachable!()
            };
            assert_eq!(
                binary.activation_ns.as_u64(),
                Utc.with_ymd_and_hms(2026, 10, 3, 22, 0, 0)
                    .unwrap()
                    .timestamp_nanos_opt()
                    .unwrap() as u64
            );
            assert_eq!(
                binary.expiration_ns.as_u64(),
                binary.activation_ns.as_u64() + 300_000_000_000
            );
        }
        let mut market = response.data;
        market.title = "Bitcoin Up or Down - January 1, 12AM-12:15AM ET".into();
        market.created_at = Some("2026-12-31T23:00:00Z".into());
        let (_, definitions) = instruments_from_market(&market, 1.into()).unwrap();
        let nautilus_model::instruments::InstrumentAny::BinaryOption(binary) = &definitions[0]
        else {
            unreachable!()
        };
        assert_eq!(
            binary.expiration_ns.as_u64(),
            Utc.with_ymd_and_hms(2027, 1, 1, 5, 15, 0)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap() as u64
        );
        market.title = "Bitcoin Up or Down - November 1, 1AM-1:05AM ET".into();
        market.created_at = Some("2026-11-01T04:00:00Z".into());
        assert!(instruments_from_market(&market, 1.into()).is_err());
        market.title = "Invalid crypto window".into();
        assert!(instruments_from_market(&market, 1.into()).is_err());
        market.variant_data = None;
        let (_, definitions) = instruments_from_market(&market, 1.into()).unwrap();
        let nautilus_model::instruments::InstrumentAny::BinaryOption(binary) = &definitions[0]
        else {
            unreachable!()
        };
        assert_eq!(binary.expiration_ns.as_u64(), i64::MAX as u64);
    }

    #[test]
    fn clearing_provider_releases_both_definitions_and_contexts() {
        let response: PredictResponse<PredictMarket> =
            serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
        let (context, definitions) = instruments_from_market(&response.data, 1.into()).unwrap();
        let mut provider = super::PredictInstrumentProvider::new(
            crate::PredictHttpClient::new("offline", None, 30).unwrap(),
        );
        provider.store.add_bulk(definitions);
        provider.contexts.insert(context.market_id, context.clone());
        assert_eq!(provider.store.get_all().len(), 2);
        assert!(provider.context(context.market_id).is_some());
        provider.clear();
        assert!(provider.store.get_all().is_empty());
        assert!(provider.context(context.market_id).is_none());
        provider.clear();
    }

    #[test]
    fn start_price_preserves_exact_decimal_and_only_populates_crypto_info() {
        for token in ["81566.995123456789123", "\"81566.995123456789123\""] {
            let mut json = include_str!("../test_data/market_2859346.json").replace(
                "\"priceFeedSymbol\":",
                &format!("\"startPrice\": {token}, \"priceFeedSymbol\":"),
            );
            let response: PredictResponse<PredictMarket> = serde_json::from_str(&json).unwrap();
            assert_eq!(
                response.data.display_title(),
                format!("{} | Start: 81566.995123456789123", response.data.title)
            );
            let (_, instruments) = instruments_from_market(&response.data, 1.into()).unwrap();
            for instrument in &instruments {
                let info = instrument.info().unwrap();
                assert_eq!(info.len(), 1);
                assert_eq!(info["start_price"], "81566.995123456789123");
            }
            json = json.replace("CRYPTO_UP_DOWN", "DEFAULT");
            let response: PredictResponse<PredictMarket> = serde_json::from_str(&json).unwrap();
            assert_eq!(response.data.display_title(), response.data.title);
            let (_, instruments) = instruments_from_market(&response.data, 1.into()).unwrap();
            assert!(
                instruments
                    .iter()
                    .all(|instrument| instrument.info().is_none())
            );
        }
        for field in ["", "\"startPrice\": null,"] {
            let json = include_str!("../test_data/market_2859346.json").replace(
                "\"priceFeedSymbol\":",
                &format!("{field} \"priceFeedSymbol\":"),
            );
            let response: PredictResponse<PredictMarket> = serde_json::from_str(&json).unwrap();
            assert!(response.data.display_title().ends_with(" | Start: TBD"));
            let (_, instruments) = instruments_from_market(&response.data, 1.into()).unwrap();
            assert!(
                instruments
                    .iter()
                    .all(|instrument| instrument.info().is_none())
            );
        }
    }

    #[test]
    fn captured_calendar_market_definitions_keep_venue_title_start_price_and_expiry() {
        for (source, start, end, price) in [
            (
                include_str!("../test_data/market_3759734_daily.json"),
                "2026-10-25T16:00:00Z",
                "2026-10-26T16:00:00Z",
                "80610.01",
            ),
            (
                include_str!("../test_data/market_3737148_hourly.json"),
                "2026-10-26T10:00:00Z",
                "2026-10-26T11:00:00Z",
                "80538.53",
            ),
        ] {
            let response: PredictResponse<PredictMarket> = serde_json::from_str(source).unwrap();
            let (_, instruments) = instruments_from_market(&response.data, 1_000.into()).unwrap();
            for instrument in instruments {
                let nautilus_model::instruments::InstrumentAny::BinaryOption(instrument) =
                    instrument
                else {
                    panic!("binary outcome")
                };
                let nanos = |value| {
                    chrono::DateTime::parse_from_rfc3339(value)
                        .unwrap()
                        .timestamp_nanos_opt()
                        .unwrap() as u64
                };
                assert_eq!(instrument.activation_ns.as_u64(), nanos(start));
                assert_eq!(instrument.expiration_ns.as_u64(), nanos(end));
                assert_eq!(
                    instrument.description.unwrap().as_str(),
                    response.data.title
                );
                assert_eq!(instrument.info.unwrap()["start_price"], price);
            }
        }
    }

    #[test]
    #[ignore = "requires PREDICT_BACKFILL_PYTHON with pyarrow installed"]
    fn backfilled_instruments_read_back_through_native_catalog() {
        let python = std::env::var("PREDICT_BACKFILL_PYTHON").unwrap();
        let response: PredictResponse<PredictMarket> =
            serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
        let (_, instruments) = instruments_from_market(&response.data, 1_000.into()).unwrap();
        assert!(instruments.iter().all(|item| item.info().is_none()));
        let root = unique_temp_catalog("predict-start-price-backfill");
        let config = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            ..CaptureConfig::default()
        };
        let sink = NautilusCatalogSink::from_config(&config).unwrap();
        sink.write_instruments(instruments.clone()).unwrap();
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../scripts/backfill_predict_start_price.py");
        let output = std::process::Command::new(python)
            .arg("-c")
            .arg(
                "import importlib.util, pathlib, sys; \
                 spec = importlib.util.spec_from_file_location('backfill', sys.argv[1]); \
                 module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module); \
                 stats = module.backfill(pathlib.Path(sys.argv[2]), lambda _: '81566.995123456789123'); \
                 assert stats['rows'] == 2 and stats['errors'] == 0, stats",
            )
            .arg(script)
            .arg(&root)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let catalog = ParquetDataCatalog::new(&root, None, None, None, None);
        let restored = catalog.instruments(None, None, None).unwrap();
        assert_eq!(restored.len(), 2);
        for item in &restored {
            assert_eq!(item.info().unwrap()["start_price"], "81566.995123456789123");
            let original = instruments
                .iter()
                .find(|old| old.id() == item.id())
                .unwrap();
            let mut expected = original.clone();
            let nautilus_model::instruments::InstrumentAny::BinaryOption(binary) = &mut expected
            else {
                panic!("expected binary option");
            };
            binary.info = item.info().cloned();
            assert_eq!(&expected, item);
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn market_instruments_and_yes_snapshot_round_trip_through_parquet_catalog() {
        register_predict_custom_data();
        let response: PredictResponse<PredictMarket> =
            serde_json::from_str(&include_str!("../test_data/market_2859346.json").replace(
                "\"priceFeedSymbol\":",
                "\"startPrice\": 81566.995123456789123, \"priceFeedSymbol\":",
            ))
            .unwrap();
        let (context, instruments) =
            instruments_from_market(&response.data, UnixNanos::from(1_000)).unwrap();
        let mut wire: PredictOrderbookWire =
            serde_json::from_str(include_str!("../test_data/orderbook_2859346.json")).unwrap();
        let pending_value: Decimal = "10.12345678901234567890123456".parse().unwrap();
        wire.settlements_pending.as_mut().unwrap().asks[0].1 = pending_value;
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
        let last_order = snapshot
            .last_order_settled
            .as_ref()
            .expect("fixture carries the most recent settled order");
        assert_eq!(last_order.id, "987654321");
        assert_eq!(last_order.market_id, context.market_id);
        assert_eq!(last_order.price.to_string(), "0.62");
        assert_eq!(last_order.kind, "LIMIT");
        assert_eq!(last_order.side, "Ask");
        assert_eq!(last_order.outcome, "Yes");
        let pending = snapshot
            .settlements_pending
            .as_ref()
            .expect("fixture carries pending settlement depth");
        assert_eq!(pending.asks.len(), 1);
        assert!(pending.bids.is_empty());
        assert_eq!(pending.asks[0].price.to_string(), "0.63");
        assert_eq!(pending.asks[0].value, pending_value);

        let root = unique_temp_catalog("predict-market-instrument-roundtrip");
        let config = CaptureConfig {
            catalog_uri: format!("file://{}", root.display()),
            ..CaptureConfig::default()
        };
        let sink = NautilusCatalogSink::from_config(&config).unwrap();
        sink.write_instruments(instruments).unwrap();
        sink.write_custom_data_batch(vec![
            snapshot.into_custom_data_with_type(data_type).unwrap(),
        ])
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

        for instrument in &restored_instruments {
            let info = instrument
                .info()
                .expect("start price survives catalog roundtrip");
            assert_eq!(info.len(), 1);
            assert_eq!(info["start_price"], "81566.995123456789123");
        }

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
        let restored_last_order = restored_snapshot
            .last_order_settled
            .as_ref()
            .expect("settled order survives parquet");
        assert_eq!(restored_last_order.id, "987654321");
        assert_eq!(restored_last_order.price.to_string(), "0.62");
        assert_eq!(restored_last_order.kind, "LIMIT");
        assert_eq!(restored_last_order.market_id, context.market_id);
        assert_eq!(restored_last_order.side, "Ask");
        assert_eq!(restored_last_order.outcome, "Yes");
        let restored_pending = restored_snapshot
            .settlements_pending
            .as_ref()
            .expect("pending settlements survive parquet");
        assert_eq!(restored_pending.asks.len(), 1);
        assert!(restored_pending.bids.is_empty());
        assert_eq!(restored_pending.asks[0].price.to_string(), "0.63");
        assert_eq!(restored_pending.asks[0].value, pending_value);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn custom_snapshot_rejects_wrong_routing_and_preserves_selector_metadata() {
        let response: PredictResponse<PredictMarket> =
            serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
        let (context, _) = instruments_from_market(&response.data, 1_000.into()).unwrap();
        let snapshot = || {
            PredictOrderbookSnapshot::from_wire(
                &context,
                serde_json::from_str(include_str!("../test_data/orderbook_2859346.json")).unwrap(),
                2_000.into(),
            )
            .unwrap()
        };
        for data_type in [
            crate::predict_orderbook_data_type(context.market_id + 1),
            nautilus_model::data::DataType::new(
                "OtherSnapshot",
                None,
                Some(context.market_id.to_string()),
            ),
        ] {
            assert!(snapshot().into_custom_data_with_type(data_type).is_err());
        }
        let selector = CryptoUpDownSelectorKey::new("BTC/USDT", "Bitcoin", 300).unwrap();
        let data_type = predict_crypto_up_down_orderbook_data_type(context.market_id, &selector);
        let custom = snapshot()
            .into_custom_data_with_type(data_type.clone())
            .unwrap();
        assert_eq!(custom.data_type, data_type);
    }

    #[test]
    fn pending_settlement_values_preserve_precision_independent_of_share_increment() {
        let response: PredictResponse<PredictMarket> =
            serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
        let (mut context, _) = instruments_from_market(&response.data, 1_000.into()).unwrap();
        context.size_precision = 2;
        let mut wire: PredictOrderbookWire =
            serde_json::from_str(include_str!("../test_data/orderbook_2859346.json")).unwrap();
        let value: Decimal = "10.12345678901234567890123456".parse().unwrap();
        wire.settlements_pending.as_mut().unwrap().asks[0].1 = value;
        let snapshot = PredictOrderbookSnapshot::from_wire(&context, wire, 2_000.into()).unwrap();
        assert_eq!(
            snapshot.settlements_pending.as_ref().unwrap().asks[0].value,
            value
        );
        let mut encoded = serde_json::to_value(&snapshot).unwrap();
        let exact: PredictOrderbookSnapshot = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(exact.settlements_pending.unwrap().asks[0].value, value);
        // Existing catalogs encoded Quantity as a decimal string.
        encoded["settlements_pending"]["asks"][0]["value"] = "10.5000000000000000".into();
        let restored: PredictOrderbookSnapshot = serde_json::from_value(encoded).unwrap();
        assert_eq!(
            restored.settlements_pending.unwrap().asks[0].value,
            Decimal::new(105, 1)
        );
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

        let mut wrong_settlement_market: PredictOrderbookWire =
            serde_json::from_str(include_str!("../test_data/orderbook_2859346.json")).unwrap();
        wrong_settlement_market
            .last_order_settled
            .as_mut()
            .expect("fixture settled order")
            .market_id += 1;
        let error = PredictOrderbookSnapshot::from_wire(
            &context,
            wrong_settlement_market,
            UnixNanos::from(2_000),
        )
        .unwrap_err();
        assert!(error.to_string().contains("lastOrderSettled marketId"));

        let mut negative_pending_value: PredictOrderbookWire =
            serde_json::from_str(include_str!("../test_data/orderbook_2859346.json")).unwrap();
        negative_pending_value
            .settlements_pending
            .as_mut()
            .expect("fixture pending settlements")
            .asks[0]
            .1 = Decimal::NEGATIVE_ONE;
        let error = PredictOrderbookSnapshot::from_wire(
            &context,
            negative_pending_value,
            UnixNanos::from(2_000),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("settlementsPending value -1 is negative")
        );
    }

    #[test]
    fn snapshot_preserves_absent_settlement_state() {
        let response: PredictResponse<PredictMarket> =
            serde_json::from_str(include_str!("../test_data/market_2859346.json")).unwrap();
        let (context, _) = instruments_from_market(&response.data, UnixNanos::from(1_000)).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../test_data/orderbook_2859346.json")).unwrap();
        value["lastOrderSettled"] = serde_json::Value::Null;
        value
            .as_object_mut()
            .expect("object fixture")
            .remove("settlementsPending");
        let wire: PredictOrderbookWire = serde_json::from_value(value).unwrap();
        let snapshot =
            PredictOrderbookSnapshot::from_wire(&context, wire, UnixNanos::from(2_000)).unwrap();
        assert!(snapshot.last_order_settled.is_none());
        assert!(snapshot.settlements_pending.is_none());

        let mut value: serde_json::Value =
            serde_json::from_str(include_str!("../test_data/orderbook_2859346.json")).unwrap();
        value["settlementsPending"] = serde_json::json!({ "asks": [], "bids": [] });
        let wire: PredictOrderbookWire = serde_json::from_value(value).unwrap();
        let snapshot =
            PredictOrderbookSnapshot::from_wire(&context, wire, UnixNanos::from(2_000)).unwrap();
        let pending = snapshot
            .settlements_pending
            .expect("present empty settlement state remains present");
        assert!(pending.asks.is_empty());
        assert!(pending.bids.is_empty());
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
