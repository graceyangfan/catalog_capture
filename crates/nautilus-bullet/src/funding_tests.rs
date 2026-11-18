// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------
use super::*;
use serde_json::{Value, json};
const INFO: &[u8] = include_bytes!("../test_data/http_funding_info_btc.json");
const HISTORY: &[u8] = include_bytes!("../test_data/http_funding_history_btc.json");
const PREMIUM: &[u8] = include_bytes!("../test_data/http_premium_index_btc.json");
fn id() -> InstrumentId {
    "BTC-USD-PERP.BULLET".into()
}
fn info() -> FundingInfo {
    parse_info(INFO, "BTC-USD").unwrap()
}
fn rows() -> Vec<FundingRateUpdate> {
    parse_settlements(HISTORY, id(), &info(), UnixNanos::from(7)).unwrap()
}

#[test]
fn captured_settlement_is_an_hourly_gross_rate_at_venue_microsecond_time() {
    let parsed = rows();
    assert_eq!(parsed.len(), 5);
    assert_eq!(
        parsed.last().unwrap().rate,
        Decimal::from_str_exact("0.0000125").unwrap()
    );
    assert_eq!(parsed.last().unwrap().interval, Some(60));
    assert_eq!(parsed.last().unwrap().next_funding_ns, None);
    assert_eq!(
        parsed.last().unwrap().ts_event.as_u64(),
        1_794_999_600_000_000_000
    );
    assert_eq!(parsed.last().unwrap().ts_init.as_u64(), 7);
}
#[test]
fn estimate_retains_distinct_source_fields_and_does_not_apply_multiplier_or_spread_twice() {
    let mut value: Value = serde_json::from_slice(PREMIUM).unwrap();
    // Synthetic distinct rates prevent a flat BTC observation from masking wrong-field routing.
    value[0]["estimatedFundingRate"] = json!("-0.0008");
    value[0]["lastFundingRate"] = json!("0.0001");
    let mut info = info();
    info.funding_multiplier = "2".into();
    let estimate = parse_estimate(
        &serde_json::to_vec(&value).unwrap(),
        id(),
        &info,
        UnixNanos::from(1),
    )
    .unwrap();
    assert_eq!(
        estimate.estimated_rate_8h,
        Decimal::from_str_exact("-0.0008").unwrap()
    );
    assert_eq!(
        estimate.reported_last_rate_8h,
        Decimal::from_str_exact("0.0001").unwrap()
    );
    assert_eq!(estimate.funding_multiplier, Decimal::from(2));
    let settled = parse_settlements(HISTORY, id(), &info, UnixNanos::from(1)).unwrap();
    assert_eq!(
        settled.last().unwrap().rate,
        Decimal::from_str_exact("0.0000125").unwrap()
    );
    assert_eq!(
        estimate.funding_spread_rate,
        Decimal::from_str_exact("0.05").unwrap()
    );
    assert_eq!(
        estimate.ts_event.as_u64(),
        value[0]["time"].as_u64().unwrap() * 1_000_000
    );
    assert_eq!(
        estimate.next_funding_ns.as_u64(),
        value[0]["nextFundingTime"].as_u64().unwrap() * 1_000_000
    );
}
#[test]
fn malformed_history_is_rejected_as_a_whole_and_estimates_cannot_seed_settlements() {
    assert!(parse_settlements(PREMIUM, id(), &info(), UnixNanos::from(1)).is_err());
    assert!(parse_estimate(HISTORY, id(), &info(), UnixNanos::from(1)).is_err());
    for kind in [
        "symbol",
        "duplicate",
        "reversed",
        "millis",
        "negative",
        "decimal",
    ] {
        let mut value: Value = serde_json::from_slice(HISTORY).unwrap();
        match kind {
            "symbol" => value[1]["symbol"] = json!("ETH-USD"),
            "duplicate" => value[1]["fundingTime"] = value[0]["fundingTime"].clone(),
            "reversed" => value.as_array_mut().unwrap().reverse(),
            "millis" => value[1]["fundingTime"] = json!(1794996000000_u64),
            "negative" => value[1]["fundingTime"] = json!(-1),
            _ => value[1]["fundingRate"] = json!("bad"),
        }
        assert!(
            parse_settlements(
                &serde_json::to_vec(&value).unwrap(),
                id(),
                &info(),
                UnixNanos::from(1)
            )
            .is_err(),
            "{kind}"
        );
    }
}
#[test]
fn rejects_missing_modern_metadata_and_unsupported_cadence() {
    for field in ["fundingMultiplier", "fundingSpreadRate"] {
        let mut value: Value = serde_json::from_slice(INFO).unwrap();
        value[0].as_object_mut().unwrap().remove(field);
        assert!(parse_info(&serde_json::to_vec(&value).unwrap(), "BTC-USD").is_err());
    }
    let mut info = info();
    info.funding_interval_hours = 8;
    assert!(parse_settlements(HISTORY, id(), &info, UnixNanos::from(1)).is_err());
    assert!(parse_estimate(PREMIUM, id(), &info, UnixNanos::from(1)).is_err());
    assert!(
        parse_estimate(
            PREMIUM,
            "BTC-USD-PERP.EXTENDED".into(),
            &info,
            UnixNanos::from(1)
        )
        .is_err()
    );
}
#[test]
fn conversion_accepts_zero_and_negative_without_inexact_division() {
    for rate in ["0", "-0.0001", "0.0001"] {
        let rate = Decimal::from_str_exact(rate).unwrap();
        assert_eq!(hourly(rate).unwrap() * Decimal::from(8), rate);
    }
    assert!(hourly(Decimal::from_str_exact("0.0000000000000000000000000001").unwrap()).is_err());
    assert!(positive_us(-1).is_err());
    assert!(positive_ms(i64::MAX).is_err());
}
#[test]
fn estimate_selector_and_payload_keep_full_instrument_identity() {
    let data_type = estimate_data_type(id());
    assert_eq!(estimate_instrument(&data_type).unwrap(), id());
    let estimate = parse_estimate(PREMIUM, id(), &info(), UnixNanos::from(1))
        .unwrap()
        .into_custom_data();
    assert_eq!(estimate.data_type, data_type);
    let mut wrong = estimate_data_type("ETH-USD-PERP.BULLET".into());
    wrong = DataType::new(
        ESTIMATE_TYPE,
        wrong.metadata().cloned(),
        Some(id().to_string()),
    );
    assert!(estimate_instrument(&wrong).is_err());
    assert!(estimate_instrument(&estimate_data_type("BTC-USD-PERP.EXTENDED".into())).is_err());
}
#[test]
fn stale_poll_cannot_publish_after_last_release_and_reacquire() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let sender = tx.into();
    let mut state = FundingState::default();
    state.command(FundingCommand::Subscribe(id())).unwrap();
    let old = state.owners[&id()].generation;
    state.command(FundingCommand::Subscribe(id())).unwrap();
    state.command(FundingCommand::Unsubscribe(id())).unwrap();
    state.apply(id(), old, rows(), &sender).unwrap();
    assert_eq!(rx.len(), 5);
    while rx.try_recv().is_ok() {}
    state.command(FundingCommand::Unsubscribe(id())).unwrap();
    state.command(FundingCommand::Subscribe(id())).unwrap();
    let new = state.owners[&id()].generation;
    assert_ne!(old, new);
    state.apply(id(), old, rows(), &sender).unwrap();
    assert!(rx.try_recv().is_err());
    state.apply(id(), new, rows(), &sender).unwrap();
    assert_eq!(rx.len(), 5);
    state.apply(id(), new, rows(), &sender).unwrap();
    assert_eq!(rx.len(), 5);
}
#[test]
fn delivery_failure_does_not_advance_settlement_cursor() {
    let (tx, rx) = mpsc::unbounded_channel();
    drop(rx);
    let sender = tx.into();
    let mut state = FundingState::default();
    state.command(FundingCommand::Subscribe(id())).unwrap();
    let generation = state.owners[&id()].generation;
    assert!(state.apply(id(), generation, rows(), &sender).is_err());
    assert_eq!(state.owners[&id()].last_us, None);
}
#[tokio::test]
async fn http_cursor_uses_microseconds_and_requests_publish_canonical_native_values() {
    use axum::{
        Router,
        extract::{Query, State},
        routing::get,
    };
    use nautilus_core::time::get_atomic_clock_realtime;
    use tokio::sync::Mutex;
    #[derive(Clone)]
    struct Server {
        queries: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
    }
    async fn history(
        State(state): State<Server>,
        Query(query): Query<BTreeMap<String, String>>,
    ) -> String {
        state.queries.lock().await.push(query.clone());
        if query.contains_key("startTime") {
            "[]".into()
        } else {
            let v: Value = serde_json::from_slice(HISTORY).unwrap();
            json!([v.as_array().unwrap().last().unwrap()]).to_string()
        }
    }
    let state = Server {
        queries: Arc::new(Mutex::new(Vec::new())),
    };
    let router = Router::new()
        .route(
            "/fapi/v1/fundingInfo",
            get(|| async { std::str::from_utf8(INFO).unwrap() }),
        )
        .route("/fapi/v1/fundingRate", get(history))
        .route(
            "/fapi/v1/premiumIndex",
            get(|| async { std::str::from_utf8(PREMIUM).unwrap() }),
        )
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let http = BulletHttpClient::new(&crate::config::BulletDataClientConfig {
        base_url_http: Some(format!("http://{address}")),
        ..Default::default()
    })
    .unwrap();
    let clock = get_atomic_clock_realtime();
    let first = http.request_settlements(id(), None, clock).await.unwrap();
    assert_eq!(first.len(), 1);
    let cursor = first[0].ts_event.as_u64() / 1000;
    assert!(
        http.request_settlements(id(), Some(cursor), clock)
            .await
            .unwrap()
            .is_empty()
    );
    let queries = state.queries.lock().await;
    assert_eq!(queries[0]["limit"], "1");
    assert_eq!(queries[1]["limit"], "1000");
    assert_eq!(queries[1]["startTime"], (cursor + 1).to_string());
    drop(queries);
    let estimate = http.request_funding_estimate(id(), clock).await.unwrap();
    assert_eq!(estimate.instrument_id, id());
    assert_eq!(
        estimate.funding_spread_rate,
        Decimal::from_str_exact("0.05").unwrap()
    );
    server.abort();
}

#[test]
fn native_catalog_roundtrip_keeps_settlement_and_estimate_in_separate_families() {
    use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
    let directory = std::env::temp_dir().join(format!(
        "bullet-funding-roundtrip-{}",
        nautilus_core::UUID4::new()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    register_bullet_custom_data();
    let mut catalog = ParquetDataCatalog::new(&directory, None, None, None, None);
    let settlements = rows();
    catalog
        .write_to_parquet(&settlements, None, None, None)
        .unwrap();
    let mut premium: Value = serde_json::from_slice(PREMIUM).unwrap();
    premium[0]["estimatedFundingRate"] = json!("0.0001234567890123456789012345");
    let estimate = parse_estimate(
        &serde_json::to_vec(&premium).unwrap(),
        id(),
        &info(),
        UnixNanos::from(9),
    )
    .unwrap();
    let source = serde_json::to_value(&estimate).unwrap();
    catalog
        .write_custom_data_batch(vec![estimate.into_custom_data()], None, None, None)
        .unwrap();
    let settled = catalog
        .query::<FundingRateUpdate>(Some(vec![id().to_string()]), None, None, None, None, true)
        .unwrap();
    assert_eq!(settled.len(), 5);
    assert_eq!(settled[4].rate, settlements[4].rate);
    assert_eq!(settled[4].ts_event, settlements[4].ts_event);
    let read = catalog
        .query_custom_data_dynamic(
            ESTIMATE_TYPE,
            Some(&[id().to_string()]),
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap();
    assert_eq!(read.len(), 1);
    let Data::Custom(custom) = &read[0] else {
        panic!()
    };
    assert_eq!(custom.data_type, estimate_data_type(id()));
    let decoded = custom
        .data
        .as_any()
        .downcast_ref::<BulletFundingEstimate>()
        .unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), source);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn worker_reacquire_discards_inflight_old_poll_and_cancels_cleanly() {
    use axum::{Router, extract::State, routing::get};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::{Mutex, oneshot};
    #[derive(Clone)]
    struct Mock {
        calls: Arc<AtomicUsize>,
        entered: mpsc::UnboundedSender<usize>,
        gate: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    }
    async fn history(State(state): State<Mock>) -> String {
        let call = state.calls.fetch_add(1, Ordering::AcqRel);
        let gate = state.gate.lock().await.take();
        state.entered.send(call).unwrap();
        if let Some(gate) = gate {
            gate.await.unwrap();
        }
        let mut rows: Value = serde_json::from_slice(HISTORY).unwrap();
        let mut row = rows.as_array_mut().unwrap().pop().unwrap();
        row["fundingRate"] = json!(if call == 0 { "0.0001" } else { "0.0002" });
        json!([row]).to_string()
    }
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let (release, gate) = oneshot::channel();
    let state = Mock {
        calls: Arc::new(AtomicUsize::new(0)),
        entered: entered_tx,
        gate: Arc::new(Mutex::new(Some(gate))),
    };
    let router = Router::new()
        .route(
            "/fapi/v1/fundingInfo",
            get(|| async { std::str::from_utf8(INFO).unwrap() }),
        )
        .route("/fapi/v1/fundingRate", get(history))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let http = BulletHttpClient::new(&crate::config::BulletDataClientConfig {
        base_url_http: Some(format!("http://{address}")),
        ..Default::default()
    })
    .unwrap();
    let (commands, rx) = mpsc::channel(16);
    let (events, mut output) = mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(run_funding_worker(
        http,
        rx,
        events.into(),
        nautilus_core::time::get_atomic_clock_realtime(),
        cancel.clone(),
        Duration::from_millis(10),
    ));
    commands
        .send(FundingCommand::Subscribe(id()))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), entered.recv())
            .await
            .unwrap(),
        Some(0)
    );
    commands
        .send(FundingCommand::Unsubscribe(id()))
        .await
        .unwrap();
    commands
        .send(FundingCommand::Subscribe(id()))
        .await
        .unwrap();
    release.send(()).unwrap();
    let DataEvent::Data(Data::FundingRate(row)) =
        tokio::time::timeout(Duration::from_secs(3), output.recv())
            .await
            .unwrap()
            .unwrap()
    else {
        panic!()
    };
    assert_eq!(row.rate, Decimal::from_str_exact("0.000025").unwrap());
    assert_eq!(row.instrument_id, id());
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(output.try_recv().is_err());
    server.abort();
}

#[test]
#[ignore = "requires BULLET_FUNDING_CATALOG; optional BULLET_FUNDING_SETTLEMENT_NS"]
fn recorded_funding_catalog_readback() -> anyhow::Result<()> {
    use crate::funding::{
        BulletFundingEstimate, ESTIMATE_TYPE, estimate_data_type, register_bullet_custom_data,
    };
    use anyhow::Context;
    use nautilus_model::{
        data::{Data, FundingRateUpdate},
        identifiers::InstrumentId,
        instruments::Instrument,
    };
    use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
    use std::collections::HashSet;

    let root = std::env::var("BULLET_FUNDING_CATALOG")?;
    let boundary = std::env::var("BULLET_FUNDING_SETTLEMENT_NS")
        .ok()
        .map(|s| s.parse::<u64>())
        .transpose()?;
    let id: InstrumentId = "BTC-USD-PERP.BULLET".into();
    register_bullet_custom_data();
    let mut catalog = ParquetDataCatalog::new(std::path::Path::new(&root), None, None, None, None);
    let instruments = catalog.instruments(None, None, None)?;
    let instrument = instruments
        .iter()
        .find(|instrument| instrument.id() == id)
        .context("missing BTC instrument definition")?;
    let info = instrument.info().context("missing instrument metadata")?;
    let contract = info
        .get("funding_rate_contract")
        .context("missing recorded funding basis")?;
    anyhow::ensure!(
        contract["kind"] == "settled_gross"
            && contract["rate_basis_minutes"] == 60
            && contract["source_rate_basis_minutes"] == 480
            && contract["funding_spread_applied"] == false,
        "recorded funding basis mismatch"
    );
    let settled = catalog.query::<FundingRateUpdate>(
        Some(vec![id.to_string()]),
        None,
        None,
        None,
        None,
        true,
    )?;
    anyhow::ensure!(!settled.is_empty(), "no Native settled funding rows");
    let mut times = HashSet::new();
    for row in &settled {
        anyhow::ensure!(
            row.instrument_id == id && row.interval == Some(60) && row.next_funding_ns.is_none(),
            "invalid settled funding contract"
        );
        anyhow::ensure!(
            times.insert(row.ts_event.as_u64()),
            "duplicate settled funding event"
        );
    }
    let rows = catalog.query_custom_data_dynamic(
        ESTIMATE_TYPE,
        Some(&[id.to_string()]),
        None,
        None,
        None,
        None,
        true,
    )?;
    anyhow::ensure!(!rows.is_empty(), "no Native funding estimate rows");
    let mut snapshots = Vec::new();
    for row in rows {
        let Data::Custom(custom) = row else {
            anyhow::bail!("unexpected standard data in estimate family")
        };
        anyhow::ensure!(
            custom.data_type == estimate_data_type(id),
            "custom envelope identity mismatch"
        );
        let estimate = custom
            .data
            .as_any()
            .downcast_ref::<BulletFundingEstimate>()
            .context("custom payload type mismatch")?;
        anyhow::ensure!(
            estimate.instrument_id == id && estimate.interval_minutes == 60,
            "estimate instrument/interval mismatch"
        );
        snapshots.push(serde_json::to_value(estimate)?);
    }
    if let Some(boundary) = boundary {
        anyhow::ensure!(
            times.contains(&boundary),
            "expected settlement was not recorded"
        );
        anyhow::ensure!(
            snapshots
                .iter()
                .any(|r| r["next_funding_ns"].as_u64() == Some(boundary)),
            "no estimate before settlement rollover"
        );
        anyhow::ensure!(
            snapshots.iter().any(|r| r["next_funding_ns"]
                .as_u64()
                .is_some_and(|next| next > boundary)),
            "no estimate after settlement rollover"
        );
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"recorded_funding_contract":contract,"settled_count":settled.len(),"estimates_count":snapshots.len(),"settled":settled,"first_estimate":snapshots.first(),"last_estimate":snapshots.last(),"expected_settlement_ns":boundary})
        )?
    );
    Ok(())
}
