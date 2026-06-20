use std::collections::BTreeSet;

use catalog_capture_core::{
    derive_perp_instrument_id, expand_option_universe, ExpiryPolicy, OptionUniverseFamily,
    OptionUniverseResolutionEventKind, OptionUniverseResolutionSummary, OptionUniverseSpec,
    OptionUniverseVenueKind, ResolvedOptionUniverse, StrikePolicy,
};
use nautilus_model::{identifiers::InstrumentId, types::Price};

use super::report::{build_option_universe_resolution_report, OptionUniverseResolutionReport};
use super::{
    render_option_universe_catalog_validation_json, render_option_universe_catalog_validation_text,
    render_option_universe_reports_json, render_option_universe_reports_text,
    render_option_universe_summaries_json, render_option_universe_summaries_text,
    OptionUniverseCatalogValidationReport,
};

#[test]
fn derive_perp_instrument_ids_build_expected_symbols() {
    let deribit_spec = OptionUniverseSpec {
        venue_id: "deribit_main".to_string(),
        underlying: "BTC".to_string(),
        settlement_currency: None,
        include_perp: true,
        families: vec![OptionUniverseFamily::Quotes],
        expiry_policy: ExpiryPolicy::Nearest { days_max: 45 },
        strike_policy: StrikePolicy::AtmRelative {
            strikes_above: 1,
            strikes_below: 1,
        },
    };
    assert_eq!(
        derive_perp_instrument_id(&deribit_spec, OptionUniverseVenueKind::Deribit)
            .expect("deribit"),
        InstrumentId::from("BTC-PERPETUAL.DERIBIT")
    );

    let bybit_spec = OptionUniverseSpec {
        settlement_currency: Some("USDT".to_string()),
        venue_id: "bybit_main".to_string(),
        ..deribit_spec.clone()
    };
    assert_eq!(
        derive_perp_instrument_id(&bybit_spec, OptionUniverseVenueKind::Bybit).expect("bybit"),
        InstrumentId::from("BTCUSDT-LINEAR.BYBIT")
    );

    let okx_spec = OptionUniverseSpec {
        venue_id: "okx_main".to_string(),
        settlement_currency: Some("USD".to_string()),
        ..deribit_spec
    };
    assert_eq!(
        derive_perp_instrument_id(&okx_spec, OptionUniverseVenueKind::Okx).expect("okx"),
        InstrumentId::from("BTC-USD-SWAP.OKX")
    );
}

#[test]
fn expand_option_universe_builds_capture_plan_from_resolved_snapshot() {
    let spec = OptionUniverseSpec {
        venue_id: "deribit_main".to_string(),
        underlying: "BTC".to_string(),
        settlement_currency: None,
        include_perp: true,
        families: vec![
            OptionUniverseFamily::Instruments,
            OptionUniverseFamily::Quotes,
            OptionUniverseFamily::IndexPrices,
        ],
        expiry_policy: ExpiryPolicy::Nearest { days_max: 45 },
        strike_policy: StrikePolicy::AtmRelative {
            strikes_above: 1,
            strikes_below: 0,
        },
    };
    let resolved = ResolvedOptionUniverse {
        resolved_at_ns: 1.into(),
        selected_expiry_ns: 2.into(),
        atm_reference: Price::from("62000"),
        atm_reference_source: Some("http_perp_ticker_mark".to_string()),
        selected_strikes: vec![Price::from("62000"), Price::from("62500")],
        perp_instrument_id: Some(InstrumentId::from("BTC-PERPETUAL.DERIBIT")),
        option_instrument_ids: vec![
            InstrumentId::from("BTC-27JUN26-62000-C.DERIBIT"),
            InstrumentId::from("BTC-27JUN26-62000-P.DERIBIT"),
            InstrumentId::from("BTC-27JUN26-62500-C.DERIBIT"),
            InstrumentId::from("BTC-27JUN26-62500-P.DERIBIT"),
        ],
        all_instrument_ids: vec![
            InstrumentId::from("BTC-27JUN26-62000-C.DERIBIT"),
            InstrumentId::from("BTC-27JUN26-62000-P.DERIBIT"),
            InstrumentId::from("BTC-27JUN26-62500-C.DERIBIT"),
            InstrumentId::from("BTC-27JUN26-62500-P.DERIBIT"),
            InstrumentId::from("BTC-PERPETUAL.DERIBIT"),
        ],
    };

    let plan = expand_option_universe(&spec, &resolved);
    assert_eq!(plan.instruments.len(), 5);
    assert_eq!(plan.quotes.len(), 5);
    assert_eq!(plan.index_prices.len(), 1);
}

#[test]
fn build_option_universe_resolution_report_renders_expected_fields() {
    let spec = OptionUniverseSpec {
        venue_id: "deribit_main".to_string(),
        underlying: "BTC".to_string(),
        settlement_currency: None,
        include_perp: true,
        families: vec![OptionUniverseFamily::Quotes],
        expiry_policy: ExpiryPolicy::Nearest { days_max: 45 },
        strike_policy: StrikePolicy::AtmRelative {
            strikes_above: 1,
            strikes_below: 1,
        },
    };
    let resolved = ResolvedOptionUniverse {
        resolved_at_ns: 11.into(),
        selected_expiry_ns: 22.into(),
        atm_reference: Price::from("62393.25"),
        atm_reference_source: Some("http_perp_ticker_mark".to_string()),
        selected_strikes: vec![Price::from("62000"), Price::from("62500")],
        perp_instrument_id: Some(InstrumentId::from("BTC-PERPETUAL.DERIBIT")),
        option_instrument_ids: vec![
            InstrumentId::from("BTC-27JUN26-62000-C.DERIBIT"),
            InstrumentId::from("BTC-27JUN26-62000-P.DERIBIT"),
        ],
        all_instrument_ids: vec![
            InstrumentId::from("BTC-27JUN26-62000-C.DERIBIT"),
            InstrumentId::from("BTC-27JUN26-62000-P.DERIBIT"),
            InstrumentId::from("BTC-PERPETUAL.DERIBIT"),
        ],
    };

    let report = build_option_universe_resolution_report(
        &spec,
        &resolved,
        &BTreeSet::new(),
        &resolved.all_instrument_ids.iter().copied().collect(),
    );
    assert_eq!(report.venue_id, "deribit_main");
    assert_eq!(
        report.selected_expiry_iso8601,
        "1970-01-01T00:00:00.000000022Z"
    );
    assert_eq!(report.new_instrument_ids.len(), 3);
}

#[test]
fn render_option_universe_reports_json_pretty_prints() {
    let reports = vec![OptionUniverseResolutionReport {
        venue_id: "okx_main".to_string(),
        underlying: "BTC".to_string(),
        resolved_at_ns: 1,
        selected_expiry_ns: 2,
        selected_expiry_iso8601: "1970-01-01T00:00:00.000000002Z".to_string(),
        atm_reference: "62469.8".to_string(),
        atm_reference_source: "http_forward_price".to_string(),
        strike_selection_mode: "atm_relative".to_string(),
        oi_ranked_top_n: None,
        selected_strikes: vec!["62250".to_string()],
        perp_instrument_id: Some("BTC-USD-SWAP.OKX".to_string()),
        option_instrument_ids: vec!["BTC-USD-260620-62500-C.OKX".to_string()],
        all_instrument_ids: vec![],
        overlapping_instrument_ids: vec![],
        new_instrument_ids: vec![],
    }];

    let rendered =
        render_option_universe_reports_json(&reports).expect("json rendering should succeed");
    assert!(rendered.contains("\"venue_id\": \"okx_main\""));
    assert!(rendered.contains('\n'));
}

#[test]
fn render_option_universe_reports_text_handles_empty_reports() {
    assert_eq!(
        render_option_universe_reports_text(&[]),
        "No option universes configured."
    );
}

#[test]
fn render_option_universe_summaries_text_handles_empty_summaries() {
    assert_eq!(
        render_option_universe_summaries_text(&[]),
        "No option universe resolution metadata found."
    );
}

#[test]
fn render_option_universe_summaries_json_pretty_prints() {
    let summaries = vec![OptionUniverseResolutionSummary {
        venue_id: "deribit_main".to_string(),
        underlying: "BTC".to_string(),
        startup_resolved_at_iso8601: "2026-06-20T00:00:00Z".to_string(),
        latest_event_kind: OptionUniverseResolutionEventKind::Refresh,
        latest_resolved_at_iso8601: "2026-06-20T00:15:00Z".to_string(),
        latest_selected_expiry_iso8601: "2026-06-26T08:00:00Z".to_string(),
        strike_selection_mode: "atm_relative".to_string(),
        refresh_count: 2,
        latest_rollover_reason: Some("atm_drift".to_string()),
        perp_instrument_id: Some("BTC-PERPETUAL.DERIBIT".to_string()),
        option_count: 2,
        option_instrument_ids: vec![
            "BTC-26JUN26-65000-C.DERIBIT".to_string(),
            "BTC-26JUN26-65000-P.DERIBIT".to_string(),
        ],
    }];

    let rendered =
        render_option_universe_summaries_json(&summaries).expect("json rendering should succeed");
    assert!(rendered.contains("\"latest_event_kind\": \"refresh\""));
    assert!(rendered.contains('\n'));
}

#[test]
fn render_option_universe_catalog_validation_text_handles_empty_reports() {
    assert_eq!(
        render_option_universe_catalog_validation_text(&[]),
        "No option universe catalog validation results."
    );
}

#[test]
fn render_option_universe_catalog_validation_json_pretty_prints() {
    let reports = vec![OptionUniverseCatalogValidationReport {
        venue_id: "okx_main".to_string(),
        underlying: "BTC".to_string(),
        perp_instrument_id: "BTC-USD-SWAP.OKX".to_string(),
        option_count: 6,
        refresh_count: 1,
        latest_rollover_reason: Some("atm_drift".to_string()),
    }];

    let rendered = render_option_universe_catalog_validation_json(&reports)
        .expect("json rendering should succeed");
    assert!(rendered.contains("\"perp_instrument_id\": \"BTC-USD-SWAP.OKX\""));
    assert!(rendered.contains('\n'));
}
