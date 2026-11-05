// Copyright (C) 2026 yfclark and contributors. Licensed under LGPL-3.0.
//! Recovery intent lifecycle: duplicate, concurrent, bounded, late and reconnect cases.
use super::*;
use nautilus_core::{UnixNanos, time::AtomicTime};

#[test]
fn recovery_waits_for_native_application_serializes_queries_and_preserves_other_trades() {
    let state: SharedRecovery = Default::default();
    let owner = ClientOrderId::from("RECOVER-1");
    let trade = TradeId::from("SETTLE-1");
    let other = TradeId::from("SETTLE-2");
    let clock = Box::leak(Box::new(AtomicTime::new(false, UnixNanos::default())));
    for id in [trade, trade, other] {
        state.lock().unwrap().request(owner, id);
    }
    assert_eq!(state.lock().unwrap().due(0), vec![owner]);
    assert!(state.lock().unwrap().due(0).is_empty());
    let guard = QueryGuard::acquire(state.clone(), owner, clock).unwrap();
    assert!(QueryGuard::acquire(state.clone(), owner, clock).is_none());
    assert!(state.lock().unwrap().due(u64::MAX).is_empty());
    drop(guard);
    assert!(
        state.lock().unwrap().contains(owner),
        "sending a report does not apply it"
    );
    assert!(state.lock().unwrap().due(1_999_999_999).is_empty());
    assert_eq!(state.lock().unwrap().due(2_000_000_000), vec![owner]);
    state.lock().unwrap().resolved(owner, trade);
    assert!(state.lock().unwrap().contains(owner));
    state.lock().unwrap().resolved(owner, other);
    assert!(!state.lock().unwrap().contains(owner));
}

#[test]
fn exhausted_and_permanent_failures_retain_evidence_and_reconnect_rearms_requests() {
    let state: SharedRecovery = Default::default();
    let owner = ClientOrderId::from("RECOVER-1");
    let clock = Box::leak(Box::new(AtomicTime::new(false, UnixNanos::default())));
    state
        .lock()
        .unwrap()
        .request(owner, TradeId::from("SETTLE-1"));
    for _ in 0..MAX_ATTEMPTS {
        assert_eq!(state.lock().unwrap().due(u64::MAX), vec![owner]);
        drop(QueryGuard::acquire(state.clone(), owner, clock).unwrap());
    }
    assert!(state.lock().unwrap().due(u64::MAX).is_empty());
    assert!(state.lock().unwrap().contains(owner));
    state.lock().unwrap().reconnect();
    assert_eq!(state.lock().unwrap().due(0), vec![owner]);
    state.lock().unwrap().block(owner);
    assert!(state.lock().unwrap().due(u64::MAX).is_empty());
    let guard = QueryGuard::acquire(state.clone(), owner, clock).unwrap();
    state
        .lock()
        .unwrap()
        .resolved(owner, TradeId::from("SETTLE-1"));
    drop(guard);
    assert!(state.lock().unwrap().is_empty());
}

#[test]
fn recovery_classifies_transient_transport_and_index_lag_separately_from_conflicting_facts() {
    use crate::http::error::Error;
    assert!(retryable(&HistoryPending.into()));
    assert!(retryable(
        &Error::Http {
            endpoint: "orders",
            status: 503
        }
        .into()
    ));
    assert!(!retryable(&Error::AccountMismatch.into()));
    assert!(!retryable(
        &Error::Http {
            endpoint: "orders",
            status: 403
        }
        .into()
    ));
    assert!(!retryable(&anyhow::anyhow!("conflicting execution facts")));
}

#[test]
fn automatic_recovery_has_a_global_concurrency_cap() {
    let mut state = SettlementRecovery::default();
    for index in 0..20 {
        state.request(
            ClientOrderId::new(format!("OWNER-{index}")),
            TradeId::from("TRADE"),
        );
    }
    let due = state.due(0);
    assert_eq!(due.len(), MAX_ACTIVE_QUERIES);
    assert!(state.due(0).is_empty());
    for owner in due {
        state.resolved(owner, TradeId::from("TRADE"));
    }
    assert_eq!(state.due(0).len(), MAX_ACTIVE_QUERIES);
}
