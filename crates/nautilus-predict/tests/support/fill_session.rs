// Copyright (C) 2026 yfclark and contributors. LGPL-3.0-or-later.
//! Test-host completion and evidence policy; never submits or reconciles orders.
use std::{io::Write, path::Path};

use anyhow::Context;
use rust_decimal::Decimal;
use serde_json::Value;

/// An elapsed entry window is not an instruction to close existing inventory.
#[derive(Clone, Copy, Default)]
pub struct SessionObservation {
    pub entry_closed: bool,
    pub entry_filled: bool,
    pub entry_window_elapsed: bool,
    pub has_entry: bool,
    pub flat: bool,
    pub orders_terminal: bool,
    pub cash_current: bool,
}

impl SessionObservation {
    pub fn completed(self) -> bool {
        self.flat
            && self.orders_terminal
            && if self.entry_filled {
                self.cash_current
            } else {
                self.entry_closed || (!self.has_entry && self.entry_window_elapsed)
            }
    }
}

/// A failed exit is not an instruction to wait indefinitely for profit.
pub fn strategy_failed(events: &[Value], entry_id: &str) -> bool {
    events.iter().any(|event| match event["type"].as_str() {
        Some(
            "Denied"
            | "Rejected"
            | "StopAttemptLimitReached"
            | "RestartRefused"
            | "StrategyCommandError"
            | "StrategyDriveFailed"
            | "EntryQueryFailed",
        ) => true,
        // Entry cancellation can race a fill; the strategy queries Native for resolution.
        Some("CancelRejected") => event["clientOrderId"] != entry_id,
        _ => false,
    })
}

/// Actual quote cash flows, separate from Native quote realized PnL.
/// This single-cycle host starts flat; only its own strategy fill journal is supplied.
pub fn cycle_cash_flow(events: &[Value]) -> anyhow::Result<Decimal> {
    let mut total = Decimal::ZERO;
    for event in events.iter().filter(|e| e["type"] == "Filled") {
        let fill = &event["fill"];
        let parse = |key: &str| -> anyhow::Result<Decimal> {
            Decimal::from_str_exact(fill[key].as_str().context("missing fill amount")?)
                .context("invalid fill amount")
        };
        let amount = parse("quantity")? * parse("price")?;
        let fee = match fill["commission"].as_str() {
            None if fill["commission"].is_null() => Decimal::ZERO,
            Some(value) => {
                let (amount, currency) = value.split_once(' ').context("invalid commission")?;
                anyhow::ensure!(currency == "USDT", "unsupported cash commission currency");
                Decimal::from_str_exact(amount)?
            }
            None => anyhow::bail!("invalid commission"),
        };
        // Inventory fees reduce shares, not collateral. Do not charge them twice.
        total += match fill["side"].as_str() {
            Some("BUY") => -amount - fee,
            Some("SELL") => amount - fee,
            _ => anyhow::bail!("missing fill side"),
        };
    }
    Ok(total)
}

/// Latest complete checkpoint is atomic; an earlier run's evidence is never reused.
pub fn checkpoint(path: &Path, report: &Value) -> anyhow::Result<()> {
    let pending = path.with_extension("json.pending");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pending)?;
    let result = (|| {
        serde_json::to_writer_pretty(&mut file, report)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::rename(&pending, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(pending);
    }
    result
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn expired_entry_window_does_not_stop_holding_or_pending_orders() {
        let settled = SessionObservation {
            entry_closed: true,
            entry_filled: true,
            entry_window_elapsed: true,
            has_entry: true,
            flat: true,
            orders_terminal: true,
            cash_current: true,
        };
        assert!(
            !SessionObservation {
                flat: false,
                ..settled
            }
            .completed()
        );
        assert!(
            !SessionObservation {
                orders_terminal: false,
                ..settled
            }
            .completed()
        );
        assert!(
            !SessionObservation {
                cash_current: false,
                ..settled
            }
            .completed()
        );
        assert!(settled.completed());
        assert!(
            !SessionObservation {
                entry_filled: false,
                entry_closed: false,
                orders_terminal: false,
                ..settled
            }
            .completed()
        );
        assert!(
            SessionObservation {
                entry_filled: false,
                cash_current: false,
                ..settled
            }
            .completed()
        );
        assert!(
            SessionObservation {
                entry_filled: false,
                entry_closed: false,
                has_entry: false,
                ..settled
            }
            .completed()
        );
    }

    #[test]
    fn exit_failure_stops_the_test_but_entry_cancel_race_remains_native_owned() {
        assert!(!strategy_failed(
            &[json!({"type":"CancelRejected","clientOrderId":"entry"})],
            "entry"
        ));
        for event in [
            json!({"type":"CancelRejected","clientOrderId":"profit"}),
            json!({"type":"StrategyDriveFailed"}),
            json!({"type":"EntryQueryFailed"}),
            json!({"type":"StopAttemptLimitReached"}),
            json!({"type":"Rejected"}),
        ] {
            assert!(strategy_failed(&[event], "entry"));
        }
    }

    #[test]
    fn cash_flow_charges_quote_fee_once_and_does_not_recharge_inventory_fee() {
        let fills = vec![
            json!({"type":"Filled","fill":{"side":"BUY","quantity":"32","price":"0.04","commission":"0.00000000 USDT","quantityCommission":"0.64"}}),
            json!({"type":"Filled","fill":{"side":"SELL","quantity":"31.36","price":"0.039","commission":"0.02446080 USDT"}}),
        ];
        assert_eq!(cycle_cash_flow(&fills).unwrap(), Decimal::new(-8142080, 8));
        let mut invalid = fills.clone();
        invalid[1]["fill"]["commission"] = json!("0.02446080 BTC");
        assert!(cycle_cash_flow(&invalid).is_err());
        invalid = fills;
        invalid[0]["fill"]["side"] = Value::Null;
        assert!(cycle_cash_flow(&invalid).is_err());
    }

    #[test]
    fn checkpoint_keeps_previous_json_when_pending_file_is_owned_elsewhere() {
        let dir =
            std::env::temp_dir().join(format!("predict-host-{}", nautilus_core::UUID4::new()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("native_checkpoint.json");
        checkpoint(&path, &json!({"sequence":1})).unwrap();
        checkpoint(&path, &json!({"sequence":2})).unwrap();
        let pending = path.with_extension("json.pending");
        std::fs::write(&pending, "owned").unwrap();
        assert!(checkpoint(&path, &json!({"sequence":3})).is_err());
        assert_eq!(std::fs::read_to_string(&pending).unwrap(), "owned");
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(path).unwrap()).unwrap()["sequence"],
            2
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
