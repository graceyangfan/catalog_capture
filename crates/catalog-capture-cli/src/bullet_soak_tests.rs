// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Explicit opt-in mainnet capture through the production runner and Native socket controller.

use std::{io::Write, path::Path, time::Duration};

use nautilus_common::{
    live::runner::try_get_system_command_sender,
    messages::{SystemCommand, system::ReconnectSocket},
};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::identifiers::{ClientId, TraderId};

use super::{PostRunReportOptions, run_capture, validate_runtime};
use crate::config::{load_config, resolve_config};

#[path = "bullet_soak_readback.rs"]
mod readback;

#[tokio::test(flavor = "current_thread")]
#[ignore = "requires public mainnet, BULLET_SOAK_CONFIG and BULLET_SOAK_RECONNECT_UNIX_SECS"]
async fn bullet_btc_combined_soak() -> anyhow::Result<()> {
    nautilus_common::logging::ensure_logging_initialized();
    let path = std::env::var("BULLET_SOAK_CONFIG")?;
    let config = resolve_config(load_config(Path::new(&path))?)?;
    validate_runtime(&config)?;
    anyhow::ensure!(
        config.runtime.capture_seconds > 0,
        "use a finite soak duration"
    );
    anyhow::ensure!(
        config.venues.len() == 1 && config.venues[0].id() == "bullet_main",
        "probe requires the dedicated bullet_main client"
    );
    anyhow::ensure!(
        config.plan.book_deltas.len() == 1
            && config.plan.trades.len() == 1
            && config.plan.mark_prices.len() == 1
            && config.plan.index_prices.len() == 1
            && config.plan.funding_rates.len() == 1
            && config.plan.custom_data_requests.len() == 1,
        "probe requires the combined BTC profile"
    );
    let root = catalog_capture_core::catalog_root_from_uri(&config.capture.catalog_uri)?;
    anyhow::ensure!(!root.exists(), "use a fresh catalog root");
    std::fs::create_dir_all(root.join("metadata"))?;
    let clock = get_atomic_clock_realtime();
    let now = clock.get_time_ns().as_u64();
    let mut times = match std::env::var("BULLET_SOAK_RECONNECT_UNIX_NS") {
        Ok(value) => value
            .split(',')
            .map(str::parse::<u64>)
            .collect::<Result<Vec<_>, _>>()?,
        Err(std::env::VarError::NotPresent) => std::env::var("BULLET_SOAK_RECONNECT_UNIX_SECS")?
            .split(',')
            .map(|s| {
                s.parse::<u64>()?
                    .checked_mul(1_000_000_000)
                    .ok_or_else(|| anyhow::anyhow!("reconnect time overflow"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
        Err(error) => return Err(error.into()),
    };
    times.sort_unstable();
    times.dedup();
    anyhow::ensure!(!times.is_empty(), "at least one reconnect is required");
    anyhow::ensure!(
        times.iter().all(|time| *time > now
            && *time + 20_000_000_000 < now + config.runtime.capture_seconds * 1_000_000_000),
        "reconnects must leave time for startup and recovery within the recording"
    );
    let trader_id = TraderId::new_checked(&config.runtime.node_name)
        .unwrap_or_else(|_| TraderId::new("CAPTURE-001"));
    let quality_root = root.clone();
    let faults = tokio::spawn(async move {
        for time in times {
            let target = time;
            tokio::time::sleep(Duration::from_nanos(
                target.saturating_sub(clock.get_time_ns().as_u64()),
            ))
            .await;
            let ts_init = clock.get_time_ns();
            let sender = try_get_system_command_sender()
                .ok_or_else(|| anyhow::anyhow!("Native system command channel unavailable"))?;
            sender.send(SystemCommand::ReconnectSocket(ReconnectSocket::new(
                trader_id,
                ClientId::from("bullet_main"),
                "public-data".into(),
                ts_init,
            )))?;
            let record = serde_json::json!({"kind":"native_controller_reconnect", "ts_init_ns":ts_init.as_u64(), "scheduled_unix_ns":time, "client_id":"bullet_main", "endpoint":"public-data"});
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(root.join("metadata/soak_reconnect_requests.jsonl"))?;
            writeln!(file, "{record}")?;
            file.sync_all()?;
            log::info!("Bullet soak injected Native reconnect: {record}");
        }
        anyhow::Ok(())
    });
    let result = run_capture(config, PostRunReportOptions::default()).await;
    if result.is_err() || !faults.is_finished() {
        faults.abort();
    }
    if let Err(error) = result {
        let quality: serde_json::Value = serde_json::from_str(
            std::fs::read_to_string(quality_root.join("metadata/capture_quality.jsonl"))?
                .lines()
                .last()
                .ok_or_else(|| anyhow::anyhow!("missing quality record"))?,
        )?;
        if !only_source_gap_failure(&quality) {
            return Err(error);
        }
        log::warn!(
            "Planned reconnects recorded explicit source gaps; original quality verdict retained: {error:#}"
        );
    }
    faults.await??;
    Ok(())
}

fn only_source_gap_failure(quality: &serde_json::Value) -> bool {
    let metrics = &quality["metrics"];
    if quality["status"] != "failed"
        || !metrics["failure"].is_null()
        || !quality["error"]
            .as_str()
            .is_some_and(|s| s.starts_with("recording is incomplete:"))
    {
        return false;
    }
    for key in [
        "dropped_items",
        "write_errors",
        "failed_workers",
        "queued_items",
        "in_flight_items",
    ] {
        if metrics["aggregated"][key].as_u64() != Some(0) {
            return false;
        }
    }
    let Some(families) = metrics["families"].as_array() else {
        return false;
    };
    let mut gaps = 0;
    for family in families {
        let failures = &family["failures"];
        if failures["evicted_through"].as_u64() != Some(0) {
            return false;
        }
        let Some(records) = failures["records"].as_array() else {
            return false;
        };
        if failures["generation"].as_u64().unwrap_or(1) > 0 && records.is_empty() {
            return false;
        }
        for record in records {
            if family["family"] != "book_deltas" || record["operation"] != "source_gap" {
                return false;
            }
            gaps += 1;
        }
    }
    gaps > 0
}

#[test]
fn planned_gap_verdict_never_hides_writer_or_admission_failures() {
    let mut quality = serde_json::json!({"status":"failed","error":"recording is incomplete: source gap","metrics":{"failure":null,"aggregated":{"dropped_items":0,"write_errors":0,"failed_workers":0,"queued_items":0,"in_flight_items":0},"families":[{"family":"book_deltas","failures":{"generation":1,"evicted_through":0,"records":[{"operation":"source_gap"}]}}]}});
    assert!(only_source_gap_failure(&quality));
    quality["metrics"]["aggregated"]["write_errors"] = 1.into();
    assert!(!only_source_gap_failure(&quality));
    quality["metrics"]["aggregated"]["write_errors"] = 0.into();
    quality["metrics"]["families"][0]["failures"]["records"][0]["operation"] = "admission".into();
    assert!(!only_source_gap_failure(&quality));
}
