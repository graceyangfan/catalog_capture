// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------
use std::{
    sync::RwLock,
    time::{Duration, Instant},
};

use catalog_capture_core::{
    CaptureMetricsSnapshot, metrics_export::process_rss_bytes, unix_time_ms,
};
use nautilus_live::node::LiveNodeHandle;

use crate::config::CapturePressureConfig;

const QUEUE_GRACE: Duration = Duration::from_secs(10);
const STALL_TIMEOUT: Duration = Duration::from_secs(30);
const REPORT_INTERVAL: Duration = Duration::from_secs(30);

/// Ask the service manager to start its own stop deadline.
pub async fn request_service_stop(unit: &std::ffi::OsStr) -> anyhow::Result<()> {
    let mut command = tokio::process::Command::new("systemctl");
    command
        .args(["--user", "--no-block", "stop", "--"])
        .arg(unit)
        .kill_on_drop(true);
    let status = tokio::time::timeout(Duration::from_secs(5), command.status()).await??;
    anyhow::ensure!(status.success(), "systemctl stop returned {status}");
    Ok(())
}

pub async fn monitor(
    config: &CapturePressureConfig,
    node: &LiveNodeHandle,
    writer_metrics: &RwLock<CaptureMetricsSnapshot>,
) -> String {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut next_report = Instant::now();
    let mut queue_above_since = None;
    let mut heartbeat = (0, Instant::now());

    loop {
        tick.tick().await;
        let now = Instant::now();
        let rss = process_rss_bytes();
        if rss.is_none() && config.max_rss_bytes.is_some() {
            return "capture pressure stop: configured RSS limit could not be monitored".into();
        }
        let native = node.metrics_snapshot();
        let depth = native.data_events.queue_depth;
        if let Ok(snapshot) = writer_metrics.try_read() {
            if snapshot.captured_at_unix_ms != heartbeat.0 || !node.is_running() {
                heartbeat = (snapshot.captured_at_unix_ms, now);
            }
            if let Some(reason) = writer_stall(&snapshot, STALL_TIMEOUT) {
                return reason;
            }
        }
        if node.is_running() && now.duration_since(heartbeat.1) >= STALL_TIMEOUT {
            return "capture pressure stop: recording actor heartbeat stalled".into();
        }
        if now >= next_report {
            let (
                writer_bytes,
                writer_oldest_ms,
                writer_sample_age_ms,
                active_writers,
                writer_in_flight_bytes,
                writer_buffered_bytes,
            ) = writer_metrics
                .try_read()
                .ok()
                .filter(|snapshot| snapshot.captured_at_unix_ms > 0)
                .map_or((None, None, None, None, None, None), |snapshot| {
                    let metrics = &snapshot.aggregated;
                    (
                        Some(metrics.queued_bytes),
                        Some(metrics.oldest_queued_ns / 1_000_000),
                        Some(unix_time_ms().saturating_sub(snapshot.captured_at_unix_ms)),
                        Some(metrics.active_writers),
                        Some(metrics.in_flight_bytes),
                        Some(metrics.buffered_bytes),
                    )
                });
            log::info!(
                "capture pressure: rss_bytes={rss:?} native_data_queue_depth={depth} native_data_dispatched={} writer_queued_bytes={writer_bytes:?} writer_oldest_ms={writer_oldest_ms:?} writer_sample_age_ms={writer_sample_age_ms:?} native_data_busy_ns={} active_writers={active_writers:?} writer_in_flight_bytes={writer_in_flight_bytes:?} writer_buffered_bytes={writer_buffered_bytes:?}",
                native.data_events.dispatched,
                native.data_events.dispatch_busy_ns,
            );
            next_report = now + REPORT_INTERVAL;
        }

        if let Some(reason) = stop_reason(config, now, rss, depth, &mut queue_above_since) {
            return reason;
        }
    }
}

fn writer_stall(snapshot: &CaptureMetricsSnapshot, timeout: Duration) -> Option<String> {
    snapshot.families.iter().find_map(|family| {
        let age = Duration::from_nanos(
            family
                .metrics
                .worker_busy_ns
                .max(family.metrics.oldest_queued_ns),
        );
        (age >= timeout).then(|| {
            format!(
                "capture pressure stop: {} writer stalled for {:.1}s",
                family.family,
                age.as_secs_f64()
            )
        })
    })
}

fn stop_reason(
    config: &CapturePressureConfig,
    now: Instant,
    rss: Option<u64>,
    queue_depth: usize,
    queue_above_since: &mut Option<Instant>,
) -> Option<String> {
    if let Some((rss, limit)) = rss.zip(config.max_rss_bytes)
        && rss >= limit
    {
        return Some(format!(
            "capture pressure stop: RSS {rss} bytes reached {limit} bytes"
        ));
    }
    if let Some(limit) = config.max_data_queue_depth
        && queue_depth >= limit
    {
        let started = *queue_above_since.get_or_insert(now);
        if now.duration_since(started) >= QUEUE_GRACE {
            return Some(format!(
                "capture pressure stop: Native data-event backlog held at or above {limit} for 10 seconds (last depth {queue_depth})"
            ));
        }
    } else {
        *queue_above_since = None;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clears_transient_backlog_and_stops_on_sustained_pressure_or_rss_limit() {
        let config = CapturePressureConfig {
            max_rss_bytes: Some(1_000),
            max_data_queue_depth: Some(100),
        };
        let start = Instant::now();
        let mut since = None;
        let mut sample = |secs, rss, depth| {
            stop_reason(
                &config,
                start + Duration::from_secs(secs),
                rss,
                depth,
                &mut since,
            )
        };
        assert!(sample(0, Some(999), 100).is_none());
        assert!(sample(9, Some(999), 99).is_none());
        assert!(sample(10, Some(999), 100).is_none());
        assert!(sample(19, Some(999), 110).is_none());
        assert!(sample(20, Some(999), 110).unwrap().contains("backlog"));
        assert!(sample(21, Some(1_000), 0).unwrap().contains("RSS"));
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn rss_guard_samples_the_running_process() {
        let config = CapturePressureConfig {
            max_rss_bytes: Some(1),
            ..Default::default()
        };
        let node = LiveNodeHandle::new();
        let writer = RwLock::new(CaptureMetricsSnapshot::default());
        let reason = tokio::time::timeout(Duration::from_secs(5), monitor(&config, &node, &writer))
            .await
            .unwrap();
        assert!(reason.contains("RSS"));
    }

    #[cfg(unix)]
    #[test]
    fn external_stop_deadline_terminates_a_stalled_writer_process() {
        use std::{
            fs,
            os::unix::{fs::PermissionsExt, process::ExitStatusExt},
            process::Command,
        };

        let root = std::env::temp_dir().join(format!(
            "capture-external-stop-{}",
            nautilus_core::UUID4::new()
        ));
        fs::create_dir_all(&root).unwrap();
        let control = root.join("systemctl");
        // The shim validates the production command and enforces the stop
        // deadline in a process outside the stalled recorder.
        fs::write(
            &control,
            r#"#!/bin/sh
set -eu
[ "$*" = "--user --no-block stop -- capture-test.service" ]
recorder=$PPID
printf '%s' "$recorder" > "$CAPTURE_TEST_STOP_REQUEST"
kill -TERM "$recorder"
(sleep 1; kill -KILL "$recorder") </dev/null >/dev/null 2>&1 &
"#,
        )
        .unwrap();
        fs::set_permissions(&control, fs::Permissions::from_mode(0o700)).unwrap();
        let marker = root.join("stop-request");
        let path = std::env::join_paths(std::iter::once(root.clone()).chain(
            std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
        ))
        .unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "pressure::tests::stalled_writer_process",
                "--ignored",
                "--nocapture",
            ])
            .env("PATH", path)
            .env("CAPTURE_TEST_STOP_REQUEST", &marker)
            .env("CAPTURE_TEST_SYSTEMD_UNIT", "capture-test.service")
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(8);
        let result = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break Some(status);
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                child.wait().unwrap();
                break None;
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        let request = fs::read_to_string(&marker);
        fs::remove_dir_all(root).unwrap();
        assert_eq!(request.unwrap(), child.id().to_string());
        assert_eq!(
            result
                .expect("external stop deadline was not enforced")
                .signal(),
            Some(9)
        );
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "subprocess for the external stop fault injection"]
    fn stalled_writer_process() {
        use std::{path::PathBuf, sync::mpsc};

        use catalog_capture_core::{
            CaptureConfig, CaptureItem, PartitionKey, background::BackgroundCaptureRuntime,
            sink::CaptureSink,
        };
        struct StalledSink(mpsc::Sender<()>);
        impl CaptureSink<u64> for StalledSink {
            fn write_batch(&mut self, _: &str, _: Vec<u64>) -> anyhow::Result<Vec<PathBuf>> {
                self.0.send(()).unwrap();
                loop {
                    std::thread::park();
                }
            }
        }
        let Some(unit) = std::env::var_os("CAPTURE_TEST_SYSTEMD_UNIT") else {
            return;
        };
        let (entered, waiting) = mpsc::channel();
        let mut writer = BackgroundCaptureRuntime::new(
            CaptureConfig {
                flush_rows: 1,
                ..Default::default()
            },
            StalledSink(entered),
        )
        .unwrap();
        writer
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("test", "STALL"),
                event_ts_ns: 1,
                init_ts_ns: Some(1),
                row_count: 1,
                estimated_bytes: 8,
                payload: 1,
            })
            .unwrap();
        waiting.recv_timeout(Duration::from_secs(2)).unwrap();
        let metrics = writer.metrics();
        assert_eq!(metrics.queued_items, 0);
        // The interval flush can be blocked even when both queue counters are zero.
        let snapshot = CaptureMetricsSnapshot {
            families: vec![catalog_capture_core::metrics_export::FamilyCaptureMetrics {
                family: "test".into(),
                metrics,
                failures: writer.failure_snapshot(),
            }],
            ..Default::default()
        };
        assert!(writer_stall(&snapshot, Duration::from_nanos(1)).is_some());
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                // Match the recorder's graceful SIGTERM behavior while its
                // writer prevents shutdown from returning.
                let _term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .unwrap();
                request_service_stop(&unit).await.unwrap();
                let _ = writer.shutdown();
            });
        panic!("stalled writer returned");
    }
}
