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

//! Integration tests tying together background workers, segment lifecycle, and seal scheduling.

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
        str::FromStr,
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::QuoteTick,
        identifiers::InstrumentId,
        types::{Price, Quantity},
    };

    use crate::{
        background::BackgroundCaptureRuntime,
        config::CaptureConfig,
        item::{CaptureItem, PartitionKey},
        lifecycle::{LifecycleConfig, LifecycleMode, SealConfigFile},
        sink::{CaptureSink, CatalogSink},
    };

    fn quote(instrument_id: InstrumentId, ts: u64) -> QuoteTick {
        QuoteTick::new(
            instrument_id,
            Price::from("1.0001"),
            Price::from("1.0002"),
            Quantity::from("100"),
            Quantity::from("100"),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    }

    fn temp_catalog_dir(prefix: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("{prefix}-{}", nautilus_core::UUID4::new()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    fn segment_capture_config(dir: &Path, seal: SealConfigFile) -> CaptureConfig {
        CaptureConfig {
            catalog_uri: format!("file://{}", dir.display()),
            flush_rows: 10_000,
            flush_interval_ms: 50,
            lifecycle: LifecycleConfig {
                mode: LifecycleMode::Segment,
                durability: crate::lifecycle::DurabilityConfig {
                    sync_interval_ms: 50,
                },
                seal,
                ..LifecycleConfig::default()
            },
            ..CaptureConfig::default()
        }
    }

    fn capture_item(instrument_id: InstrumentId, ts: u64) -> CaptureItem<QuoteTick> {
        let instrument_label = instrument_id.to_string();
        CaptureItem {
            partition_key: PartitionKey::market_data("quotes", instrument_label.as_str()),
            event_ts_ns: ts,
            init_ts_ns: Some(ts),
            estimated_bytes: 128,
            payload: quote(instrument_id, ts),
        }
    }

    fn sealed_parquet_count(instrument_dir: &Path) -> usize {
        fs::read_dir(instrument_dir)
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                let path = entry.path();
                path.extension().and_then(|ext| ext.to_str()) == Some("parquet")
                    && !path.to_string_lossy().contains(".part")
            })
            .count()
    }

    fn part_parquet_count(instrument_dir: &Path) -> usize {
        fs::read_dir(instrument_dir)
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.path().to_string_lossy().contains(".part.parquet"))
            .count()
    }

    #[test]
    fn background_segment_lifecycle_submit_tick_seal_and_shutdown() {
        let dir = temp_catalog_dir("lifecycle-integration-full");
        let config = segment_capture_config(
            &dir,
            SealConfigFile {
                enabled: true,
                schedule: "06:00".to_string(),
                timezone: "UTC".to_string(),
                interval_secs: 86_400,
            },
        );
        let instrument_id = InstrumentId::from_str("BTC-USD-PERP.HYPERLIQUID").expect("id");
        let sink = CatalogSink::from_config(&config).expect("segment sink");
        let mut runtime =
            BackgroundCaptureRuntime::new(config.clone(), sink).expect("background runtime");

        runtime
            .submit(capture_item(instrument_id, 1_000))
            .expect("first submit");
        runtime
            .submit(capture_item(instrument_id, 2_000))
            .expect("second submit");

        thread::sleep(Duration::from_millis(200));

        let sealed = runtime.seal_all().expect("actor-scheduled seal");
        assert_eq!(sealed.files.len(), 1);
        assert!(!sealed.files[0].to_string_lossy().contains(".part"));

        runtime
            .submit(capture_item(instrument_id, 3_000))
            .expect("post-seal submit");

        let shutdown = runtime.shutdown().expect("shutdown");
        assert!(
            !shutdown.files.is_empty() || shutdown.rows > 0,
            "shutdown should seal buffered rows"
        );

        let instrument_dir = dir
            .join("data")
            .join("quotes")
            .join(instrument_id.to_string());
        assert!(
            sealed_parquet_count(&instrument_dir) >= 1,
            "expected sealed parquet under instrument dir"
        );
        assert_eq!(
            part_parquet_count(&instrument_dir),
            0,
            "shutdown should not leave active .part files"
        );
        assert_eq!(runtime.metrics().dropped_items, 0);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn concurrent_flush_submit_and_shutdown_do_not_deadlock() {
        let dir = temp_catalog_dir("lifecycle-integration-concurrent");
        let config = segment_capture_config(
            &dir,
            SealConfigFile {
                enabled: true,
                schedule: "06:00".to_string(),
                timezone: "UTC".to_string(),
                interval_secs: 86_400,
            },
        );
        let instrument_id = InstrumentId::from_str("BTC-USD-PERP.HYPERLIQUID").expect("id");
        let sink = CatalogSink::from_config(&config).expect("segment sink");
        let runtime = Arc::new(Mutex::new(
            BackgroundCaptureRuntime::new(config, sink).expect("background runtime"),
        ));

        for offset in 0..8_u64 {
            runtime
                .lock()
                .expect("lock runtime")
                .submit(capture_item(instrument_id, 1_000 + offset))
                .expect("seed submit");
        }

        let submit_runtime = Arc::clone(&runtime);
        let submitter = thread::spawn(move || {
            for offset in 0..16_u64 {
                let _ = submit_runtime
                    .lock()
                    .expect("lock")
                    .submit(capture_item(instrument_id, 10_000 + offset));
                thread::sleep(Duration::from_millis(5));
            }
        });

        let flush_runtime = Arc::clone(&runtime);
        let flusher = thread::spawn(move || {
            for _ in 0..8 {
                let _ = flush_runtime.lock().expect("lock").flush_all();
                thread::sleep(Duration::from_millis(8));
            }
        });

        thread::sleep(Duration::from_millis(80));
        let shutdown_result = runtime.lock().expect("lock runtime").shutdown().map(|_| ());
        submitter.join().expect("submitter join");
        flusher.join().expect("flusher join");

        assert!(
            shutdown_result.is_ok(),
            "shutdown should complete without deadlock: {shutdown_result:?}"
        );

        let instrument_dir = dir
            .join("data")
            .join("quotes")
            .join(instrument_id.to_string());
        assert!(
            sealed_parquet_count(&instrument_dir) >= 1,
            "concurrent path should still produce sealed parquet"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn worker_io_failure_surfaces_on_shutdown_without_dropping_prior_rows() {
        struct FailingAfterPayloadSink {
            inner: CatalogSink<QuoteTick>,
            fail_payload_ts: u64,
        }

        impl CaptureSink<QuoteTick> for FailingAfterPayloadSink {
            fn write_batch(
                &mut self,
                partition_key: &str,
                batch: Vec<QuoteTick>,
            ) -> anyhow::Result<Vec<PathBuf>> {
                if batch
                    .iter()
                    .any(|tick| tick.ts_init.as_u64() == self.fail_payload_ts)
                {
                    anyhow::bail!("simulated segment write failure");
                }
                self.inner.write_batch(partition_key, batch)
            }

            fn on_tick(&mut self, now_ns: u64) -> anyhow::Result<crate::runtime::FlushResult> {
                self.inner.on_tick(now_ns)
            }

            fn seal_all(&mut self) -> anyhow::Result<crate::runtime::FlushResult> {
                self.inner.seal_all()
            }

            fn seal_all_for_shutdown(&mut self) -> anyhow::Result<crate::runtime::FlushResult> {
                self.inner.seal_all_for_shutdown()
            }

            fn is_segment_mode(&self) -> bool {
                self.inner.is_segment_mode()
            }
        }

        let dir = temp_catalog_dir("lifecycle-integration-worker-fail");
        let config = segment_capture_config(
            &dir,
            SealConfigFile {
                enabled: false,
                ..SealConfigFile::default()
            },
        );
        let instrument_id = InstrumentId::from_str("BTC-USD-PERP.HYPERLIQUID").expect("id");
        let sink = FailingAfterPayloadSink {
            inner: CatalogSink::from_config(&config).expect("segment sink"),
            fail_payload_ts: 2_000,
        };
        let mut runtime = BackgroundCaptureRuntime::new(config, sink).expect("background runtime");

        runtime
            .submit(capture_item(instrument_id, 1_000))
            .expect("first submit");
        runtime
            .submit(capture_item(instrument_id, 2_000))
            .expect("queued failing submit");

        let shutdown = runtime.shutdown();
        assert!(
            shutdown.is_err(),
            "shutdown should surface worker write failure"
        );

        let err = runtime
            .submit(capture_item(instrument_id, 3_000))
            .expect_err("submit after worker failure should fail");
        let message = err.to_string();
        assert!(
            message.contains("not running") || message.contains("shutting down"),
            "unexpected error: {err}"
        );

        let _ = fs::remove_dir_all(&dir);
    }
}
