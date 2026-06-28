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
    collections::VecDeque,
    sync::{mpsc, Arc, Condvar, Mutex, MutexGuard},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};

use crate::{
    config::{CaptureConfig, OverflowPolicy},
    item::CaptureItem,
    metrics::{CaptureMetrics, FlushReason},
    runtime::{CaptureRuntime, FlushResult},
    sink::CaptureSink,
};

type WorkerReply = std::result::Result<FlushResult, String>;

type WorkerBatch<T> = (
    Vec<CaptureItem<T>>,
    Option<FlushReason>,
    bool,
    bool,
    bool,
    Vec<mpsc::Sender<WorkerReply>>,
    Vec<mpsc::Sender<WorkerReply>>,
    Vec<mpsc::Sender<WorkerReply>>,
);

#[derive(Debug)]
struct QueueState<T> {
    queue: VecDeque<CaptureItem<T>>,
    metrics: CaptureMetrics,
    flush_reason: Option<FlushReason>,
    tick_requested: bool,
    seal_requested: bool,
    shutdown_requested: bool,
    flush_waiters: Vec<mpsc::Sender<WorkerReply>>,
    seal_waiters: Vec<mpsc::Sender<WorkerReply>>,
    shutdown_waiters: Vec<mpsc::Sender<WorkerReply>>,
}

impl<T> Default for QueueState<T> {
    fn default() -> Self {
        Self {
            queue: VecDeque::new(),
            metrics: CaptureMetrics::default(),
            flush_reason: None,
            tick_requested: false,
            seal_requested: false,
            shutdown_requested: false,
            flush_waiters: Vec::new(),
            seal_waiters: Vec::new(),
            shutdown_waiters: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct BackgroundCaptureRuntime<T, S> {
    config: CaptureConfig,
    state: Arc<(Mutex<QueueState<T>>, Condvar)>,
    worker: Option<JoinHandle<()>>,
    _marker: std::marker::PhantomData<S>,
}

impl<T, S> BackgroundCaptureRuntime<T, S>
where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    pub fn new(config: CaptureConfig, sink: S) -> Result<Self> {
        let state = Arc::new((Mutex::new(QueueState::default()), Condvar::new()));
        let worker_state = Arc::clone(&state);
        let worker_config = config.clone();
        let worker = thread::Builder::new()
            .name("catalog-capture-worker".to_string())
            .spawn(move || worker_loop(worker_config, sink, worker_state))
            .context("failed to spawn background capture worker thread")?;

        Ok(Self {
            config,
            state,
            worker: Some(worker),
            _marker: std::marker::PhantomData,
        })
    }

    pub fn submit(&self, item: CaptureItem<T>) -> Result<()> {
        self.ensure_worker_running()?;
        let (lock, cvar) = &*self.state;
        let mut state = lock_queue_state(lock)?;

        if state.shutdown_requested {
            return Err(anyhow!("capture runtime is shutting down"));
        }

        let capacity = self.config.queue_capacity.max(1);
        if state.queue.len() >= capacity {
            match self.config.overflow_policy {
                OverflowPolicy::DropNewest => {
                    state.metrics.dropped_items += 1;
                    log::warn!(
                        "catalog-capture: dropped newest queued item for partition {} (queue capacity {capacity})",
                        item.partition_key.stable_key()
                    );
                    return Ok(());
                }
                OverflowPolicy::DropOldest => {
                    if let Some(dropped) = state.queue.pop_front() {
                        state.metrics.dropped_items += 1;
                        log::warn!(
                            "catalog-capture: dropped oldest queued item for partition {} (queue capacity {capacity})",
                            dropped.partition_key.stable_key()
                        );
                    }
                }
                OverflowPolicy::FailFast => {
                    return Err(anyhow!(
                        "capture queue full at capacity {} for catalog {}",
                        capacity,
                        self.config.catalog_uri
                    ));
                }
            }
        }

        state.queue.push_back(item);
        state.metrics.accepted_items += 1;
        cvar.notify_one();
        Ok(())
    }

    pub fn flush_all(&self) -> Result<FlushResult> {
        self.request_flush(false)
    }

    pub fn seal_all(&self) -> Result<FlushResult> {
        self.request_seal()
    }

    pub fn shutdown(&mut self) -> Result<FlushResult> {
        if self.worker.is_none() {
            return Ok(FlushResult::default());
        }
        let result = self.request_flush(true)?;
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| anyhow!("background capture worker thread panicked"))?;
        }
        Ok(result)
    }

    pub fn metrics(&self) -> CaptureMetrics {
        let (lock, _) = &*self.state;
        match lock_queue_state(lock) {
            Ok(state) => {
                let mut metrics = state.metrics.clone();
                metrics.queued_items = state.queue.len() as u64;
                metrics
            }
            Err(_) => CaptureMetrics::default(),
        }
    }

    pub fn queue_depth(&self) -> usize {
        let (lock, _) = &*self.state;
        lock_queue_state(lock)
            .map(|state| state.queue.len())
            .unwrap_or(0)
    }

    fn ensure_worker_running(&self) -> Result<()> {
        let Some(worker) = &self.worker else {
            return Ok(());
        };
        if worker.is_finished() {
            return Err(anyhow!("background capture worker is not running"));
        }
        Ok(())
    }

    fn request_flush(&self, shutdown: bool) -> Result<FlushResult> {
        self.ensure_worker_running()?;
        let (tx, rx) = mpsc::channel();
        let (lock, cvar) = &*self.state;
        {
            let mut state = lock_queue_state(lock)?;
            if shutdown {
                state.shutdown_requested = true;
                state.flush_reason = Some(FlushReason::Shutdown);
                state.shutdown_waiters.push(tx);
            } else {
                state.flush_reason = merge_flush_reason(state.flush_reason, FlushReason::Manual);
                state.flush_waiters.push(tx);
            }
        }
        cvar.notify_one();

        match rx.recv() {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(err)) => Err(anyhow!(err)),
            Err(err) => Err(anyhow!(
                "background capture worker reply failed: {err} (worker may have exited)"
            )),
        }
    }

    fn request_seal(&self) -> Result<FlushResult> {
        self.ensure_worker_running()?;
        let (tx, rx) = mpsc::channel();
        let (lock, cvar) = &*self.state;
        {
            let mut state = lock_queue_state(lock)?;
            state.seal_requested = true;
            state.seal_waiters.push(tx);
        }
        cvar.notify_one();

        match rx.recv() {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(err)) => Err(anyhow!(err)),
            Err(err) => Err(anyhow!(
                "background capture worker reply failed: {err} (worker may have exited)"
            )),
        }
    }
}

fn lock_queue_state<T>(lock: &Mutex<QueueState<T>>) -> Result<MutexGuard<'_, QueueState<T>>> {
    lock.lock()
        .map_err(|_| anyhow!("background capture queue state poisoned"))
}

fn worker_interval(config: &CaptureConfig, segment_mode: bool) -> Duration {
    if segment_mode {
        Duration::from_millis(config.lifecycle.durability.sync_interval_ms.max(1))
    } else {
        Duration::from_millis(config.flush_interval_ms.max(1))
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
}

fn worker_loop<T, S>(config: CaptureConfig, sink: S, state: Arc<(Mutex<QueueState<T>>, Condvar)>)
where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    let segment_mode = sink.is_segment_mode();
    let mut runtime = CaptureRuntime::new(config.clone(), sink);
    let flush_interval = worker_interval(&config, segment_mode);
    let mut worker_failed = false;

    loop {
        if worker_failed {
            break;
        }

        let (
            batch,
            flush_reason,
            tick_requested,
            seal_requested,
            should_shutdown,
            flush_waiters,
            seal_waiters,
            shutdown_waiters,
        ) = match collect_worker_batch(&state, flush_interval, segment_mode) {
            Ok(batch) => batch,
            Err(err) => {
                log::error!("catalog-capture: background worker exiting: {err}");
                break;
            }
        };

        let flush_result = process_batch(
            &mut runtime,
            batch,
            flush_reason,
            tick_requested,
            seal_requested,
            should_shutdown,
        );
        let metrics_snapshot = runtime.metrics.clone();

        if let Ok(mut queue_state) = lock_queue_state(&state.0) {
            queue_state.metrics = metrics_snapshot;
        }

        let worker_should_exit = match &flush_result {
            Ok(result) => {
                notify_waiters(
                    &flush_waiters,
                    &seal_waiters,
                    &shutdown_waiters,
                    Ok(result.clone()),
                );
                should_shutdown
            }
            Err(err) => {
                let message = err.to_string();
                notify_waiters(
                    &flush_waiters,
                    &seal_waiters,
                    &shutdown_waiters,
                    Err(message),
                );
                worker_failed = true;
                true
            }
        };

        if worker_should_exit {
            break;
        }
    }
}

fn collect_worker_batch<T>(
    state: &Arc<(Mutex<QueueState<T>>, Condvar)>,
    flush_interval: Duration,
    segment_mode: bool,
) -> Result<WorkerBatch<T>> {
    let (lock, cvar) = &**state;
    let mut queue_state = lock_queue_state(lock)?;

    if queue_state.queue.is_empty()
        && queue_state.flush_reason.is_none()
        && !queue_state.tick_requested
        && !queue_state.seal_requested
        && !queue_state.shutdown_requested
    {
        let waited = cvar
            .wait_timeout(queue_state, flush_interval)
            .map_err(|_| anyhow!("background capture queue state poisoned"))?;
        queue_state = waited.0;

        if waited.1.timed_out() {
            if segment_mode {
                queue_state.tick_requested = true;
            } else {
                queue_state.flush_reason =
                    merge_flush_reason(queue_state.flush_reason, FlushReason::Interval);
            }
        }
    }

    let batch: Vec<CaptureItem<T>> = queue_state.queue.drain(..).collect();
    let flush_reason = if queue_state.shutdown_requested {
        Some(FlushReason::Shutdown)
    } else {
        queue_state.flush_reason
    };
    let tick_requested = queue_state.tick_requested;
    let seal_requested = queue_state.seal_requested;
    queue_state.flush_reason = None;
    queue_state.tick_requested = false;
    queue_state.seal_requested = false;
    let flush_waiters = std::mem::take(&mut queue_state.flush_waiters);
    let seal_waiters = std::mem::take(&mut queue_state.seal_waiters);
    let shutdown_waiters = std::mem::take(&mut queue_state.shutdown_waiters);
    let should_shutdown = queue_state.shutdown_requested;

    Ok((
        batch,
        flush_reason,
        tick_requested,
        seal_requested,
        should_shutdown,
        flush_waiters,
        seal_waiters,
        shutdown_waiters,
    ))
}

fn notify_waiters(
    flush_waiters: &[mpsc::Sender<WorkerReply>],
    seal_waiters: &[mpsc::Sender<WorkerReply>],
    shutdown_waiters: &[mpsc::Sender<WorkerReply>],
    reply: WorkerReply,
) {
    for waiter in flush_waiters
        .iter()
        .chain(seal_waiters)
        .chain(shutdown_waiters)
    {
        let _ = waiter.send(reply.clone());
    }
}

fn process_batch<T, S>(
    runtime: &mut CaptureRuntime<T, S>,
    batch: Vec<CaptureItem<T>>,
    flush_reason: Option<FlushReason>,
    tick_requested: bool,
    seal_requested: bool,
    should_shutdown: bool,
) -> Result<FlushResult>
where
    S: CaptureSink<T>,
{
    let mut aggregated = FlushResult::default();
    let mut last_error: Option<anyhow::Error> = None;

    for item in batch {
        let partition = item.partition_key.stable_key();
        match runtime.submit(item) {
            Ok(partial) => merge_flush_into(&mut aggregated, partial),
            Err(err) => {
                runtime.metrics.dropped_items += 1;
                log::warn!(
                    "catalog-capture: failed to submit queued item for partition {partition}: {err}"
                );
                last_error = Some(err);
            }
        }
    }

    if tick_requested {
        merge_runtime_step(
            runtime,
            &mut aggregated,
            &mut last_error,
            "durability tick",
            |runtime| runtime.on_tick(now_ns()),
        );
    }

    if let Some(reason) = flush_reason {
        let step = if should_shutdown && runtime.sink.is_segment_mode() {
            "shutdown seal"
        } else {
            "flush"
        };
        merge_runtime_step(runtime, &mut aggregated, &mut last_error, step, |runtime| {
            if should_shutdown && runtime.sink.is_segment_mode() {
                runtime.seal_all_for_shutdown()
            } else {
                runtime.flush_all_with_reason(reason)
            }
        });
    } else if seal_requested {
        merge_runtime_step(
            runtime,
            &mut aggregated,
            &mut last_error,
            "seal",
            |runtime| runtime.seal_all(),
        );
    }

    if let Some(err) = last_error {
        Err(err)
    } else {
        Ok(aggregated)
    }
}

fn merge_runtime_step<T, S>(
    runtime: &mut CaptureRuntime<T, S>,
    aggregated: &mut FlushResult,
    last_error: &mut Option<anyhow::Error>,
    label: &str,
    step: impl FnOnce(&mut CaptureRuntime<T, S>) -> Result<FlushResult>,
) where
    S: CaptureSink<T>,
{
    match step(runtime) {
        Ok(partial) => merge_flush_into(aggregated, partial),
        Err(err) => {
            log::error!("catalog-capture: background worker {label} failed: {err}");
            *last_error = Some(err);
        }
    }
}

fn merge_flush_into(target: &mut FlushResult, partial: FlushResult) {
    target.rows += partial.rows;
    target.bytes += partial.bytes;
    target.files.extend(partial.files);
}

fn merge_flush_reason(current: Option<FlushReason>, next: FlushReason) -> Option<FlushReason> {
    Some(match (current, next) {
        (Some(FlushReason::Shutdown), _) | (_, FlushReason::Shutdown) => FlushReason::Shutdown,
        (Some(FlushReason::Seal), _) | (_, FlushReason::Seal) => FlushReason::Seal,
        (Some(FlushReason::Manual), _) | (_, FlushReason::Manual) => FlushReason::Manual,
        (Some(FlushReason::Budget), _) | (_, FlushReason::Budget) => FlushReason::Budget,
        (Some(FlushReason::Interval), _) | (_, FlushReason::Interval) => FlushReason::Interval,
        (Some(FlushReason::Bytes), _) | (_, FlushReason::Bytes) => FlushReason::Bytes,
        _ => FlushReason::Rows,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use anyhow::Result;

    use super::BackgroundCaptureRuntime;
    use crate::{
        config::{CaptureConfig, OverflowPolicy},
        item::{CaptureItem, PartitionKey},
        lifecycle::{LifecycleConfig, LifecycleMode},
        sink::CaptureSink,
    };

    #[derive(Clone, Default)]
    struct TestSink {
        batches: Arc<Mutex<Vec<Vec<u64>>>>,
        fail_on_payload: Option<u64>,
    }

    impl CaptureSink<u64> for TestSink {
        fn write_batch(&mut self, _partition_key: &str, batch: Vec<u64>) -> Result<Vec<PathBuf>> {
            if let Some(payload) = self.fail_on_payload {
                if batch.contains(&payload) {
                    anyhow::bail!("simulated write failure for payload {payload}");
                }
            }
            self.batches.lock().expect("batches poisoned").push(batch);
            Ok(Vec::new())
        }
    }

    #[test]
    fn timed_flush_drains_tail_batch() {
        let sink = TestSink::default();
        let batches = Arc::clone(&sink.batches);
        let mut runtime = BackgroundCaptureRuntime::new(
            CaptureConfig {
                flush_rows: 10,
                queue_capacity: 10,
                flush_interval_ms: 20,
                ..CaptureConfig::default()
            },
            sink,
        )
        .expect("runtime should start");

        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "TEST"),
                event_ts_ns: 1,
                init_ts_ns: Some(1),
                estimated_bytes: 8,
                payload: 42,
            })
            .expect("submit should succeed");

        std::thread::sleep(Duration::from_millis(80));

        let written = batches.lock().expect("batches poisoned").clone();
        assert_eq!(written, vec![vec![42]]);
        let _ = runtime.shutdown().expect("shutdown should succeed");
    }

    #[test]
    fn chunked_sink_keeps_interval_flush_under_segment_lifecycle_config() {
        let sink = TestSink::default();
        let batches = Arc::clone(&sink.batches);
        let mut runtime = BackgroundCaptureRuntime::new(
            CaptureConfig {
                flush_rows: 10,
                queue_capacity: 10,
                flush_interval_ms: 20,
                lifecycle: LifecycleConfig {
                    mode: LifecycleMode::Segment,
                    ..LifecycleConfig::default()
                },
                ..CaptureConfig::default()
            },
            sink,
        )
        .expect("runtime should start");

        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("custom_data", "TEST"),
                event_ts_ns: 1,
                init_ts_ns: Some(1),
                estimated_bytes: 8,
                payload: 99,
            })
            .expect("submit should succeed");

        std::thread::sleep(Duration::from_millis(80));

        let written = batches.lock().expect("batches poisoned").clone();
        assert_eq!(
            written,
            vec![vec![99]],
            "chunked sink should interval-flush even when lifecycle.mode = segment"
        );
        let _ = runtime.shutdown().expect("shutdown should succeed");
    }

    #[test]
    fn drop_oldest_evicts_queued_item_when_full() {
        let sink = TestSink::default();
        let batches = Arc::clone(&sink.batches);
        let mut runtime = BackgroundCaptureRuntime::new(
            CaptureConfig {
                flush_rows: 2,
                queue_capacity: 2,
                flush_interval_ms: 1_000,
                overflow_policy: OverflowPolicy::DropOldest,
                ..CaptureConfig::default()
            },
            sink,
        )
        .expect("runtime should start");

        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "TEST"),
                event_ts_ns: 1,
                init_ts_ns: Some(1),
                estimated_bytes: 8,
                payload: 1,
            })
            .expect("submit should succeed");
        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "TEST"),
                event_ts_ns: 2,
                init_ts_ns: Some(2),
                estimated_bytes: 8,
                payload: 2,
            })
            .expect("submit should succeed");
        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "TEST"),
                event_ts_ns: 3,
                init_ts_ns: Some(3),
                estimated_bytes: 8,
                payload: 3,
            })
            .expect("submit should succeed");

        let _ = runtime.shutdown().expect("shutdown should succeed");
        let written = batches.lock().expect("batches poisoned").clone();
        assert!(
            written.iter().flatten().any(|item| *item == 3),
            "latest item should be retained after drop_oldest policy"
        );
    }

    #[test]
    fn process_batch_continues_after_single_item_failure() {
        let sink = TestSink {
            fail_on_payload: Some(2),
            ..TestSink::default()
        };
        let batches = Arc::clone(&sink.batches);
        let mut runtime = BackgroundCaptureRuntime::new(
            CaptureConfig {
                flush_rows: 1,
                queue_capacity: 10,
                flush_interval_ms: 1_000,
                ..CaptureConfig::default()
            },
            sink,
        )
        .expect("runtime should start");

        for payload in [1_u64, 2, 3] {
            runtime
                .submit(CaptureItem {
                    partition_key: PartitionKey::market_data("quotes", "TEST"),
                    event_ts_ns: payload,
                    init_ts_ns: Some(payload),
                    estimated_bytes: 8,
                    payload,
                })
                .expect("submit should succeed");
        }

        let shutdown = runtime.shutdown();
        assert!(shutdown.is_err(), "shutdown should report worker failure");
        let written = batches.lock().expect("batches poisoned").clone();
        let flattened: Vec<u64> = written.into_iter().flatten().collect();
        assert!(flattened.contains(&1));
        assert!(flattened.contains(&3));
        assert!(!flattened.contains(&2));
    }

    #[test]
    fn submit_fails_after_worker_exits() {
        let sink = TestSink {
            fail_on_payload: Some(9),
            ..TestSink::default()
        };
        let runtime = BackgroundCaptureRuntime::new(
            CaptureConfig {
                flush_rows: 1,
                queue_capacity: 4,
                flush_interval_ms: 1_000,
                ..CaptureConfig::default()
            },
            sink,
        )
        .expect("runtime should start");

        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "TEST"),
                event_ts_ns: 9,
                init_ts_ns: Some(9),
                estimated_bytes: 8,
                payload: 9,
            })
            .expect("submit should succeed");

        std::thread::sleep(Duration::from_millis(50));

        let err = runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "TEST"),
                event_ts_ns: 10,
                init_ts_ns: Some(10),
                estimated_bytes: 8,
                payload: 10,
            })
            .expect_err("submit should fail once worker has exited");
        assert!(
            err.to_string().contains("not running"),
            "unexpected error: {err}"
        );
    }
}
