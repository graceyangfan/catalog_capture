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
    sync::{Arc, Condvar, Mutex, MutexGuard, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};

use crate::{
    config::{CaptureConfig, OverflowPolicy},
    item::CaptureItem,
    metrics::{CaptureMetrics, FlushReason},
    runtime::{CaptureRuntime, FlushResult},
    sink::CaptureSink,
};

type WorkerReply = std::result::Result<FlushResult, String>;

#[derive(Debug, Default)]
enum SealRequest {
    #[default]
    None,
    All,
    Partitions(std::collections::BTreeSet<String>),
}

type WorkerBatch<T> = (
    Vec<CaptureItem<T>>,
    Option<FlushReason>,
    bool,
    SealRequest,
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
    seal_requested: SealRequest,
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
            seal_requested: SealRequest::None,
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
        self.request_seal(None)
    }

    /// Drains accepted data and seals only the retiring partition.
    pub fn seal_partition(&self, key: &str) -> Result<FlushResult> {
        self.request_seal(Some(key))
    }

    pub fn shutdown(&mut self) -> Result<FlushResult> {
        if self.worker.is_none() {
            return Ok(FlushResult::default());
        }
        let result = self.request_flush(true);
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| anyhow!("background capture worker thread panicked"))?;
        }
        result
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
            return Err(anyhow!("background capture worker is not running"));
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

    fn request_seal(&self, partition: Option<&str>) -> Result<FlushResult> {
        self.ensure_worker_running()?;
        let (tx, rx) = mpsc::channel();
        let (lock, cvar) = &*self.state;
        {
            let mut state = lock_queue_state(lock)?;
            match partition {
                None => state.seal_requested = SealRequest::All,
                Some(key) => match &mut state.seal_requested {
                    SealRequest::All => {}
                    SealRequest::Partitions(keys) => {
                        keys.insert(key.to_string());
                    }
                    SealRequest::None => {
                        state.seal_requested = SealRequest::Partitions([key.to_string()].into());
                    }
                },
            }
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
    let mut next_interval = Instant::now() + flush_interval;
    let mut write_failure: Option<String> = None;

    loop {
        let (
            batch,
            flush_reason,
            tick_requested,
            seal_requested,
            should_shutdown,
            flush_waiters,
            seal_waiters,
            shutdown_waiters,
        ) = match collect_worker_batch(&state, flush_interval, segment_mode, &mut next_interval) {
            Ok(batch) => batch,
            Err(err) => {
                log::error!("catalog-capture: background worker exiting: {err}");
                break;
            }
        };

        let mut flush_result = process_batch(
            &mut runtime,
            batch,
            flush_reason,
            tick_requested,
            seal_requested,
            should_shutdown,
        );
        if let Err(error) = &flush_result {
            write_failure.get_or_insert_with(|| error.to_string());
        }
        // Keep receiving data, but never report a clean final recording after
        // a prior write failure. Retrying consumed batches could duplicate rows.
        if should_shutdown && let Some(error) = &write_failure {
            flush_result = Err(anyhow!("capture had a write failure: {error}"));
        }
        let metrics_snapshot = runtime.metrics.clone();

        if let Ok(mut queue_state) = lock_queue_state(&state.0) {
            queue_state.metrics = metrics_snapshot;
        }

        // Write/submit failures are reported to waiters but must not kill the family
        // worker: snapshot custom data (e.g. DeribitBookSummary) can hit recoverable
        // catalog interval conflicts, and aborting the whole stream permanently drops
        // all subsequent polls for that family.
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
                log::error!(
                    "catalog-capture: background worker batch failed (continuing): {message}"
                );
                notify_waiters(
                    &flush_waiters,
                    &seal_waiters,
                    &shutdown_waiters,
                    Err(message),
                );
                should_shutdown
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
    next_interval: &mut Instant,
) -> Result<WorkerBatch<T>> {
    let (lock, cvar) = &**state;
    let mut queue_state = lock_queue_state(lock)?;

    if queue_state.queue.is_empty()
        && queue_state.flush_reason.is_none()
        && !queue_state.tick_requested
        && matches!(queue_state.seal_requested, SealRequest::None)
        && !queue_state.shutdown_requested
    {
        let wait_for = next_interval.saturating_duration_since(Instant::now());
        let waited = cvar
            .wait_timeout(queue_state, wait_for)
            .map_err(|_| anyhow!("background capture queue state poisoned"))?;
        queue_state = waited.0;
    }

    let now = Instant::now();
    if now >= *next_interval {
        // Chunked: new catalog parquet. Segment: append open *.part (not a new day file).
        queue_state.flush_reason =
            merge_flush_reason(queue_state.flush_reason, FlushReason::Interval);
        if segment_mode {
            // Durability fsync of open part writers (after the memory flush above).
            queue_state.tick_requested = true;
        }
        *next_interval = now + flush_interval;
    }

    let batch: Vec<CaptureItem<T>> = queue_state.queue.drain(..).collect();
    let flush_reason = if queue_state.shutdown_requested {
        Some(FlushReason::Shutdown)
    } else {
        queue_state.flush_reason
    };
    let tick_requested = queue_state.tick_requested;
    let seal_requested = std::mem::take(&mut queue_state.seal_requested);
    queue_state.flush_reason = None;
    queue_state.tick_requested = false;
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
    seal_requested: SealRequest,
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

    // Seal includes the memory flush. A coincident interval flush must never
    // consume a seal request and report an unclosed file as successfully sealed.
    if should_shutdown && runtime.sink.is_segment_mode() {
        merge_runtime_step(
            runtime,
            &mut aggregated,
            &mut last_error,
            "shutdown seal",
            |runtime| runtime.seal_all_for_shutdown(),
        );
    } else if matches!(seal_requested, SealRequest::All) {
        merge_runtime_step(
            runtime,
            &mut aggregated,
            &mut last_error,
            "seal",
            |runtime| runtime.seal_all(),
        );
    } else {
        if let SealRequest::Partitions(keys) = seal_requested {
            for key in keys {
                merge_runtime_step(
                    runtime,
                    &mut aggregated,
                    &mut last_error,
                    "partition seal",
                    |runtime| runtime.seal_partition(&key),
                );
            }
        }
        if let Some(reason) = flush_reason {
            merge_runtime_step(
                runtime,
                &mut aggregated,
                &mut last_error,
                "flush",
                |runtime| runtime.flush_all_with_reason(reason),
            );
        }
    }

    if tick_requested && !should_shutdown {
        merge_runtime_step(
            runtime,
            &mut aggregated,
            &mut last_error,
            "durability tick",
            |runtime| runtime.on_tick(now_ns()),
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
    use std::time::{Duration, Instant};
    use std::{
        path::PathBuf,
        sync::{Arc, Condvar, Mutex},
    };

    use anyhow::Result;

    use super::{
        BackgroundCaptureRuntime, QueueState, SealRequest, collect_worker_batch, process_batch,
    };
    use crate::runtime::{CaptureRuntime, FlushResult};
    use crate::{
        config::{CaptureConfig, OverflowPolicy},
        item::{CaptureItem, PartitionKey},
        lifecycle::{LifecycleConfig, LifecycleMode},
        metrics::FlushReason,
        sink::CaptureSink,
    };

    #[derive(Clone, Default)]
    struct TestSink {
        batches: Arc<Mutex<Vec<Vec<u64>>>>,
        fail_on_payload: Option<u64>,
    }

    impl CaptureSink<u64> for TestSink {
        fn write_batch(&mut self, _partition_key: &str, batch: Vec<u64>) -> Result<Vec<PathBuf>> {
            if let Some(payload) = self.fail_on_payload
                && batch.contains(&payload)
            {
                anyhow::bail!("simulated write failure for payload {payload}");
            }
            self.batches.lock().expect("batches poisoned").push(batch);
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct OrderedSink {
        steps: Vec<&'static str>,
        rows: Vec<u64>,
    }

    impl CaptureSink<u64> for OrderedSink {
        fn write_batch(&mut self, _: &str, batch: Vec<u64>) -> Result<Vec<PathBuf>> {
            self.steps.push("write");
            self.rows.extend(batch);
            Ok(Vec::new())
        }
        fn seal_all(&mut self) -> Result<FlushResult> {
            self.steps.push("seal");
            Ok(FlushResult {
                files: vec!["sealed.parquet".into()],
                ..FlushResult::default()
            })
        }
        fn seal_all_for_shutdown(&mut self) -> Result<FlushResult> {
            self.steps.push("shutdown");
            Ok(FlushResult::default())
        }
        fn on_tick(&mut self, _: u64) -> Result<FlushResult> {
            self.steps.push("sync");
            Ok(FlushResult::default())
        }
        fn is_segment_mode(&self) -> bool {
            true
        }
    }

    #[test]
    fn coincident_flush_seal_and_tick_closes_file_after_writing() {
        let mut runtime = CaptureRuntime::new(CaptureConfig::default(), OrderedSink::default());
        let item = CaptureItem {
            partition_key: PartitionKey::market_data("quotes", "TEST"),
            event_ts_ns: 1,
            init_ts_ns: Some(1),
            row_count: 1,
            estimated_bytes: 8,
            payload: 42,
        };
        let result = process_batch(
            &mut runtime,
            vec![item],
            Some(FlushReason::Interval),
            true,
            SealRequest::All,
            false,
        )
        .unwrap();
        assert_eq!(runtime.sink.rows, vec![42]);
        assert_eq!(runtime.sink.steps, vec!["write", "seal", "sync"]);
        assert_eq!(result.files, vec![PathBuf::from("sealed.parquet")]);
    }

    #[test]
    fn shutdown_supersedes_seal_and_does_not_tick_closed_writers() {
        let mut runtime = CaptureRuntime::new(CaptureConfig::default(), OrderedSink::default());
        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "TEST"),
                event_ts_ns: 1,
                init_ts_ns: Some(1),
                row_count: 1,
                estimated_bytes: 8,
                payload: 42,
            })
            .unwrap();
        process_batch(
            &mut runtime,
            vec![],
            Some(FlushReason::Shutdown),
            true,
            SealRequest::All,
            true,
        )
        .unwrap();
        assert_eq!(runtime.sink.rows, vec![42]);
        assert_eq!(runtime.sink.steps, vec!["write", "shutdown"]);
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
                row_count: 1,
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
    fn interval_deadline_flushes_a_non_empty_queue() {
        let state = Arc::new((Mutex::new(QueueState::default()), Condvar::new()));
        state
            .0
            .lock()
            .expect("queue state poisoned")
            .queue
            .push_back(CaptureItem {
                partition_key: PartitionKey::market_data("trades", "TEST"),
                event_ts_ns: 1,
                init_ts_ns: Some(1),
                row_count: 1,
                estimated_bytes: 8,
                payload: 42,
            });

        let interval = Duration::from_secs(1);
        let mut next_interval = Instant::now() - interval;
        let batch = collect_worker_batch(&state, interval, true, &mut next_interval)
            .expect("worker batch collection should succeed");

        assert_eq!(batch.0.len(), 1);
        assert_eq!(batch.1, Some(FlushReason::Interval));
        assert!(batch.2, "segment durability tick should also be scheduled");
        assert!(next_interval > Instant::now());
    }

    #[test]
    fn flush_after_shutdown_returns_instead_of_waiting_for_a_missing_worker() {
        let sink = TestSink::default();
        let mut runtime = BackgroundCaptureRuntime::new(CaptureConfig::default(), sink)
            .expect("runtime should start");

        runtime.shutdown().expect("shutdown should succeed");
        let error = runtime
            .flush_all()
            .expect_err("flush after shutdown should fail");

        assert!(error.to_string().contains("not running"));
    }

    #[test]
    fn chunked_sink_keeps_interval_flush_under_segment_lifecycle_config() {
        // TestSink reports is_segment_mode=false → uses flush_interval_ms path.
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
                row_count: 1,
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

    #[derive(Clone, Default)]
    struct SegmentModeTestSink {
        batches: Arc<Mutex<Vec<Vec<u64>>>>,
        ticks: Arc<Mutex<u64>>,
    }

    impl CaptureSink<u64> for SegmentModeTestSink {
        fn write_batch(&mut self, _partition_key: &str, batch: Vec<u64>) -> Result<Vec<PathBuf>> {
            self.batches.lock().expect("batches poisoned").push(batch);
            Ok(Vec::new())
        }

        fn on_tick(&mut self, _now_ns: u64) -> Result<crate::runtime::FlushResult> {
            *self.ticks.lock().expect("ticks poisoned") += 1;
            Ok(crate::runtime::FlushResult::default())
        }

        fn is_segment_mode(&self) -> bool {
            true
        }
    }

    #[test]
    fn segment_sink_interval_flushes_memory_and_ticks() {
        let sink = SegmentModeTestSink::default();
        let batches = Arc::clone(&sink.batches);
        let ticks = Arc::clone(&sink.ticks);
        let mut runtime = BackgroundCaptureRuntime::new(
            CaptureConfig {
                flush_rows: 10_000,
                queue_capacity: 10,
                flush_interval_ms: 1_000,
                lifecycle: LifecycleConfig {
                    mode: LifecycleMode::Segment,
                    durability: crate::lifecycle::DurabilityConfig {
                        sync_interval_ms: 20,
                    },
                    ..LifecycleConfig::default()
                },
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
                row_count: 1,
                estimated_bytes: 8,
                payload: 7,
            })
            .expect("submit should succeed");

        std::thread::sleep(Duration::from_millis(100));

        let written = batches.lock().expect("batches poisoned").clone();
        assert_eq!(
            written,
            vec![vec![7]],
            "segment mode must interval-flush into the open part (not wait only for row_group_rows)"
        );
        assert!(
            *ticks.lock().expect("ticks poisoned") >= 1,
            "segment mode should also durability-tick"
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
                row_count: 1,
                estimated_bytes: 8,
                payload: 1,
            })
            .expect("submit should succeed");
        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "TEST"),
                event_ts_ns: 2,
                init_ts_ns: Some(2),
                row_count: 1,
                estimated_bytes: 8,
                payload: 2,
            })
            .expect("submit should succeed");
        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "TEST"),
                event_ts_ns: 3,
                init_ts_ns: Some(3),
                row_count: 1,
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

    /// Row-threshold tests: use chunked lifecycle so `flush_rows` is the batch trigger
    /// (production default is segment → `row_group_rows`, which would swallow tiny batches).
    fn unit_flush_config(flush_rows: usize, queue_capacity: usize) -> CaptureConfig {
        CaptureConfig {
            flush_rows,
            queue_capacity,
            flush_interval_ms: 10,
            lifecycle: LifecycleConfig {
                mode: LifecycleMode::Chunked,
                ..LifecycleConfig::default()
            },
            ..CaptureConfig::default()
        }
    }

    fn wait_queue_empty(runtime: &BackgroundCaptureRuntime<u64, TestSink>) {
        for _ in 0..100 {
            if runtime.queue_depth() == 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!(
            "queue did not drain in time (depth={})",
            runtime.queue_depth()
        );
    }

    #[test]
    fn process_batch_continues_after_single_item_failure() {
        let sink = TestSink {
            fail_on_payload: Some(2),
            ..TestSink::default()
        };
        let batches = Arc::clone(&sink.batches);
        let mut runtime = BackgroundCaptureRuntime::new(unit_flush_config(1, 10), sink)
            .expect("runtime should start");

        for payload in [1_u64, 2, 3] {
            runtime
                .submit(CaptureItem {
                    partition_key: PartitionKey::market_data("quotes", "TEST"),
                    event_ts_ns: payload,
                    init_ts_ns: Some(payload),
                    row_count: 1,
                    estimated_bytes: 8,
                    payload,
                })
                .expect("submit should succeed");
        }

        wait_queue_empty(&runtime);

        // Worker stays up after a single write failure so later items still land.
        assert!(
            runtime
                .shutdown()
                .expect_err("prior write failure must be reported")
                .to_string()
                .contains("payload 2")
        );
        let written = batches.lock().expect("batches poisoned").clone();
        let flattened: Vec<u64> = written.into_iter().flatten().collect();
        assert!(flattened.contains(&1));
        assert!(flattened.contains(&3));
        assert!(!flattened.contains(&2));
    }

    #[test]
    fn worker_continues_accepting_after_write_failure() {
        let sink = TestSink {
            fail_on_payload: Some(9),
            ..TestSink::default()
        };
        let batches = Arc::clone(&sink.batches);
        let mut runtime = BackgroundCaptureRuntime::new(unit_flush_config(1, 4), sink)
            .expect("runtime should start");

        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "TEST"),
                event_ts_ns: 9,
                init_ts_ns: Some(9),
                row_count: 1,
                estimated_bytes: 8,
                payload: 9,
            })
            .expect("submit should succeed");

        wait_queue_empty(&runtime);

        runtime
            .submit(CaptureItem {
                partition_key: PartitionKey::market_data("quotes", "TEST"),
                event_ts_ns: 10,
                init_ts_ns: Some(10),
                row_count: 1,
                estimated_bytes: 8,
                payload: 10,
            })
            .expect("submit should still succeed after prior write failure");

        wait_queue_empty(&runtime);
        assert!(
            runtime
                .shutdown()
                .expect_err("prior write failure must be reported")
                .to_string()
                .contains("payload 9")
        );
        let written = batches.lock().expect("batches poisoned").clone();
        let flattened: Vec<u64> = written.into_iter().flatten().collect();
        assert!(
            flattened.contains(&10),
            "item after failed write should still be captured: {flattened:?}"
        );
        assert!(
            !flattened.contains(&9),
            "failed payload should not appear in written batches"
        );
    }
}
