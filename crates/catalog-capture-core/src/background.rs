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
    collections::{HashSet, VecDeque},
    sync::{Arc, Condvar, Mutex, MutexGuard, mpsc},
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};

use crate::{
    config::{CaptureConfig, OverflowPolicy},
    failures::{AffectedPartitions, FailureOperation, FailureSnapshot, FailureTracker},
    item::CaptureItem,
    metrics::{CaptureMetrics, FlushReason},
    runtime::{CaptureRuntime, FlushResult},
    sink::CaptureSink,
};

type WorkerReply = std::result::Result<FlushResult, String>;

#[derive(Debug)]
enum QueueEntry<T> {
    Data(CaptureItem<T>),
    Recovery(CaptureItem<T>),
    Initialization(CaptureItem<T>),
    Gap(Option<String>),
    Seal {
        partition: Option<String>,
        reply: Option<mpsc::Sender<WorkerReply>>,
    },
}

#[derive(Debug)]
struct QueuedEntry<T> {
    entry: QueueEntry<T>,
    enqueued_at: Instant,
}

impl<T> From<QueueEntry<T>> for QueuedEntry<T> {
    fn from(entry: QueueEntry<T>) -> Self {
        Self {
            entry,
            enqueued_at: Instant::now(),
        }
    }
}

type WorkerBatch<T> = (
    VecDeque<QueuedEntry<T>>,
    Option<FlushReason>,
    bool,
    bool,
    Vec<mpsc::Sender<WorkerReply>>,
    Vec<mpsc::Sender<WorkerReply>>,
);

#[derive(Debug)]
struct QueueState<T> {
    queue: VecDeque<QueuedEntry<T>>,
    queued_bytes: usize,
    // Coalesce only until new affected data is admitted after the marker.
    pending_gaps: HashSet<Option<String>>,
    queued_gap_count: usize,
    peak_queued_items: usize,
    peak_queued_bytes: usize,
    in_flight_items: usize,
    in_flight_bytes: usize,
    worker_started_at: Option<Instant>,
    admitted_items: u64,
    overflow_dropped_items: u64,
    metrics: CaptureMetrics,
    flush_reason: Option<FlushReason>,
    tick_requested: bool,
    shutdown_requested: bool,
    flush_waiters: Vec<mpsc::Sender<WorkerReply>>,
    shutdown_waiters: Vec<mpsc::Sender<WorkerReply>>,
}

impl<T> Default for QueueState<T> {
    fn default() -> Self {
        Self {
            queue: VecDeque::new(),
            queued_bytes: 0,
            pending_gaps: HashSet::new(),
            queued_gap_count: 0,
            peak_queued_items: 0,
            peak_queued_bytes: 0,
            in_flight_items: 0,
            in_flight_bytes: 0,
            worker_started_at: None,
            admitted_items: 0,
            overflow_dropped_items: 0,
            metrics: CaptureMetrics::default(),
            flush_reason: None,
            tick_requested: false,
            shutdown_requested: false,
            flush_waiters: Vec::new(),
            shutdown_waiters: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct BackgroundCaptureRuntime<T, S> {
    config: CaptureConfig,
    failures: FailureTracker,
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
        let runtime = CaptureRuntime::new(config.clone(), sink);
        let failures = runtime.failures.clone();
        let worker = thread::Builder::new()
            .name("catalog-capture-worker".to_string())
            .spawn(move || worker_loop(runtime, worker_state))
            .context("failed to spawn background capture worker thread")?;

        Ok(Self {
            config,
            failures,
            state,
            worker: Some(worker),
            _marker: std::marker::PhantomData,
        })
    }

    pub fn submit(&self, item: CaptureItem<T>) -> Result<()> {
        self.submit_entry(item, QueueEntry::Data)
    }

    /// Atomically admits a checkpoint after a partition seal in the same queue entry.
    pub fn submit_after_partition_seal(&self, item: CaptureItem<T>) -> Result<()> {
        self.submit_entry(item, QueueEntry::Recovery)
    }

    pub fn submit_initialization(&self, item: CaptureItem<T>) -> Result<()> {
        self.submit_entry(item, QueueEntry::Initialization)
    }

    fn submit_entry(
        &self,
        item: CaptureItem<T>,
        entry: fn(CaptureItem<T>) -> QueueEntry<T>,
    ) -> Result<()> {
        self.ensure_worker_running()?;
        let (lock, cvar) = &*self.state;
        let mut state = lock_queue_state(lock)?;

        if state.shutdown_requested {
            return Err(anyhow!("capture runtime is shutting down"));
        }

        let capacity = self.config.queue_capacity.max(1);
        let byte_capacity = self.config.ingress_max_bytes();
        if item.estimated_bytes > byte_capacity {
            state.overflow_dropped_items += 1;
            self.record_admission_failure(&mut state, &item, false);
            let message = format!(
                "capture item exceeds queue byte capacity {byte_capacity} for partition {}",
                item.partition_key.stable_key()
            );
            if self.config.overflow_policy == OverflowPolicy::FailFast {
                return Err(anyhow!(message));
            }
            log::warn!("catalog-capture: {message}");
            return Ok(());
        }
        while state.queue.len().saturating_sub(state.queued_gap_count) >= capacity
            || state.queued_bytes.saturating_add(item.estimated_bytes) > byte_capacity
        {
            match self.config.overflow_policy {
                OverflowPolicy::DropNewest => {
                    state.overflow_dropped_items += 1;
                    self.record_admission_failure(&mut state, &item, false);
                    log::warn!(
                        "catalog-capture: dropped newest queued item for partition {} (queue capacity {capacity} items / {byte_capacity} bytes)",
                        item.partition_key.stable_key()
                    );
                    return Ok(());
                }
                OverflowPolicy::DropOldest => {
                    let oldest_data = state.queue.iter().position(|entry| {
                        matches!(
                            entry.entry,
                            QueueEntry::Data(_)
                                | QueueEntry::Recovery(_)
                                | QueueEntry::Initialization(_)
                        )
                    });
                    if let Some(
                        QueueEntry::Data(dropped)
                        | QueueEntry::Recovery(dropped)
                        | QueueEntry::Initialization(dropped),
                    ) = oldest_data
                        .and_then(|index| state.queue.remove(index))
                        .map(|queued| queued.entry)
                    {
                        state.queued_bytes -= dropped.estimated_bytes;
                        state.overflow_dropped_items += 1;
                        self.record_admission_failure(&mut state, &dropped, true);
                        log::warn!(
                            "catalog-capture: dropped oldest queued item for partition {} (queue capacity {capacity} items / {byte_capacity} bytes)",
                            dropped.partition_key.stable_key()
                        );
                    } else {
                        state.overflow_dropped_items += 1;
                        self.record_admission_failure(&mut state, &item, false);
                        return Err(anyhow!("capture queue contains only pending seal commands"));
                    }
                }
                OverflowPolicy::FailFast => {
                    state.overflow_dropped_items += 1;
                    self.record_admission_failure(&mut state, &item, false);
                    return Err(anyhow!(
                        "capture queue full at capacity {capacity} items / {byte_capacity} bytes for catalog {}",
                        self.config.catalog_uri
                    ));
                }
            }
        }

        state
            .pending_gaps
            .remove(&Some(item.partition_key.stable_key()));
        state.pending_gaps.remove(&None);
        state.queued_bytes += item.estimated_bytes;
        state.queue.push_back(entry(item).into());
        state.admitted_items += 1;
        record_queue_peak(&mut state);
        cvar.notify_one();
        Ok(())
    }

    fn record_admission_failure(
        &self,
        state: &mut QueueState<T>,
        item: &CaptureItem<T>,
        at_front: bool,
    ) {
        self.failures.record(
            Some(&item.partition_key.stable_key()),
            FailureOperation::Admission,
            Some((item.event_ts_ns, item.event_ts_ns)),
        );
        let requested = Some(item.partition_key.stable_key());
        let key = if state.pending_gaps.contains(&None)
            || (state.pending_gaps.len() >= self.config.max_active_partitions.max(1)
                && !state.pending_gaps.contains(&requested))
        {
            None
        } else {
            requested
        };
        if at_front {
            // Eviction may remove the only data separating two older cuts. Rebuild
            // only on this opt-in policy, keeping cuts around surviving data and
            // bounding controls even through an arbitrarily long eviction burst.
            state.queue.push_front(QueueEntry::Gap(key).into());
            let mut pending = HashSet::new();
            let mut count = 0;
            state.queue.retain(|queued| match &queued.entry {
                QueueEntry::Gap(key) => {
                    if pending.contains(&None) || !pending.insert(key.clone()) {
                        return false;
                    }
                    count += 1;
                    true
                }
                QueueEntry::Data(item)
                | QueueEntry::Recovery(item)
                | QueueEntry::Initialization(item) => {
                    pending.remove(&Some(item.partition_key.stable_key()));
                    pending.remove(&None);
                    true
                }
                QueueEntry::Seal { .. } => true,
            });
            state.pending_gaps = pending;
            state.queued_gap_count = count;
        } else if state.pending_gaps.insert(key.clone()) {
            state.queued_gap_count += 1;
            state.queue.push_back(QueueEntry::Gap(key).into());
        }
        record_queue_peak(state);
        self.state.1.notify_one();
    }

    pub fn failures_since(&self, cursor: &mut u64) -> Option<AffectedPartitions> {
        self.failures.changed_partitions(cursor)
    }

    #[must_use]
    pub fn failure_snapshot(&self) -> FailureSnapshot {
        self.failures.snapshot()
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

    /// Enqueues an ordered seal without waiting for the writer thread.
    pub fn enqueue_seal_all(&self) -> Result<()> {
        self.enqueue_seal(None, None)
    }

    /// Seals preceding rows for this partition before subsequent queue entries.
    pub fn enqueue_seal_partition(&self, key: &str) -> Result<()> {
        self.enqueue_seal(Some(key), None)
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
        if let Ok(mut state) = lock_queue_state(&self.state.0) {
            state.queue.shrink_to_fit();
        }
        let result = result?;
        if self.config.overflow_policy == OverflowPolicy::FailFast
            && self.metrics().dropped_items > 0
        {
            return Err(anyhow!("capture rejected or lost data before shutdown"));
        }
        Ok(result)
    }

    pub fn metrics(&self) -> CaptureMetrics {
        let (lock, _) = &*self.state;
        match lock_queue_state(lock) {
            Ok(state) => {
                let mut metrics = state.metrics.clone();
                metrics.accepted_items = state.admitted_items;
                metrics.dropped_items += state.overflow_dropped_items;
                metrics.queue_capacity = self.config.queue_capacity.max(1) as u64;
                metrics.queue_max_bytes = self.config.ingress_max_bytes() as u64;
                metrics.queued_items = state.queue.len() as u64;
                metrics.queued_bytes = state.queued_bytes as u64;
                metrics.oldest_queued_ns = state
                    .queue
                    .front()
                    .map_or(0, |entry| elapsed_ns(entry.enqueued_at));
                metrics.in_flight_items = state.in_flight_items as u64;
                metrics.in_flight_bytes = state.in_flight_bytes as u64;
                metrics.worker_busy_ns = state.worker_started_at.map_or(0, elapsed_ns);
                metrics.peak_queued_items = state.peak_queued_items as u64;
                metrics.peak_queued_bytes = state.peak_queued_bytes as u64;
                if !state.shutdown_requested
                    && self
                        .worker
                        .as_ref()
                        .is_some_and(|worker| worker.is_finished())
                {
                    metrics.write_errors += 1;
                    metrics.failed_workers = 1;
                }
                metrics
            }
            Err(_) => CaptureMetrics {
                failed_workers: 1,
                ..Default::default()
            },
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

        self.receive_reply(rx)
    }

    fn request_seal(&self, partition: Option<&str>) -> Result<FlushResult> {
        let (tx, rx) = mpsc::channel();
        self.enqueue_seal(partition, Some(tx))?;
        self.receive_reply(rx)
    }

    fn receive_reply(&self, receiver: mpsc::Receiver<WorkerReply>) -> Result<FlushResult> {
        loop {
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(result) => return result.map_err(anyhow::Error::msg),
                Err(mpsc::RecvTimeoutError::Timeout) => self.ensure_worker_running()?,
                Err(error) => {
                    return Err(anyhow!("background capture worker reply failed: {error}"));
                }
            }
        }
    }

    fn enqueue_seal(
        &self,
        partition: Option<&str>,
        reply: Option<mpsc::Sender<WorkerReply>>,
    ) -> Result<()> {
        self.ensure_worker_running()?;
        let (lock, cvar) = &*self.state;
        {
            let mut state = lock_queue_state(lock)?;
            if state.shutdown_requested {
                return Err(anyhow!("capture runtime is shutting down"));
            }
            // Reserve bounded control headroom so full data admission cannot starve seals.
            if state.queue.len().saturating_sub(state.queued_gap_count)
                >= self
                    .config
                    .queue_capacity
                    .max(1)
                    .saturating_add(self.config.max_active_partitions.max(1))
            {
                return Err(anyhow!("capture queue full while enqueueing seal"));
            }
            state.queue.push_back(
                QueueEntry::Seal {
                    partition: partition.map(str::to_owned),
                    reply,
                }
                .into(),
            );
            record_queue_peak(&mut state);
        }
        cvar.notify_one();
        Ok(())
    }
}

fn record_queue_peak<T>(state: &mut QueueState<T>) {
    state.peak_queued_items = state.peak_queued_items.max(state.queue.len());
    state.peak_queued_bytes = state.peak_queued_bytes.max(state.queued_bytes);
}

impl<T, S> Drop for BackgroundCaptureRuntime<T, S> {
    fn drop(&mut self) {
        let Some(worker) = self.worker.take() else {
            return;
        };
        // Also covers partially constructed actors and early runner errors. The
        // worker owns the sink, so signal its normal drain path before joining.
        let (lock, cvar) = &*self.state;
        if let Ok(mut state) = lock.lock() {
            state.shutdown_requested = true;
            state.flush_reason = Some(FlushReason::Shutdown);
        }
        cvar.notify_one();
        if worker.join().is_err() {
            log::error!("catalog-capture: background worker panicked during drop");
        }
    }
}

fn lock_queue_state<T>(lock: &Mutex<QueueState<T>>) -> Result<MutexGuard<'_, QueueState<T>>> {
    lock.lock()
        .map_err(|_| anyhow!("background capture queue state poisoned"))
}

/// Flush and fsync are independent deadlines; neither waits for file rotation.
struct WorkerSchedule {
    flush_interval: Duration,
    next_flush: Instant,
    sync_interval: Option<Duration>,
    next_sync: Option<Instant>,
}

impl WorkerSchedule {
    fn new(config: &CaptureConfig, segment_mode: bool) -> Self {
        let now = Instant::now();
        let flush_interval = Duration::from_millis(config.flush_interval_ms.max(1));
        let sync_interval = (segment_mode && config.lifecycle.durability.sync_interval_ms > 0)
            .then(|| Duration::from_millis(config.lifecycle.durability.sync_interval_ms));
        Self {
            flush_interval,
            next_flush: now + flush_interval,
            sync_interval,
            next_sync: sync_interval.map(|interval| now + interval),
        }
    }

    fn wait_for(&self) -> Duration {
        self.next_sync
            .map_or(self.next_flush, |sync| self.next_flush.min(sync))
            .saturating_duration_since(Instant::now())
    }

    fn due(&mut self, now: Instant) -> (bool, bool) {
        let flush = now >= self.next_flush;
        if flush {
            self.next_flush = now + self.flush_interval;
        }
        let sync = self.next_sync.is_some_and(|deadline| now >= deadline);
        if sync {
            self.next_sync = self.sync_interval.map(|interval| now + interval);
        }
        (flush, sync)
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
}

fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

fn worker_loop<T, S>(mut runtime: CaptureRuntime<T, S>, state: Arc<(Mutex<QueueState<T>>, Condvar)>)
where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    let segment_mode = runtime.sink.is_segment_mode();
    let mut schedule = WorkerSchedule::new(&runtime.config, segment_mode);
    let mut write_failure: Option<String> = None;
    let mut spare = VecDeque::new();

    loop {
        let (
            mut batch,
            flush_reason,
            tick_requested,
            should_shutdown,
            flush_waiters,
            shutdown_waiters,
        ) = match collect_worker_batch(&state, &mut schedule, std::mem::take(&mut spare)) {
            Ok(batch) => batch,
            Err(err) => {
                log::error!("catalog-capture: background worker exiting: {err}");
                break;
            }
        };

        let mut flush_result = process_batch(
            &mut runtime,
            &mut batch,
            flush_reason,
            tick_requested,
            should_shutdown,
        );
        spare = batch;
        if let Err(error) = &flush_result {
            write_failure.get_or_insert_with(|| error.to_string());
            runtime.metrics.write_errors += 1;
        }
        // Keep receiving data, but never report a clean final recording after
        // a prior write failure. Retrying consumed batches could duplicate rows.
        if should_shutdown && let Some(error) = &write_failure {
            flush_result = Err(anyhow!("capture had a write failure: {error}"));
        }
        runtime.metrics.active_writers = runtime.sink.active_writers() as u64;
        let metrics_snapshot = runtime.metrics.clone();

        if let Ok(mut queue_state) = lock_queue_state(&state.0) {
            queue_state.metrics = metrics_snapshot;
            queue_state.in_flight_items = 0;
            queue_state.in_flight_bytes = 0;
            queue_state.worker_started_at = None;
        }

        // An ordinary write fault preserves liveness; only the explicit shutdown
        // barrier ends the worker. The lifetime error remains latched for final status.
        let worker_should_exit = match &flush_result {
            Ok(result) => {
                notify_waiters(&flush_waiters, &shutdown_waiters, Ok(result.clone()));
                should_shutdown
            }
            Err(err) => {
                let message = err.to_string();
                log::error!(
                    "catalog-capture: background worker batch failed (continuing): {message}"
                );
                notify_waiters(&flush_waiters, &shutdown_waiters, Err(message));
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
    schedule: &mut WorkerSchedule,
    spare: VecDeque<QueuedEntry<T>>,
) -> Result<WorkerBatch<T>> {
    let (lock, cvar) = &**state;
    let mut queue_state = lock_queue_state(lock)?;

    if queue_state.queue.is_empty()
        && queue_state.flush_reason.is_none()
        && !queue_state.tick_requested
        && !queue_state.shutdown_requested
    {
        let wait_for = schedule.wait_for();
        let waited = cvar
            .wait_timeout(queue_state, wait_for)
            .map_err(|_| anyhow!("background capture queue state poisoned"))?;
        queue_state = waited.0;
    }

    let now = Instant::now();
    let (flush_due, sync_due) = schedule.due(now);
    if flush_due {
        queue_state.flush_reason =
            merge_flush_reason(queue_state.flush_reason, FlushReason::Interval);
    }
    queue_state.tick_requested |= sync_due;

    // Move the queue in constant time. Producers never wait for a large drain/copy.
    let batch = std::mem::replace(&mut queue_state.queue, spare);
    queue_state.pending_gaps.clear();
    queue_state.queued_gap_count = 0;
    queue_state.in_flight_items = batch.len();
    queue_state.in_flight_bytes = queue_state.queued_bytes;
    queue_state.worker_started_at = Some(now);
    queue_state.queued_bytes = 0;
    let flush_reason = if queue_state.shutdown_requested {
        Some(FlushReason::Shutdown)
    } else {
        queue_state.flush_reason
    };
    let tick_requested = queue_state.tick_requested;
    queue_state.flush_reason = None;
    queue_state.tick_requested = false;
    let flush_waiters = std::mem::take(&mut queue_state.flush_waiters);
    let shutdown_waiters = std::mem::take(&mut queue_state.shutdown_waiters);
    let should_shutdown = queue_state.shutdown_requested;

    Ok((
        batch,
        flush_reason,
        tick_requested,
        should_shutdown,
        flush_waiters,
        shutdown_waiters,
    ))
}

fn notify_waiters(
    flush_waiters: &[mpsc::Sender<WorkerReply>],
    shutdown_waiters: &[mpsc::Sender<WorkerReply>],
    reply: WorkerReply,
) {
    for waiter in flush_waiters.iter().chain(shutdown_waiters) {
        let _ = waiter.send(reply.clone());
    }
}

fn process_batch<T, S>(
    runtime: &mut CaptureRuntime<T, S>,
    batch: &mut VecDeque<QueuedEntry<T>>,
    flush_reason: Option<FlushReason>,
    tick_requested: bool,
    should_shutdown: bool,
) -> Result<FlushResult>
where
    S: CaptureSink<T>,
{
    let mut aggregated = FlushResult::default();
    let mut last_error: Option<anyhow::Error> = None;

    for queued in batch.drain(..) {
        runtime
            .metrics
            .queue_wait
            .record(queued.enqueued_at.elapsed());
        let item = match queued.entry {
            QueueEntry::Gap(key) => {
                merge_runtime_step(
                    runtime,
                    &mut aggregated,
                    &mut last_error,
                    "gap boundary",
                    |runtime| runtime.break_continuity(key.as_deref()),
                );
                continue;
            }
            QueueEntry::Initialization(item) => {
                merge_runtime_step(
                    runtime,
                    &mut aggregated,
                    &mut last_error,
                    "initialization",
                    |runtime| runtime.submit_initialization(item),
                );
                continue;
            }
            QueueEntry::Data(item) => item,
            QueueEntry::Recovery(item) => {
                merge_runtime_step(
                    runtime,
                    &mut aggregated,
                    &mut last_error,
                    "recovery seal",
                    |runtime| runtime.seal_partition(&item.partition_key.stable_key()),
                );
                merge_runtime_step(
                    runtime,
                    &mut aggregated,
                    &mut last_error,
                    "recovery initialization",
                    |runtime| runtime.submit_initialization(item),
                );
                continue;
            }
            QueueEntry::Seal { partition, reply } => {
                let result = match partition {
                    Some(key) => runtime.seal_partition(&key),
                    None => runtime.seal_all(),
                };
                if let Some(reply) = reply {
                    let _ = reply.send(result.as_ref().cloned().map_err(ToString::to_string));
                }
                match result {
                    Ok(partial) => merge_flush_into(&mut aggregated, partial),
                    Err(error) => last_error = Some(error),
                }
                continue;
            }
        };
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

    // FIFO seals above cannot be moved across subsequent data or checkpoints.
    if should_shutdown && runtime.sink.is_segment_mode() {
        merge_runtime_step(
            runtime,
            &mut aggregated,
            &mut last_error,
            "shutdown seal",
            |runtime| runtime.seal_all_for_shutdown(),
        );
    } else if let Some(reason) = flush_reason {
        merge_runtime_step(
            runtime,
            &mut aggregated,
            &mut last_error,
            "flush",
            |runtime| runtime.flush_all_with_reason(reason),
        );
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
        BackgroundCaptureRuntime, QueueEntry, QueueState, WorkerSchedule, collect_worker_batch,
        process_batch,
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
            &mut [
                QueueEntry::Data(item),
                QueueEntry::Seal {
                    partition: None,
                    reply: None,
                },
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
            Some(FlushReason::Interval),
            true,
            false,
        )
        .unwrap();
        assert_eq!(runtime.sink.rows, vec![42]);
        assert_eq!(runtime.sink.steps, vec!["write", "seal", "sync"]);
        assert_eq!(result.files, vec![PathBuf::from("sealed.parquet")]);
    }

    #[test]
    fn fifo_seals_separate_same_instrument_checkpoints_and_later_data() {
        let mut runtime = CaptureRuntime::new(unit_flush_config(1, 16), OrderedSink::default());
        let mut batch = [
            QueueEntry::Data(queued_item(1, 8)),
            QueueEntry::Seal {
                partition: None,
                reply: None,
            },
            QueueEntry::Data(queued_item(2, 8)),
            QueueEntry::Seal {
                partition: None,
                reply: None,
            },
            QueueEntry::Data(queued_item(3, 8)),
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        process_batch(&mut runtime, &mut batch, None, false, false).unwrap();
        assert_eq!(runtime.sink.rows, [1, 2, 3]);
        assert_eq!(
            runtime.sink.steps,
            ["write", "seal", "write", "seal", "write"]
        );
        assert_eq!(runtime.metrics.flushed_rows, 3);
    }

    #[test]
    fn shutdown_preserves_queued_seal_and_does_not_tick_closed_writers() {
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
            &mut [QueueEntry::Seal {
                partition: None,
                reply: None,
            }]
            .into_iter()
            .map(Into::into)
            .collect(),
            Some(FlushReason::Shutdown),
            true,
            true,
        )
        .unwrap();
        assert_eq!(runtime.sink.rows, vec![42]);
        assert_eq!(runtime.sink.steps, vec!["write", "seal", "shutdown"]);
    }

    #[test]
    fn failing_oldest_partition_reports_the_triggering_rejected_item_too() {
        let config = CaptureConfig {
            flush_rows: 100,
            max_active_partitions: 1,
            ..Default::default()
        };
        let mut runtime = CaptureRuntime::new(
            config,
            TestSink {
                fail_on_payload: Some(1),
                ..Default::default()
            },
        );
        let first = queued_item(1, 8);
        let mut next = queued_item(2, 8);
        next.partition_key = PartitionKey::market_data("test", "OTHER");
        let first_key = first.partition_key.stable_key();
        let next_key = next.partition_key.stable_key();
        let mut batch = [QueueEntry::Data(first), QueueEntry::Data(next)]
            .into_iter()
            .map(Into::into)
            .collect();
        assert!(process_batch(&mut runtime, &mut batch, None, false, false).is_err());
        let affected = runtime.failures.changed_partitions(&mut 0).unwrap();
        assert!(affected.contains(&first_key));
        assert!(affected.contains(&next_key));
        assert_eq!(
            affected,
            crate::failures::AffectedPartitions::Family,
            "the one-entry history evicted an unobserved fault and must widen its scope"
        );
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
    fn dropping_a_runtime_drains_rows_without_waiting_for_the_interval() {
        let sink = TestSink::default();
        let batches = Arc::clone(&sink.batches);
        let mut config = unit_flush_config(1_000, 8);
        config.flush_interval_ms = 60_000;
        let runtime = BackgroundCaptureRuntime::new(config, sink).unwrap();
        runtime.submit(queued_item(42, 8)).unwrap();
        drop(runtime);
        assert_eq!(*batches.lock().unwrap(), [vec![42]]);
    }

    #[test]
    fn interval_deadline_flushes_a_non_empty_queue() {
        let state = Arc::new((Mutex::new(QueueState::default()), Condvar::new()));
        state
            .0
            .lock()
            .expect("queue state poisoned")
            .queue
            .push_back(
                QueueEntry::Data(CaptureItem {
                    partition_key: PartitionKey::market_data("trades", "TEST"),
                    event_ts_ns: 1,
                    init_ts_ns: Some(1),
                    row_count: 1,
                    estimated_bytes: 8,
                    payload: 42,
                })
                .into(),
            );

        let mut schedule = WorkerSchedule::new(&CaptureConfig::default(), true);
        schedule.next_flush = Instant::now() - Duration::from_secs(1);
        schedule.next_sync = Some(schedule.next_flush);
        let batch = collect_worker_batch(&state, &mut schedule, Default::default())
            .expect("worker batch collection should succeed");
        assert_eq!(batch.0.len(), 1);
        assert_eq!(batch.1, Some(FlushReason::Interval));
        assert!(batch.2, "segment durability tick should also be scheduled");
        assert!(schedule.next_flush > Instant::now());
    }

    #[test]
    fn flush_and_sync_deadlines_are_independent_and_zero_sync_is_disabled() {
        let mut config = CaptureConfig {
            flush_interval_ms: 2_000,
            ..Default::default()
        };
        config.lifecycle.durability.sync_interval_ms = 10_000;
        let mut schedule = WorkerSchedule::new(&config, true);
        let flush_at = schedule.next_flush;
        assert_eq!(schedule.due(flush_at), (true, false));
        assert_eq!(
            schedule.next_sync.unwrap() - flush_at,
            Duration::from_secs(8)
        );
        let sync_at = schedule.next_sync.unwrap();
        // A write-triggered flush before fsync does not move the sync deadline.
        schedule.next_flush = sync_at + Duration::from_secs(1);
        assert_eq!(schedule.due(sync_at), (false, true));
        config.lifecycle.durability.sync_interval_ms = 0;
        let mut schedule = WorkerSchedule::new(&config, true);
        assert_eq!(schedule.due(schedule.next_flush), (true, false));
        assert!(schedule.next_sync.is_none());
        assert!(WorkerSchedule::new(&config, false).next_sync.is_none());
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

    #[test]
    fn segment_flush_and_sync_use_their_own_intervals() {
        struct TimerSink {
            flushed: std::sync::mpsc::Sender<()>,
            synced: std::sync::mpsc::Sender<()>,
            rows: Arc<Mutex<Vec<u64>>>,
        }
        impl CaptureSink<u64> for TimerSink {
            fn write_batch(&mut self, _: &str, batch: Vec<u64>) -> Result<Vec<PathBuf>> {
                self.rows.lock().unwrap().extend(batch);
                let _ = self.flushed.send(());
                Ok(Vec::new())
            }
            fn on_tick(&mut self, _: u64) -> Result<FlushResult> {
                let _ = self.synced.send(());
                Ok(FlushResult::default())
            }
            fn is_segment_mode(&self) -> bool {
                true
            }
        }
        for (flush_ms, sync_ms, expect_flush) in
            [(10, 3_600_000, true), (3_600_000, 10, false), (10, 0, true)]
        {
            let (flushed, writes) = std::sync::mpsc::channel();
            let (synced, ticks) = std::sync::mpsc::channel();
            let rows = Arc::new(Mutex::new(Vec::new()));
            let mut config = CaptureConfig {
                flush_rows: 10_000,
                flush_interval_ms: flush_ms,
                ..Default::default()
            };
            config.lifecycle.mode = LifecycleMode::Segment;
            config.lifecycle.durability.sync_interval_ms = sync_ms;
            let mut runtime = BackgroundCaptureRuntime::new(
                config,
                TimerSink {
                    flushed,
                    synced,
                    rows: Arc::clone(&rows),
                },
            )
            .unwrap();
            runtime.submit(queued_item(7, 8)).unwrap();
            let ready = if expect_flush {
                writes.recv_timeout(Duration::from_secs(2))
            } else {
                ticks.recv_timeout(Duration::from_secs(2))
            };
            let written_before_shutdown = rows.lock().unwrap().clone();
            let other_timer_pending = if expect_flush {
                ticks.try_recv().is_err()
            } else {
                writes.try_recv().is_err()
            };
            runtime.shutdown().unwrap();
            assert!(
                ready.is_ok(),
                "the shorter configured deadline must execute"
            );
            assert!(
                other_timer_pending,
                "one deadline must not trigger the other operation"
            );
            assert_eq!(
                written_before_shutdown,
                if expect_flush { vec![7] } else { vec![] }
            );
            assert_eq!(*rows.lock().unwrap(), [7]);
        }
    }

    #[test]
    fn a_single_sync_stall_can_fill_either_sync_interval_queue_without_stopping_recording() {
        struct BlockedSync {
            entered: Option<std::sync::mpsc::Sender<()>>,
            release: std::sync::mpsc::Receiver<()>,
            rows: Arc<Mutex<Vec<u64>>>,
        }
        impl CaptureSink<u64> for BlockedSync {
            fn write_batch(&mut self, _: &str, batch: Vec<u64>) -> Result<Vec<PathBuf>> {
                self.rows.lock().unwrap().extend(batch);
                Ok(Vec::new())
            }
            fn on_tick(&mut self, _: u64) -> Result<FlushResult> {
                if let Some(entered) = self.entered.take() {
                    entered.send(()).unwrap();
                    self.release.recv().unwrap();
                }
                Ok(FlushResult::default())
            }
            fn is_segment_mode(&self) -> bool {
                true
            }
        }
        // Scaled deadlines exercise the actual worker. This measures no hardware latency.
        for sync_ms in [2, 10] {
            let (entered, waiting) = std::sync::mpsc::channel();
            let (release, proceed) = std::sync::mpsc::channel();
            let rows = Arc::new(Mutex::new(Vec::new()));
            let mut config = unit_flush_config(1, 2);
            config.queue_max_bytes = Some(16);
            config.overflow_policy = OverflowPolicy::FailFast;
            config.lifecycle.durability.sync_interval_ms = sync_ms;
            let mut runtime = BackgroundCaptureRuntime::new(
                config,
                BlockedSync {
                    entered: Some(entered),
                    release: proceed,
                    rows: Arc::clone(&rows),
                },
            )
            .unwrap();
            waiting.recv_timeout(Duration::from_secs(2)).unwrap();
            let first = runtime.submit(queued_item(1, 8));
            let second = runtime.submit(queued_item(2, 8));
            let rejected = runtime.submit(queued_item(3, 8));
            release.send(()).unwrap();
            first.unwrap();
            second.unwrap();
            assert!(rejected.is_err());
            runtime.flush_all().unwrap();
            runtime.submit(queued_item(4, 8)).unwrap();
            assert!(runtime.shutdown().is_err());
            assert_eq!(*rows.lock().unwrap(), vec![1, 2, 4]);
        }
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
        assert!(runtime.metrics().write_errors > 0);
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

    struct PausedSink {
        entered: Option<std::sync::mpsc::Sender<()>>,
        release: std::sync::mpsc::Receiver<()>,
        rows: Arc<Mutex<Vec<u64>>>,
    }

    impl CaptureSink<u64> for PausedSink {
        fn write_batch(&mut self, _: &str, batch: Vec<u64>) -> Result<Vec<PathBuf>> {
            if let Some(entered) = self.entered.take() {
                entered.send(()).unwrap();
                self.release.recv().unwrap();
            }
            self.rows.lock().unwrap().extend(batch);
            Ok(Vec::new())
        }
    }

    fn queued_item(payload: u64, bytes: usize) -> CaptureItem<u64> {
        CaptureItem {
            partition_key: PartitionKey::market_data("test", "TEST"),
            event_ts_ns: payload,
            init_ts_ns: Some(payload),
            row_count: 1,
            estimated_bytes: bytes,
            payload,
        }
    }

    struct PausedSegmentSink {
        inner: PausedSink,
        sealed_rows: Arc<Mutex<Vec<Vec<u64>>>>,
    }

    impl CaptureSink<u64> for PausedSegmentSink {
        fn write_batch(&mut self, key: &str, batch: Vec<u64>) -> Result<Vec<PathBuf>> {
            self.inner.write_batch(key, batch)
        }
        fn seal_all(&mut self) -> Result<FlushResult> {
            self.sealed_rows
                .lock()
                .unwrap()
                .push(self.inner.rows.lock().unwrap().clone());
            Ok(FlushResult::default())
        }
        fn is_segment_mode(&self) -> bool {
            true
        }
    }

    #[test]
    fn stalled_writer_accepts_burst_and_ordered_seal_without_blocking_producer() {
        let (entered, waiting) = std::sync::mpsc::channel();
        let (release, proceed) = std::sync::mpsc::channel();
        let rows = Arc::new(Mutex::new(Vec::new()));
        let sealed_rows = Arc::new(Mutex::new(Vec::new()));
        let mut config = unit_flush_config(1, 32_768);
        config.max_total_buffer_bytes = 64 * 1024 * 1024;
        let runtime = BackgroundCaptureRuntime::new(
            config,
            PausedSegmentSink {
                inner: PausedSink {
                    entered: Some(entered),
                    release: proceed,
                    rows: Arc::clone(&rows),
                },
                sealed_rows: Arc::clone(&sealed_rows),
            },
        )
        .unwrap();
        runtime.submit(queued_item(1, 8)).unwrap();
        waiting.recv_timeout(Duration::from_secs(2)).unwrap();
        // 16k items exceed the previous 8192-slot profile while the writer is paused.
        let (done, completed) = std::sync::mpsc::channel();
        let producer = std::thread::spawn(move || {
            let result = (|| -> Result<()> {
                for value in 2..=16_001 {
                    runtime.submit(queued_item(value, 8))?;
                }
                runtime.enqueue_seal_all()?;
                runtime.submit(queued_item(16_002, 8))
            })();
            let _ = done.send(result);
            runtime
        });
        let burst = completed.recv_timeout(Duration::from_secs(5));
        release.send(()).unwrap();
        let mut runtime = producer.join().unwrap();
        let shutdown = runtime.shutdown();
        burst
            .expect("producer must not wait for the stalled disk")
            .unwrap();
        shutdown.unwrap();
        let expected = (1..=16_002).collect::<Vec<_>>();
        assert_eq!(*rows.lock().unwrap(), expected);
        let seals = sealed_rows.lock().unwrap();
        assert_eq!(seals[0], expected[..16_001]);
        assert_eq!(seals[1], expected);
        let metrics = runtime.metrics();
        assert_eq!(metrics.dropped_items, 0);
        assert_eq!(metrics.accepted_items, 16_002);
        assert_eq!(metrics.peak_queued_bytes, 16_001 * 8);
        assert_eq!(metrics.peak_queued_items, 16_002);
        assert_eq!(metrics.queued_items, 0);
    }

    #[test]
    fn byte_budget_rejects_variable_payloads_before_item_capacity() {
        let (entered, waiting) = std::sync::mpsc::channel();
        let (release, proceed) = std::sync::mpsc::channel();
        let rows = Arc::new(Mutex::new(Vec::new()));
        let mut config = unit_flush_config(1, 100);
        config.max_buffer_bytes = 8;
        config.max_total_buffer_bytes = 1024;
        config.queue_max_bytes = Some(16);
        config.overflow_policy = OverflowPolicy::FailFast;
        let mut runtime = BackgroundCaptureRuntime::new(
            config,
            PausedSink {
                entered: Some(entered),
                release: proceed,
                rows: Arc::clone(&rows),
            },
        )
        .unwrap();
        runtime.submit(queued_item(1, 8)).unwrap();
        waiting.recv_timeout(Duration::from_secs(2)).unwrap();
        runtime.submit(queued_item(2, 8)).unwrap();
        runtime.submit(queued_item(3, 8)).unwrap();
        let rejected = runtime.submit(queued_item(4, 1));
        {
            let mut state = runtime.state.0.lock().unwrap();
            state.queue.front_mut().unwrap().enqueued_at = Instant::now() - Duration::from_secs(3);
            state.worker_started_at = Some(Instant::now() - Duration::from_secs(2));
        }
        let metrics = runtime.metrics();
        let faults = runtime.failure_snapshot();
        release.send(()).unwrap();
        let shutdown = runtime.shutdown();
        assert_eq!(metrics.queued_bytes, 16);
        assert_eq!(metrics.queue_capacity, 100);
        assert_eq!(metrics.queue_max_bytes, 16);
        assert_eq!(metrics.in_flight_items, 1);
        assert_eq!(metrics.in_flight_bytes, 8);
        assert!(metrics.oldest_queued_ns >= 3_000_000_000);
        assert!(metrics.worker_busy_ns >= 2_000_000_000);
        assert_eq!(faults.records.len(), 1);
        assert_eq!(faults.records[0].event_range_ns, Some((4, 4)));

        assert!(
            rejected.is_err(),
            "bytes, rather than 100 item slots, bound this queue"
        );
        assert_eq!(*rows.lock().unwrap(), vec![1, 2, 3]);
        assert_eq!(runtime.metrics().queued_bytes, 0);
        let metrics = runtime.metrics();
        assert_eq!(metrics.dropped_items, 1);
        assert_eq!(metrics.in_flight_items, 0);
        assert_eq!(metrics.in_flight_bytes, 0);
        assert_eq!(metrics.worker_busy_ns, 0);
        assert_eq!(metrics.oldest_queued_ns, 0);
        assert_eq!(metrics.sink_write.count, 3);
        assert_eq!(metrics.queue_wait.count, 4);
        assert!(metrics.queue_wait.max_ns >= 3_000_000_000);
        assert!(
            shutdown.is_err(),
            "an admission gap invalidates a clean recording"
        );
    }

    #[test]
    fn drop_oldest_reports_the_evicted_partition_and_keeps_later_rows() {
        let (entered, waiting) = std::sync::mpsc::channel();
        let (release, proceed) = std::sync::mpsc::channel();
        let rows = Arc::new(Mutex::new(Vec::new()));
        let mut config = unit_flush_config(1, 2);
        config.overflow_policy = OverflowPolicy::DropOldest;
        let mut runtime = BackgroundCaptureRuntime::new(
            config,
            PausedSink {
                entered: Some(entered),
                release: proceed,
                rows: Arc::clone(&rows),
            },
        )
        .unwrap();
        let item = |value| {
            let mut item = queued_item(value, 8);
            item.partition_key = PartitionKey::market_data("test", value);
            item
        };
        runtime.submit(item(1)).unwrap();
        waiting.recv_timeout(Duration::from_secs(2)).unwrap();
        for value in 2..=4 {
            runtime.submit(item(value)).unwrap();
        }
        let mut cursor = 0;
        let affected = runtime.failures_since(&mut cursor).unwrap();
        let bad = affected.contains(&item(2).partition_key.stable_key());
        let healthy = [1, 3, 4]
            .into_iter()
            .all(|value| !affected.contains(&item(value).partition_key.stable_key()));
        release.send(()).unwrap();
        runtime.shutdown().unwrap();
        assert!(
            bad && healthy,
            "report the victim rather than the newly admitted item"
        );
        assert_eq!(*rows.lock().unwrap(), [1, 3, 4]);
        assert_eq!(runtime.metrics().dropped_items, 1);
    }

    #[test]
    fn ingress_drop_counters_survive_worker_flush() {
        let (entered, waiting) = std::sync::mpsc::channel();
        let (release, proceed) = std::sync::mpsc::channel();
        let rows = Arc::new(Mutex::new(Vec::new()));
        let mut config = unit_flush_config(1, 2);
        config.overflow_policy = OverflowPolicy::DropNewest;
        let mut runtime = BackgroundCaptureRuntime::new(
            config,
            PausedSink {
                entered: Some(entered),
                release: proceed,
                rows: Arc::clone(&rows),
            },
        )
        .unwrap();
        runtime.submit(queued_item(1, 8)).unwrap();
        waiting.recv_timeout(Duration::from_secs(2)).unwrap();
        for payload in [2, 3, 4] {
            runtime.submit(queued_item(payload, 8)).unwrap();
        }
        assert_eq!(runtime.metrics().dropped_items, 1);
        release.send(()).unwrap();
        runtime.flush_all().unwrap();
        let metrics = runtime.metrics();
        runtime.shutdown().unwrap();
        assert_eq!(
            metrics.dropped_items, 1,
            "a worker snapshot cannot erase ingress losses"
        );
        assert_eq!(metrics.accepted_items, 3);
        assert_eq!(*rows.lock().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn repeated_oldest_eviction_keeps_gap_controls_bounded_around_surviving_data() {
        let (entered, waiting) = std::sync::mpsc::channel();
        let (release, proceed) = std::sync::mpsc::channel();
        let rows = Arc::new(Mutex::new(Vec::new()));
        let mut config = unit_flush_config(1, 2);
        config.overflow_policy = OverflowPolicy::DropOldest;
        let mut runtime = BackgroundCaptureRuntime::new(
            config,
            PausedSink {
                entered: Some(entered),
                release: proceed,
                rows: Arc::clone(&rows),
            },
        )
        .unwrap();
        runtime.submit(queued_item(1, 8)).unwrap();
        waiting.recv_timeout(Duration::from_secs(2)).unwrap();
        for payload in 2..=501 {
            runtime.submit(queued_item(payload, 8)).unwrap();
        }
        let metrics = runtime.metrics();
        release.send(()).unwrap();
        runtime.shutdown().unwrap();
        assert!(
            metrics.queued_items <= 4,
            "controls must be bounded by surviving data"
        );
        assert_eq!(metrics.dropped_items, 498);
        assert_eq!(*rows.lock().unwrap(), [1, 500, 501]);
    }

    #[test]
    fn drop_oldest_evicts_enough_bytes_and_oversized_items_preserve_the_queue() {
        let (entered, waiting) = std::sync::mpsc::channel();
        let (release, proceed) = std::sync::mpsc::channel();
        let rows = Arc::new(Mutex::new(Vec::new()));
        let mut config = unit_flush_config(1, 100);
        config.max_buffer_bytes = 8;
        config.max_total_buffer_bytes = 16;
        config.overflow_policy = OverflowPolicy::DropOldest;
        let mut runtime = BackgroundCaptureRuntime::new(
            config,
            PausedSink {
                entered: Some(entered),
                release: proceed,
                rows: Arc::clone(&rows),
            },
        )
        .unwrap();
        runtime.submit(queued_item(1, 8)).unwrap();
        waiting.recv_timeout(Duration::from_secs(2)).unwrap();
        runtime.submit(queued_item(2, 6)).unwrap();
        runtime.submit(queued_item(3, 6)).unwrap();
        runtime.submit(queued_item(4, 12)).unwrap();
        runtime.submit(queued_item(5, 17)).unwrap();
        let metrics = runtime.metrics();
        release.send(()).unwrap();
        runtime.shutdown().unwrap();
        assert_eq!(metrics.queued_items, 3); // gap, admitted data, then another gap
        assert_eq!(metrics.queued_bytes, 12);
        assert_eq!(runtime.metrics().dropped_items, 3);
        assert_eq!(*rows.lock().unwrap(), vec![1, 4]);
    }
}
