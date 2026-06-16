use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex, mpsc},
    thread::{self, JoinHandle},
    time::Duration,
};

use anyhow::{Result, anyhow};

use crate::{
    config::{CaptureConfig, OverflowPolicy},
    item::CaptureItem,
    metrics::{CaptureMetrics, FlushReason},
    runtime::{CaptureRuntime, FlushResult},
    sink::CaptureSink,
};

type WorkerReply = std::result::Result<FlushResult, String>;

#[derive(Debug)]
struct QueueState<T> {
    queue: VecDeque<CaptureItem<T>>,
    metrics: CaptureMetrics,
    flush_reason: Option<FlushReason>,
    shutdown_requested: bool,
    flush_waiters: Vec<mpsc::Sender<WorkerReply>>,
    shutdown_waiters: Vec<mpsc::Sender<WorkerReply>>,
}

impl<T> Default for QueueState<T> {
    fn default() -> Self {
        Self {
            queue: VecDeque::new(),
            metrics: CaptureMetrics::default(),
            flush_reason: None,
            shutdown_requested: false,
            flush_waiters: Vec::new(),
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
    pub fn new(config: CaptureConfig, sink: S) -> Self {
        let state = Arc::new((Mutex::new(QueueState::default()), Condvar::new()));
        let worker_state = Arc::clone(&state);
        let worker_config = config.clone();
        let worker = thread::Builder::new()
            .name("catalog-capture-worker".to_string())
            .spawn(move || worker_loop(worker_config, sink, worker_state))
            .expect("background capture worker should start");

        Self {
            config,
            state,
            worker: Some(worker),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn submit(&self, item: CaptureItem<T>) -> Result<()> {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().expect("queue state poisoned");

        if state.shutdown_requested {
            return Err(anyhow!("capture runtime is shutting down"));
        }

        let capacity = self.config.queue_capacity.max(1);
        if state.queue.len() >= capacity {
            match self.config.overflow_policy {
                OverflowPolicy::DropNewest => {
                    state.metrics.dropped_items += 1;
                    return Ok(());
                }
                OverflowPolicy::DropOldest => {
                    if state.queue.pop_front().is_some() {
                        state.metrics.dropped_items += 1;
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

    pub fn shutdown(&mut self) -> Result<FlushResult> {
        if self.worker.is_none() {
            return Ok(FlushResult::default());
        }
        let result = self.request_flush(true)?;
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        Ok(result)
    }

    pub fn metrics(&self) -> CaptureMetrics {
        let (lock, _) = &*self.state;
        let state = lock.lock().expect("queue state poisoned");
        let mut metrics = state.metrics.clone();
        metrics.active_partitions = state.queue.len() as u64;
        metrics
    }

    pub fn queue_depth(&self) -> usize {
        let (lock, _) = &*self.state;
        let state = lock.lock().expect("queue state poisoned");
        state.queue.len()
    }

    fn request_flush(&self, shutdown: bool) -> Result<FlushResult> {
        let (tx, rx) = mpsc::channel();
        let (lock, cvar) = &*self.state;
        {
            let mut state = lock.lock().expect("queue state poisoned");
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
            Err(err) => Err(anyhow!("background capture worker reply failed: {err}")),
        }
    }
}

fn worker_loop<T, S>(config: CaptureConfig, sink: S, state: Arc<(Mutex<QueueState<T>>, Condvar)>)
where
    T: Send + 'static,
    S: CaptureSink<T> + Send + 'static,
{
    let mut runtime = CaptureRuntime::new(config.clone(), sink);
    let flush_interval = Duration::from_millis(config.flush_interval_ms.max(1));

    loop {
        let (batch, flush_reason, should_shutdown, flush_waiters, shutdown_waiters) = {
            let (lock, cvar) = &*state;
            let mut queue_state = lock.lock().expect("queue state poisoned");

            if queue_state.queue.is_empty()
                && queue_state.flush_reason.is_none()
                && !queue_state.shutdown_requested
            {
                let waited = cvar
                    .wait_timeout(queue_state, flush_interval)
                    .expect("queue wait poisoned");
                queue_state = waited.0;

                if waited.1.timed_out() {
                    queue_state.flush_reason =
                        merge_flush_reason(queue_state.flush_reason, FlushReason::Interval);
                }
            }

            let batch: Vec<CaptureItem<T>> = queue_state.queue.drain(..).collect();
            let flush_reason = if queue_state.shutdown_requested {
                Some(FlushReason::Shutdown)
            } else {
                queue_state.flush_reason
            };
            queue_state.flush_reason = None;
            let flush_waiters = std::mem::take(&mut queue_state.flush_waiters);
            let shutdown_waiters = std::mem::take(&mut queue_state.shutdown_waiters);
            let should_shutdown = queue_state.shutdown_requested;

            (
                batch,
                flush_reason,
                should_shutdown,
                flush_waiters,
                shutdown_waiters,
            )
        };

        let flush_result = process_batch(&mut runtime, batch, flush_reason);
        let metrics_snapshot = runtime.metrics.clone();

        {
            let (lock, _) = &*state;
            let mut queue_state = lock.lock().expect("queue state poisoned");
            queue_state.metrics = metrics_snapshot;
        }

        match &flush_result {
            Ok(result) => {
                for waiter in flush_waiters {
                    let _ = waiter.send(Ok(result.clone()));
                }
                for waiter in shutdown_waiters {
                    let _ = waiter.send(Ok(result.clone()));
                }
            }
            Err(err) => {
                let message = err.to_string();
                for waiter in flush_waiters {
                    let _ = waiter.send(Err(message.clone()));
                }
                for waiter in shutdown_waiters {
                    let _ = waiter.send(Err(message.clone()));
                }
            }
        }

        if should_shutdown {
            break;
        }
    }
}

fn process_batch<T, S>(
    runtime: &mut CaptureRuntime<T, S>,
    batch: Vec<CaptureItem<T>>,
    flush_reason: Option<FlushReason>,
) -> Result<FlushResult>
where
    S: CaptureSink<T>,
{
    let mut aggregated = FlushResult::default();

    for item in batch {
        let partial = runtime.submit(item)?;
        aggregated.rows += partial.rows;
        aggregated.bytes += partial.bytes;
        aggregated.files.extend(partial.files);
    }

    if let Some(reason) = flush_reason {
        let partial = runtime.flush_all_with_reason(reason)?;
        aggregated.rows += partial.rows;
        aggregated.bytes += partial.bytes;
        aggregated.files.extend(partial.files);
    }

    Ok(aggregated)
}

fn merge_flush_reason(current: Option<FlushReason>, next: FlushReason) -> Option<FlushReason> {
    Some(match (current, next) {
        (Some(FlushReason::Shutdown), _) | (_, FlushReason::Shutdown) => FlushReason::Shutdown,
        (Some(FlushReason::Manual), _) | (_, FlushReason::Manual) => FlushReason::Manual,
        (Some(FlushReason::Interval), _) | (_, FlushReason::Interval) => FlushReason::Interval,
        (Some(FlushReason::Bytes), _) | (_, FlushReason::Bytes) => FlushReason::Bytes,
        _ => FlushReason::Rows,
    })
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{Arc, Mutex},
    };
    use std::time::Duration;

    use anyhow::Result;

    use super::BackgroundCaptureRuntime;
    use crate::{
        config::{CaptureConfig, OverflowPolicy},
        item::{CaptureItem, PartitionKey},
        sink::CaptureSink,
    };

    #[derive(Clone, Default)]
    struct TestSink {
        batches: Arc<Mutex<Vec<Vec<u64>>>>,
    }

    impl CaptureSink<u64> for TestSink {
        fn write_batch(&self, batch: Vec<u64>) -> Result<Vec<PathBuf>> {
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
        );

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
        );

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
}
