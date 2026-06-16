#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    Rows,
    Bytes,
    Interval,
    Shutdown,
    Manual,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlushReasonMetrics {
    pub row_threshold: u64,
    pub byte_threshold: u64,
    pub interval: u64,
    pub shutdown: u64,
    pub manual: u64,
}

impl FlushReasonMetrics {
    pub fn record(&mut self, reason: FlushReason) {
        match reason {
            FlushReason::Rows => self.row_threshold += 1,
            FlushReason::Bytes => self.byte_threshold += 1,
            FlushReason::Interval => self.interval += 1,
            FlushReason::Shutdown => self.shutdown += 1,
            FlushReason::Manual => self.manual += 1,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CaptureMetrics {
    pub accepted_items: u64,
    pub dropped_items: u64,
    pub flushed_batches: u64,
    pub flushed_rows: u64,
    pub completed_files: u64,
    pub completed_file_bytes: u64,
    pub active_partitions: u64,
    pub flush_reasons: FlushReasonMetrics,
}

impl CaptureMetrics {
    #[must_use]
    pub fn average_file_bytes(&self) -> u64 {
        if self.completed_files == 0 {
            0
        } else {
            self.completed_file_bytes / self.completed_files
        }
    }
}
