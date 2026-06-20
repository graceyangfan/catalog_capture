use crate::metrics::FlushReason;

#[derive(Debug)]
pub struct PartitionBuffer<T> {
    pub items: Vec<T>,
    pub pending_rows: usize,
    pub pending_bytes: usize,
    pub min_ts_ns: Option<u64>,
    pub max_ts_ns: Option<u64>,
}

impl<T> Default for PartitionBuffer<T> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            pending_rows: 0,
            pending_bytes: 0,
            min_ts_ns: None,
            max_ts_ns: None,
        }
    }
}

impl<T> PartitionBuffer<T> {
    pub fn push(&mut self, item: T, event_ts_ns: u64, estimated_bytes: usize) {
        self.pending_rows += 1;
        self.pending_bytes += estimated_bytes;
        self.min_ts_ns = Some(
            self.min_ts_ns
                .map_or(event_ts_ns, |value| value.min(event_ts_ns)),
        );
        self.max_ts_ns = Some(
            self.max_ts_ns
                .map_or(event_ts_ns, |value| value.max(event_ts_ns)),
        );
        self.items.push(item);
    }

    #[must_use]
    pub fn should_flush_reason(
        &self,
        flush_rows: usize,
        max_buffer_bytes: usize,
    ) -> Option<FlushReason> {
        if self.pending_rows >= flush_rows {
            Some(FlushReason::Rows)
        } else if self.pending_bytes >= max_buffer_bytes {
            Some(FlushReason::Bytes)
        } else {
            None
        }
    }

    pub fn take(&mut self) -> Self {
        std::mem::take(self)
    }
}
