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
    collections::{BTreeSet, VecDeque},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

/// Local persistence failures. These observations do not certify venue continuity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureOperation {
    Admission,
    Write,
    Sync,
    Seal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PartitionFailure {
    pub generation: u64,
    /// None means the operation could not identify a narrower scope.
    pub partition: Option<String>,
    pub operation: FailureOperation,
    pub occurrences: u64,
    pub first_observed_unix_ms: u64,
    pub last_observed_unix_ms: u64,
    /// Source-event bounds, when known; not a certified missing-data interval.
    pub event_range_ns: Option<(u64, u64)>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct FailureSnapshot {
    pub generation: u64,
    /// Readers older than this point must conservatively treat the family as affected.
    pub evicted_through: u64,
    pub records: VecDeque<PartitionFailure>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AffectedPartitions {
    Family,
    Partitions(BTreeSet<String>),
}

impl AffectedPartitions {
    #[must_use]
    pub fn contains(&self, key: &str) -> bool {
        match self {
            Self::Family => true,
            Self::Partitions(keys) => keys.contains(key),
        }
    }
}

/// A bounded, non-destructive fault history shared by admission, writer and observers.
/// It is independent of the data queue so a full queue cannot suppress a failure.
#[derive(Debug, Clone)]
pub struct FailureTracker {
    state: Arc<Mutex<FailureSnapshot>>,
    capacity: usize,
}

impl FailureTracker {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Arc::default(),
            capacity: capacity.max(1),
        }
    }

    pub fn record(
        &self,
        partition: Option<&str>,
        operation: FailureOperation,
        event_range_ns: Option<(u64, u64)>,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.generation = state.generation.saturating_add(1);
        let generation = state.generation;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |time| time.as_millis() as u64);
        // Coalesce only adjacent observations. Advancing a retained record past a
        // reader's cursor must never hide a different intervening partition.
        if let Some(last) = state.records.back_mut()
            && last.partition.as_deref() == partition
            && last.operation == operation
        {
            last.generation = generation;
            last.occurrences = last.occurrences.saturating_add(1);
            last.last_observed_unix_ms = now;
            last.event_range_ns = match (last.event_range_ns, event_range_ns) {
                (Some((a, b)), Some((c, d))) => Some((a.min(c), b.max(d))),
                _ => None,
            };
            return;
        }
        if state.records.len() == self.capacity
            && let Some(oldest) = state.records.pop_front()
        {
            state.evicted_through = oldest.generation;
        }
        state.records.push_back(PartitionFailure {
            generation,
            partition: partition.map(str::to_owned),
            operation,
            occurrences: 1,
            first_observed_unix_ms: now,
            last_observed_unix_ms: now,
            event_range_ns,
        });
    }

    #[must_use]
    pub fn snapshot(&self) -> FailureSnapshot {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    /// Independent readers keep their own cursor; healthy polling allocates nothing.
    pub fn changed_partitions(&self, cursor: &mut u64) -> Option<AffectedPartitions> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if *cursor == state.generation {
            return None;
        }
        let previous = *cursor;
        *cursor = state.generation;
        if previous < state.evicted_through || previous > state.generation {
            return Some(AffectedPartitions::Family);
        }
        let mut keys = BTreeSet::new();
        for failure in state
            .records
            .iter()
            .filter(|failure| failure.generation > previous)
        {
            let Some(key) = &failure.partition else {
                return Some(AffectedPartitions::Family);
            };
            keys.insert(key.clone());
        }
        Some(AffectedPartitions::Partitions(keys))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observers_are_independent_and_partition_scoped() {
        let faults = FailureTracker::new(2);
        let (mut actor, mut journal) = (0, 0);
        assert!(faults.changed_partitions(&mut actor).is_none());
        faults.record(Some("A"), FailureOperation::Write, Some((10, 20)));
        for cursor in [&mut actor, &mut journal] {
            let affected = faults.changed_partitions(cursor).unwrap();
            assert!(affected.contains("A"));
            assert!(!affected.contains("B"));
        }
        assert!(faults.changed_partitions(&mut actor).is_none());
    }

    #[test]
    fn sustained_fault_is_bounded_but_visible_to_existing_reader() {
        let faults = FailureTracker::new(2);
        faults.record(Some("A"), FailureOperation::Admission, Some((10, 10)));
        let mut cursor = 0;
        faults.changed_partitions(&mut cursor).unwrap();
        for ts in 11..100 {
            faults.record(Some("A"), FailureOperation::Admission, Some((ts, ts)));
        }
        let snapshot = faults.snapshot();
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.records[0].occurrences, 90);
        assert_eq!(snapshot.records[0].event_range_ns, Some((10, 99)));
        assert!(
            faults
                .changed_partitions(&mut cursor)
                .unwrap()
                .contains("A")
        );
    }

    #[test]
    fn eviction_and_unknown_scope_are_conservative() {
        let faults = FailureTracker::new(2);
        for key in ["A", "B", "C"] {
            faults.record(Some(key), FailureOperation::Write, None);
        }
        assert_eq!(faults.snapshot().records.len(), 2);
        assert_eq!(
            faults.changed_partitions(&mut 0),
            Some(AffectedPartitions::Family)
        );
        let mut cursor = faults.snapshot().generation;
        faults.record(None, FailureOperation::Sync, None);
        assert_eq!(
            faults.changed_partitions(&mut cursor),
            Some(AffectedPartitions::Family)
        );
    }
}
