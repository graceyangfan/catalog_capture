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

use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::metrics::{CaptureMetrics, FlushReasonMetrics};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct FamilyCaptureMetrics {
    pub family: String,
    pub metrics: CaptureMetrics,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CaptureMetricsSnapshot {
    pub captured_at_unix_ms: u64,
    pub enabled_background_workers: usize,
    pub process_rss_bytes: Option<u64>,
    pub aggregated: CaptureMetrics,
    pub families: Vec<FamilyCaptureMetrics>,
}

#[must_use]
pub fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

/// Best-effort resident set size for soak dashboards.
#[must_use]
pub fn process_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        read_linux_rss_bytes()
    }
    #[cfg(target_os = "macos")]
    {
        read_macos_rss_bytes()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

#[cfg(target_os = "linux")]
fn read_linux_rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages = statm.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    let page_size = rust_page_size();
    Some(pages.saturating_mul(page_size))
}

#[cfg(target_os = "macos")]
fn read_macos_rss_bytes() -> Option<u64> {
    use std::mem::MaybeUninit;

    use libc::{getrusage, rusage, RUSAGE_SELF};

    let mut usage = MaybeUninit::<rusage>::uninit();
    if unsafe { getrusage(RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
    // macOS documents ru_maxrss in bytes.
    Some(usage.ru_maxrss as u64)
}

#[cfg(target_os = "linux")]
fn rust_page_size() -> u64 {
    unsafe { libc::sysconf(libc::_SC_PAGESIZE) as u64 }
}

#[must_use]
pub fn render_prometheus(snapshot: &CaptureMetricsSnapshot) -> String {
    let mut out = String::new();
    append_snapshot_help(&mut out);
    append_runtime_gauges(&mut out, snapshot);
    append_metrics_block(&mut out, "catalog_capture", "", &snapshot.aggregated);
    for family in &snapshot.families {
        let labels = format!(r#"{{family="{}"}}"#, escape_label(&family.family));
        append_metrics_block(&mut out, "catalog_capture", &labels, &family.metrics);
    }
    out
}

#[must_use]
pub fn render_json(snapshot: &CaptureMetricsSnapshot) -> String {
    serde_json::to_string(snapshot).unwrap_or_else(|_| "{}".to_string())
}

fn append_snapshot_help(out: &mut String) {
    out.push_str("# HELP catalog_capture_info Capture metrics snapshot metadata\n");
    out.push_str("# TYPE catalog_capture_info gauge\n");
    out.push_str(
        "# HELP catalog_capture_enabled_background_workers Active background worker threads\n",
    );
    out.push_str("# TYPE catalog_capture_enabled_background_workers gauge\n");
    out.push_str("# HELP catalog_capture_process_rss_bytes Process resident set size in bytes\n");
    out.push_str("# TYPE catalog_capture_process_rss_bytes gauge\n");
    out.push_str("# HELP catalog_capture_accepted_items_total Accepted capture items\n");
    out.push_str("# TYPE catalog_capture_accepted_items_total counter\n");
    out.push_str("# HELP catalog_capture_dropped_items_total Dropped capture items\n");
    out.push_str("# TYPE catalog_capture_dropped_items_total counter\n");
    out.push_str("# HELP catalog_capture_active_partitions Buffered partitions awaiting flush\n");
    out.push_str("# TYPE catalog_capture_active_partitions gauge\n");
    out.push_str("# HELP catalog_capture_queued_items Background queue depth\n");
    out.push_str("# TYPE catalog_capture_queued_items gauge\n");
    out.push_str("# HELP catalog_capture_buffered_bytes Summed partition buffer bytes\n");
    out.push_str("# TYPE catalog_capture_buffered_bytes gauge\n");
    out.push_str("# HELP catalog_capture_flushed_rows_total Rows flushed to catalog\n");
    out.push_str("# TYPE catalog_capture_flushed_rows_total counter\n");
    out.push_str("# HELP catalog_capture_completed_files_total Completed parquet files\n");
    out.push_str("# TYPE catalog_capture_completed_files_total counter\n");
    out.push_str(
        "# HELP catalog_capture_completed_file_bytes_total Completed parquet file bytes\n",
    );
    out.push_str("# TYPE catalog_capture_completed_file_bytes_total counter\n");
    out.push_str("# HELP catalog_capture_flush_reasons_total Flush invocations by reason\n");
    out.push_str("# TYPE catalog_capture_flush_reasons_total counter\n");
}

fn append_runtime_gauges(out: &mut String, snapshot: &CaptureMetricsSnapshot) {
    append_line(
        out,
        "catalog_capture_info",
        "",
        &format!(
            r#"{{captured_at_unix_ms="{}"}}" 1"#,
            snapshot.captured_at_unix_ms
        ),
    );
    append_line(
        out,
        "catalog_capture_enabled_background_workers",
        "",
        &snapshot.enabled_background_workers.to_string(),
    );
    if let Some(rss) = snapshot.process_rss_bytes {
        append_line(out, "catalog_capture_process_rss_bytes", "", &rss.to_string());
    }
}

fn append_metrics_block(out: &mut String, prefix: &str, labels: &str, metrics: &CaptureMetrics) {
    append_line(out, &format!("{prefix}_accepted_items_total"), labels, &metrics.accepted_items.to_string());
    append_line(out, &format!("{prefix}_dropped_items_total"), labels, &metrics.dropped_items.to_string());
    append_line(out, &format!("{prefix}_active_partitions"), labels, &metrics.active_partitions.to_string());
    append_line(out, &format!("{prefix}_queued_items"), labels, &metrics.queued_items.to_string());
    append_line(out, &format!("{prefix}_buffered_bytes"), labels, &metrics.buffered_bytes.to_string());
    append_line(out, &format!("{prefix}_flushed_rows_total"), labels, &metrics.flushed_rows.to_string());
    append_line(out, &format!("{prefix}_completed_files_total"), labels, &metrics.completed_files.to_string());
    append_line(
        out,
        &format!("{prefix}_completed_file_bytes_total"),
        labels,
        &metrics.completed_file_bytes.to_string(),
    );
    append_flush_reasons(out, prefix, labels, &metrics.flush_reasons);
}

fn append_flush_reasons(
    out: &mut String,
    prefix: &str,
    family_labels: &str,
    reasons: &FlushReasonMetrics,
) {
    let entries = [
        ("rows", reasons.row_threshold),
        ("bytes", reasons.byte_threshold),
        ("interval", reasons.interval),
        ("seal", reasons.seal),
        ("shutdown", reasons.shutdown),
        ("manual", reasons.manual),
        ("budget", reasons.budget),
    ];
    for (reason, value) in entries {
        let labels = merge_labels(family_labels, &format!(r#"reason="{reason}""#));
        append_line(
            out,
            &format!("{prefix}_flush_reasons_total"),
            &labels,
            &value.to_string(),
        );
    }
}

fn merge_labels(family_labels: &str, extra: &str) -> String {
    if family_labels.is_empty() {
        format!("{{{extra}}}")
    } else {
        let inner = family_labels.trim_start_matches('{').trim_end_matches('}');
        format!("{{{inner},{extra}}}")
    }
}

fn append_line(out: &mut String, name: &str, labels: &str, value: &str) {
    out.push_str(name);
    out.push_str(labels);
    out.push(' ');
    out.push_str(value);
    out.push('\n');
}

fn escape_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::CaptureMetrics;

    #[test]
    fn prometheus_includes_key_soak_gauges() {
        let snapshot = CaptureMetricsSnapshot {
            captured_at_unix_ms: 1_700_000_000_000,
            enabled_background_workers: 2,
            process_rss_bytes: Some(128 * 1024 * 1024),
            aggregated: CaptureMetrics {
                dropped_items: 3,
                active_partitions: 4,
                queued_items: 5,
                buffered_bytes: 6,
                ..CaptureMetrics::default()
            },
            families: vec![FamilyCaptureMetrics {
                family: "quotes".to_string(),
                metrics: CaptureMetrics {
                    accepted_items: 10,
                    ..CaptureMetrics::default()
                },
            }],
        };

        let body = render_prometheus(&snapshot);
        assert!(body.contains("catalog_capture_dropped_items_total 3"));
        assert!(body.contains("catalog_capture_active_partitions 4"));
        assert!(body.contains("catalog_capture_queued_items 5"));
        assert!(body.contains(r#"catalog_capture_accepted_items_total{family="quotes"} 10"#));
        assert!(body.contains("catalog_capture_process_rss_bytes"));
    }

    #[test]
    fn json_roundtrip_contains_families() {
        let snapshot = CaptureMetricsSnapshot {
            families: vec![FamilyCaptureMetrics {
                family: "trades".to_string(),
                metrics: CaptureMetrics::default(),
            }],
            ..CaptureMetricsSnapshot::default()
        };
        let json = render_json(&snapshot);
        assert!(json.contains("\"family\":\"trades\""));
    }
}