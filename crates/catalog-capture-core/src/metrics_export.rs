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

/// Per-job counters for `[[capture.custom_data_requests]]` (request/poll path).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CustomDataRequestJobMetrics {
    pub index: usize,
    pub type_name: String,
    pub identifier: Option<String>,
    pub in_flight: bool,
    pub polls: u64,
    pub rows: u64,
    pub skipped_inflight: u64,
    pub timeouts: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CaptureMetricsSnapshot {
    pub captured_at_unix_ms: u64,
    pub enabled_background_workers: usize,
    pub process_rss_bytes: Option<u64>,
    pub aggregated: CaptureMetrics,
    pub families: Vec<FamilyCaptureMetrics>,
    /// Request-path jobs (`request_data` poll timers). Empty when no requests configured.
    pub custom_data_requests: Vec<CustomDataRequestJobMetrics>,
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
    append_custom_data_request_metrics(&mut out, snapshot);
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
    out.push_str(
        "# HELP catalog_capture_process_rss_bytes Process RSS in bytes (Linux: resident; macOS: footprint)\n",
    );
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
    out.push_str(
        "# HELP catalog_capture_custom_data_request_polls_total request_data poll attempts\n",
    );
    out.push_str("# TYPE catalog_capture_custom_data_request_polls_total counter\n");
    out.push_str(
        "# HELP catalog_capture_custom_data_request_rows_total rows accepted from request responses\n",
    );
    out.push_str("# TYPE catalog_capture_custom_data_request_rows_total counter\n");
    out.push_str(
        "# HELP catalog_capture_custom_data_request_skipped_inflight_total poll ticks skipped while a request was in flight\n",
    );
    out.push_str("# TYPE catalog_capture_custom_data_request_skipped_inflight_total counter\n");
    out.push_str(
        "# HELP catalog_capture_custom_data_request_timeouts_total in-flight request timeouts cleared for re-fire\n",
    );
    out.push_str("# TYPE catalog_capture_custom_data_request_timeouts_total counter\n");
    out.push_str(
        "# HELP catalog_capture_custom_data_request_in_flight whether a request is currently in flight (1/0)\n",
    );
    out.push_str("# TYPE catalog_capture_custom_data_request_in_flight gauge\n");
}

fn append_custom_data_request_metrics(out: &mut String, snapshot: &CaptureMetricsSnapshot) {
    if snapshot.custom_data_requests.is_empty() {
        return;
    }

    let mut total_polls = 0_u64;
    let mut total_rows = 0_u64;
    let mut total_skipped = 0_u64;
    let mut total_timeouts = 0_u64;
    let mut in_flight_jobs = 0_u64;

    for job in &snapshot.custom_data_requests {
        total_polls = total_polls.saturating_add(job.polls);
        total_rows = total_rows.saturating_add(job.rows);
        total_skipped = total_skipped.saturating_add(job.skipped_inflight);
        total_timeouts = total_timeouts.saturating_add(job.timeouts);
        if job.in_flight {
            in_flight_jobs = in_flight_jobs.saturating_add(1);
        }

        let id = job.identifier.as_deref().unwrap_or("");
        let labels = format!(
            r#"{{index="{}",type_name="{}",id="{}"}}"#,
            job.index,
            escape_label(&job.type_name),
            escape_label(id)
        );
        append_line(
            out,
            "catalog_capture_custom_data_request_polls_total",
            &labels,
            &job.polls.to_string(),
        );
        append_line(
            out,
            "catalog_capture_custom_data_request_rows_total",
            &labels,
            &job.rows.to_string(),
        );
        append_line(
            out,
            "catalog_capture_custom_data_request_skipped_inflight_total",
            &labels,
            &job.skipped_inflight.to_string(),
        );
        append_line(
            out,
            "catalog_capture_custom_data_request_timeouts_total",
            &labels,
            &job.timeouts.to_string(),
        );
        append_line(
            out,
            "catalog_capture_custom_data_request_in_flight",
            &labels,
            if job.in_flight { "1" } else { "0" },
        );
    }

    // Aggregate totals (no labels) for simple alerts / dashboards.
    append_line(
        out,
        "catalog_capture_custom_data_request_polls_total",
        "",
        &total_polls.to_string(),
    );
    append_line(
        out,
        "catalog_capture_custom_data_request_rows_total",
        "",
        &total_rows.to_string(),
    );
    append_line(
        out,
        "catalog_capture_custom_data_request_skipped_inflight_total",
        "",
        &total_skipped.to_string(),
    );
    append_line(
        out,
        "catalog_capture_custom_data_request_timeouts_total",
        "",
        &total_timeouts.to_string(),
    );
    append_line(
        out,
        "catalog_capture_custom_data_request_in_flight",
        "",
        &in_flight_jobs.to_string(),
    );
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
        append_line(
            out,
            "catalog_capture_process_rss_bytes",
            "",
            &rss.to_string(),
        );
    }
}

fn append_metrics_block(out: &mut String, prefix: &str, labels: &str, metrics: &CaptureMetrics) {
    append_line(
        out,
        &format!("{prefix}_accepted_items_total"),
        labels,
        &metrics.accepted_items.to_string(),
    );
    append_line(
        out,
        &format!("{prefix}_dropped_items_total"),
        labels,
        &metrics.dropped_items.to_string(),
    );
    append_line(
        out,
        &format!("{prefix}_active_partitions"),
        labels,
        &metrics.active_partitions.to_string(),
    );
    append_line(
        out,
        &format!("{prefix}_queued_items"),
        labels,
        &metrics.queued_items.to_string(),
    );
    append_line(
        out,
        &format!("{prefix}_buffered_bytes"),
        labels,
        &metrics.buffered_bytes.to_string(),
    );
    append_line(
        out,
        &format!("{prefix}_flushed_rows_total"),
        labels,
        &metrics.flushed_rows.to_string(),
    );
    append_line(
        out,
        &format!("{prefix}_completed_files_total"),
        labels,
        &metrics.completed_files.to_string(),
    );
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
            custom_data_requests: Vec::new(),
        };

        let body = render_prometheus(&snapshot);
        assert!(body.contains("catalog_capture_dropped_items_total 3"));
        assert!(body.contains("catalog_capture_active_partitions 4"));
        assert!(body.contains("catalog_capture_queued_items 5"));
        assert!(body.contains(r#"catalog_capture_accepted_items_total{family="quotes"} 10"#));
        assert!(body.contains("catalog_capture_process_rss_bytes"));
    }

    #[test]
    fn prometheus_includes_custom_data_request_counters() {
        let snapshot = CaptureMetricsSnapshot {
            custom_data_requests: vec![CustomDataRequestJobMetrics {
                index: 0,
                type_name: "DeribitBookSummary".to_string(),
                identifier: Some("BTC:option".to_string()),
                in_flight: true,
                polls: 7,
                rows: 100,
                skipped_inflight: 2,
                timeouts: 1,
            }],
            ..CaptureMetricsSnapshot::default()
        };
        let body = render_prometheus(&snapshot);
        assert!(body.contains("catalog_capture_custom_data_request_polls_total 7"));
        assert!(body.contains("catalog_capture_custom_data_request_rows_total 100"));
        assert!(body.contains("catalog_capture_custom_data_request_skipped_inflight_total 2"));
        assert!(body.contains("catalog_capture_custom_data_request_timeouts_total 1"));
        assert!(body.contains(
            r#"catalog_capture_custom_data_request_polls_total{index="0",type_name="DeribitBookSummary",id="BTC:option"} 7"#
        ));
        assert!(body.contains(
            r#"catalog_capture_custom_data_request_in_flight{index="0",type_name="DeribitBookSummary",id="BTC:option"} 1"#
        ));
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

    #[test]
    fn json_includes_custom_data_requests() {
        let snapshot = CaptureMetricsSnapshot {
            custom_data_requests: vec![CustomDataRequestJobMetrics {
                index: 0,
                type_name: "DeribitBookSummary".to_string(),
                identifier: Some("BTC:option".to_string()),
                in_flight: false,
                polls: 3,
                rows: 9,
                skipped_inflight: 0,
                timeouts: 0,
            }],
            ..CaptureMetricsSnapshot::default()
        };
        let json = render_json(&snapshot);
        assert!(json.contains("\"type_name\":\"DeribitBookSummary\""));
        assert!(json.contains("\"polls\":3"));
        assert!(json.contains("\"rows\":9"));
    }
}
