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

use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use catalog_capture_core::{
    metrics_export::{render_json, render_prometheus},
    CaptureMetricsSnapshot,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::config::MetricsExportRuntimeConfig;

pub fn spawn_metrics_server(
    config: &MetricsExportRuntimeConfig,
    snapshot: Arc<RwLock<CaptureMetricsSnapshot>>,
) -> Result<(JoinHandle<()>, broadcast::Sender<()>)> {
    let bind_addr = config.bind_addr.clone();
    let port = config.port;
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let mut shutdown_listener = shutdown_rx.resubscribe();

    let handle = tokio::spawn(async move {
        let listener = match TcpListener::bind((bind_addr.as_str(), port)).await {
            Ok(listener) => listener,
            Err(error) => {
                log::error!("metrics server failed to bind {bind_addr}:{port}: {error}");
                return;
            }
        };

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    let Ok((mut stream, _)) = accept_result else {
                        continue;
                    };
                    let snapshot = Arc::clone(&snapshot);
                    tokio::spawn(async move {
                        if let Err(error) = serve_connection(&mut stream, &snapshot).await {
                            log::error!("metrics server connection error: {error}");
                        }
                    });
                }
                result = shutdown_listener.recv() => {
                    if result.is_ok() || result.is_err() {
                        break;
                    }
                }
            }
        }
    });

    Ok((handle, shutdown_tx))
}

async fn serve_connection(
    stream: &mut tokio::net::TcpStream,
    snapshot: &Arc<RwLock<CaptureMetricsSnapshot>>,
) -> Result<()> {
    let mut buffer = [0_u8; 2048];
    let read = stream
        .read(&mut buffer)
        .await
        .context("failed to read HTTP request")?;
    if read == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buffer[..read]);
    let mut lines = request.lines();
    let request_line = lines.next().unwrap_or_default();
    let path = request_line.split_whitespace().nth(1).unwrap_or_default();

    let snapshot = snapshot
        .read()
        .map_err(|error| anyhow::anyhow!("metrics snapshot lock poisoned: {error}"))?
        .clone();

    let (status, content_type, body) = match path {
        "/metrics" => (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            render_prometheus(&snapshot),
        ),
        "/metrics.json" => ("200 OK", "application/json", render_json(&snapshot)),
        "/health" | "/healthz" => ("200 OK", "text/plain; charset=utf-8", "ok".to_string()),
        _ => (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n".to_string(),
        ),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .context("failed to write HTTP response")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use catalog_capture_core::{metrics::CaptureMetrics, CaptureMetricsSnapshot};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::serve_connection;

    #[tokio::test]
    async fn serves_prometheus_and_json_endpoints() {
        let snapshot = Arc::new(RwLock::new(CaptureMetricsSnapshot {
            aggregated: CaptureMetrics {
                dropped_items: 2,
                active_partitions: 1,
                ..CaptureMetrics::default()
            },
            ..CaptureMetricsSnapshot::default()
        }));

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let snapshot_for_server = Arc::clone(&snapshot);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            serve_connection(&mut stream, &snapshot_for_server)
                .await
                .expect("serve");
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write");
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.expect("read");
        let body = String::from_utf8_lossy(&response);
        assert!(body.contains("catalog_capture_dropped_items_total 2"));
        server.await.expect("server");
    }
}
