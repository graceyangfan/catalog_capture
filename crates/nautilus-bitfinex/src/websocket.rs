// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2026 yfclark and contributors. All rights reserved.
//  https://github.com/graceyangfan/catalog_capture
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  you may not use this file except in compliance with the License.
// -------------------------------------------------------------------------------------------------

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, bail, ensure};
use nautilus_common::{live::sender::EventSender, messages::DataEvent};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::SocketControl;
use nautilus_model::{
    data::{Data, OrderBookDeltas},
    identifiers::InstrumentId,
};
use nautilus_network::{
    RECONNECTED,
    http::create_standard_nautilus_headers,
    mode::ReconnectRequestOutcome,
    transport::Message,
    websocket::{EpochMessageHandler, WebSocketClient, WebSocketConfig},
};
use serde_json::{Value, json, value::RawValue};
use tokio::{sync::mpsc, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{book::RawBook, data::book_invalidation};

const FLAGS: u64 = 32_768 | 65_536 | 131_072;
const TIMEOUT: Duration = Duration::from_secs(15);
const CHANNEL_IDLE_TIMEOUT: Duration = Duration::from_secs(45);
const RETRY_DELAY: Duration = Duration::from_secs(4);
const MAX_UNVERIFIED_FRAMES: usize = 32_768;
const RAW_CAPACITY: usize = 4096;
type Frame = (u64, UnixNanos, Message);

#[derive(Debug, Clone, Copy)]
pub(crate) enum Command {
    Subscribe(InstrumentId, usize),
    Unsubscribe(InstrumentId),
}

#[derive(Debug, Clone, Copy)]
enum Control {
    Configure,
    Subscribe(InstrumentId, usize),
    Unsubscribe(u64),
}

#[derive(Debug)]
struct Channel {
    instrument: InstrumentId,
    depth: usize,
    book: RawBook,
    has_snapshot: bool,
    deadline: Option<Instant>,
    last_seen: Instant,
    unverified: Vec<OrderBookDeltas>,
}

impl Channel {
    fn new(instrument: InstrumentId, depth: usize) -> Self {
        let now = Instant::now();
        Self {
            instrument,
            depth,
            book: RawBook::default(),
            has_snapshot: false,
            deadline: Some(now + TIMEOUT),
            last_seen: now,
            unverified: Vec::new(),
        }
    }

    fn handle(
        &mut self,
        payload: &RawValue,
        checksum: Option<i32>,
        sequence: u64,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<Vec<OrderBookDeltas>>> {
        self.last_seen = Instant::now();
        if let Some(expected) = checksum {
            ensure!(self.has_snapshot, "R0 checksum before snapshot");
            let actual = self.book.checksum();
            ensure!(
                expected == actual,
                "R0 checksum mismatch for {}: expected {expected}, calculated {actual}",
                self.instrument,
            );
            self.deadline = None;
            return Ok(Some(std::mem::take(&mut self.unverified)));
        }
        if payload.get() == r#""hb""# {
            return Ok(None);
        }
        let rows: Vec<&RawValue> =
            serde_json::from_str(payload.get()).context("invalid R0 book payload")?;
        let snapshot = rows.first().is_none_or(|row| row.get().starts_with('['));
        let deltas = if snapshot {
            ensure!(
                !self.has_snapshot,
                "unexpected R0 snapshot on an active channel"
            );
            ensure!(
                rows.len() <= 2 * self.depth,
                "R0 snapshot exceeds subscribed depth"
            );
            let deltas = self
                .book
                .snapshot(&rows, self.instrument, sequence, ts_event, ts_init)?;
            self.has_snapshot = true;
            deltas
        } else {
            ensure!(self.has_snapshot, "R0 update before snapshot");
            self.book
                .update(payload, self.instrument, sequence, ts_event, ts_init)?
        };
        ensure!(
            self.unverified.len() < MAX_UNVERIFIED_FRAMES,
            "R0 checksum backlog exceeded capacity"
        );
        self.deadline
            .get_or_insert_with(|| Instant::now() + TIMEOUT);
        self.unverified.push(deltas);
        Ok(None)
    }
}

pub(crate) struct Session {
    client: WebSocketClient,
    frames: mpsc::Receiver<Frame>,
    commands: mpsc::Receiver<Command>,
    sender: EventSender<DataEvent>,
    cancellation: CancellationToken,
    overflow: Arc<AtomicBool>,
    desired: BTreeMap<InstrumentId, usize>,
    channels: HashMap<u64, Channel>,
    epoch: u64,
    sequence: Option<u64>,
    configured: bool,
    pending: Option<(Control, Instant)>,
    retry_at: Option<Instant>,
    failed: bool,
}

impl Session {
    pub(crate) async fn connect(
        url: String,
        sender: EventSender<DataEvent>,
        cancellation: CancellationToken,
        socket: SocketControl,
        commands: mpsc::Receiver<Command>,
    ) -> anyhow::Result<Self> {
        let (tx, frames) = mpsc::channel(RAW_CAPACITY);
        let overflow = Arc::new(AtomicBool::new(false));
        let handler_overflow = Arc::clone(&overflow);
        let clock = get_atomic_clock_realtime();
        let handler: EpochMessageHandler = Arc::new(move |epoch, message| {
            if let Err(mpsc::error::TrySendError::Full(_)) =
                tx.try_send((epoch, clock.get_time_ns(), message))
            {
                handler_overflow.store(true, Ordering::Release);
            }
        });
        let config = WebSocketConfig {
            url,
            headers: create_standard_nautilus_headers(),
            heartbeat_interval_secs: None,
            heartbeat_payload: None,
            heartbeat_timeout_secs: None,
            reconnect_backoff_factor: Some(2.0),
            reconnect_max_attempts: None,
            writer_capacity: Some(128),
            backend: Default::default(),
            proxy_url: None,
            connect_timeout_ms: Some(10_000),
            reconnect_delay_initial_ms: Some(4_000),
            reconnect_delay_max_ms: Some(30_000),
            reconnect_jitter_ms: Some(500),
            idle_timeout_ms: Some(45_000),
            max_message_size_bytes: Some(2 * 1024 * 1024),
            max_frame_size_bytes: Some(2 * 1024 * 1024),
        };
        let client = WebSocketClient::epoch_builder()
            .config(config)
            .epoch_handler(handler)
            .state_sink(socket.sink())
            .cancellation_token(cancellation.clone())
            .connect()
            .await?;
        let reconnect = client.reconnect_handle();
        socket.register(move || reconnect.request_reconnect());
        Ok(Self {
            epoch: client.connection_epoch(),
            client,
            frames,
            commands,
            sender,
            cancellation,
            overflow,
            desired: BTreeMap::new(),
            channels: HashMap::new(),
            sequence: None,
            configured: false,
            pending: None,
            retry_at: None,
            failed: false,
        })
    }

    pub(crate) async fn run(mut self) {
        let cancellation = self.cancellation.clone();
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        loop {
            let result = tokio::select! {
                biased;
                () = cancellation.cancelled() => break,
                _ = tick.tick() => self.check_health(),
                command = self.commands.recv() => match command {
                    Some(Command::Subscribe(id, depth)) => { self.desired.insert(id, depth); Ok(()) }
                    Some(Command::Unsubscribe(id)) => { self.desired.remove(&id); Ok(()) }
                    None => break,
                },
                frame = self.frames.recv() => match frame {
                    Some((epoch, ts_init, message)) => self.handle_frame(epoch, ts_init, &message),
                    None => break,
                }
            };
            if let Err(error) = result {
                self.invalidate(&error);
            }
            let sent = tokio::select! {
                biased;
                () = cancellation.cancelled() => break,
                sent = self.reconcile() => sent,
            };
            if let Err(error) = sent {
                self.invalidate(&error);
            }
        }
        self.client.disconnect().await;
    }

    fn check_health(&mut self) -> anyhow::Result<()> {
        if self.failed {
            if self
                .retry_at
                .is_some_and(|deadline| deadline <= Instant::now())
            {
                self.retry_at = None;
                let outcome = self.client.reconnect_handle().request_reconnect();
                if matches!(
                    outcome,
                    ReconnectRequestOutcome::Closed | ReconnectRequestOutcome::Unsupported
                ) {
                    log::error!("Bitfinex transport cannot recover: {outcome:?}");
                    self.cancellation.cancel();
                }
            }
            return Ok(());
        }
        ensure!(
            !self.overflow.swap(false, Ordering::AcqRel),
            "Bitfinex receive queue overflow"
        );
        ensure!(self.client.is_active(), "Bitfinex transport disconnected");
        let now = Instant::now();
        ensure!(
            !self.pending.is_some_and(|(_, deadline)| deadline <= now),
            "Bitfinex control response timed out"
        );
        for channel in self.channels.values() {
            ensure!(
                now.duration_since(channel.last_seen) < CHANNEL_IDLE_TIMEOUT,
                "Bitfinex R0 channel stopped responding for {}",
                channel.instrument,
            );
            ensure!(
                !channel.deadline.is_some_and(|deadline| deadline <= now),
                "Bitfinex R0 snapshot/checksum timed out for {}",
                channel.instrument
            );
        }
        Ok(())
    }

    fn invalidate(&mut self, error: &anyhow::Error) {
        if self.failed {
            return;
        }
        self.failed = true;
        self.retry_at = Some(Instant::now() + RETRY_DELAY);
        let ts = get_atomic_clock_realtime().get_time_ns();
        for &instrument in self.desired.keys() {
            if let Err(error) = self.publish(book_invalidation(
                instrument,
                self.sequence.unwrap_or(0),
                ts,
            )) {
                log::error!("Failed to publish Bitfinex invalidation for {instrument}: {error}");
            }
        }
        self.channels.clear();
        log::warn!("Bitfinex R0 epoch {} invalidated: {error:#}", self.epoch);
    }

    async fn reconcile(&mut self) -> anyhow::Result<()> {
        if self.failed || self.pending.is_some() {
            return Ok(());
        }
        let (control, request) = if !self.configured {
            (Control::Configure, json!({"event":"conf", "flags":FLAGS}))
        } else if let Some((&id, _)) = self
            .channels
            .iter()
            .find(|(_, channel)| self.desired.get(&channel.instrument) != Some(&channel.depth))
        {
            (
                Control::Unsubscribe(id),
                json!({"event":"unsubscribe", "chanId":id}),
            )
        } else if let Some((&id, &depth)) = self.desired.iter().find(|(id, _)| {
            !self
                .channels
                .values()
                .any(|channel| channel.instrument == **id)
        }) {
            (
                Control::Subscribe(id, depth),
                json!({
                    "event":"subscribe", "channel":"book", "symbol":format!("t{}", id.symbol),
                    "prec":"R0", "freq":"F0", "len":depth.to_string(),
                }),
            )
        } else {
            return Ok(());
        };
        tokio::time::timeout(
            TIMEOUT,
            self.client
                .send_text_on_connection(request.to_string(), None, self.epoch),
        )
        .await
        .context("Bitfinex control write timed out")??;
        self.pending = Some((control, Instant::now() + TIMEOUT));
        Ok(())
    }

    fn handle_frame(
        &mut self,
        epoch: u64,
        ts_init: UnixNanos,
        message: &Message,
    ) -> anyhow::Result<()> {
        if epoch != self.client.connection_epoch() || epoch < self.epoch {
            return Ok(());
        }
        if epoch > self.epoch {
            self.invalidate(&anyhow::anyhow!("Bitfinex transport epoch replaced"));
            self.epoch = epoch;
            self.sequence = None;
            self.pending = None;
            self.configured = false;
            self.failed = false;
            self.retry_at = None;
        }
        if self.failed {
            return Ok(());
        }
        ensure!(
            !self.overflow.swap(false, Ordering::AcqRel),
            "Bitfinex receive queue overflow"
        );
        let text = message.as_text().context("non-text Bitfinex frame")?;
        if text == RECONNECTED {
            return Ok(());
        }
        if !text.trim_start().starts_with('[') {
            let value = serde_json::from_str(text).context("invalid Bitfinex control frame")?;
            return self.handle_control(&value);
        }
        let frame: Vec<&RawValue> =
            serde_json::from_str(text).context("invalid Bitfinex stream frame")?;
        let [channel, payload, tail @ ..] = frame.as_slice() else {
            bail!("incomplete Bitfinex stream frame");
        };
        let channel_id =
            serde_json::from_str(channel.get()).context("invalid Bitfinex channel ID")?;
        let (checksum, tail) = if payload.get() == r#""cs""# {
            let [checksum, rest @ ..] = tail else {
                bail!("missing Bitfinex checksum")
            };
            (
                Some(serde_json::from_str(checksum.get()).context("invalid Bitfinex checksum")?),
                rest,
            )
        } else {
            (None, tail)
        };
        let [sequence, timestamp, ..] = tail else {
            bail!("missing Bitfinex sequence or timestamp")
        };
        let sequence = serde_json::from_str(sequence.get()).context("invalid Bitfinex sequence")?;
        ensure!(
            self.sequence
                .is_none_or(|last| last.checked_add(1) == Some(sequence)),
            "Bitfinex sequence break: previous {:?}, received {sequence}",
            self.sequence
        );
        self.sequence = Some(sequence);
        let timestamp = serde_json::from_str::<u64>(timestamp.get())?
            .checked_mul(1_000_000)
            .context("invalid Bitfinex timestamp")?;
        let channel = self
            .channels
            .get_mut(&channel_id)
            .context("unknown Bitfinex channel")?;
        if self.desired.get(&channel.instrument) != Some(&channel.depth) {
            return Ok(());
        }
        if let Some(verified) =
            channel.handle(payload, checksum, sequence, timestamp.into(), ts_init)?
        {
            // The whole group becomes available to Nautilus at checksum release.
            // In particular, a recorder checkpoint can occur while frames wait here.
            let available = get_atomic_clock_realtime().get_time_ns();
            for mut deltas in verified {
                deltas.ts_init = available;
                for row in &mut deltas.deltas {
                    row.ts_init = available;
                }
                self.publish(deltas)?;
            }
        }
        Ok(())
    }

    fn handle_control(&mut self, value: &Value) -> anyhow::Result<()> {
        match value["event"].as_str() {
            Some("info") => {
                ensure!(
                    value["code"].is_null()
                        && value["version"].as_u64() == Some(2)
                        && value["platform"]["status"].as_u64() == Some(1),
                    "Bitfinex unavailable: {value}"
                );
                return Ok(());
            }
            Some("error") => bail!("Bitfinex control error: {value}"),
            Some("pong") => return Ok(()),
            _ => {}
        }
        let (pending, _) = self
            .pending
            .take()
            .context("unexpected Bitfinex control response")?;
        match (pending, value["event"].as_str()) {
            (Control::Configure, Some("conf")) => {
                ensure!(
                    value["status"].as_str() == Some("OK")
                        && value["flags"].as_u64() == Some(FLAGS),
                    "Bitfinex rejected WebSocket flags: {value}"
                );
                self.configured = true;
            }
            (Control::Subscribe(id, depth), Some("subscribed")) => {
                ensure!(
                    value["channel"].as_str() == Some("book")
                        && value["symbol"].as_str() == Some(&format!("t{}", id.symbol))
                        && value["prec"].as_str() == Some("R0")
                        && value["freq"].as_str() == Some("F0")
                        && value["len"]
                            .as_str()
                            .and_then(|len| len.parse::<usize>().ok())
                            == Some(depth),
                    "Bitfinex acknowledged a different book profile: {value}",
                );
                let chan = value["chanId"].as_u64().context("missing R0 channel ID")?;
                ensure!(
                    !self.channels.contains_key(&chan),
                    "duplicate R0 channel ID {chan}"
                );
                self.channels.insert(chan, Channel::new(id, depth));
            }
            (Control::Unsubscribe(chan), Some("unsubscribed")) => {
                ensure!(
                    value["status"].as_str() == Some("OK")
                        && value["chanId"].as_u64() == Some(chan),
                    "Bitfinex rejected unsubscribe: {value}"
                );
                self.channels.remove(&chan);
            }
            _ => bail!("unexpected Bitfinex control response: {value}"),
        }
        Ok(())
    }

    fn publish(&self, deltas: OrderBookDeltas) -> anyhow::Result<()> {
        self.sender
            .send(DataEvent::Data(Data::BookDeltas(Box::new(deltas))))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_model::{
        enums::{BookAction, BookType, RecordFlag},
        orderbook::OrderBook,
    };

    fn handle_test(
        channel: &mut Channel,
        frame: Value,
        sequence: u64,
        now: UnixNanos,
    ) -> anyhow::Result<Option<Vec<OrderBookDeltas>>> {
        let text = frame.to_string();
        let parts: Vec<&RawValue> = serde_json::from_str(&text)?;
        let checksum = (parts[1].get() == r#""cs""#)
            .then(|| serde_json::from_str(parts[2].get()))
            .transpose()?;
        channel.handle(parts[1], checksum, sequence, now, now)
    }

    #[test]
    fn publishes_mbo_batches_only_after_matching_checksum() {
        let id = InstrumentId::from("BTCUSD.BITFINEX");
        let mut channel = Channel::new(id, 250);
        let now = UnixNanos::from(1);
        // Synthetic same-price orders and a 64-bit ID that cannot round-trip via f64.
        let snapshot = json!([
            1,
            [[9007199254740993_u64, 100, 2], [7, 100, 9e-8], [8, 101, -4]],
            1,
            0
        ]);
        let first_crc = crc32fast::hash(b"7:9e-8:8:-4:9007199254740993:2") as i32;
        assert!(
            handle_test(&mut channel, snapshot, 1, now)
                .unwrap()
                .is_none()
        );
        let checksum = json!([1, "cs", first_crc, 2, 0]);
        let frames = handle_test(&mut channel, checksum, 2, now)
            .unwrap()
            .unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].deltas[0].action, BookAction::Clear);
        assert_eq!(frames[0].deltas[1].order.order_id, 9007199254740993_u64);
        let mut book = OrderBook::new(id, BookType::L3_MBO);
        book.apply_deltas(&frames[0]).unwrap();
        assert_eq!(book.bids(None).next().unwrap().len(), 2);

        let reprice = json!([1, [7, 99, 3], 3, 0]);
        handle_test(&mut channel, reprice, 3, now).unwrap();
        let remove = json!([1, [9007199254740993_u64, 0, 1], 4, 0]);
        handle_test(&mut channel, remove, 4, now).unwrap();
        let checksum = json!([1, "cs", crc32fast::hash(b"7:3:8:-4") as i32, 5, 0]);
        let frames = handle_test(&mut channel, checksum, 5, now)
            .unwrap()
            .unwrap();
        assert_eq!(frames.len(), 2);
        let move_rows = &frames[0].deltas;
        assert_eq!(move_rows[0].action, BookAction::Delete);
        assert_eq!(move_rows[1].action, BookAction::Add);
        assert_eq!(move_rows[0].order.order_id, move_rows[1].order.order_id);
        assert!(!RecordFlag::F_LAST.matches(move_rows[0].flags));
        assert!(RecordFlag::F_LAST.matches(move_rows[1].flags));
        for frame in &frames {
            book.apply_deltas(frame).unwrap();
        }
        assert_eq!(book.bids(None).next().unwrap().first().unwrap().order_id, 7);
        assert_eq!(book.bids(None).count(), 1);

        let update = json!([1, [7, 99, 9], 6, 0]);
        handle_test(&mut channel, update, 6, now).unwrap();
        let wrong = json!([1, "cs", first_crc, 7, 0]);
        assert!(handle_test(&mut channel, wrong, 7, now).is_err());
    }
}
