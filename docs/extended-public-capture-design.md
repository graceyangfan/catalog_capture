# Extended public capture contract

The `nautilus-extended` crate provides public market data through Nautilus
interfaces. This document defines wire semantics, state ownership and lifecycle
requirements.

## Goal

Record Extended public market data into the same Nautilus canonical families
already used by the capture actor:

- `instruments`
- `order_book_deltas`
- `trades`
- `mark_prices`
- `index_prices`
- `funding_rates`

The supported scope is read-only market data. It does not include
order signing, account streams, execution, RFQ trading, private data, BBO, or
candles.

## Protocol facts

The public API documentation defines the payload semantics through the v1
streams. The official Python SDK exposes the same families through the RPC v2
endpoint `wss://api.starknet.extended.exchange/stream.extended.exchange/v2/rpc`:

| Family | RPC v2 subscription | Documented cadence | Capture decision |
|---|---|---:|---|
| Full L2 | `scope = "orderbooks"`, `depth = "full"` | 100 ms | Primary L2 path |
| Trades | `scope = "trades"` | venue push | `TradeTick` |
| Mark | `scope = "prices"`, `type = "mark"` | on change | `MarkPriceUpdate` |
| Index | `scope = "prices"`, `type = "index"` | on change | `IndexPriceUpdate` |
| Funding | `scope = "funding-rates"` | venue push | `FundingRateUpdate` |

The full order book starts with a snapshot, emits deltas between snapshots,
and publishes another snapshot approximately every minute. In RPC v2, `seq`
is monotonic across the entire multiplexed connection, not independently for
each subscription. A gap or out-of-order value invalidates that connection
generation.

Source: [Extended API documentation](https://api.docs.extended.exchange/).

## Transport and protocol authority

The adapter uses one multiplexed RPC v2 connection per data client. The official
Python SDK defines the endpoint, selectors and envelopes; the public API
reference defines payload fields. Parse wire payloads directly, including
absolute delta quantity `c`, and retain fixtures for these contracts.

A connection sequence failure recovers the entire generation and gates every
book until its next snapshot. Book recovery cannot backfill missed trades or
prices; report that interval as a health discontinuity.

## RPC v2 wire contract

The physical endpoint is:

```text
wss://api.starknet.extended.exchange/stream.extended.exchange/v2/rpc
```

The upgrade request uses the standard Nautilus `User-Agent`, which the official
documentation requires. The public mainnet REST and RPC v2 probes succeeded
without account credentials, so the first data-only config has no API key or
private-key surface. Private/account support can add credentials later without
changing public topic identity.

The server sends WebSocket protocol Ping frames every 15 seconds and expects a
Pong within 10 seconds. Nautilus' WebSocket transport handles these control
frames; they are never passed to the RPC parser and never consume `seq`. The
adapter does not add a competing text heartbeat. It enables a 45-second
dead-peer timeout, tolerating two missing server pings. It does not enable one
global application-data idle timeout because a valid funding-only or
mark-only subscription may legitimately remain quiet. Instead, every ready
book has a 90-second stream-staleness deadline, longer than the documented
approximately one-minute periodic snapshot cadence; expiry recovers the whole
connection generation.

Control requests use JSON-RPC-like envelopes with a client-generated string
`id`, `jsonrpc = "2.0"`, a method and optional params. Supported methods needed
by this adapter are `subscribe`, `unsubscribe`, `ping` and
`list-subscriptions`. A successful subscribe response contains the canonical
subscription ID; unsubscribe is correlated only by request ID.
If an unsubscribe response also includes a subscription ID, it must match the
pending canonical topic; the official SDK contract also permits that field to
be absent.

| Logical family | Selector | Canonical subscription | Stream type |
|---|---|---|---|
| Full L2 | `orderbooks`, market, `depth = "full"`, `rfqOnly = false` | `orderbooks.BTC-USD` | `ORDERBOOKS.SNAPSHOT` / `ORDERBOOKS.DELTA` |
| Trades | `trades`, market | `trades.BTC-USD` | `TRADES` |
| Funding | `funding-rates`, market | `funding-rates.BTC-USD` | `FUNDING_RATES` |
| Mark | `prices`, `type = "mark"`, market | `prices.mark.BTC-USD` | `PRICES.MP` |
| Index | `prices`, `type = "index"`, market | `prices.index.BTC-USD` | `PRICES.IP` |

Stream-data envelopes contain `type`, `data`, `ts`, `seq` and `subscription`.
Control responses do not participate in the stream sequence. The adapter first
parses explicit `{ id, error }` responses, then successful `{ id, result }`
responses, and only then stream-data envelopes.

Selector values are validated locally before desired intent is changed. This
is required because the mainnet probe accepted valid requests but returned no
response for an invalid order-book depth during the bounded test window. Every
control request also has an epoch-bound timeout; timeout or ambiguous write
completion never commits a new live state.

## Native integration

The implementation uses maintained Nautilus networking and subscription
primitives directly, while borrowing the Binance book-recovery invariants at
the correct scope:

| Nautilus primitive or pattern | Extended use |
|---|---|
| `WebSocketClient::epoch_builder` | Every inbound frame carries the transport epoch that owns it; stale-epoch frames are discarded |
| `WebSocketReconnectHandle` | A global sequence/parse/queue failure requests one transport replacement; duplicate requests are naturally coalesced |
| `send_text_on_connection` | Subscribe, unsubscribe and control requests belong to one epoch and are never replayed automatically onto a replacement socket |
| `SubscriptionState` | Desired, pending subscribe, confirmed, pending unsubscribe and reference counts remain separate; reconnect replays desired topics in deterministic order |
| `SnapshotGate` / `PendingSnapshot` | The gate prevents snapshot acceptance while the epoch-bound subscribe write is in flight; separate RPC state requires a matching ACK, and missing snapshots are timed |
| Binance `BookSyncTracker` invariant | Validation, gate transition and canonical emission are serialized so a replacement snapshot cannot interleave with old deltas |
| Binance `BookRecovery` invariant | Recovery has one owner, bounded attempts and stale work cannot accept/fail a newer generation |

Binance's concrete `BookSyncTracker` and `BookRecovery` types are not reused.
Binance recovers one book by buffering diffs and bridging a REST snapshot;
Extended v2 recovers the entire RPC connection and receives authoritative
WebSocket snapshots. The Extended equivalent is therefore one connection
recovery state plus per-book snapshot gates, not one independent recovery task
per book.

Canonical data follows the same parser conventions as Binance:

- snapshots emit one checked `OrderBookDeltas` batch containing `CLEAR + ADD`
  rows with `F_SNAPSHOT`, and only the last row adds `F_LAST`;
- deltas emit absolute `UPDATE` or zero-size `DELETE` rows, with `F_LAST` on the
  final row;
- prices and quantities are parsed directly from decimal strings at instrument
  precision, never through `f64`;
- one venue frame remains one `OrderBookDeltas` queue item, including a
  multi-thousand-level snapshot, so levels do not flood the recorder queue as
  separate messages;
- the adapter emits canonical Nautilus data only. `CatalogCaptureActor` owns
  Parquet policy and never participates in venue recovery.

## State ownership

| State | Complete identity | Lifetime and reset |
|---|---|---|
| Transport | data client + connection epoch | Replaced by Nautilus WebSocket reconnect; old epoch can no longer publish |
| Global stream sequence | connection epoch | Starts at zero and advances for every stream-data frame; discarded on reconnect |
| Desired subscription | family + market + profile | Survives reconnect; removed only after the last local owner unsubscribes |
| Pending RPC request | connection epoch + request ID + operation + topic | Completed by matching ACK/error/timeout; stale-epoch responses cannot mutate current state |
| Book readiness | instrument ID + connection epoch | Opens only after a current-epoch subscribe ACK and authoritative snapshot |
| Trade replay dedupe | instrument ID + venue trade ID | Survives reconnect to suppress replay; removed on final unsubscribe or client shutdown |
| Instrument metadata | canonical `InstrumentId` | Loaded by HTTP bootstrap and refreshed independently of socket generations |

Code uses a typed `ExtendedTopic { family, market, profile }` as logical
identity and derives the canonical wire string once. `SubscriptionState::new('.')`
tracks that string for acknowledgment and reference counting; parsers route
through the typed topic retained with the confirmed subscription, not by
re-parsing arbitrary inbound strings.

The adapter processes connection state on one owning task. Parser work may use
helpers, but state mutation and event publication stay ordered on that task;
there is no per-frame task spawning around sequence validation.

## Subscription scope

The recorder uses one RPC connection and subscribes per requested market, not
to all-market selectors in the first version. This keeps venue identity,
subscription ownership and catalog partitioning unambiguous while retaining
the global connection sequence.

The recorder only subscribes to the full L2 stream. It does not subscribe to
`depth=1`: that channel is a faster BBO snapshot but is not the L2 data needed
for book reconstruction. It does not throttle or aggregate RPC v2 book frames;
every accepted venue delta is emitted as one canonical batch.

For a market that needs executable depth:

```toml
[[capture.book_deltas]]
instrument_id = "BTC-USD-PERP.EXTENDED"
book_type = "L2_MBP"
```

## Subscription lifecycle

One handler task owns RPC request correlation, global sequence validation,
subscription state and routing. The lifecycle is:

1. Validate the family, market and profile against the instrument cache.
2. Add a local reference in `SubscriptionState`. Only the first reference
   creates physical subscribe intent; later consumers share that topic.
3. Close the book snapshot gate, allocate a monotonically increasing request
   ID, record `(epoch, request_id, operation, topic)`, then send with
   `send_text_on_connection`. Open the gate only after that epoch-bound write
   succeeds; this is the same transport-ordering role as Nautilus'
   `SnapshotGate`, not an RPC confirmation.
4. Accept an ACK only from the owning epoch and request ID. The returned
   subscription string must equal the expected canonical topic before
   `confirm_subscribe`. Write success without this ACK is still unconfirmed.
5. A book becomes ready only after both the matching ACK and the first valid
   current-epoch snapshot. Subscribe ACK alone never means the book is
   synchronized. A stream-data frame for an unconfirmed topic fails the
   generation closed rather than adding an unbounded pre-ACK buffer.
6. Removing a non-final reference changes no wire state. Removing the last
   reference marks pending unsubscribe and suppresses further routing for that
   topic immediately. The venue ACK completes teardown only if the topic still
   has zero references; a resubscribe makes the old unsubscribe ACK stale and
   preserves the renewed topic and family state.
7. A subscribe rejection or timeout leaves desired intent pending for bounded
   recovery. An unsubscribe rejection retains pending teardown rather than
   restoring an owner that no longer exists.
8. Reconnect increments the transport epoch, abandons old pending requests,
   resets confirmed state to pending desired topics, closes every book gate and
   resets the global sequence. The active receive owner is established before
   epoch-bound resubscribe requests are sent and awaited.
9. Each successful ACK commits that topic's live state. RPC v2
   `list-subscriptions` remains an explicit diagnostic operation, matching the
   official SDK; it is not inserted into the high-volume startup or reconnect
   path.

The official server replaces an existing subscription when the same topic is
subscribed again. Mainnet testing confirmed that duplicate order-book
subscription produces another full snapshot. The adapter therefore never uses
duplicate subscribe as a keepalive or recovery shortcut; reference counting
and request correlation prevent accidental replacement.

Profile changes are competing semantics, not widening. The first adapter slice
supports only full L2, so requesting another depth while a book is owned is
rejected rather than silently replacing the live topic.

## Order-book semantics

The RPC socket owns connection sequence state and each requested book owns a
snapshot gate:

```text
generation
last_connection_seq
book_gates: instrument_id -> AwaitingSnapshot | Ready
```

The state machine is:

1. The first stream-data frame for a new RPC connection must have `seq = 0`.
   The official SDK states that sequence restarts at zero. Mainnet probes
   observed `seq = 0`, but that frame may belong to any subscribed family; in
   the multi-market probe it was an ETH trade replay, not a book snapshot.
2. Every subsequent stream-data frame, regardless of subscription, must have
   `seq = last_connection_seq + 1`. Per-topic gaps are expected and must not be
   interpreted as data loss.
3. A global duplicate, skip or out-of-order sequence invalidates the entire
   connection generation before the frame is routed to a family parser.
4. A venue snapshot for a confirmed subscription resets the Nautilus managed
   book with a canonical `CLEAR + ADD levels` `OrderBookDeltas` batch, carries
   snapshot flags and transitions that book from `AwaitingSnapshot` to
   `Ready`.
5. A snapshot uses `q` as its absolute quantity; current mainnet snapshots omit
   `c` even though the documentation example includes it. A delta must carry
   both signed change `q` and absolute quantity `c`; the adapter uses `c` as
   the Nautilus level quantity. A zero `c` becomes `BookAction::Delete`, and a
   positive `c` becomes an absolute `BookAction::Update`.
6. The connection sequence is a transport-integrity signal and resets to zero
   after reconnect. It therefore must not be copied into Nautilus' canonical
   book sequence. The adapter owns a monotonically increasing sequence per
   instrument for emitted `OrderBookDeltas`; every row in one batch carries
   that sequence and the batch ends with `F_LAST`.
7. An empty delta consumes its valid connection sequence but emits no book
   batch. It must never be translated into `CLEAR`.
8. A wrong subscription, wrong market, malformed payload, missing delta `c`,
   or book delta before that instrument's snapshot fails the generation closed.
   The invalid frame is not forwarded to the capture actor.
9. Reconnect starts a new connection generation, resubscribes desired topics
   and waits for a fresh snapshot for every book before forwarding its deltas.
   Each fresh snapshot is recorded in `order_book_deltas` with a canonical
   book sequence greater than the last event emitted before reconnect.
10. The parser uses `OrderBookDeltas::new_checked` (or the current equivalent)
   and validates instrument identity, side, price/size precision, non-negative
   quantities and batch termination before emission. A malformed frame fails
   closed; it does not become a partial batch.

RPC acknowledgements and errors do not carry the stream-data sequence and are
handled separately. Stream frames are sequence-checked and parsed in socket
receive order before any asynchronous family dispatch; the client must not
spawn unordered per-frame work that can advance global state out of order.

The venue's approximately one-minute snapshots and the recorder's hourly
segment-boundary snapshot are different events. The former is part of the
source stream and receives the next canonical per-instrument sequence. The
latter is a synthetic replay checkpoint from Nautilus' managed book and may
reuse the managed book sequence. RPC v2 global sequence continuity is
validated in the live adapter before family routing, but it is not stored in
the standard order-book sequence field because it is connection-global and
resets after reconnect. Catalog replay validation checks increasing canonical
book sequence and legal `F_SNAPSHOT` resets. Synthetic segment snapshots are
also legal reset boundaries.

The adapter must not request a REST snapshot and then stitch it into a live
sequence. REST order book data is a probe/bootstrap tool only; the WebSocket
snapshot is authoritative for a live sequence generation.

## Instrument identity

The initial scope is active non-RFQ perpetual markets from
`GET /api/v1/info/markets`:

```text
Extended market: BTC-USD
Nautilus symbol: BTC-USD-PERP
Nautilus id:     BTC-USD-PERP.EXTENDED
```

Instrument metadata supplies price precision, size precision, increments and
the canonical currencies. The WebSocket parser must resolve the instrument
from a shared immutable/read-mostly cache populated by the HTTP bootstrap; it
must not refetch metadata on every frame.

For BTC-USD mainnet, the market and asset endpoints report base `BTC` and
collateral `USD`. The USD asset maps to an underlying USDC contract on-chain,
but the venue's API identity remains `USD`. The Nautilus instrument therefore
uses `BTC` base and `USD` quote/settlement; the adapter must not hardcode or
rename settlement to `USDC`.

Price precision/increment come from `minPriceChange`; size
precision/increment come from `minOrderSizeChange`. `assetPrecision` and
`collateralAssetPrecision` describe assets and are not substitutes for order
increments. The first slice accepts only `type = PERPETUAL`, `active = true`,
`status = ACTIVE` and `isRfq = false`.

Instrument refresh may replace metadata for an existing ID and emit an
`Instrument` event. It must not silently add spot or RFQ instruments to a
perpetual capture plan.

## Data mapping

| Family | `ts_event` source | `ts_init` source |
|---|---|---|
| Order book | envelope `ts` | Nautilus clock once per received frame |
| Trade | row `T` | Nautilus clock once per received frame |
| Funding | `data.T` | Nautilus clock once per received frame |
| Mark/index | positive `data.ts`, otherwise envelope `ts` | Nautilus clock once per received frame |

All numeric wire values are parsed from strings or typed JSON integers into
exact Rust/Nautilus domain values. Envelope and row timestamps are checked for
non-negative millisecond-to-nanosecond conversion; invalid values fail the
connection generation rather than falling back silently, except for the
explicit mark/index zero-timestamp rule above.

### Trades

Emit standard `TradeTick` with venue trade ID, side, price, quantity, event
timestamp and receive/init timestamp. Extended also exposes trade type values
such as `TRADE`, `LIQUIDATION`, and `DELEVERAGE`. `TradeTick` does not preserve
that classification. A later opt-in custom family may record the raw trade
envelope, but the first standard capture should not duplicate every trade by
default.

RPC v2 replays recent trades on subscription. Mainnet testing returned an
ascending 50-row batch, and a clean second connection replayed the same 50
trade IDs exactly. The adapter therefore owns a bounded per-instrument
`TradeReplayState`:

- the first batch of a cold subscription seeds recent IDs and is not emitted;
- after reconnect, the first replay batch emits only IDs not retained from the
  previous generation, in venue event order;
- later batches also suppress exact `(instrument_id, trade_id)` duplicates;
- the recent-ID set survives reconnect but is removed on final unsubscribe or
  shutdown;
- replay exhaustion cannot prove a long outage complete, so a global sequence
  break still marks a data-quality discontinuity.

Venue trade IDs are JSON integers currently around `2.1e18`, above the exact
range of IEEE-754 doubles. They are deserialized directly into an integer and
converted to `TradeId` text without any floating-point intermediate. `BUY` and
`SELL` represent taker side and map to the corresponding Nautilus aggressor
side. The item field `T`, not envelope `ts`, is `TradeTick.ts_event`.

### Funding

Emit `FundingRateUpdate` from the public funding stream. Funding is perpetual
only. The documented stream contains rates actually applied to hourly funding
payments, even though rates are calculated each minute. It is not an estimated
next-funding feed. `data.T` is the event timestamp and `data.f` is parsed as an
exact decimal. Historical funding is a separate REST path and is not part of
the live public recorder. Mainnet RPC v2 testing observed the exact stream type
`FUNDING_RATES` with the documented `{m, f, T}` payload; the adapter retains
that frame as a parser fixture.

### Mark and index

Use `data.ts` when it is positive; otherwise fall back to envelope `ts`.
Mainnet mark-price frames currently carried `data.ts = 0`, while index-price
frames carried a valid calculation time. Preserve local receive time as
`ts_init` and do not clamp venue event time when exchange and host clocks are
skewed. Clock skew is a health metric, not a reason to rewrite source time.

### Ticker and open interest

The current public WebSocket contract does not define a separate ticker or
open-interest stream. Trades, mark, index and funding are the canonical live
families for this integration. Market statistics and open-interest history are
REST data and are intentionally out of scope, not invented as fake ticker
channels.

## Integration

Venue protocol and lifecycle state belong to `crates/nautilus-extended`.
The CLI registers `ExtendedDataClientFactory` behind `venue-extended` and owns
capture configuration. The adapter has no dependency on `catalog-capture-*`.

```bash
./scripts/cargo.sh build --release -p catalog-capture-cli \
  --no-default-features --features venue-extended
```

## Failure, reconnect and shutdown

The implementation uses one RPC v2 connection for each configured Extended
data client. There is no v1 fallback. The Nautilus WebSocket epoch is the only
authority for physical connection ownership.

Transport loss uses Nautilus automatic reconnect with exponential backoff and
jitter. A replacement epoch resets the global sequence and live ACK state but
preserves desired subscriptions and trade replay dedupe. A protocol failure
such as a sequence gap, unknown subscription, payload/type mismatch, parser
failure or missing book snapshot requests one explicit reconnect and rejects
the rest of the old epoch.

Book snapshot waits use an explicit ten-second timeout. A protocol failure
marks the current epoch failed and asks Nautilus for one replacement; repeated
requests for that epoch are coalesced. Initial connection has a bounded retry
policy, while ordinary post-connect network unavailability follows the
Nautilus WebSocket reconnect policy and may continue retrying under the process
supervisor policy.
Each valid snapshot or delta resets that ready book's staleness deadline; final
unsubscribe, reconnect and shutdown cancel the old deadline owner.

Inbound delivery is bounded. The epoch handler uses a bounded raw-frame queue;
overflow is a generation failure and requests reconnect instead of silently
dropping a frame. The WebSocket also sets an explicit maximum message size with
headroom above observed full-book snapshots. Full snapshots are emitted as one
canonical batch, so the catalog ingress queue sees one item rather than one item
per level.

Shutdown first closes subscription admission, then cancels the handler owner,
pending RPC waiters, snapshot deadlines and recovery work, and finally closes
the socket. Old epochs and late ACKs cannot publish or repopulate state after
shutdown. No detached reconnect task survives the data client.

Initial resource bounds are deliberately small and explicit:

| Bound | Initial value | Failure behavior |
|---|---:|---|
| Raw inbound frames | 256 | Mark generation unhealthy and reconnect |
| WebSocket message size | 2 MiB | Reject frame and reconnect |
| Dead-peer timeout | 45 seconds | Reconnect when no frame, including Ping/Pong, arrives |
| Ready-book staleness | 90 seconds | Reconnect when one desired book emits no data |
| Book snapshot wait | 10 seconds | Reconnect and retry within recovery budget |
| RPC request timeout | 10 seconds | Keep desired state pending; do not claim ACK |

The observed BTC-USD snapshot was about 169 KiB and roughly 6,000 levels, so
the frame limit leaves substantial headroom without permitting unbounded
allocation. These constants can become configuration only after measurements
show a real deployment need.

## Protocol validation

Raw-frame fixtures cover connection-global sequencing, repeated initial trade
replays, duplicate-subscribe book replacement and exact large trade IDs. Snapshot
levels and deltas retain their distinct wire shapes. Venue timestamps may lead
local ingestion time and must not be clamped. Short protocol probes establish
wire semantics; unattended stability requires separate soak and recovery checks.

## Validation

Offline tests cover exact selectors and ACK identities, reference counting,
stale epochs, reconnect, connection-global sequencing, book initialization and
replacement, absolute quantities, trade replay deduplication, timestamp mapping
and bounded-queue failures. Use the [test guide](../tests/README.md) for execution.

A public smoke must verify accepted subscriptions, fresh snapshots after forced
reconnect, strictly increasing canonical book sequences, unique trade IDs,
Native catalog readback and a populated managed book. Funding validation needs
an observation window containing an actual funding event. Short protocol probes
do not establish unattended stability or completeness across an outage.

## Supported scope

The crate implements exact RPC models, public data families, Native lifecycle
integration, a CLI feature and a BTC capture profile. Operational validation must
cover forced reconnect reconciliation, snapshot replacement, catalog readback
and a funding window long enough to observe an actual event. Private account
streams and execution are outside this public capture contract.
