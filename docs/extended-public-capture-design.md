# Extended Public Capture Design

Status: design baseline for a public-data-only integration.

This integration will be implemented inside this repository. The legacy
`extened_exchange` workspace is protocol reference material only; this project
will not depend on that workspace or link its crate.

The venue crate is named `nautilus-extended`. It is a long-lived Nautilus
adapter boundary, not a catalog-specific helper: public data capture is its
first consumer, while a future `ExecutionClient` and private/account support
can be added without renaming or moving the protocol implementation.

## Goal

Record Extended public market data into the same Nautilus canonical families
already used by the capture actor:

- `instruments`
- `order_book_deltas`
- `trades`
- `mark_prices`
- `index_prices`
- `funding_rates`

The first release is a read-only market-data adapter. It does not include
order signing, account streams, execution, RFQ trading, private data, BBO, or
candles.

## Protocol Facts

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

## SDK And Documentation Authority

The implementation uses each source at its actual scope rather than treating
all SDKs as interchangeable:

| Source | Use in this project | Boundary |
|---|---|---|
| Official API documentation | v1 URLs, message types, payload field semantics, cadence and book reconnect rules | Authoritative payload contract |
| Official Python SDK (`x10xchange/python_sdk`) | RPC v2 endpoint, selectors, envelopes, lifecycle intent and REST models | Authoritative v2 wire reference; its lifecycle implementation and convenience order-book helper are not copied |
| Official TypeScript examples (`x10xchange/examples`) | REST market schemas, order construction and signing examples | No complete public market-data WebSocket client; not a streaming adapter dependency |
| Legacy local Rust workspace (`extened_exchange`) | Cross-check REST naming and old protocol assumptions | REST-only reference; never a crate dependency |

The official Python SDK is useful for comparing lifecycle behavior. Its v1
client opens one connection per topic and leaves reconnect ownership to the
caller. Its RPC v2 client multiplexes topics, retains subscription intent and
resubscribes after reconnect. The SDK does not declare either client deprecated
or recommended over the other.

The SDK's `OrderbookQuantityModel` only exposes `p/q`, and its convenience
helper adds `q` for a delta. That is insufficient for lossless capture because
the official wire contract also exposes `c`; the Rust adapter therefore parses
the wire envelope directly and records the documented absolute level quantity.
This discrepancy is covered by fixtures before live capture is enabled.

The latest inspected official branch reports SDK version 2.6.0. Its RPC test
surface covers a basic candle subscribe/message/unsubscribe flow, but not
reconnect, sequence gaps, multi-topic ordering, replacement failure or stale
responses. The SDK is therefore authoritative for endpoint, selector and
envelope shape, but it is not the reliability implementation to copy verbatim.
In particular, the Rust adapter must not copy these SDK behaviors:

- dispatching every frame in an independent asynchronous task before global
  sequence state is serialized;
- awaiting resubscribe RPC results before the receive loop that resolves those
  results is running;
- continuing to dispatch the current frame after a sequence-break callback;
- removing desired subscriptions when one resubscribe attempt fails;
- dropping local unsubscribe intent before the venue acknowledgement;
- logging a parse/handler failure and continuing the same generation.

## Transport Decision: RPC v2 Only

The adapter implements the official SDK's RPC v2 public stream only. It does
not implement v1 fallback or expose a user-selectable transport switch.
Extended does not currently state that one public transport is recommended or
that v1 is deprecated; this is our product decision based on the SDK and
mainnet evidence:

- one connection covers all five required families and reduces socket/task
  count for a long-running multi-market recorder;
- the official Python client confirms that RPC v2 is designed around retained
  subscription intent and reconnect reconciliation, while Nautilus supplies
  the stronger implementation primitives used here;
- the connection-level sequence provides one completeness signal across book,
  trade, price and funding events;
- mainnet RPC v2 payloads match v1 payload semantics and rebuilt the same
  BTC-USD book in the comparison probe;
- one maintained transport gives the adapter one subscription registry, one
  reconnect model and one fixture set. No fallback can silently switch sequence
  semantics during recording.

The tradeoff is a larger recovery scope: one connection gap may have dropped
any multiplexed topic, so the adapter must restart the connection generation
and gate every subscribed book until its new snapshot arrives. Trades and
price events cannot be backfilled by that snapshot; the gap is recorded as a
health discontinuity for downstream exclusion.

RPC v2 is not yet described in the public API reference. The official Python
SDK is therefore the authority for endpoint, selectors and envelope shape,
while the public v1 documentation remains the authority for payload fields and
book semantics. The SDK's reconnect code is evidence of intended behavior, not
the reliability implementation. Live fixtures and contract tests protect this
boundary.

## RPC v2 Wire Contract

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

## Nautilus Adapter Alignment

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

The implementation should be reviewed against these upstream source areas at
the Nautilus revision pinned by this repository:

| Upstream source | Contract reused or adapted |
|---|---|
| `crates/network/src/websocket/client.rs` | Connection epochs, reconnect handle and epoch-bound control writes |
| `crates/network/src/websocket/subscription.rs` | Desired/confirmed/pending subscription ownership and reference counting |
| `crates/live/src/book/snapshot.rs` | Snapshot gate and pending-snapshot timeout semantics |
| `crates/live/src/book/recovery.rs` | Single recovery owner, bounded recovery and stale-work rejection |
| `crates/adapters/binance/src/book/sync.rs` | Serialized validation, synchronization and publication |
| `crates/adapters/binance/src/book/recovery.rs` | Recovery attempt ownership; not Binance's REST snapshot algorithm |

This is a semantic comparison, not a source copy. Extended selectors, global
sequence, replay behavior and snapshot rules remain venue-specific.

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

## State Ownership

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

## Fastest-Channel Policy

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

## Subscription And Generation Lifecycle

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

## Order-Book Semantics

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

## Instrument Identity

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

## Data-Fidelity Decisions

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

## Repository Shape

Add a small feature-gated adapter crate rather than placing venue protocol code
in the CLI:

```text
crates/nautilus-extended/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── common.rs
    ├── config.rs
    ├── data.rs
    ├── factories.rs
    ├── http.rs
    └── websocket/
        ├── mod.rs
        ├── messages.rs
        └── parse.rs
```

`websocket/mod.rs` owns the connection epoch, global sequence, RPC request
correlation, subscription lifecycle, snapshot gates and reconnect handling.
`messages.rs` owns only the wire envelope and typed topic/request identities;
`parse.rs` converts venue payloads into canonical Nautilus data. The adapter is
kept in these three lifecycle-oriented modules until their behavior is large
enough to justify a further split.

Public names follow maintained adapter conventions:
`ExtendedInstrumentProviderConfig`, `ExtendedDataClientConfig`,
`ExtendedHttpClient`, `ExtendedDataClient`, and
`ExtendedDataClientFactory`. `lib.rs` re-exports the public construction
surface used by the runner. Venue protocol code never depends on
`catalog-capture-*`; the recorder is only one consumer of the adapter.

There is intentionally no execution, signing, account, candles, BBO, or
Python binding surface in the first adapter slice. The CLI links the crate
behind `venue-extended`; the default all-venues build can include it only
after the public path is live-tested. A lean production build may use:

```bash
cargo build --release -p catalog-capture-cli \
  --no-default-features --features venue-extended
```

The CLI additions are limited to:

- `VenueRuntimeConfig::Extended { id, environment }`;
- `kind = "extended"` parsing and validation;
- `ExtendedDataClientFactory` registration in the `LiveNode` builder;
- Extended examples and a public-only feature-gated integration test.

## Failure, Reconnection And Shutdown

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

## Mainnet Protocol Evidence

Direct raw-frame probes were run against mainnet on 2026-09-26 without SDK
models or account credentials. They established:

- a common BTC-USD v1/v2 window rebuilt the same final top three bid and ask
  levels; RPC v2 carried a contiguous connection sequence;
- one RPC connection with BTC-USD and ETH-USD across ten requested topics
  delivered 685 stream frames with global `seq = 0..684` and no gap;
- the first frame was an ETH trade replay, not a book snapshot, proving that
  sequence initialization cannot depend on subscription order or family;
- `ping` and `list-subscriptions` succeeded, and the returned live set exactly
  matched all ten canonical topic IDs;
- BTC full-book snapshot size was about 169 KiB with roughly 6,000 levels and
  `d = "f"`; snapshot levels contained `p/q`, while deltas contained `p/q/c`;
- duplicate subscribe of the same order-book topic was acknowledged and caused
  another full snapshot, confirming replacement rather than no-op semantics;
- two consecutive fresh connections each began with the same ascending 50-row
  BTC trade replay followed by a fresh book snapshot. All 50 exact trade IDs
  overlapped across generations;
- trade IDs were around `2.1e18`, beyond exact JavaScript `Number` range;
- mark frames carried `data.ts = 0` while index frames carried non-zero
  calculation timestamps;
- the venue clock was about two to three seconds ahead of the probe host, so
  valid `ts_event > ts_init` must be tolerated;
- an invalid depth selector received no bounded response and did not appear in
  `list-subscriptions`, reinforcing local validation and request timeouts;
- funding subscription returned a live `FUNDING_RATES` frame with the
  documented `{m, f, T}` shape; that exact frame is retained as a fixture.

This proves current wire shape, ordering, replacement and replay behavior, not
long-term operational stability. Release acceptance still requires a mainnet
soak with forced transport reconnects, successful automatic reconciliation,
fresh snapshots for every book, at least one real funding event, zero
unreported sequence gaps and Parquet readback.

## Tests Before Live Capture

The crate must have fixture tests for:

- market-to-instrument identity and precision;
- exact RPC selectors, canonical topic IDs and ACK/error correlation;
- protocol Ping/Pong excluded from RPC sequence and application-data routing;
- dead-peer timeout and per-book staleness configuration;
- two local owners producing one physical subscribe and only the final release
  producing unsubscribe;
- stale ACKs and old-epoch data unable to mutate the current generation;
- stream data arriving before its matching subscribe ACK rejected closed;
- reconnect starting the receive owner before resubscribe ACKs are awaited;
- failed resubscribe retaining desired intent for bounded retry;
- subscribe/unsubscribe ACK topic mismatch failing reconciliation;
- snapshot flags and `CLEAR + levels` reconstruction;
- snapshot absolute `q` and delta absolute `c` versus signed change `q`;
- delete at zero quantity;
- empty delta ignored without clearing the book;
- second periodic snapshot resetting the managed book;
- initial `seq = 0` accepted from a non-book topic;
- interleaved global sequence accepted across several markets and families;
- per-topic sequence gaps accepted while global `seq + 1` is continuous;
- global duplicate, gap, and out-of-order sequences rejected;
- unknown topic, topic/payload market mismatch and parser failure requesting
  reconnect before later old-generation events can publish;
- reconnect generation requiring a fresh snapshot for every desired book;
- duplicate physical subscribe causing a replacement snapshot in the mock, so
  reference-count tests cannot pass vacuously;
- cold trade replay suppressed, reconnect replay deduplicated and unseen trades
  emitted in event order;
- trade IDs above `2^53` retained exactly;
- mark `data.ts = 0` falling back to envelope `ts` without clamping to
  `ts_init`;
- funding timestamp/rate mapping;
- bounded raw queue overflow becoming a health/reconnect failure.

The live probe should record one selected perpetual for a short bounded run and
then use the existing Rust `validate-order-book` command to prove:

```text
snapshot batches > 0
source sequence strictly increases per instrument
connection-level sequence gap metric remains zero
every desired subscription receives its matching ACK
forced reconnect yields one fresh snapshot per desired book
trade replay creates no duplicate TradeTick IDs
ParquetCatalog read succeeds
OrderBook::apply_deltas succeeds
final book is non-empty when the venue stream supplied levels
```

The funding path additionally needs a run long enough to observe at least one
real hourly event. Only after these probes are stable should we add more markets
or an all-market subscription mode.

## Implementation Order

1. Pin and compile against the repository's current official Nautilus
   `origin/develop` revision before adding the adapter; do not introduce a
   compatibility layer for older Nautilus APIs.
2. Add `nautilus-extended`, configuration, provider, exact market models and
   instrument parser tests.
3. Implement RPC envelopes, typed topics, epoch-bound request correlation,
   `SubscriptionState` reconciliation and global sequence tests.
4. Implement full-L2 parsing/gates, trade replay dedupe, mark/index fallback
   timestamps and funding mapping from captured fixtures.
5. Implement `ExtendedDataClient` subscriptions, connection recovery,
   factories and teardown tests.
6. Wire `venue-extended` into the capture CLI and add one BTC-USD profile.
7. Run short mainnet capture/readback, forced reconnect reconciliation, natural
   periodic snapshot validation and a funding observation run of at least one
   hour.

The first production slice is the public-data side of a long-lived Nautilus
adapter. Execution and private account support can be added later in the same
`nautilus-extended` crate without changing this capture contract.
