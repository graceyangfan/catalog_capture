# Predict execution internals

This reference describes component ownership and runtime behavior. Supported
commands and venue semantics are defined by the
[adapter contract](../predict-execution-adapter-design.md). Reproducible checks
and source provenance are in the [validation reference](predict-validation.md)
and [test guide](../../crates/nautilus-predict/tests/README.md).

## Component assembly

```text
HTTP / WebSocket DTOs
        |
HTTP collection + parse + immutable context + fill evidence
        |
Orders | Wallet | Account | Reports
        |
Lifecycle + maintenance + PredictExecutionClient
        |
Native Engine / Cache / Portfolio -> strategy callbacks
```

| Component | Responsibility |
| --- | --- |
| http/collect | Complete, bounded raw pagination with explicit scope and page limits |
| parse | Pure wallet/REST conversion, exact units and protocol identity validation |
| context | Immutable Native-derived order and report views for async workers |
| fills | Confirmed raw executions, queued delivery and provisional settlement notices |
| orders | Command validation, signed-hash correlation, submission and removal |
| orders/routing | Associate signed hashes with numeric REST IDs |
| wallet | Consume acknowledged wallet facts and deliver Native events/reports |
| account | Collect chain cash and OPEN orders, then project Native reservations |
| reports | Target, bulk, fill and position report collection and preparation |
| metadata | Prepare market definitions and contract signing nonces |
| observations | Account/strategy-scoped custom-data payloads |
| lifecycle | Own connection generations, observers, authentication and shutdown |
| maintenance | Dispatch Native query intents and retire mirrored contexts |
| mod.rs | Implement the Native client interface and assemble components |

Native owns order transitions, positions, risk, report application, recovery
scheduling and retention. Commands produce venue writes; report collection
produces observations for Native application. Both REST and wallet fills pass
through the same evidence owner.

## State ownership

| State | Key and owner | Update and retirement |
| --- | --- | --- |
| Orders and positions | Native Engine/Cache/Portfolio | Native applies facts before publishing strategy callbacks |
| WorkerOrder and ReportContext | Client order ID, signed hash or query scope; context | Native observers supply immutable views; owner purge controls retirement |
| Pending submissions and query intents | Client order ID; orders::OrderRequests | Commands register ownership before transmission; terminal Native observations retire it |
| Numeric REST route | Signed hash; orders/routing | Create receipts and independent reports supply the numeric removal ID |
| Raw/queued executions | Trade ID and signed hash; fills::FillEvidence | REST/WS share validation and deduplication; retirement requires application and resolved ownership |
| Cash baseline and reservations | Account/environment, block and wallet epoch; account | One collector updates remote facts; Native reservation changes update the local projection |
| Signing inputs | Instrument definition or verifying contract; metadata | Background preparation writes values; submission reads them; explicit client setup resets nonces |
| Custom data | Account and strategy; observations | Publish immutable payloads through the Native data channel |

Client-ID and hash indexes share the same WorkerOrder objects. Native
OrderContext supplies common immutable terms; Predict adds applied-fill, original
quote-intent and settlement facts needed outside the core thread. Open/inflight
flags preserve Native emulation semantics; closed status derives from OrderStatus.

One account collector handles periodic refresh and explicit QueryAccount.
Requests arriving during IO remain pending on that collector. Local reservation
projection continues while HTTP/RPC is held. Only reservation changes advance
resource_revision; transient order status changes do not trigger cash reads.
Explicit queries permit authenticated reads during a wallet gap, while periodic
reads require a wallet ACK.

## Tasks and deadlines

The client's Native TaskGroup owns generation admission, cancellation and bounded
shutdown. Up to four long-lived tasks have independent IO lifetimes:

| Task | Responsibility |
| --- | --- |
| Wallet execution | Consume acknowledged order and settlement facts |
| Account reports | Serialize periodic and explicit cash/OPEN collection |
| Signing metadata | Prepare market inputs and missing nonces when submission is enabled |
| Authentication | Renew expiring JWTs when an authentication signer is configured |

Submit, cancel and QueryOrder use bounded one-shot workers. Workers own their
request deadlines and outcome handling; Native owns report collection scheduling
and core-thread finalization. Venue rejection, an unsent command and an ambiguous
transmitted write retain distinct outcomes. Trading commands are not replayed.

| Schedule or deadline | Default or bound | Purpose |
| --- | --- | --- |
| Native maintenance | Optional, 1 second | Dispatch pending queries and bounded cleanup; idle ticks send no HTTP |
| Account refresh | 30 seconds; configurable 1..86400 | Collect external cash changes and OPEN reservations |
| Market preparation | Half the metadata TTL; default 15 seconds | Refresh market signing inputs; instrument updates and wallet ACK also notify it |
| Missing nonce retry | 1, 2, 4 seconds, capped at 30 seconds | Retry missing slots after a recovery round; no timer while all slots are valid |
| WS protocol tick | 50 milliseconds | Check ACK, token expiry and transport state locally |
| JWT renewal | 80% of remaining lifetime; retry 1..30 seconds | Renew authentication and publish replacement credentials |
| Command/collection deadline | Default 30 seconds | Bound an asynchronous operation, including pagination |
| Causal cash retry | Up to 5 attempts with 1-second waits | Recover a changed block or a balance snapshot older than a confirmed fill |
| Initial account application | 10-millisecond checks within the request deadline | Wait for Portfolio to apply the first reported account |

Market HTTP and nonce RPC run in separate branches of the metadata task. Up to
four missing contracts are read concurrently; each has its own deadline and
canonical-block check and publishes its cache slot independently. An invalidation
wakes an idle retry wait. Explicit connect starts nonce preparation immediately
but awaits account initialization and best-effort market preparation instead of
nonce readiness. Automatic wallet reconnect preserves valid contract nonces.

Authentication renews in the same task using a renewal wait and bounded-backoff
retry. The current wallet remains usable until replacement credentials are
published or the token expires. Replacement requires a fresh wallet ACK.

The public reconnect_reconciliation_interval_secs and with_reconciliation_clock
names control maintenance. Native owns socket reconnect and order/position
recovery scheduling.

## Transport and parsing

Public HTTP uses one request/status/decode helper. Native request_with_params
serializes typed queries, and Native Url encodes category path segments. Public
catalogue and private account collections reject cursor cycles and enforce page
bounds. Private error handling excludes credentials and raw signed payloads.

The wallet client assembles Native WebSocketConfig, epoch callbacks, bounded
queues and TaskSlot ownership. The handler owns wallet subscription, ACK, token
rotation and fact dispatch. Transport reconnect and backoff belong to Native.
Both layers share generation cancellation.

Public and private parsers share borrowed RawValue and heartbeat codecs while
preserving their envelope requirements. Wallet ACK and account provenance are
strictly validated. Decimal quantities retain their original exact tokens.
PredictQuoteType is shared, with title-case HTTP and uppercase wallet decoding.
Strict signing-market DTOs and account-history projections retain their distinct
required fields. Pending settlement values retain their venue-defined meaning;
they are not converted into orderbook share quantities or confirmed executions.

Target, bulk and venue-only reports share pure converters. Signed hashes are
validated at collection boundaries and compared as B256 values. Positive signing
amounts use the common Decimal-to-wei conversion. A verified cancellation or
expiry can publish before trade-history collection finishes; an older OPEN
snapshot cannot reopen a terminal order or discard its late real fills.

## Native API integration

| Concern | API or retained adapter responsibility |
| --- | --- |
| Batch modification | ExecutionClient's default fanout delivers each child to Predict's explicit ModifyRejected path |
| Submitted/rejected events | ExecutionEventEmitter supplies standard events and account identity |
| Checked event dispatch | OrderEventFactory plus try_send_order_event where no equivalent checked emitter API exists |
| Owned order lookup | ExecutionClientCore::get_order; scoped cache borrows for related fields |
| Worker views | Native OrderContext plus Predict's immutable execution facts |
| Mass status | Bundled order/fill preparation preserves atomic own/external recovery and current-holdings lookback; Native admits and applies reports |
| Submission continuity | Validate intent at command entry, continuity before preparation and again under the registration guard |
| Venue identity | Validate verifier, token, units, numeric REST route and wallet provenance at protocol boundaries |

Comparisons with maintained adapters and the official SDK are documented in the
[comparison basis](predict-validation.md). These
references guide ownership and API use; venue-specific nonce and settlement
semantics remain Predict responsibilities.

## Performance and retention boundaries

Signing and immutable snapshot construction occur before shared registration
locks. No network await holds those guards. Optional telemetry measures account
lock wait and complete registration, including nested waits and index updates;
samples publish after guard release. Disabled telemetry reads no measurement
clock. The [benchmark exporters](../../crates/nautilus-predict/tests/README.md#benchmarks-and-explicit-json-exporters)
separate preparation, HTTP and callback measurements.

Wallet normalization borrows applied/queued indexes. Cold evidence validation,
large report finalization, Native physical purge and final large-object disposal
remain linear costs. Background HTTP does not isolate synchronous finalization
on the Native core thread. A bounded retirement scan does not guarantee constant
memory or a hard callback pause bound.

Cleanup follows Native retention, rechecks current ownership and preserves
pending/unapplied facts. Large final references are released outside shared
guards. Credentials belong to the host; mock venues, strategies and recorders
remain under tests or benches. Production exports are client/config/factory
interfaces, protocol converters, custom data and opt-in telemetry.
