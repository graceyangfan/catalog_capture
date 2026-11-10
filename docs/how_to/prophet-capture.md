# Record Binance Prophet market data

The `venue-prophet` feature records the public Binance Pick campaign feed. Run
from the repository root:

```sh
./scripts/cargo.sh build -p catalog-capture-cli --no-default-features --features venue-prophet
./target/debug/catalog-capture-cli validate --config examples/capture.prophet.toml
./target/debug/catalog-capture-cli run --config examples/capture.prophet.toml
```

The profile uses fixed market IDs from the current campaign. Replace the example
market ID in both selectors to record another market. The client loads up to
eight configured markets from the current season's event list. Instrument IDs
take the form `<marketId>-0.PROPHET`; `0` refers to the first venue outcome,
which can be named Yes or a team name. The definition includes the season,
event slug, Binance market and topic IDs, and both outcome token IDs.

| Selector | Catalog record |
| --- | --- |
| `capture.book_depth` with `book_type = "L2_MBP"` | Full supplied depth snapshot and its `BinaryOption` definition |
| `ProphetLastTradePrice` with the decimal market ID as identifier | Latest price observations with source and receive times |

Latest price frames can repeat and have no fill identity or quantity. They are
recorded as custom data. Instrument currency `SCORE` represents campaign points
in the Nautilus model. This profile needs neither an API key nor a login.

The client resolves the two public `subKeys` for each active market, waits for
the WebSocket acknowledgement, and fetches a REST depth snapshot when depth is
requested. Reconnect opens a new connection and resolves its subscription keys
again. Older source timestamps cannot replace a newer published book. A quiet
market may only supply the REST depth snapshot during a short run.

Prophet uses the Binance website's `predictx` BAPI and WebSocket protocol.
Binance Wallet's signed SAPI uses separate routes and may report different depth
for the same outcome token. Store the two sources under separate identities.

## Source and catalog layout

| Data | Source | Catalog directory |
| --- | --- | --- |
| Instrument definitions | Prophet season query and `predictx/event/list` | `data/binary_option/<instrument_id>/` |
| Depth | `PREDICTX_OBOOK` and the `predictx/order-book` startup snapshot | `data/order_book_depths/<instrument_id>/` |
| Latest price | `PREDICTX_TRADE` | `data/custom/ProphetLastTradePrice/<market_id>/` |

All paths are relative to the catalog root. For a bounded run, set
`runtime.capture_seconds` to a positive value and check that the sealed
Parquet files appear under these directories. A quiet market may emit no
latest-price frames during that interval. Run the offline reconnect test with:

```sh
./scripts/cargo.sh test -p nautilus-binance-prediction --lib --locked
```
