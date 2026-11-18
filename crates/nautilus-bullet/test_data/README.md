# Bullet regression fixtures

These inputs support decoding, precision and lifecycle regressions. They are not
continuous recordings or availability/completeness evidence. Mutated boundary
cases and mock instruments in tests are synthetic.

| Fixture | Provenance |
| --- | --- |
| `ws_depth_snapshot.json` | Official [DepthUpdate example](https://tradingapi.bullet.xyz/docs/ws/#depthupdate); whitespace compacted |
| `ws_mark_price.json` | Official [mark-price example](https://tradingapi.bullet.xyz/docs/ws/#markprice); whitespace compacted |
| `ws_agg_trade.json` | Exact JSON string from `agg_trade_round_trips_wire_payload` in [wire interface source](https://github.com/bulletxyz/bullet-ws-interface/blob/6c8c6339344adccb5f327ae9df5f0856b37538db/src/server.rs); upstream testnet capture dated 2026-05-24 |
| `http_exchange_info_mainnet.json` | Public mainnet exchangeInfo response captured 2026-11-18; native currencies, filters and precision retained |
| `ws_mainnet_btc_probe.json` | Selected mainnet messages captured 2026-11-18 from depth100, aggTrade and markPrice; original values, reformatted JSON; used for decoding, oracle precision and repeated trade IDs |
| `http_funding_info_btc.json` | Public mainnet funding metadata captured 2026-11-18; includes required multiplier/spread fields |
| `http_premium_index_btc.json` | Public mainnet estimate/last-rate snapshot captured 2026-11-18; exact decimal tokens and millisecond timestamps |
| `http_funding_history_btc.json` | Public mainnet settled history captured 2026-11-18; exact rates and microsecond timestamps |

The SDK reference is pinned to
[`79e25490d6de2acf9a25d02a791f1f7745950ef5`](https://github.com/bulletxyz/bullet-rust-sdk/tree/79e25490d6de2acf9a25d02a791f1f7745950ef5).
The wire interface is a supplementary source, not asserted to be that SDK's
resolved dependency revision. Full protocol and funding semantics are in the
[adapter contract](../../../docs/bullet-public-capture-design.md).
