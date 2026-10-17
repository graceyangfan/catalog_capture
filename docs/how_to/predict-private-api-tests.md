# Predict private API calibration

Use offline tests for routine development and explicitly selected read-only
probes for credential and venue calibration. Order-write runs are a separate,
supervised operation. Current API support and ownership are described in the
[adapter contract](../predict-execution-adapter-design.md); test layers and fixture
policy are in the [test guide](../../crates/nautilus-predict/tests/README.md).

## Local configuration

Create the local file only if it does not already exist:

```bash
mkdir -p env
[ -e env/predict-execution.toml ] || cp examples/predict-execution.toml.example env/predict-execution.toml
chmod 600 env/predict-execution.toml
```

The API-key file is configured separately. Keep both files private and ignored
by Git. Do not put wallet addresses, private keys, signatures or JWTs in docs.

| Field | Meaning |
| --- | --- |
| `privy.mode` | `eoa` for a plain wallet; `predict_account` for a Kernel trading account |
| `privy.private_key` | Exported Privy/owner private key; never a mnemonic |
| `privy.owner_address` | Optional check against the address derived from that key |
| `privy.predict_account` | Trading account holding collateral and positions; empty in EOA mode |
| `network.api_key_file` | API-key TOML, resolved relative to the execution TOML |
| `network.rpc_url` | BNB RPC supporting EIP-1898 canonical block-hash reads |
| `network.environment` | Network used consistently for signing, API and chain identity |

Keep `test.allow_order_writes = false` for this guide. Decimal financial limits
are quoted TOML strings. Markets, token IDs, outcomes, prices and quantities are
discovered or calculated at runtime, not configured as fixed market IDs.

## Signing wallet and trading account

In `predict_account` mode, the exported owner key signs authentication and orders
for the separate Predict trading account. JWT identity, order maker/signer and
balance reads refer to that trading account. The owner wallet may have no balance
while the trading account has funds.

```toml
[privy]
mode = "predict_account"
private_key = ""
owner_address = ""
predict_account = ""
```

In `eoa` mode, leave `predict_account` empty. The key-derived address is both
signing and trading identity. The optional owner guard checks the key in either
mode. The official SDK verifies the Kernel owner when a Predict Account is used.

`/v1/account` verifies identity and account metadata; it is not a collateral
balance endpoint or a wallet-directory service. Balance calibration reads the
configured trading account on-chain. A cross-chain deposit address must not be
assumed to be that account without ownership verification.

## Read-only authentication and signing

Select one named probe; do not run the entire ignored-test set:

```bash
cargo test -p nautilus-predict --features execution --locked --test private_api_live mainnet_privy_private_api_and_native_account_read_only -- --ignored --nocapture
cargo test -p nautilus-predict --features execution --locked --test private_api_live mainnet_resting_preflight_and_sdk_signing_read_only -- --ignored --nocapture
```

The first probe authenticates, checks account identity, reads private orders and
holdings, acknowledges the wallet topic, and initializes reported cash through
Native Portfolio. The signing probe discovers parameters and compares Rust
amounts, hash and signature with the official Python SDK through a private pipe.
It does not submit the signed order or change allowance.

An RPC without canonical block-hash reads cannot be used for balance calibration;
the adapter does not combine unrelated `latest` reads. Keep credential-bearing
transport frame/header tracing disabled.

## Read-only renewal and query commands

These wrappers authenticate and connect a Rust strategy through the product host
and LiveNode. Their relays allow venue reads, authentication and read-only RPC,
while rejecting order writes and chain transactions.

```bash
uv run --python 3.12 --with predict-sdk==0.0.22 --with websocket-client --with websockets --with python-dotenv --with requests python tests/run_predict_session_readonly.py
uv run --python 3.12 --with predict-sdk==0.0.22 --with websocket-client --with websockets --with python-dotenv --with requests python tests/run_predict_queries_readonly.py
```

The session probe requires a flat account without active orders. Two early token
renewals exercise wallet subscribe ACK, heartbeat echo, QueryAccount and Native
readiness. It verifies the renewal path without waiting for natural expiry.

The query probe checks explicit QueryAccount and QueryOrder. A real historical
Canceled response repairs deliberately stale local state, and a repeated query
must not duplicate the Native cancellation event. This is not a live cancellation
of an active order. Outputs preserve protocol quantities with identities and
credentials redacted; new captures do not overwrite existing files.

## Offline replay and supervised execution

Normal regressions use saved JSON and loopback services, with no operator key.
They cover resource bounds, standard rejections, partial fills, replay deduplication,
net inventory, reported cash and strategy callbacks. See the test guide for commands.

The [Rust strategy guide](predict-rust-strategy-calibration.md) describes custom
snapshot signals, cancel/replacement, maker take-profit and taker stop-loss
calibration. SDK-valid quantities do not establish venue minimum notional, and a
preflight quote does not guarantee later acceptance or a profitable exit. Any
funded run needs its own explicit scope, budget, capture supervision and cleanup.

Protocol references: [official SDK](https://github.com/PredictDotFun/sdk),
[order guide](https://dev.predict.fun/how-to-create-or-cancel-orders-679306m0),
[wallet events](https://dev.predict.fun/subscription-topics-1915507m0).
