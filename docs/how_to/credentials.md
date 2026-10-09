# Venue credentials

Two modes only:

| Mode | When | Data client |
|------|------|-------------|
| **Public** (default) | No env keys, or incomplete pair | `api_key` / `api_secret` = `None` |
| **Authenticated** | Full pair in environment | key + secret injected |

General venue configuration never contains secrets. The Binance Spot SBE and
Predict exceptions below use dedicated ignored, permission-checked TOML files.

## Public (usual case)

Do nothing. Leave API env vars unset.

```bash
cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-dvol.toml
```

## Authenticated

Set **both** key and secret (OKX also needs passphrase):

```bash
export DERIBIT_API_KEY='...'
export DERIBIT_API_SECRET='...'
```

| Venue | Env vars |
|-------|----------|
| Binance (non-SBE) | `BINANCE_API_KEY` + `BINANCE_API_SECRET` |
| Deribit | `DERIBIT_API_KEY` + `DERIBIT_API_SECRET` |
| Bybit | `BYBIT_API_KEY` + `BYBIT_API_SECRET` |
| OKX | `OKX_API_KEY` + `OKX_API_SECRET` + `OKX_API_PASSPHRASE` |
| Hyperliquid | `HYPERLIQUID_PRIVATE_KEY` |

Optional per-`[[venues]].id` override:

```bash
export CAPTURE_VENUE_DERIBIT_MAIN_API_KEY='...'
export CAPTURE_VENUE_DERIBIT_MAIN_API_SECRET='...'
```

Only a **complete** pair is used. Key without secret (or the reverse) → public.

## Binance Spot SBE exception

`kind = "binance_spot"` deliberately does **not** fall back to public JSON
streams. It records the SBE stream and therefore requires a complete Binance
**Ed25519** key pair. Set the normal Binance variables, use an id-scoped pair,
or create the dedicated ignored TOML file:

```bash
export CAPTURE_VENUE_BINANCE_SPOT_SBE_API_KEY='...'
export CAPTURE_VENUE_BINANCE_SPOT_SBE_PRIVATE_KEY='...'
```

The key must have access to Binance Spot SBE market data; no trade permission is
needed for this recorder. Missing or partial credentials make `run` fail before
the node starts rather than silently selecting the JSON transport.

`BINANCE_SBE_CREDENTIALS_FILE` overrides the default
`env/binance-spot-sbe.toml`. The file must contain only `api_key` and
`private_key` and must not be group/world readable. `private_key` is the local
Ed25519 signing key, not Binance's HMAC Secret Key. It is parsed as data, never
executed as a shell script. The legacy `api_secret` field is rejected.

For the Predict + Binance recorder, create the two ignored files:

```bash
mkdir -p env
cp examples/predictfun.credentials.toml.example env/predictfun.toml
cp examples/binance-spot-sbe.credentials.toml.example env/binance-spot-sbe.toml
chmod 600 env/predictfun.toml env/binance-spot-sbe.toml
```

Set `api_key` in `env/predictfun.toml`. Set the Binance API key and matching
Ed25519 `private_key` in `env/binance-spot-sbe.toml`:

```toml
api_key = "the-api-key-id-returned-by-binance"
private_key = '''
-----BEGIN PRIVATE KEY-----
the-complete-local-ed25519-private-key
-----END PRIVATE KEY-----
'''
```

The private key is mapped internally to NautilusTrader's generic
`BinanceDataClientConfig.api_secret` field because that upstream field carries
the signing credential for both Binance authentication modes. The user-facing
SBE file intentionally calls it `private_key`; `api_secret` is not accepted.

Build the single product CLI with the two required adapters, then validate and run the
canonical 5m/15m profile. Credential paths are passed explicitly; they are not placed in
the capture profile or shell history as values.

```bash
cargo +1.99.0 build --release -p catalog-capture-cli \
  --no-default-features --features venue-binance,venue-predict

PREDICT_CREDENTIALS_FILE="$PWD/env/predictfun.toml" \
BINANCE_SBE_CREDENTIALS_FILE="$PWD/env/binance-spot-sbe.toml" \
  ./target/release/catalog-capture-cli validate \
  --config examples/capture.predict-binance-spot-sbe-btc-updown.toml

PREDICT_CREDENTIALS_FILE="$PWD/env/predictfun.toml" \
BINANCE_SBE_CREDENTIALS_FILE="$PWD/env/binance-spot-sbe.toml" \
  ./target/release/catalog-capture-cli run \
  --config examples/capture.predict-binance-spot-sbe-btc-updown.toml
```
