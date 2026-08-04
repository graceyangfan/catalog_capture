# Venue credentials

Two modes only:

| Mode | When | Data client |
|------|------|-------------|
| **Public** (default) | No env keys, or incomplete pair | `api_key` / `api_secret` = `None` |
| **Authenticated** | Full pair in environment | key + secret injected |

Secrets never go in TOML.

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
| Binance | `BINANCE_API_KEY` + `BINANCE_API_SECRET` |
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
