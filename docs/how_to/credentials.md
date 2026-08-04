# Venue credentials (optional)

**Default:** public market data only — no API keys required.

When a venue needs authenticated REST/WS, set credentials in the **process
environment** (or a local `.env` loaded by the CLI). **Never put secrets in TOML.**

## Variable names

| Venue | Environment variables |
|-------|------------------------|
| Binance Futures | `BINANCE_API_KEY`, `BINANCE_API_SECRET` |
| Deribit | `DERIBIT_API_KEY`, `DERIBIT_API_SECRET` |
| Bybit | `BYBIT_API_KEY`, `BYBIT_API_SECRET` |
| OKX | `OKX_API_KEY`, `OKX_API_SECRET`, `OKX_API_PASSPHRASE` (or `OKX_PASSPHRASE`) |
| Hyperliquid | `HYPERLIQUID_PRIVATE_KEY` (or `HL_PRIVATE_KEY`) |

### Per-venue-id override

If you run multiple configs for the same kind, scope by `[[venues]].id`
(non-alphanumeric → `_`, uppercased):

```bash
# id = "deribit_main"
export CAPTURE_VENUE_DERIBIT_MAIN_API_KEY=...
export CAPTURE_VENUE_DERIBIT_MAIN_API_SECRET=...
```

Scoped vars take precedence over the global `VENUE_*` names.

## Usage

```bash
# optional local dotenv (gitignored)
cp .env.example .env   # if present; or create your own

export DERIBIT_API_KEY=...
export DERIBIT_API_SECRET=...

cargo run -p catalog-capture-cli -- run --config examples/capture.deribit-dvol.toml
```

Logs report `credentials=from_env` or `credentials=public` without printing secrets.

## Safety

- Keep `.env` out of git (see `.gitignore`).
- Prefer least-privilege / read-only keys for capture-only deployments.
- CI and public smoke tests should stay on public endpoints.
