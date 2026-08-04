# Venue credentials (optional)

**Default: public market data only — no API keys used.**

Most catalog capture (quotes, trades, book summary polls, option chains, etc.)
uses **public** venue endpoints. The CLI is designed so that:

| Situation | Behavior |
|-----------|----------|
| No API env vars | Public clients (`api_key` / `api_secret` = `None`) |
| Fake / placeholder keys in env (`none`, `xxx`, `test`, …) | Treated as **absent**; public clients |
| Incomplete pair (key without secret) | **Not** injected; public clients |
| Real keys present by accident | **Still ignored** unless you opt in (see below) |

Nautilus adapters may fall back to env vars when config keys are `None`. In
public mode the CLI therefore **clears known credential env vars** before
building data clients, so leftover fake keys cannot affect the session.

## Opt-in authenticated clients

Only when you truly need private endpoints:

```bash
export CAPTURE_USE_VENUE_CREDENTIALS=1   # required opt-in
export DERIBIT_API_KEY='...'
export DERIBIT_API_SECRET='...'
```

Without `CAPTURE_USE_VENUE_CREDENTIALS=1|true|yes|on`, credentials are never
passed into data clients.

## Variable names (when opted in)

| Venue | Environment variables |
|-------|------------------------|
| Binance Futures | `BINANCE_API_KEY`, `BINANCE_API_SECRET` |
| Deribit | `DERIBIT_API_KEY`, `DERIBIT_API_SECRET` (or `CLIENT_ID` / `CLIENT_SECRET`) |
| Bybit | `BYBIT_API_KEY`, `BYBIT_API_SECRET` |
| OKX | `OKX_API_KEY`, `OKX_API_SECRET`, `OKX_API_PASSPHRASE` |
| Hyperliquid | `HYPERLIQUID_PRIVATE_KEY` |

### Per-venue-id override

```bash
# [[venues]] id = "deribit_main"
export CAPTURE_VENUE_DERIBIT_MAIN_API_KEY=...
export CAPTURE_VENUE_DERIBIT_MAIN_API_SECRET=...
```

Scoped vars take precedence over global `VENUE_*` names.

## Safety

- Never put secrets in TOML.
- Prefer `.env` only for local opt-in auth (gitignored).
- Logs show `credentials=from_env` or `credentials=public` — never print secrets.
- CI and smoke/soak should stay on public endpoints (do not set the opt-in flag).
