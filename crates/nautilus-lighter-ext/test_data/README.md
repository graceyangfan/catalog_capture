# Public protocol fixtures

Collected from Lighter mainnet on 2026-10-20. Messages are public `/events`, `order_book`, `bo_market_stats` and `up_down_candle` responses. No account credentials or private messages are included. Candle batches and book levels are sampled there. `trades.json` contains one normal trade and one market-settlement row from the public `/trades?market_id=4255&market_type=bo&sort_by=timestamp&limit=100` response, retaining the complete upstream `LighterTrade` shape.

Tests explicitly change the oracle timestamp to the event boundary when mocking the exact-boundary endpoint, and synthesize zero/missing OI, duplicates, nonce gaps and reconnect frames. These mutations test contracts; they are not claims of additional mainnet observations. Mainnet acceptance is separately covered by the opt-in catalog recording test and its readback evidence.
