# Matched sports and esports capture

Record matched Predict / Polymarket markets through the existing Nautilus data
adapters. The example discovers LoL, CS2, Dota 2, Valorant,
College Football, NBA and NFL full-match winners. Each market must independently
pass bilateral activity and book gates. Maps/games are optional; soccer and
non-moneyline propositions are outside the maintained profile.

This path discovers, matches, selects, subscribes, rotates and writes Native
Parquet. It does not run strategies or start analysis after capture. Downstream
tools such as Polars read sealed files and the associated metadata separately.

## Run

From the repository root:

```bash
./scripts/cargo.sh build -p catalog-capture-cli --no-default-features --features venue-polymarket
./target/debug/catalog-capture-cli validate --config examples/capture.matched-sports.toml
./target/debug/catalog-capture-cli run --config examples/capture.matched-sports.toml
```

`venue-polymarket` also enables Predict support. The profile requires exactly
one Predict and one Polymarket client. Predict credentials use the existing
[credential configuration](credentials.md); PM public data needs no trading key.
Configuration validation does not fetch a live matched catalogue.

The example runs until stopped (`capture_seconds=0`). For a finite check, copy
the TOML, set a bounded duration and use a fresh `catalog_uri`. No background
capture is started by these documentation or test changes.

## Selection controls

| Setting | Default | Meaning |
| --- | --- | --- |
| `tag_ids` | `[84]` | Up to eight Predict category tags; the example explicitly broadens this list |
| `include_match_winners` | `true` | Include full-match winners; disable for a maps-only profile |
| `include_map_winners` | `false` | Include explicitly matched map/game winners when enabled |
| `max_markets` | `1` | Active slots; valid incumbents retain their slots |
| `preload_next` | `true` | Record at most one extra qualified candidate before handover |
| `preparation_timeout_secs` | `30` | Bound a preparation attempt without all definitions/usable initial books |
| `preparation_retry_secs` | `30` | Cooldown before a timed-out candidate can be retried |
| `refresh_secs` | `30` | Sleep after a discovery attempt, in addition to request duration |
| `max_pages` | `100` | Per-tag page cap; reaching an unfinished cap fails the refresh |
| `max_activity_candidates` | `256` | Cap statistics enrichment, independently of complete binding facts; at most eight concurrent requests |
| `activity_scan_pages` | `4` | Per-tag 50-row volume-ranked pages when the activity pool exceeds its budget; 1–20 |
| `observation_timeout_secs` | `30` | Close an observation interval after a required book becomes quiet |
| `selection_candidates` | `5` | Maximum candidates receiving book/history checks per scan; at least `max_markets + 1` with preloading, otherwise `max_markets` |
| `min_volume24h_usd` | `100` | Minimum market-level 24h volume on each venue |
| `max_spread` | `0.05` | Maximum absolute ask-minus-bid probability spread on each checked book |
| `depth_band` | `0.03` | Absolute distance from each side's own best quote for summing depth |
| `min_depth_shares` | `100` | Minimum shares on each bid/ask side within that band |
| `recent_trade_window_secs` | `300` | Historical lookback; no startup waiting period |
| `max_last_trade_age_secs` | `120` | Maximum age of the newest sampled execution on each venue |
| `min_recent_trade_times` | `2` | Minimum distinct execution seconds inside the lookback on each venue |

Dollar, spread, depth and activity thresholds are provisional configurable
admission settings, not empirically calibrated trading or execution limits.
The former `activity_window_secs`, `min_recent_volume_usd` and
`min_active_intervals`, `prefetch_secs` and `near_start_secs` controls have been
removed; old TOML keys are rejected.

### First-scan selection

1. Match the REST catalogue through `polymarketConditionIds` and Gamma; preserve
   every validated pair for incumbent retention, independently of admission score.
2. Before Predict statistics, filter known insufficient/unknown PM volume or disabled books.
   If the remaining pool exceeds the statistics budget, use bounded per-tag
   volume-ranked REST pages, with confirmed live candidates first. This shortlist
   never removes validated ownership. Unavailable enrichment defers admissions.
   Exclude new admissions with unknown/insufficient bilateral market-level 24h
   volume or disabled PM order books. Confirmed live markets get first priority;
   future and unknown schedules can enter immediately when all checks pass.
   There is no start-time proximity or lookahead cutoff.
3. Within a priority group rank by Predict 24h volume, then PM 24h volume, then ascending
   stable Predict ID. Check at most `selection_candidates` pairs in that order.
4. Fetch Predict YES and both PM outcome books. Sort levels, reject empty,
   one-sided, crossed/locked, invalid or duplicate-price books. Each book must
   pass spread and **both** near-best depth floors. The books are checked
   separately; complementary liquidity is never summed. One snapshot cannot
   establish persistent liquidity, and an unchanged quote is not a broken feed.
5. Read the newest **50 public execution rows per venue**. Require recent
   executions at multiple distinct seconds and a sufficiently recent last trade
   on both venues. These timestamps are existing history: admission can succeed
   on the first scan without a warm-up timer. `has_more` is audited; no complete
   recent dollar volume, exact fill-frequency comparison or unique participants
   are inferred from this bounded page. Insufficient evidence defers admission.
6. Qualifying candidates fill free slots. With `preload_next=true`, one extra
   qualified candidate is subscribed and recorded as a preparation. Valid
   incumbents remain subscribed when their score or activity falls.

The bounded shortlist can omit a lower-volume market with better books; raise
its cap for wider coverage. Up to four pairs are checked concurrently, each with
five concurrent read-only requests. The whole scan retains its 120-second
budget. API latency still determines first-selection time; there is no fixed
15-minute observation wait.

Required selection data uses REST: Predict `/v1/categories`,
volume-ranked `/v1/markets` when needed, `/v1/markets/{id}/stats`, `/v1/markets/{id}/orderbook`, `/v1/orders/matches`,
PM Gamma, CLOB `/book` and Data API `/v2/trades` with `taker_only=true`.
Predict public matches have no private signer filter. PM's latest page uses the
API's default minimum size; it is not an all-microfill archive. Neither venue's
Native TradeTick projection is used for turnover selection.

Optional REST `category.stats.holdersCount` is retained as **Predict event-level
context**. Missing/malformed counts remain unknown. It is neither a market-level
count nor compared against PM's paginated token-holder lists, and does not block
admission. GraphQL and full holder enumeration are not selection dependencies.
A common per-market holder definition is required before adding that tie-break.

For alternative tags and full-match/map-only profiles, see the
[category table](../concepts/matched-sports-discovery.md#category-and-market-type-scope).
Set `include_match_winners=false` and `include_map_winners=true` for maps only;
at least one market type must be enabled. Capacity is global across categories
and maps, with no per-event quota. A full slot is not replaced merely because
a newly discovered event is hotter.

A past scheduled time alone does not establish live play. Map live priority needs
an explicit current-map number; conflicting live flags become unknown. Retirement
still follows matching/closure/catalogue eligibility, not score changes. There is
one additional preparation slot when `preload_next=true`; `max_markets` bounds
active slots while total matched ownership is at most `max_markets + 1`. See
[selection and handover](../concepts/matched-sports-discovery.md).

## Match contract

Admission requires one valid `polymarketConditionIds` value, an exact target
condition, the locally supported PM protocol version (`v1`), compatible market
types, and identical nonempty rules after whitespace normalization. Map/game
numbers must agree; explicit finished states are excluded.

Each Predict `indexSet` is bound to a unique PM asset through outcome names and
explicit team aliases. Category and outcome `variantDetails` can supply those
aliases. Missing or ambiguous direction is rejected: array position, first
result and fuzzy team-name guesses are not fallbacks. A supplied PM parent-event
identity must also agree.

The actor requests missing instrument definitions and persists ready definitions
before starting their market-data subscriptions. This happens per instrument;
there is no atomic four-instrument subscription barrier.

## Recorded data

| Venue | Definitions | Market data |
| --- | --- | --- |
| Predict | YES and NO | Only YES (`indexSet=1`) `OrderBookDepth` and `TradeTick` |
| Polymarket | Both outcome assets | Each asset's `OrderBookDeltas` and `TradeTick` |

One pair therefore has four definitions and three market-data instruments.
PM outcome streams are both retained; do not assume their ticks are independent
or perfectly mirrored. This recorder does not deduplicate cross-outcome trades.
The maintained PM adapter computes effective deltas for this profile: subsequent
full books produce net changes instead of repeated unchanged books. It is not
a raw WebSocket packet archive. Initial/recovery snapshots remain baselines.

Files use the Native catalogue partitions:

```text
data/order_book_depths/<Predict YES instrument>/...parquet
data/order_book_deltas/<PM outcome instrument>/...parquet
data/trades/<instrument>/...parquet
data/binary_option/<instrument>/...parquet
```

Instrument layout is controlled by the Native instrument type; see
[catalog layout](../concepts/catalog_layout.md). The example seals market-data
segments on UTC hour boundaries. Retirement and shutdown seal partial hours;
recovery or resource limits can create additional segments. Flush intervals
write buffered bytes and do not themselves define file boundaries.

Read sealed `.parquet`, not active `.part.parquet`. File boundaries and replay
continuity are different concepts: an hourly seal does not mean a feed gap,
and a recovery must not be disguised as continuous data.

## Rotation and quality

Discovery repeatedly lists the catalogue, rather than deriving the next slug
or assuming a fixed match duration. New and overlapping events can be selected
when there is a free slot, without waiting for settlement of unrelated events.
Complete successful results retain valid incumbents and fill remaining slots.
A failed/partial identity discovery retains the previous set. Failed admission
statistics or ranked-list requests preserve complete binding facts and do not
block independently observed terminal updates. Identified PM parent `ended=true`
also excludes a market when parent identity is unambiguous and `live=true` does
not conflict. Missing terminal fields defer to explicit closure; match duration
is never guessed.
A successful empty set removes the dynamic subscriptions.

The actor keeps one preparation sticky while it remains valid. Its four
instrument definitions use the existing metadata requests; Predict YES and both
PM outcome streams subscribe through the maintained adapters. Preparation data
is recorded immediately to the normal Native Parquet partitions, including
trades, rather than buffered for replay at promotion.

Readiness requires all four accepted definitions, subscribed recording streams,
an initial snapshot for each of the three books, and usable two-sided noncrossed
books. PM book usability comes from the managed Native cache; the actor does not
build a parallel order book. Before promotion, sink flush barriers process the
prepared data. An ended incumbent then exits and its partial files seal; the
prepared market keeps its subscriptions, book state and open files. The next
ordinary increment is recorded without a reset or history replay. Hourly seals
apply to active and prepared markets alike, with PM checkpoint baselines.

Preparation times out independently of discovery refreshes. The actor exits and
seals only the failed candidate, observes a cooldown, and may try another ranked
candidate. Late metadata cannot revive retired subscriptions. A later valid
snapshot completes a retry; metadata alone or an empty book cannot. The old
market stays while a prepared or qualified successor remains available; unrelated
quiet catalogue entries do not postpone retirement. A missing/not-yet-published or unqualified
successor cannot be preloaded. Sink errors do not authorize promotion.

The preparation adds at most four definitions, three data instruments and six
market-data subscriptions beyond the active budget. Startup estimates include
this reserve. Increase partition limits if raising `max_markets`. Stop cancels
both roles and retries while preserving any independently configured static
ownership. Use `preload_next=false` for the previous bounded active-only mode.

This closes the local handover startup gap when a prepared stream is ready;
it does not establish upstream losslessness or cover events before subscription.
A quiet book can close its observation interval without being a broken stream.
`prepared` is stronger than `comparable_open`: it requires metadata and book
usability, while the latter is only recent callback observation. Ranking changes
alone do not evict valid active or prepared markets.

Retain these runtime outputs with recordings:

- `matched_sports_capture.jsonl`: generation, admissions, retirements, discovery
  results, preparation/readiness/timeout/promotion transitions, 24h volume/book
  summaries and observation transitions;
- `metadata/capture_quality.jsonl`: admission/persistence errors and run status;
- Native continuity metadata and instrument definitions: reconstruction context.

`comparable_open` only means all three configured books have been observed
recently, with an initial snapshot requirement. It does not certify nonempty,
noncrossed, executable books, upstream losslessness or venue clock alignment.

Offline checks require only fixed fixtures and temporary catalogs:

```bash
./scripts/cargo.sh test -p catalog-capture-cli
./scripts/cargo.sh test -p catalog-capture-runtime-adapter matched_sports
./scripts/cargo.sh test -p nautilus-predict http::sports
```

For a recorded PM instrument, use the existing `validate-order-book` command.
No historical recording, research snapshot or analysis export is required by
the matching tests.
