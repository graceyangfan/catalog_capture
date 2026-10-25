# Predict MARKET contract and evidence

The [architecture](../predict-execution-adapter-design.md#market-strategy-input)
defines supported Native orders and parameters. This reference records SDK
arithmetic, actual venue observations and the limits of replay. Historical
captures are immutable; later fixes are tested against them, not written into them.

## SDK contract

Comparison uses official TS SDK 1.3.8, commit
[5ff2b4a](https://github.com/PredictDotFun/sdk/tree/5ff2b4a1c54cbbeb0fd661e17246cd12a9af8486),
and the recorded Python SDK 0.0.22 probes. Amount vectors and provenance are under
[fixtures](../../crates/nautilus-predict/tests/fixtures/).

| Input or fact | Meaning |
| --- | --- |
| Native MARKET GTC / FOK | Omit isFillOrKill / send true; LIMIT FOK and IOC unsupported |
| market_price | Strategy reference, before per-order extra slippage; not a Native limit-price field |
| params[market_order_slippage_bps] | Sole slippage input; omitted means SDK zero, no client fallback |
| Quantity mode | Signed amounts prepared once; isMinAmountOut=false |
| Native quote_quantity BUY | Maximum USDT principal; zero resolved extra slippage; floor derived shares |
| MARKET signed expiration | SDK now + five minutes; not a matching or recovery timeout |
| HTTP create receipt | Transport acknowledgement; not proof of Accepted, Filled or complete recovery |
| REST amountFilled | Zero/nonzero recovery signal; do not infer universal share units |
| Wallet Success / confirmed matches | Actual execution quantity, price, liquidity and fee evidence |

`isMinAmountOut` is not a Native strategy parameter or a planned requirement of
this adapter. The current HTTP request always sends false. Both camel-case and
snake-case params are rejected, even when their value is false/null, so a strategy
cannot silently select a different amount model. Quote-budget BUY uses Native
quote_quantity and the same default signed model. Historical SDK minimum-out
captures are comparison evidence only, not supported Native submission examples.

## Fields that must not be interpreted interchangeably

| Field / observation | Actual meaning in the supported path | Dangerous interpretation |
| --- | --- | --- |
| Native market_price | Selected outcome's reference before extra slippage; caller supplies it, no automatic book fetch | Current executable ask, or a separate Native limit-price field |
| HTTP pricePerShare | LIMIT exact quote; MARKET SDK-derived reference/VWAP, potentially different from worst-tier signed ratio | A universal identical price bound across LIMIT and MARKET |
| makerAmount / takerAmount | BUY: USDT offered / gross shares; SELL: gross shares offered / USDT received | Maker/taker liquidity role, or amounts with the same asset on both sides |
| market_order_slippage_bps | Applied once while preparing signed MARKET amounts, then forwarded as corresponding HTTP metadata | A second independent allowance to apply again, a fee rate, or a fresh price lookup |
| Native quantity | Base shares, or maximum USDT principal for quote_quantity BUY before Native conversion | Net inventory after share fees, or always identical to an SDK minimum-out amount |
| YES/NO reference | Price of the order's own instrument; a NO ask is complemented from the YES bid | Reusing the YES ask for a NO BUY or complementing a Native NO quote twice |
| Accepted | Exchange lifecycle acknowledgement; matching/settlement may still fail | Proof of a fill or a cue for adapter-owned cancellation |
| noMarketMatch | Rejection reason; does not identify which price/depth/amount condition failed | Definitive proof of malformed parameters, market closure, wrong IDs, or partial-tail cancellation |
| REST amountFilled | Endpoint cumulative signal; exact executions come from settlements/matches | Universally gross shares, net shares or USDT without unit evidence |

For the current single-reference default BUY of Q shares, signed collateral is
floor(p * Q * (1 + bps/10000)), capped at Q collateral; signed taker amount stays Q.
SELL signs Q shares and floor(p * Q * (1 - bps/10000)) collateral. Calculations use
integer wei at each specified SDK step. The signed ratio establishes economic
terms; zero bps does not make a stale p current, guarantee liquidity or include fees.
For example, the retained Native BUY reference .39 with 300 bps signed a .4017
ratio and actually filled at .40. Those are compatible values, not two competing
execution prices. This does not establish universal per-match enforcement for
every unsupported mode.

SDK depth helpers can reduce the quantity before signing when their input book
is too small. That is pre-order sizing, not an exchange partial fill. Current
Native submission preserves the explicit base quantity instead of silently doing
that reduction. A MARKET reference far below available BUY prices can therefore
be structurally valid and still receive noMarketMatch.

The SDK `getMarketOrderAmounts(book, ...)` calculates from its argument; it does
not fetch a book. Predict's command entry uses an explicit strategy reference and
performs no orderbook query. This single-reference policy is not a claim that
multi-level SDK sizing and its VWAP always produce identical signed terms.

For 100 BUY shares with asks 40 at .01 and 60 at .02, the pinned SDK returns:

| Mode | Extra bps | pricePerShare | makerAmount | takerAmount | target amount |
| --- | ---: | ---: | ---: | ---: | ---: |
| Default | 0 | .016 | 2 | 100 | 100 |
| Default | 100 | .016 | 2.02 | 100 | 100 |
| Minimum-out | 0 | .016 | 1.6 | 80 | 100 |
| Minimum-out | 100 | .016 | 1.6 | 79.2 | 100 |

Target shares and signed minimum-out are distinct. The value helper alone is not
a hard budget order: later quantity/slippage calculations can increase signed
collateral. Adapter quote sizing deliberately restricts the supported mapping;
minimum-out and multi-tier budget semantics are not silently approximated.

## Recorded venue observations

All filenames below are under
[private_api fixtures](../../crates/nautilus-predict/tests/fixtures/private_api/).
Sanitized public identities may be rebound; source metadata remains authoritative.

| Fixture | Observed behavior | Boundary |
| --- | --- | --- |
| market_fok_no_match_20261024.json | Signed MARKET FOK BUY: 201, Accepted, noMarketMatch, final REST CANCELLED and zero fills | Zero-match termination; no successful all-or-none or SELL guarantee |
| market_parameter_matrix_no_match_20261024.json | FOK true, omitted and false each returned 201, wallet Accepted then noMarketMatch, final REST CANCELLED and zero filled | Zero-match cross-transport lifecycle; not partial-tail, positive execution or IOC semantics |
| native_market_no_match_20261024.json | Actual Native Strategy/Risk/Execution through crate; zero-match outcomes | Confirms integrated no-match path only |
| native_market_fill_post_only_rejected_20261025.json | Native MARKET BUY 3.5 at .40 from reference .39; fee .07 SHARES; net 3.43; post-only TP rejected | Subsequent SELL was a separate SDK fallback, not a completed Native automatic cycle |
| native_non_fok_full_plan_two_settlements_20261025.json | GTC with FOK omitted: full 5.83-share match plan, settlements 5 and .83 at .24; fee .1166; net 5.7134 | Two settlements are a full plan, not an unmatched partial tail |
| native_market_filled_atomic_remainder_20261025.json | Venue FILLED; gross 7.649999999999999999; fee .152999999999999999; net 7.497; one-wei planned remainder canceled | Original Native run retained PendingCancel; later offline repair is separate evidence |
| live_unexecutable_rejections_20261025.json | Ten HTTP 400 responses for expired or all-zero-signature LIMIT/MARKET requests, including identical repeats; no wallet events and hash lookups 404 | Gateway validation only; no valid signed order, partial fill or active-original duplicate conclusion |
| live_crate_signed_parameter_rejections_20261025.json | Crate signer/DTO/private HTTP: LIMIT FOK and MARKET post-only explicitly rejected; LIMIT slippage and MARKET 10000-bps targets masked by expiry; all four hashes 404, no matching wallet event | Signed expiry fixed to 1; test-only outer-option injection, no Native strategy live submission or live matching claim |
| live_market_history_readonly_20261025.json | Own-crate GETs: six prior no-match MARKET orders remain CANCELLED/zero; two BUYs and one SELL remain FILLED; exact one-wei shortfall retained | Later historical snapshots, not original wallet/HTTP ordering or a global amount-unit specification; no trading writes |
| live_chain_atomic_rounding_20261025.json | Two public successful receipts independently decode to the same gross fills/fees; counterparty signed orders reproduce the one-wei split-rounding difference; chain status differs from CLOB status | Public execution evidence, not unexecuted order visibility, original HTTP options or complete counterparty history |
| live_order_lifecycle_survey_20261025.json | Own-crate 45 read-only requests: real maker/taker pages, 24 hash lookups, one canceled LIMIT and two FILLED orders short by one wei; a LIMIT SELL has three maker executions summing to its full quantity | Historical snapshots, not original wallet timing or proof of partial-tail cancellation; bounded pages explicitly report truncation |

The full-plan capture includes complementary NO maker bids at .76 and public
pair-mint transfers for 5 and .83 shares. The separate SDK exit sold 5.7134 at .21;
net proceeds 1.17581772 minus entry 1.3992 give cash change -.22338228 USDT.
Displayed best-ask size did not force an unmatched tail. These facts do not establish
hidden liquidity, a universal fill policy or a profitability result.

In the atomic-remainder incident, removal was noop and Native stayed PendingCancel.
Mapping FILLED to Canceled would change venue truth. The repair uses complete raw
settlements to normalize only Native working quantity via reconciliation; original
signed terms, actual fills, fees and inventory remain intact. Complete-history
proof is required for that normalization, not for delivering a known true fill.
No successful Native TP/SELL or final payout is inferred for that original run.

A wallet noMarketMatch rejection followed by REST CANCELLED is preserved as
Rejected if Native already applied that fact, for both MARKET GTC and FOK.
The cross-transport terminal equivalence does not depend on all-or-none execution.
With the wallet message missing,
recovery uses the actual Canceled snapshot and invents no rejection reason.

The retained parameter matrix already contains this zero-fill sequence for
`baseline_fok`, `fok_omitted` and `fok_false`. Omission matches the adapter's
MARKET GTC request convention. This supplies venue evidence beyond the original
FOK-only capture for the terminal equivalence; another funded no-match probe is
unnecessary to establish that point. These were accepted requests without an
executable match, not malformed signatures or identities. They do not establish
how a partially executed remainder terminates.

## Who terminated a no-match order

The eight-case SDK matrix has openBeforeCleanup=0 and no run order to remove;
its Accepted -> noMarketMatch -> REST CANCELLED sequence supplies independent
zero-match termination evidence. The old Native no-match probe also contains
StrategyOrderAccepted -> NativeCancelRequested because that test strategy cancels
inside on_order_accepted. Its final CANCELLED alone cannot prove who terminated
the order. Exchange event timestamps and local callback timestamps use different
clocks; their apparent order is not sufficient causal proof.

The production adapter does not issue a remove POST merely because it sees
Accepted or noMarketMatch. Known zero-fill noMatch with no pending settlement
maps to OrderRejected and is already closed; do not add a strategy cancel just to
close it again. If only a REST CANCELLED observation is available, map that venue
fact to Canceled without inventing the missing rejection reason. If trades or
pending settlements exist, noMatch alone does not establish the remainder's
terminal status: retain executions and request independent reconciliation.

The six recorded default-model cases now replay through actual Native submission,
wallet parsing, subscriber callbacks and REST recovery. Signed amounts/reference
are checked against the capture; omitted/false default flags are canonicalized.
Original wallet events are combined with later real historical REST responses;
only loopback identities and signing fields are rebound. Callbacks see
Accepted then Rejected in Cache, and zero removal POSTs throughout. The two SDK
minimum-out cases remain parser comparisons and never become Native admission
tests. This derived replay establishes integration behavior, not a new live run
or an unmatched partially filled tail.

The later read-only capture queried nine already-known hashes through the crate.
It contains twelve relayed requests: auth challenge, authentication, account
identity and nine order GETs; creation, cancellation and chain writes are blocked.
The official SDK and crate both reproduced all nine original hashes before
identity redaction. The six default-model no-match snapshots are now used directly
in the Native replay instead of reconstructing their REST wrappers.

All nine observed MARKET amount fields equal gross share wei: BUY uses signed
takerAmount and SELL uses signed makerAmount. The six canceled orders have zero
amountFilled; the two BUY and one SELL terminals use actual gross cumulative
shares. This supports those concrete default-model samples, not other order
types/modes or a schema guarantee. Recovery therefore still does not convert an
arbitrary amountFilled into a fill. The atomic BUY still reports FILLED with
amount=7650000000000000000 and amountFilled=7649999999999999999. A remainder
in the plan must not veto that terminal or manufacture an extra execution.

## Public chain study of the atomic remainder

Read-only RPC collected the two original settlements in block 129677447, their
public inputs/receipts and current getOrderStatus results. The exchange log
address is the official non-yield CTFExchange
0x8BC070BEdAB741406F4B1Eb65A72bee27894B689. Decoding uses official Python SDK
0.0.22 ABI; Rust independently decodes the public Order prefix and OrderFilled
logs and reproduces every order hash. The transaction destination is a router,
not the exchange. Only the first two argument slots are decoded: the unknown
router selector and trailing authorization/fee arguments are not asserted.

Both counterparty signed BUYs quote .74 for the complementary outcome; the
Native BUY quotes .26. Their observed collateral fill amounts are
1697400615384615383 and 3963599384615384617 wei. For each counterparty, the
signed shares/collateral ratio is 50/37. Integer division gives:

| Per-match conversion | Share wei | Discarded fractional share wei |
| --- | ---: | ---: |
| floor(1697400615384615383 * 50 / 37) | 2293784615384615382 | 16/37 |
| floor(3963599384615384617 * 50 / 37) | 5356215384615384617 | 21/37 |
| Sum of individual conversions | 7649999999999999999 | 1 |
| Convert combined collateral once | 7650000000000000000 | 0 |

These exact integer conversions reproduce the recorded gross fills; the match
plan already contained the one-wei residual before settlement. Consequently the
shortfall did not originate in Native precision conversion or share-fee mapping.
This is an arithmetic reproduction from input and log facts, not a claim about
which unverified backend/router function chose the split. Increasing signed
quantity/collateral to compensate would change intent and would not guarantee
that a different split has no residual.

The taker's chain status currently reports isFilledOrCancelled=false,
remaining=1, while the independent CLOB observation is FILLED. On these BUYs,
chain remaining is the maker asset (USDT), not gross share remaining. This is
particularly clear for the first counterparty: 17.76 signed USDT minus the
observed collateral fill equals chain remaining 16.062599384615384617 USDT;
the remaining signed share quantity is different. The taker's one collateral
wei and one share wei are numerically equal here, but are different units.
Never use this chain flag/remaining to reopen a CLOB terminal or override its
FILLED status. The existing Native repair retains original signed quantity,
exact executions/fees and normalizes only terminal working quantity.

The public counterparties supply real partial-consumption research examples:
their original signed quantities are 24 and 30 shares, with this match consuming
2.293784615384615382 and 5.356215384615384617 shares. The first still has the
corresponding nonzero chain remainder at the later observation. The second's
chain flag is true with zero remaining; that combined filled-or-cancelled flag
alone does not distinguish later completion from on-chain cancellation.
Neither flag proves whether an order was active in the off-chain book. HTTP
strategy, FOK/post-only/slippage flags are absent from these signed inputs.

OrderFilled.fee also requires care. Both counterparties' protocol fee events
are nonzero, but a later ERC-1155 transfer from the router returns that exact
quantity in the same receipt. Their net received shares equal gross fills.
The taker receives gross minus the actual fee, matching wallet evidence and
net inventory 7.497. Do not import a protocol fee before accounting for refunds,
or sum the taker OrderFilled and OrdersMatched summary as two executions.
The three independent Rust chain regressions cover these boundaries.

Historical-state eth_call and both bounded log-range queries were rejected by
the configured RPC provider. Those failures are retained; no complete order
history is inferred. Public-chain evidence can supplement executed orders,
on-chain cancellations and amounts. It cannot reveal a never-executed signed
CLOB order, REST removal/no-match reasons, or original parameter options.

## Valid signatures and unsuitable execution parameters

Valid signing, valid API parameters and an executable quote are separate checks.
A correctly signed order with an uncompetitive price is not necessarily malformed.
The retained BNB matrix used an OPEN market, observed ask .84 and BUY references
.01/.02. All eight requests returned 201, wallet Accepted then noMarketMatch,
REST CANCELLED, zero filled and no OPEN order before cleanup. The cases include
FOK true/false/omitted, minimum-out at zero/100 bps, omitted slippage/minimum-out
flags and two price/quantity pairs. This is actual valid-signature evidence;
it does not establish a price-distance rejection threshold.

In the zero-slippage default cases, reference price and signed collateral/share
ratio moved together. The recording therefore cannot isolate which field the
matcher used. SDK arithmetic also permits MARKET VWAP to differ from the signed
worst-tier ratio. Do not impose LIMIT's exact price/amount equality on MARKET.
For the minimum-out/100-bps case, signed shares are 99 while the reported target
is 100; setting the flag changes how the backend derives target size. Native
minimum-out admission remains unsupported.

The next useful valid-signature comparisons, if separately authorized for a
venue environment that can guarantee no economic execution, are:

| Priority | Parameter comparison | Documented error vocabulary / question | Current evidence |
| --- | --- | --- | --- |
| 1 | LIMIT pricePerShare versus exact signed BUY/SELL amounts | create_order_limit_amounts_mismatch | Local builder rejects mismatch; no recorded venue response |
| 1 | MARKET price/slippage metadata versus SDK-prepared default-model signed amounts | create_order_market_amounts_mismatch, create_order_invalid_slippage_bps | Six supported-mode no-match cases exist; inconsistent metadata not observed; minimum-out is not a Native capture target |
| 2 | Order strategy versus FOK, post-only and slippage options | create_order_fill_or_kill_not_supported, create_order_post_only_not_supported, create_order_slippage_non_market | Actual expired-signed controls establish LIMIT FOK and MARKET post-only rejection; LIMIT slippage remains masked by expiry |
| 2 | Value, share-size and price-precision boundaries | create_order_min_order_value_not_met, create_order_min_order_size_not_met, create_order_price_precision_too_high | SDK arithmetic limits and market metadata are not universal API acceptance guarantees |
| 3 | Correctly bound outcome token in a non-trading/market-order-disabled market | create_order_market_not_trading, create_order_market_orders_disabled | Capture trading status and token/domain provenance; no arbitrary token substitution |

These are capture candidates, not already observed responses or guaranteed
no-fill orders. The official ErrorCode schema names failures but does not define
their validation order. Changing an unsigned option does not revoke a valid
economic signature; an ignored option or moved book can still permit execution.
A distant LIMIT can rest and fill later. A distant MARKET can match after a book
change. Immediate cancellation after acceptance cannot guarantee zero execution.
An expired signed control only establishes expiry rejection when that check masks
the parameter under investigation. Accordingly, under the operator's current
strict zero-fill constraint, use existing valid-order recordings and offline
admission/response replay; do not send these executable candidates to mainnet.

On 2026-10-25, an authorized expired-only probe used the crate's own signer,
request DTOs, authenticated HTTP transport and wallet client on OPEN ETH market
52261. The signature covered expiration=1; production DTO admission was not
weakened. An ignored, test-only module changed outer request options and called
the same private secret-body POST function, with no exported bypass API. Thus
this is crate HTTP/signing evidence, not a full Native strategy run. It sent:

| Case | Actual HTTP 400 error | Interpretation |
| --- | --- | --- |
| LIMIT + isFillOrKill=true | create_order_fill_or_kill_not_supported | Explicit MARKET-only option rejection |
| MARKET + isPostOnly=true | create_order_post_only_not_supported | Explicit LIMIT-only option rejection |
| LIMIT + slippageBps=100 | create_order_expiry_too_soon | Target slippage rule not reached/proven |
| MARKET + slippageBps=10000 | create_order_expiry_too_soon | Target slippage range not reached/proven |

After two consecutive expiry-masked targets, the probe stopped. Amount-mismatch
cases were not transmitted. Four order-hash lookups returned 404; no corresponding
parsed wallet event was observed. HTTP JSON responses, the sanitized signed-term
projection, market/book and historical crate diagnostics are retained unchanged.
Wallet collection is a finite parsed diagnostic projection, not a raw-frame
recording. Signing locally and matching SDK vectors does not prove that the
venue reached cryptographic validation after the earlier parameter/expiry error.

The observed MARKET post-only response exposed a missing typed classification:
the old crate reported Unknown/Unknown. It now reports InvalidRequest with
PostOnlyUnsupported. This is distinct from PostOnlyWouldCross or a wallet
rejectedPostOnly event. Two offline regressions fail on the old mapping: private
HTTP replay and subscriber-visible Native rejection. The latter checks that Cache
is already Rejected with zero fills inside the callback, due_post_only is false,
and no acceptance, cancellation, fill or automatic POST retry is fabricated.

For any future authorized capture, vary one parameter at a time and preserve
the exact request, SDK amounts, matching outcome book and trading status, HTTP
body, ordered wallet messages, per-order REST status/matches and scoped removal
result. Distinguish automatic noMatch termination from a caller's removal POST.
A 201 followed by removed, without observing matching, proves neither fillability
nor a parameter fault. Keep raw primary recordings immutable; label reordered
or identity-rebound replays as derived.

## Offline verification

```bash
cargo test -p nautilus-predict --features execution --test market_recorded_fill --locked
cargo test -p nautilus-predict --features execution --test execution terminal_recovery:: --locked
cargo test -p nautilus-predict --features execution --test execution market_fok:: --locked
cargo test -p nautilus-predict --features execution --test execution market_value:: --locked
```

`market_recorded_fill` parses real captured lifecycle/economic fields and applies
Native order/position accounting. `terminal_recovery` rebinds identity/signing to
loopback REST and applies Native reconciliation, testing base/quote sizing,
PendingCancel/PendingUpdate, fill-first/report-first, cold replay, exact fees and
incomplete history. These are derived offline executions, not new live orders.
The broader [validation matrix](predict-execution-validation.md) covers actual
Strategy/LiveNode callbacks and independence from recovery IO.
The recorded valid-signature parameter-matrix test covers all eight actual
no-match sequences through the wallet report parser, preserving the rejection
reason, TIF, requested quantity and zero confirmed fills. It restores only omitted
envelope identity and does not claim Native minimum-out submission support.

## Remaining venue evidence

### Bounded historical survey

The new survey records actual responses through the crate's private HTTP client;
the relay blocks creation, removal and chain writes. All 45 requests returned
200: authentication/account (3), match pages (6), order-list pages (4), by-hash
lookups (24), and market metadata (8). Maker history ended after nine rows;
unfiltered/taker history reached the two-page cap. Twenty-six distinct order
hashes were discovered; only the first 24 were looked up. This is not a complete
account-history claim. Default and OPEN order-list queries were empty at this
observation; the FILLED filter returned 16 LIMIT orders over two pages.

A real LIMIT SELL of 4.1748 shares has three distinct maker settlements at .50:
1.51999, .89 and 1.76481 shares, each with zero COLLATERAL commission. The final
REST state is FILLED. Two enclosing match events have aggregate amountFilled=5
shares, which must not be substituted for this maker's smaller execution leg.
Across maker queries, four BUY legs report zero SHARES fees and five SELL legs
report zero COLLATERAL fees. Native replay preserves the own leg, exact amounts,
liquidity role and fee, then the order model naturally progresses through
PartiallyFilled, PartiallyFilled, Filled. Overlapping pages are deliberately
introduced only in the offline replay to check that three executions stay three;
that scheduling is not an observed live duplicate.

The survey also preserves a zero-fill CANCELLED LIMIT and two MARKET BUYs whose
FILLED cumulative amounts are one wei below their requests (7.65 and 33.399).
Later signed expiration or current market CLOSED metadata must not rewrite those
existing terminal facts. No meaningful unmatched remainder, canceled partial
LIMIT, EXPIRED/INVALIDATED terminal, rejectedDuplicate, failed settlement or
partial-fill noMarketMatch sequence was established by this survey. There is no
new production mapping change based on these missing observations.

### Highest-value capture gaps

| Priority | Missing situation | What a useful recording must distinguish | Current evidence / action |
| --- | --- | --- | --- |
| 1 | Supported MARKET reference/slippage boundary against changing depth | Selected-outcome reference, signed ratio and actual per-match prices; coherent but uncompetitive quote versus inconsistent metadata | Existing .80/.84 no-match and .39 +300-bps/.40 fill are different observations, not a controlled threshold experiment; no new executable order under strict zero-fill |
| 1 | Real partial match with economically meaningful remainder | plannedFill, remainderCancelled, Submitted/Success, true terminal, and whether any remove POST preceded it | Full plans and one-wei residual exist; neither establishes general non-FOK partial-tail behavior |
| 1 | noMarketMatch after a partial fill or a match plan but before settlement completion | Confirmed fills versus display quantityFilled=0; original-order disposition versus remainder rejection | No original JSON recording of this sequence; derived schedules do not prove it occurs; retain independent terminal evidence |
| 2 | rejectedDuplicate for a valid original | Rejected attempt versus original order; same full hash/terms and independent original status | No original recorded duplicate feedback; expired/invalid repeats do not test this; do not infer original closure |
| 2 | TransactionFailed and any subsequent retry | Settlement identity, real on-chain outcome, later Success under same/new ID, and final order state | No original failed-settlement event found; observe naturally rather than deliberately causing a funded failed settlement |
| 2 | Natural GTD/market end and removal lock rejection | Order expiry versus instrument close versus caller cancel; removed/noop/rejected and positive lock deadline | Cancellation evidence exists; no original wallet orderExpired or positive-lock rejection found; historical/read-only supplementation first |
| 3 | STP-triggered cancellation | Policy in the request, both owned order hashes, reason and actual fill/terminal consequences | STP option acceptance is recorded; actual STP enforcement is not; do not manufacture a self-match |

The inspected registered primary recordings contain only zero-display noMatch
rejections, and searches of the original operational JSON found no
rejectedDuplicate, orderTransactionFailed or orderExpired envelope. This is an
evidence gap, not proof that the exchange never emits them. The inspected
Subscription Topics documentation lists those types but not orderMarketMatchPlan.
The latter must remain a recorded observation, not a complete documented terminal
contract. In original retained partial-probe wallet files, the three distinct
plans satisfy plannedFill + remainderCancelled = amount; the residuals are zero,
zero and one wei. Do not extrapolate this to economically partial execution.

Read-only historical GETs can supplement final states and signed terms. They
cannot recover an absent wallet event: wallet subscriptions have no snapshot.
For a later naturally observed case, keep raw wallet JSON, HTTP replies, Native
events and every create/remove transmission intent in one collector sequence
with receive-monotonic timestamps. Venue timestamps alone cannot establish
whether the strategy canceled first. Include selected instrument/token, YES/NO
book transformation, reference age, quantity mode, signed amounts, fee mode and
settlement IDs. This is evidence collection, not a new strategy parameter set.

Do not submit another expired control simply to obtain the same front-door
error. A valid distant LIMIT/MARKET is not guaranteed zero-fill, so the remaining
matching/duplicate/expiry scenarios are not automatically authorized as live
tests by a strict zero-fill request. isMinAmountOut remains an internal false
constant, not a proposed feature or capture target.

Successful FOK all-or-none enforcement, SELL FOK parity, genuinely unmatched
partial-tail lifecycle and universal per-match price enforcement are not proven
by zero-match or full-plan captures. Multiple settlements alone do not demonstrate
a partial unmatched order. IOC/FAK and minimum-out remain unsupported.

Live probe scripts under root tests/ are operator tools with explicit submission
controls. They are not part of the ordinary offline commands above. Inspect saved
JSON instead of submitting additional orders to review a historical result.

Primary references: [TS amount builder](https://github.com/PredictDotFun/sdk/blob/5ff2b4a1c54cbbeb0fd661e17246cd12a9af8486/src/OrderBuilder.ts),
[TS types](https://github.com/PredictDotFun/sdk/blob/5ff2b4a1c54cbbeb0fd661e17246cd12a9af8486/src/Types.ts),
[CreateOrderData](https://dev.predict.fun/createorderdata-14037465d0.md),
[ErrorCode](https://dev.predict.fun/errorcode-14037470d0.md).
