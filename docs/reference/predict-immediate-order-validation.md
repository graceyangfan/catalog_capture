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
| live_native_zero_fill_20261025/market/ | Native FOK/GTC: Accepted, strategy cancel intent, wallet noMarketMatch, Native Rejected; one remove returned noop; REST CANCELLED with zero filled | No successful MARKET removal or matching; supervisor performs explicit SDK/book checks |
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

## Who terminated a no-match order

The SDK matrix has openBeforeCleanup=0 and no run order to remove. Its Accepted ->
noMarketMatch -> REST CANCELLED sequence independently establishes zero-match
termination. The Native probe also contains a cancel intent from its acceptance
callback, so its final CANCELLED snapshot alone cannot identify the terminating
actor. Venue and callback timestamps use different clocks.

The adapter maps known zero-fill noMatch without pending settlements to
OrderRejected. Accepted/noMatch does not itself issue a remove POST. A REST-only
CANCELLED observation maps to Canceled without inventing a rejection reason.
When executions or pending settlements exist, noMatch alone is insufficient to
establish the original order's terminal state.

Six default-model matrix cases replay through Native submission, wallet parsing,
subscriber callbacks and REST recovery. Economic terms and original wallet facts
are retained; identities and signing fields are rebound to loopback services.
Callbacks observe Accepted then Rejected with zero removal POSTs. The two SDK
minimum-out cases cover parser behavior only.

The read-only history fixture contains nine known MARKET hashes: six zero-filled
CANCELLED orders and three FILLED orders. Their amount fields equal gross share
wei in these samples, using BUY takerAmount and SELL makerAmount. This does not
establish universal units. In the atomic BUY, amount=7650000000000000000 and
amountFilled=7649999999999999999 coexist with FILLED; the difference cannot
manufacture another execution or veto the terminal observation.

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

The ErrorCode schema names amount, precision, trading-status and strategy-option
errors, but does not specify validation order. Local SDK/builder checks and
zero-match observations do not prove venue rejection of an inconsistent request.

The signed expired controls in live_crate_signed_parameter_rejections_20261025.json
use the crate's signer, HTTP DTOs and wallet client. A test-only outer-option
injection preserves signed expiration=1; it does not expose a production bypass.
The observed responses are:

| Case | HTTP 400 error | Evidence limit |
| --- | --- | --- |
| LIMIT + isFillOrKill=true | create_order_fill_or_kill_not_supported | Explicit strategy-option rejection |
| MARKET + isPostOnly=true | create_order_post_only_not_supported | Explicit strategy-option rejection |
| LIMIT + slippageBps=100 | create_order_expiry_too_soon | Slippage validation was masked |
| MARKET + slippageBps=10000 | create_order_expiry_too_soon | Slippage validation was masked |

The four hash lookups returned 404 and no corresponding parsed wallet event was
observed. Amount-mismatch requests were not transmitted. The wallet diagnostic
is a finite parsed projection, not a raw-frame recording. These controls establish
HTTP/signing behavior, not Native strategy matching or validation precedence for
unexpired orders.

MARKET post-only maps to InvalidRequest/PostOnlyUnsupported, distinct from
PostOnlyWouldCross and wallet rejectedPostOnly. HTTP replay and Native subscriber
checks assert a Rejected Cache state with zero fills and due_post_only=false;
they produce no acceptance, cancellation or automatic POST retry.

A distant price, immediate cancellation or an unsigned option cannot guarantee
zero economic execution. Live probes require the explicit scope and controls in
the [operator guide](../how_to/predict-rust-strategy-calibration.md#supervised-funded-runs).
Use the retained recordings for ordinary offline validation.

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

live_order_lifecycle_survey_20261025.json contains bounded maker/taker and order
pages, 24 hash lookups and market metadata. The taker/unfiltered queries reached
their page caps; the recording is not complete account history.

One LIMIT SELL of 4.1748 shares has three distinct maker settlements at .50:
1.51999, .89 and 1.76481, each with zero COLLATERAL commission. Enclosing match
amountFilled=5 is an aggregate and cannot replace this order's smaller execution
leg. Derived replay preserves the own leg, liquidity role and fee, progressing
through PartiallyFilled, PartiallyFilled and Filled. Synthetic page overlap tests
keep these three executions distinct.

The survey also retains a zero-fill CANCELLED LIMIT and two FILLED MARKET BUYs
short by one wei. A later signed expiry or CLOSED market does not rewrite those
terminal facts. The original captures remain separate from replay schedules.

### Coverage gaps

| Situation | Established evidence | Unproven boundary |
| --- | --- | --- |
| Reference/slippage versus depth | Separate no-match and positive-fill observations | Controlled price-distance thresholds and universal per-match enforcement |
| Economically meaningful partial tail | Full plans and one-wei residual | General non-FOK unmatched-remainder lifecycle |
| noMarketMatch with executions or pending settlement | Synthetic ordering controls | Original wallet sequence and disposition of the original order |
| Duplicate of a valid original | Invalid/expired repeat controls | Real rejectedDuplicate behavior and original-order status |
| Failed settlement followed by retry | Synthetic settlement schedules | Original TransactionFailed and later Success identities |
| Natural expiry and positive removal lock | Explicit cancellation captures | Natural GTD/market-end termination and lock rejection |
| STP enforcement | Submitted-option acceptance | Actual cancellation/matching consequences |
| FOK execution | Zero-match and one full-fill example | General all-or-none and SELL parity |

These gaps do not prove that the exchange lacks the listed behavior. Multiple
settlements do not establish an unmatched partial order. IOC/FAK and minimum-out
remain unsupported Native mappings.

Historical GETs can establish final states and signed terms, but cannot recover
missing wallet events because wallet subscriptions have no snapshot. Causal
analysis requires raw wallet facts, HTTP responses, Native events and create/remove
transmission intents with receive-monotonic timestamps. Venue timestamps alone
cannot establish whether the strategy canceled first. Collection procedures and
submission controls belong to the operator guide; ordinary tests use saved data.

Primary references: [TS amount builder](https://github.com/PredictDotFun/sdk/blob/5ff2b4a1c54cbbeb0fd661e17246cd12a9af8486/src/OrderBuilder.ts),
[TS types](https://github.com/PredictDotFun/sdk/blob/5ff2b4a1c54cbbeb0fd661e17246cd12a9af8486/src/Types.ts),
[CreateOrderData](https://dev.predict.fun/createorderdata-14037465d0.md),
[ErrorCode](https://dev.predict.fun/errorcode-14037470d0.md).
