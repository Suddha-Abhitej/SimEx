# 0014. Order model, per-account balances, and the fill model

- **Status:** Accepted. **Amends ADR 0005** (see "ADR 0005, amended" below).
- **Date:** 2026-10-19

## Context

Week 2 builds the exchange core: an order model (#7) and a fill simulator against the live Gemini
book (#8). The fill model is the project's largest risk: if it is too generous, every score is
meaningless (PLAN). Three facts from the live data shape it:

- **Gemini trades only 4 to 11 times a minute per pair** (ADR 0011). The plan's "resting limit
  orders fill once live trades clear the volume ahead" would almost never fill a resting order.
- **Gemini's API has no market order type**; its "market" orders are aggressive IOC limits.
- **Fees are high.** Gemini's API / ActiveTrader entry tier (30-day volume >= $0) is **60 bps
  maker, 120 bps taker**, schedule dated **2026-09-01**,
  <https://www.gemini.com/fees/api-fee-schedule>. A personal account pays exactly this.

**Headline finding: a round trip costs 1.8 to 2.4%** (maker plus taker, or taker twice). Fees
will dominate most strategies. **Consequence for the evaluator (not built here):** results must
always report gross P&L, fee drag and net P&L separately. Net alone would make nearly every agent
look equally bad, and the benchmark would teach nothing.

## Decision

### Orders (#7)

- **Types.** Limit, good till cancelled: the part that crosses fills at once as taker and the rest
  rests as maker. Market: IOC inside a per-pair price band (default 50 bps from the best opposite
  price when it takes effect), never rests; what doesn't fill inside the band expires. Cancel.
- **Idempotency.** `(account_id, client_order_id)` names one order forever. The same key with the
  same request returns that order as it is now, accepted or rejected; the same key with a
  different request is refused (`idempotency_key_reused`). A malformed request is refused without
  being recorded. Order IDs derive from the key, so a replay gives the same IDs.
- **States.** PENDING (accepted, waiting out the anti-cheat delay) to OPEN, PARTIALLY_FILLED,
  FILLED, CANCELLED or EXPIRED; or REJECTED. Terminal states are final and the filled quantity
  only grows.
- **Cancels go through the same delay as orders.** An instant cancel would be a free option:
  place, watch a faster feed, cancel just before an adverse fill. Fills before the cancel takes
  effect stand.
- **Decimal, strings and grids (ADR 0004).** Quantities and prices arrive as strings (a JSON
  number has been through a float) and must sit exactly on the pair's price tick and size
  increment, at or above its minimum size. Off-grid orders are rejected, never rounded.
  `tools/gemini_symbols.py` checks the table against Gemini's `/v1/symbols/details`; the
  values were **confirmed live on 2026-10-18** for all three pairs. Note Gemini's naming: the
  price tick is `quote_increment`, while `tick_size` is the size increment.
- **Time is the market-data clock** (`received_at_ns`), not the wall clock: an order takes effect
  at arrival plus the delay, so a replay reproduces every fill.

### Balances: one writer per account (an exception to one writer per pair)

Books and matching have one writer per pair. Cash can't: one USD balance pays for BTC, ETH and
SOL orders. So **balances are owned by a per-account writer**, and this is recorded in CLAUDE.md's
hard rules so a later change doesn't fold it into the pair engines. The account writer accepts
orders and cancels, reserves funds, and settles the fills the pair engines report; pair engines
never touch balances.

- **Reservations.** A buy holds the notional at its limit (a market buy: the band edge, rounded
  outward to the tick) plus the higher of the two fees on it, rounded up. A sell holds its
  quantity. A finished order releases what it still holds.
- **Fees** are charged in the quote currency on the order's running totals: each fill pays the
  increase in `ceil(maker notional x maker rate + taker notional x taker rate)`, rounded up to
  1e-8. Splitting an order into many fills never costs more than one fill would, and never more
  than was reserved. Not rounded to cents: a one-cent minimum would exceed the whole proceeds of a
  minimum-size sale (the property tests found exactly that).
- **Double entry.** Every movement has a counterparty: `_market` (the outside world the simulated
  fills trade with), `_fees`, and `_external` (where deposits come from). Each asset sums to zero
  over all accounts.
- The tier is configurable (`EXCHANGE_MAKER_FEE_BPS`, `EXCHANGE_TAKER_FEE_BPS`); it never changes
  with simulated volume in v1.

### The fill model (#8)

**Market orders and the crossing part of limits** walk the book as it stands when they take
effect, level by level, inside the band. If the book is thinner than the order, it fills what
the band holds and the rest expires; an empty opposite side rejects. How often the band binds is
counted per pair. The band keeps walks inside the top levels the audit proves exactly (ADR 0012).

**A depletion map** keeps simulated fills from eating the same real liquidity twice: "we consumed
X at price p" is subtracted from the live size. An entry ends when the venue sends a new size for
that level, when the next snapshot refresh arrives, or after a configurable age, whichever comes
first; expiries are counted. (Without the age, a quiet level could stay depleted forever.)

**Resting limit orders: the queue model.** Each order tracks `ahead`, the venue quantity in front
of it at its price: the live size when it starts resting (back of the queue), or 0 at a price that
improves on the best. It advances by:

- **(a) a print at its price:** the print's quantity clears `ahead` first, then fills the order;
  our own orders at a price queue among themselves by arrival;
- **(b) a trade-through:** a print beyond its price means the whole level traded, so it fills;
- **(c) the venue book crossing its price:** the real opposite side reaching an order that sits
  inside the spread would have traded with it on a real venue, so it fills as maker at its price,
  up to the crossing size, recorded in the depletion map;
- **(d) the level shrinking**, which is either a fill or a cancel.

**What we can't know, and how each case is treated.** A Gemini fill shows twice: a print on
`md.trades` and a smaller level on `md.level2`, arriving at different times. Shrinks go into a
per-level buffer; prints at that price claim them within a window W, and a print that arrives
first waits to claim the shrink that follows. The claimed part is a fill and already counted by
(a), so it doesn't advance the queue twice. The unclaimed part is a cancel, and **where in the
queue the cancelled order sat is unknowable.** It is treated proportionally,
`ahead -= cancelled x ahead / level size before`: the expected value if a cancel is equally
likely anywhere. Two bracket settings bound it: pessimistic (every cancel behind us; fills only
from prints) and optimistic (every cancel ahead). Growth at the price is orders joining behind us.

**W is measured, not guessed** (#8), by `tools/measure_fill_window.py` on the recordings. For
each pair it reports the time from each shrink to the print that explains it (an offline pass
pairing each print with the nearest shrinks at its price, before or after it), and, running the
engine's own matcher at candidate windows, how much print quantity is matched and how much shrink
quantity is left unmatched. W is the smallest candidate that matches at least 99% of what a 5 s
window matches; the result and the unmatched share at that W are in Measured below.

**How pessimistic: no maker fill without a print.** A cancel only moves an order forward; only
(a), (b) and (c) fill it. A cancel can bring an order to the front, but nothing fills until
someone trades at or through its price, or the real book crosses it. Simulated maker volume from
one print never exceeds the print. **Maker fills are counted by cause**, (a), (b) and (c)
separately. (c) is the only cause that fills without a print, so an invented fill would hide
there: if it ever dominates the mix, that is a red flag the report must show.

**After a resync** (30-minute refreshes, gaps) `ahead = min(ahead, new level size)`; rejoining the
back at every refresh would wipe every resting order twice an hour. Prints that arrive while the
book isn't SYNCED are ignored and **counted**, so the cost of that pessimism can be checked
against the ~0.5 s refresh windows.

**Not modelled in v1:** market impact (our orders never move the price, per the PLAN); our
liquidity beyond the depletion map; virtual orders matching each other (they only ever trade with
the real market, never at prices it didn't show); hidden or reserve liquidity (the honesty check
measures how often prints appear where no level was visible); auctions, halts, self-trade
prevention; volume-based fee tiers; venue rejections beyond grids, minimums and funds; latency
beyond the fixed delay.

### How we know the fill model is honest (the week-5 fill-accuracy report)

On recordings, through `simex.recorder.replay`:

**A. Taker accuracy, with real aggressive orders as probes.** Group real prints into the orders
that caused them (same exchange timestamp and aggressor side). Submit a simulated market order of
the same side and total size at the same moment; compare average prices in bps (positive: the
simulated fill was worse for the trader).

- At delay 0 (reconstruction): the walk on the book just before the trade must reproduce the real
  prints. Unacceptable: under 95% exact, or any simulated fill better than the real one.
- At 200 ms (realism): unacceptable if the signed mean is below -0.5 bp, |median| above 1 bp, p95
  above 5 bps on BTC and ETH or 10 bps on SOL, or the filled fraction below 99% where the real
  order filled.

**B. Maker realism, with passive probes.** Small simulated orders at the best bid and ask every N
minutes: fill probability within 10, 60 and 300 s, time to fill, and markouts (mid move 1, 10 and
60 s after a fill).

- Hard invariant: no maker fill without a backing print or real crossing, and no more maker
  volume per print than the print. Unacceptable: any violation.
- Fills by cause, (a), (b) and (c), with (c)'s share as an explicit check.
- The default fill rate must lie inside the pessimistic/optimistic bracket; all three are shown.
- Adverse selection: simulated maker markouts must be no better than real prints' at the same
  prices. Unacceptable: more than 1 bp better on average.

A maker fill's price is its limit by construction, so A covers the PLAN's "price vs real prints
in bps" and B covers whether and when makers fill.

### Hooks for #9 (stale-data guard) and #10 (anti-cheat delay)

`MarketGate.check(product, now)` is checked when an order arrives and again when it takes effect,
and gates fills on resting orders: the book must be SYNCED and not catching up, with its last
level2 message at most 5 s old. The delay is `delay_ns` (200 ms, ADR 0011's default, applied to
orders and cancels). #9 and #10 implement these; the order model only calls them.

### ADR 0005, amended

This ADR changes two points of ADR 0005, explicitly (CLAUDE.md):

- **"Optionally, orders fill only against trades that happen after arrival" (point 5)** is no
  longer a mode of the main fill path: with 4 to 11 prints a minute it would leave market orders
  waiting minutes for a print. It may come back as an off-by-default stress mode; #8 doesn't
  build it.
- **The 2 s lag threshold (point 3)** is relaxed to the 5 s level2-age rule (ADR 0011): lag
  compares the exchange's clock with ours, and the laptop's clock was 540 ms off, so a 2 s lag
  check isn't trustworthy without a synced clock. The guard always applies the 5 s age, measured
  on our own receive clock, and adds the lag check only when the clock is known to be synced.

The rest of ADR 0005 stands: fills at arrival plus a delay at least as long as feed lag, the
stale-data guard, exchange timestamps for candles.

### Implementation (#8)

`simex.exchange.PairEngine`, one per pair, owns its `OrderBook`, the depletion map, the resting
orders' queue state and a `ShrinkMatcher` (`simex.exchange.attribution`, the same class the W
measurement runs). It takes accepted orders from the account writer and reports fills and state
changes back to it. `on_event` first does what is due by the event's `received_at_ns` (orders
and cancels taking effect, depletion and matcher expiry) against the book as it stood, then
applies the event; `advance` does the same for quiet periods. Orders taking effect at the same
moment go in arrival order. Settings: `EXCHANGE_MATCH_WINDOW_MS` (W),
`EXCHANGE_DEPLETION_MAX_AGE_SECONDS` (10), `EXCHANGE_CANCEL_ATTRIBUTION`
(`proportional`, `pessimistic`, `optimistic`).

Counters (`PairEngine.stats`, counts and quantities), for the honesty report:
`maker_fills:print`, `maker_fills:trade_through`, `maker_fills:book_cross`, `taker_fills`,
`market_orders` and `band_binds` (the band's binding rate), `depletion_expired:venue_update`,
`depletion_expired:snapshot`, `depletion_expired:age`, `prints_ignored_unsynced` (the cost of
pausing while not SYNCED), `prints_no_aggressor`, `cancels:<mode>`, and
`rejected_on_effect:<reason>`.

## Consequences

- Taker fills are as honest as the audited book; maker fills are honest within a stated bracket,
  and every maker fill traces to a print or a real crossing.
- Agents pay Gemini's real entry-tier fees, which will dominate most strategies; scores must
  separate gross, fees and net.
- The account writer is a second kind of writer next to the pair engines, kept on purpose and
  recorded in CLAUDE.md.
- Off-grid orders are refused rather than rounded, so an agent must respect each pair's grid.
- Market impact is out until v2, so large orders look better than they would be; the band limits
  how far an order can walk.
- W, the band's binding rate, depletion expiries, ignored prints and the cause mix are measured,
  not assumed, and land in this ADR's Measured section with #8.

## Measured

**W:** to be measured on the recordings with `tools/measure_fill_window.py` (the engine default,
500 ms, is a placeholder until then).
