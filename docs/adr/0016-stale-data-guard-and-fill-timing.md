# 0016. Stale-data guard and complete fill timing rules

- **Status:** Accepted (amended 2026-11-09)
- **Date:** 2026-10-19
- **Supersedes:** [0005](0005-fills-at-arrival-with-anti-cheat-delay.md), whose rules were
  amended across 0014 and 0015.

## Decision

This record is the complete current rule set for order timing, feed safety, and fills. Later
changes must amend this record rather than requiring readers to reconstruct the rules from older
ADRs.

### Market-data clock and delivery

- Market-data events are applied per pair in `(received_at_ns, topic, partition, offset)` order
  below the live runner watermark (ADR 0015). Late records are applied once immediately and
  counted.
- Orders and cancels are stamped on the market-data clock when they arrive. The configured
  fixed anti-cheat delay defaults to 200 ms (`EXCHANGE_ORDER_DELAY_MS`); each takes effect at
  arrival plus that delay. The delay is deliberately applied to cancels too. There is no jitter.
  A faster bot can gain at most our feed lag minus its own lag, so a delay at or above our
  rolling p99 feed lag removes that edge 99% of the time. The measured depth p99 is 155 ms on
  the dev laptop, clock-corrected. This must be re-measured on Oracle, where the runner's
  hold-back and watermark lag also change.
- Exchange timestamps are used for candles and features, never for order age or level2
  staleness. Money remains exact `Decimal`.

### Stale-data guard

Each `PairEngine` publishes an immutable `MarketStatus` after every event: book state, the last
level2 receive time, the latest lag and excess lag, the best bid and ask, whether it is catching
up, and whether the rolling lag baseline is established. `MarketGate.check(product, now)` is a
pure function of that snapshot and `now`.

The gate is closed when any of these holds:

1. The book is not `SYNCED`, or it is catching up after a resync and before its snapshot.
2. The last `md.level2` message is older than `EXCHANGE_STALE_AFTER_MS` (default 5000), measured
   on SimEx's receive clock.
3. After ten minutes of samples, the latest level2 lag exceeds the rolling ten-minute median by
   more than `EXCHANGE_MAX_EXCESS_LAG_MS` (default 2000).
4. Absolute level2 lag exceeds `EXCHANGE_MAX_ABS_LAG_MS` (default 30000), as a backstop.

Before `EXCHANGE_LAG_BASELINE_MINUTES` (default 10) of lag samples, only rules 1, 2 and 4 apply.
The rolling median makes a constant clock offset cancel out. The stale check uses `md.level2` and
the ingestor's liveness ping only; `md.trades` never refreshes it. Gemini can be silent on trades
for minutes without closing the gate.

Orders are checked at arrival and again at effect. Either check can reject with a distinct
`book_not_synced`, `catching_up`, `stale_level2` (including age), or `feed_lagging` (including
excess) reason. Resting orders remain on the book while closed, but prints received while closed
are discarded and counted as `prints_ignored_stale`; they are not applied retroactively.

Cancels are never rejected by this gate, so an agent can always get out. They still wait the
200 ms anti-cheat delay. Honouring cancels during a closure is deliberately generous: prints
ignored during it are gone, so a fill that would have happened is not later manufactured.
Holding cancels instead could trap an agent during a feed outage.

### Fill model

- Limit orders are good-till-cancelled. Their crossing part fills immediately as taker at effect;
  the remainder rests as maker. Market orders are IOC walks inside the per-pair 50 bps band at
  effect; any remainder expires.
- Resting orders fill only from venue prints (or the explicitly configured optimistic
  `EXCHANGE_BOOK_CROSS=fill` bracket). Prints clear venue quantity ahead first, then our orders
  by arrival; trade-throughs fill. Book shrinks are matched to prints within the measured
  300 ms window, with unmatched shrinks treated as cancels.
- The depletion map prevents simulated orders from consuming the same live liquidity twice.
  Fees, reservations, account balances, one writer per pair, and one account writer follow ADR
  0014. The system is simulation-only and never places real orders.
- Replay and live application are byte-identical for released market records and deterministic
  order IDs; the stale guard cannot create a fill or a take-effect while closed.

### Ledger failure and restart boundaries

- A fill reported by the engine is a fact, not a request. A ledger timeout or write failure must
  never reject or drop it. The account writer retries with backoff; persistent failure halts
  market-event processing and new order entry, alerts, and requires intervention. This is the
  same fail-closed family as reconciliation mismatches.
- Version 1 does not persist pair-engine queue state. On every process restart, resting orders
  return to the back of the queue at the first synchronized book (`ahead` equals the current
  venue quantity); an order improving the best price uses `ahead = 0` as in ADR 0014. The
  `crossed` flag resets to false and the depletion map is discarded. Persisting queue state is
  a possible later optimization only if measured restart frequency justifies its hot-path writes.

## Consequences

The guard pauses fills and rejects new orders during feed failure without pretending that ignored
prints can be recovered later. A feed outage can leave resting orders open until their owner
cancels them, but the always-honoured cancel path avoids trapping an agent. The rolling baseline
needs ten minutes before it can detect excess lag, so absolute lag and receive-clock age remain
the early protections.

## Amendment: excess-lag baseline coverage (2026-11-09, #113)

Before this implementation correction, rule 3 was normally inactive at defaults: its warm-up
compared the span of samples already trimmed to ten minutes against a ten-minute threshold.
Only an exact receive-time endpoint match could establish the baseline, and it would not latch.
The earlier overnight baseline run, live gate test and #24 load measurements are **not evidence
that excess-lag enforcement ran**. Book synchronization/catch-up, receive-clock staleness and
absolute lag protections continued to apply.

The correction dated 2026-11-09 tracks elapsed coverage from the first usable lag sample,
independently of the unchanged rolling ten-minute median window. Missing exchange timestamps
do not start coverage. Coverage and samples survive resyncs and `awaiting_snapshot` periods,
because lag describes transport delivery rather than book state. A new engine, including after
a process restart or runtime rebuild, loses both and restarts warm-up; neither is persisted.

Eligibility also requires at least three samples in the current window. Three is the smallest
odd count whose median has a majority, a structural guard rather than a statistical sufficiency
claim. One sample yields zero excess; with two, one delayed observation pulls the midpoint and
can spuriously close the gate. After a long outage, fewer than three current samples temporarily
disable the excess check without resetting elapsed coverage. Three samples can re-enable it
within a second; uniformly slow returning samples can then have near-zero excess, while the
independent absolute-lag and receive-age rules remain in force.

This records the implementation correction date, not a deployment date: existing processes gain
the safeguard only after deploying the fix and warming up. Operators should watch gate-closure
rates and expect `simex_exchange_lag_baseline_established` to reach 1 after ten minutes of usable
coverage with sufficient current samples, visible in the provisioned Grafana exchange dashboard.
Excess closures retain `feed_lagging:excess_ns=...`. This amendment reports synthetic regression
validation, not a new live ten-minute measurement or a change to the accepted thresholds.
