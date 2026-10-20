# 0005. Fills at order arrival, with a stale-data guard and anti-cheat delay

- **Status:** Superseded by [0016](0016-stale-data-guard-and-fill-timing.md); amended by [0014](0014-order-model-and-fill-model.md) (point 5 is no
  longer part of the fill path; point 3's 2 s lag check is relaxed to the 5 s level2-age rule)
- **Date:** 2026-10-13

## Context

The live feed reaches SimEx with a small network delay. LLM agents take seconds to decide, so the
delay itself barely matters. Two things do: filling orders at stale prices when the feed falls
behind, and bots on a faster direct exchange feed trading against SimEx's slightly older book for
guaranteed fake profit (latency arbitrage).

## Decision

1. Candles and features use the exchange's event timestamps, not arrival time.
2. Feed lag (exchange time vs. received time) is measured per message and published as a metric.
3. A stale-data guard pauses fills and rejects orders while lag exceeds a threshold (starting at 2
   seconds) or a sequence gap is unrecovered.
4. Orders fill against the book at the moment they arrive, after a fixed anti-cheat delay at least as
   long as measured feed lag. Price movement between an agent's decision and arrival is real
   slippage.
5. Optionally, orders fill only against trades that happen after arrival.

## Consequences

- No fills at stale prices; faster outside feeds give no advantage.
- Agents see slightly worse fills than an idealized simulator, which is the point.
- The delay setting must be tuned from measured lag and documented.
