# 0025. Ingestor clock-skew diagnostic

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

The first live run recorded persistently negative receive-minus-exchange lag
([ADR 0017](0017-event-sourced-ledger.md#measured), #74). That means the clocks disagree and
can make data appear fresher than it is, yet previously looked like a healthy lag metric.
Clock verification and correction belong to the operator. The ingestor can detect sustained
negative observations without a network request or a clock offset estimate.

## Decision

Observe level2 lag once per configured venue/product per received message, using the existing
raw integer lag and injected performance clock. Snapshots and updates count even while awaiting
a resync snapshot: transport lag is independent of book validity. Trades, heartbeats, ping replies,
control frames and audit responses neither refresh nor reset this observation coverage.

Confirm a condition after **at least 60 seconds of consecutive strictly negative observations**,
with **at least three distinct messages**. The interval is between the first and latest usable
observations; elapsed time without another observation cannot confirm it. The 60-second debounce
is an operational choice, not an empirically measured safety threshold: it uses the existing
gap monitor's one-minute scale and spans multiple Prometheus scrapes. Three observations prevent
two endpoints alone from confirming a condition. There is no negative-offset tuning threshold.

A level2 message without an exchange timestamp, a gap **longer than** the existing
`INGEST_STALE_AFTER_SECONDS` setting, or a disconnect breaks coverage. Require a fresh complete
window on return. Resync/awaiting-snapshot state alone does not break coverage. A nonnegative
observation, including exactly zero, ends the episode. Log one WARNING per confirmed episode,
naming venue/product, duration, count and latest lag, and instructing the operator to check host
clock synchronization before relying on absolute lag. Missing data and reconnects are not
recovery: preserve the warning latch until a nonnegative observation, so reconfirming after an
outage does not repeat the warning. A new ingestor process starts fresh; nothing is persisted.

Export `simex_ingest_clock_skew_suspected{venue,product_id}` from startup for every configured
product:

- **1:** a confirmed negative-lag window whose latest usable observation is no older than
  `INGEST_STALE_AFTER_SECONDS`.
- **0:** not currently confirmed, including startup, warm-up, a nonnegative observation, missing
  timestamps, disconnection or stale coverage. **0 is not evidence of clock synchronization.**

A pure scrape callback expires the gauge when level2 stops; it does not mutate the episode,
advance observation coverage or require a polling task. The gauge remains queryable after the
single log warning has passed. This adds no dashboard or Prometheus alert rule.

Detection only: no timestamp correction, lag arithmetic, histogram, gate threshold or anti-cheat
delay changes. Do not add runtime NTP queries or promote `ntplib` from the dev extra: the existing
manual probe may check NTP, but the runtime diagnostic consumes observations it already has.

## Consequences

Sustained negative lag becomes visible in logs and metrics without per-message warning noise or
an unbounded sample buffer. The detector identifies clock disagreement, not which clock is wrong
or the exact offset. It misses skew smaller than real transport lag and negative episodes
interrupted by nonnegative observations. Neither a quiet warning nor a zero gauge validates
absolute feed-lag measurements; verify host synchronization independently, including on the host
outside a devcontainer. Historical measurements remain qualified as recorded, and this ADR
claims synthetic regression validation only, not a new live clock validation or clock repair.
