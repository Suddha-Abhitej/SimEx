# 0011. Venue adapters, envelope v3, and Gemini's gap rule

- **Status:** Accepted
- **Date:** 2026-10-15
- **Extends:** 0007 (envelope and topics) and 0008 (gap resync), without reversing them.

## Context

ADR 0010 moved the live source from Coinbase to Gemini and kept Coinbase for feed research only.
The ingestor was written for Coinbase, and everything downstream must not depend on which venue
the data came from (CLAUDE.md). Gemini's feed differs from Coinbase's in ways that matter for
gaps, so its rules were measured before building: `tools/gemini_probe.py`, 300 s, BTCUSD, ETHUSD
and SOLUSD, 2026-10-15, from a laptop with its clock offset measured by NTP.

- **Volume:** depth diffs about 9.4 msg/s per symbol (178 to 295 level changes/s), trades about
  0.1 to 0.2 msg/s per symbol (5 to 11 a minute). 2.06 GB/day raw JSON, 0.29 GB/day gzipped.
  Diffs arrive a median 94 ms apart; the longest silence on any symbol was 0.6 s (1.0 s on an
  earlier run).
- **Lag**, corrected for a +540 ms clock offset: depth p50 51 ms, p99 155 ms; trades p50 40 ms,
  p99 105 ms; minimum 32 ms. Gemini's `E` is epoch nanoseconds.
- **Snapshots.** With `?snapshot=-1` the snapshot is a `depthUpdate` with `U == u` and the whole
  book (BTC 4,548 levels / 142 KB, ETH 2,099 / 59 KB, SOL 1,746 / 47 KB), carrying `s`. It is sent
  after every SUBSCRIBE to a depth stream, including an UNSUBSCRIBE and SUBSCRIBE on a live
  connection. In 8,480 ordinary diffs, `u - U` was never 0. A diff can still arrive between the
  SUBSCRIBE and its snapshot.
- **Update IDs.** U and u are one counter shared by every market on the exchange: about 7,800 IDs
  a second against our 730 level changes, with all three symbols interleaved in the same range.
  Read as a timestamp it would be December 2025, so it is a counter, not a clock.
- **Chaining.** Excluding the first diff after a snapshot, **100 % of diffs had U == previous u**
  (BTC 2,823, ETH 2,864, SOL 2,790): no skips, no overlaps, no IDs going backwards. Every first
  diff after a snapshot straddled it: U <= L < u, where L is the snapshot's ID.
- **One-shot depth request:** 200 levels (100 a side), not a full book, with `lastUpdateId` equal
  to the stream's latest u.
- **Trade IDs** are exchange-wide: within a symbol they skip IDs and a few arrived out of order.
- **Limits** (conninfo): request weight 7,000 per 10 s, 300 connection attempts per 5 minutes.

## Decision

**Venue adapters.** The ingestor core (`simex.ingest.client.Ingestor`) keeps everything
venue-neutral: the connection loop and backoff, liveness, the resync rate limiter, resync
markers and gap records (ADR 0008), snapshot refresh (ADR 0009), the fault injector, publishing
and metrics. A `VenueAdapter` (`simex/ingest/venue.py`) only knows its exchange: the URL, the
frames that subscribe and that resync given products, an optional ping, and `parse()`, which
reads a message, runs the venue's gap rule and says what to publish. `GeminiAdapter` is the live
venue; `CoinbaseAdapter` is the existing Coinbase code, unchanged in behaviour.

**Venue choice.** `SIMEX_VENUE=gemini` is the default. `SIMEX_VENUE=coinbase` refuses to start
unless `SIMEX_COINBASE_RESEARCH=1`, and then publishes to **`research.coinbase.md.*`**. Gemini
publishes to `md.*`, so research data can't reach the book or matching engine even by
misconfiguration. Every metric gains a `venue` label with its name unchanged, except
`simex_ingest_coinbase_errors`, which becomes `simex_ingest_exchange_errors`.

**Envelope v3** adds `venue` and, on level2, `snapshot` (this message is a full book) and
`update_ids` (the venue's `[U, u]`, or null). The payload is still the raw bytes. `product_id` is
always the normalized `BTC-USD` form. Because a Gemini snapshot is only recognizable from the
subscription state, the adapter decides and the header says so; consumers never re-infer it.
Gap records become v2 with `venue` and `id_jump`.

**Shared decoder.** `simex.marketdata.decode(envelope)` turns any venue's envelope into
`BookUpdate`, `Trade` and `ResyncMarker` events with `Decimal` prices and sizes (ADR 0004), and
reads v1 and v2 envelopes as Coinbase. Consumers never parse a venue's payload themselves.

**Gemini's gap rule**, per product, after a snapshot with ID L:

| Diff | Meaning | Action |
| --- | --- | --- |
| `U == u` while a SUBSCRIBE for its depth stream is pending | Snapshot | New baseline L = u |
| First after the snapshot, `U <= L < u` | Straddles the snapshot | Apply |
| First after the snapshot, `u <= L` | Sent before the snapshot was taken | Skip; count as `stale_after_snapshot` (no alarm) |
| `U == previous u` | Normal | Apply |
| `U > previous u` (first diff: `U > L`) | **Gap** | Resync this product only |
| `U < previous u < u` | Overlap, never seen | Apply (values are absolute); drift alarm |
| `u <= previous u` | Stale, never seen | Skip; drift alarm |
| `U == u` with no SUBSCRIBE pending | Never seen | Not trusted as a snapshot; drift alarm |

A gap publishes a resync marker for that product only, records it on `md.gaps` with
`missing_messages: null` (the IDs are exchange-wide, so the jump is not a count of this
product's lost updates) and `id_jump` = U - previous u for reference, and resubscribes that
product's depth stream on the same connection. If the snapshot doesn't arrive within
`INGEST_RESYNC_TIMEOUT_SECONDS`, the ingestor reconnects. The ADR 0009 refresh uses the same
per-product resubscribe. A lost snapshot breaks no chain (there is no baseline yet), so every
pair must also get a snapshot within that timeout of each connect, or the ingestor reconnects.
Drift kinds are counted in `simex_ingest_protocol_drift_total` and
logged as errors; all but `stale_after_snapshot` should stay at 0.

**Strict, not tolerant.** The rule tolerates nothing the probe didn't see, so a protocol change
shows up as a gap or a drift alarm instead of a silently wrong book. The probe's five minutes and
the ingestor's ten-minute live run (below: 0 gaps, no drift beyond `stale_after_snapshot`) are the
evidence so far; **the rule gets its real proof in the week-1 24-hour run** (0 unrecovered gaps,
drift counters at 0). If that run shows overlaps are normal, this ADR is revisited.

**Trades** are published as received. Lost trades can't be detected (exchange-wide IDs), so this
is a known limit: only disconnects are recorded as possible trade loss. Consumers dedupe by
trade ID and assume no order (`simex.marketdata.TradeDeduper`).

**Liveness.** Gemini has no heartbeats channel. The ingestor sends a `ping` request every
`INGEST_PING_INTERVAL_SECONDS` (2 s) and reconnects after `INGEST_STALE_AFTER_SECONDS` (5 s, now
the default for every venue) without a sign of life. **Only level2 messages, ping replies and
other control frames count; trades never do**, either way: Gemini pairs went 38 s (BTC), 96 s
(SOL) and 212 s (ETH) without a trade in the live runs, so trade silence must not trip the
timer, and a trickle of trades must not hide a dead depth stream (both are tested; Coinbase's
heartbeats count as control frames). Level2 diffs arrive about every 100 ms and the longest
silence measured was 1 s, so a healthy connection is never quiet for 5 s and a lost ping reply
needs no separate timeout. `simex_ingest_ping_rtt_seconds` reports the round trip. The same rule
holds downstream: stale-data guards use the level2 age from
`simex_ingest_last_message_age_seconds`, never the `market_trades` age.

**Defaults for the matching engine** (ADR 0005): anti-cheat delay **200 ms**, which covers the
155 ms p99 depth lag, and stale-data threshold 5 s. The lag was measured on a laptop whose clock
was about 0.5 s behind and drifting, corrected by an NTP offset taken separately; **both values
are to be re-measured on the Oracle server with a synced clock.**

## Consequences

- The book builder (#5) and everything after it read `md.*` through one decoder and work for any
  adapter. A synthetic market (ADR 0010) can be added later as another adapter.
- A Gemini gap costs one product about one resubscribe (a 47 to 142 KB snapshot), not all three
  as on Coinbase. Snapshot refreshes cost about 250 KB per 30 minutes.
- `missing_messages` is null for Gemini gaps; dashboards count gaps, not lost messages.
- Metric queries and dashboards must include the `venue` label; `coinbase_errors` is renamed.
- The `md.heartbeats` topic is Coinbase-only; Gemini publishes nothing there.
- **Known limit for the fill model:** Gemini trades about 4 to 11 times a minute per pair, so a
  queue model that fills resting limit orders only from trades at the order's price would rarely
  fill them. The fill simulator (#5 onwards) must also consider the book shrinking at the order's
  price level, not only trades.
- The one-shot depth request returns the top 200 levels at an exact update ID. A periodic
  top-of-book audit (compare our book at that ID) belongs with the order book, not the ingestor;
  it is a follow-up, not part of this decision.
- If Gemini changes its snapshot or ID semantics, the drift alarms and the gap-rate alarm
  (ADR 0008) are what catch it.

## Measured

Live validation on 2026-10-15 from a Windows laptop against the local Docker stack, with the
md.* topics deleted first so no Coinbase data remained in them:

| Run | Result |
| --- | --- |
| Normal, 10 min | 16,787 level2 messages (28 msg/s), 84 trades. 0 gaps, 0 resyncs, 0 reconnects, 0 exchange errors, `awaiting_snapshot` 0 for every pair. Drift: only `stale_after_snapshot`, once per product (right after the initial snapshot). Ping round trip 78 ms. |
| `SIMEX_INGEST_DROP_EVERY=500`, 5 min | 22 injected drops, 22 injected gaps, 22 single-product resubscribes. Recovery 72 to 435 ms, mean 161 ms. 0 reconnects, 0 suppressed resyncs, 0 feed gaps, `awaiting_snapshot` 0 at the end; 20 level2 updates flagged `awaiting_snapshot` while books resynced. Drift: `stale_after_snapshot` only (BTC 4, SOL 2). |
| `INGEST_SNAPSHOT_REFRESH_SECONDS=90`, about 3.5 min | 6 refreshes, 2 per product, all completed, 30 s apart in order BTC, ETH, SOL. Mean refresh time BTC 156 ms, ETH 109 ms, SOL 78 ms. No timeouts, no reconnects. |

**Compared with Coinbase** (ADR 0008): a Coinbase resync took 0.6 to 1.0 s and invalidated all
three books, because its sequence counter is per connection. On Gemini a resync took about
0.16 s on average and invalidated only the product whose chain broke; the other books never
stopped. A Gemini snapshot is also 47 to 142 KB, against several MB on Coinbase.

Feed lag from these runs is not usable (the laptop clock was about 0.5 s behind, so raw lag read
about -540 ms); the corrected figures above come from the probe. Lag is to be re-measured on the
Oracle server with a synced clock.
