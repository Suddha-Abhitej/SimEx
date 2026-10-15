# 0012. Order book library and service, Valkey book records, and the book audit

- **Status:** Accepted
- **Date:** 2026-10-15

## Context

Issue #5 turns `md.level2` into a live level-2 book per product. The matching engine will fill
simulated orders against it, so correctness matters more than speed. Several things are already
settled:

- the venue-independent decoder (`simex.marketdata`, ADR 0011);
- per-product resync markers and the `awaiting_snapshot` flag (ADR 0008);
- the cold start: a time-based seek over a window that always holds a snapshot (ADR 0009).

Readers outside the service (MCP quotes, the private dashboard) need the book without running it
themselves. The week-1 gate also asks for proof that the book matches the exchange.

## Decision

**A pure library, `simex.book.OrderBook`.** It has no I/O and no venue imports; its only input is
`BookUpdate` and `ResyncMarker` events from `simex.marketdata.decode`.

- **Storage.** Prices and sizes are `Decimal` (ADR 0004). Bids and asks are `SortedDict`s, best
  price first.
- **States:**
  - EMPTY: no snapshot yet.
  - SYNCED: the only state that answers queries.
  - RESYNCING: waiting for the next snapshot.
- **Rules:**
  - The envelope's `snapshot` flag replaces the whole book; the book never infers a snapshot from
    a payload.
  - A level's size is the full size at that price, and 0 removes the level.
  - Updates flagged `awaiting_snapshot` are skipped, and so are updates while not SYNCED.
  - A resync marker clears the book and moves it to RESYNCING.
- **The crossed check runs after a whole message (one `BookUpdate`) is applied, never
  mid-message.** A venue may move both sides in one message, so an intermediate state can look
  crossed when the result isn't. A message that leaves best bid >= best ask drops the book to
  RESYNCING and is counted (`simex_book_crossed_total`, which should stay 0).
- **Queries** (`best_bid`, `best_ask`, `spread`, `mid`, `depth(n)`, `walk(side, quantity)`)
  raise `BookNotReady` unless the book is SYNCED, so stale data never leaves it. `walk` returns
  the filled quantity, notional, average price and levels touched without changing the book. The
  matching engine will embed this class in-process, one writer per pair.
- **Continuity.** The book does not re-check update-ID continuity: that rule is per venue and
  belongs to the ingestor (ADR 0011), which reports breaks through markers and
  `awaiting_snapshot`. The book keeps `last_update_id` for the audit.

**The service, `python -m simex.book`.**

- **Cold start (ADR 0009).** It assigns every `md.level2` partition, with no consumer group and
  no commits, and seeks to `now - 2 x refresh interval` (clamped to the log start). It logs an
  error if `retention.ms` is under 2x the refresh interval.
- **Fallback.** If a book is still EMPTY once every partition has caught up, the partitions are
  re-read once from the log start, feeding only the books that were EMPTY when the re-read began
  (books already SYNCED must not see old data again). After that the book waits for a live
  snapshot.
- **One writer per pair.** A dispatcher routes records by Kafka key (the product ID; nothing is
  parsed) to one queue and one asyncio task per product, which decodes and applies.
- **`catching_up` is a latch.** It is true only during the startup catch-up, until the book's
  partition has passed the end offset seen at startup and its queue has drained. It turns true
  again only for books the one-time log-start re-read feeds, until that re-read catches up. In
  steady state it never flips back: a record that is queued but not yet applied, or a quiet spell,
  is not catching up. Staleness after startup is the reader's 5 s rule on `received_at_ns`. (The
  first version derived it from "queue non-empty" at publish time and flapped in steady state;
  see Measured.)
- **Throughput.** Measured on an x86 dev container, decoding and applying a 20-level diff takes
  about 100 us (about 9,800 msg/s) and a 4,548-level snapshot about 45 ms. A full window (about
  one hour, 100k messages at the measured 28 msg/s) replays in about 10 s; allow 2 to 3x that on
  the 2-core ARM server.
- **Memory.** About 285 bytes per level: BTC 4,548 levels 1.3 MB, ETH 0.6 MB, SOL 0.5 MB.
- **Partitions.** `md.level2` has 3 partitions, but the three pairs' keys hash onto only two of
  them (on the laptop, p1 stayed empty). That is fine: ordering is per key, and two products
  sharing a partition only share a dispatcher's read loop, not a book. It does make load uneven,
  so if more pairs are added, revisit the partition count or use an explicit partitioner.

**Valkey record, one per product.** It is written every `BOOK_PUBLISH_INTERVAL_SECONDS` (0.25)
with one atomic `SET key value PX 2000`:

```
simex:book:<venue>:<product_id>     e.g. simex:book:gemini:BTC-USD
{"v":1, "venue", "product_id", "state", "catching_up",
 "best_bid":[price,size], "best_ask":[price,size], "mid", "spread",
 "bids":[[price,size] x N], "asks":[[price,size] x N], "levels":{"bids","asks"},
 "last_update_id", "exchange_ts_ns", "received_at_ns", "last_snapshot_at_ns", "published_at_ns"}
```

`N` is `BOOK_PUBLISH_DEPTH` (10). Prices and sizes are Decimal strings. Every price field is null
and the level lists are empty unless the state is SYNCED.

**Readers may use a book only if all of these hold** (`simex.book.record.usable`):

1. The key exists. The 2 s TTL makes a dead service's books vanish, and Valkey's `allkeys-lru`
   eviction also reads as "not ready".
2. `state == "SYNCED"` and `catching_up` is false.
3. `now - received_at_ns <= 5 s` (ADR 0011's stale threshold). `received_at_ns` is when the
   ingestor received the last update applied to the book; ingestor and readers share a host clock
   in production.

The `market_trades` age never drives a reader's stale check.

**Book audit** (issue #46, built after #5). Gemini's one-shot `depth` request returns the top
200 levels (100 a side) at an update ID, `lastUpdateId` (L). ADR 0011's probe found L equal to
the stream's latest `u`; the live audit run (see Measured) refines that: it holds only when the
request lands between the stream's 100 ms batches. Gemini answers at the moment it is asked, so
about a third of the time L falls inside the next batch, and no point in the stream matches it.

- **The ingestor (only it talks to the exchange)** sends one per product every
  `INGEST_AUDIT_SECONDS` (default 120). The products take turns, one request every
  `INGEST_AUDIT_SECONDS / pairs` (40 s for three pairs), so the requests are staggered and never
  burst. A product waiting for a snapshot is skipped for that turn. The ingestor publishes the
  response **in-band on `md.level2`** as a `type: "audit"` envelope keyed by the product, with
  `update_id` (L), `aligned`, `reason`, and Gemini's response spliced in byte for byte as the
  payload. Older consumers skip unknown types, and partition order puts the audit at an exact
  point in the product's stream. Venues without such a request (Coinbase) send no audits.
- **Exactness rule** (`simex.ingest.audit.AuditAligner`). The ingestor tracks the last `u` it
  published per product:
  - if that `u == L` when the response arrives, it publishes the audit right away, `aligned`;
  - if it is already past `L`, it publishes the audit right away, unaligned;
  - otherwise the response arrived before its diff, and it is **held**, keyed by product. After
    publishing each level2 message the ingestor checks the held audit: a message ending at
    exactly `L` releases it right behind that message, `aligned`; a message that jumps past `L`
    releases it unaligned. It is also released unaligned if the hold reaches
    `INGEST_AUDIT_HOLD_SECONDS` (2 s, `timeout`), if a resync starts for the product (after the
    resync marker, `resync`), or if a newer audit for the product arrives (`superseded`).
  - An audit the stream has passed gets one of two reasons. **`inside_diff`**: `L` falls strictly
    inside the product's message that passed it (`U < L < u`) and isn't the end of the message
    before it; there is no point in the stream to compare at. This is the normal live case: the
    answer reaches us 10 to 90 ms before the batch that straddles `L`, is held, and that batch
    releases it. **`stream_past`**: the stream had already passed `L` at a message boundary (`L`
    is the previous message's `u`, which on Gemini is also this diff's `U`, or a snapshot's ID).
    The remaining reasons (`timeout`, `resync`, `superseded`) are any other cause.
  - Nothing is ever dropped: every response is published once, so the counts add up.
  - **Not built: retry on `inside_diff`.** Asking again right away would turn about 87% of turns
    into an exact compare (at about 64% aligned per try) for about one extra request per product
    every few minutes. The gate below is met without it, so it is left as a possible later
    improvement.
- **The book's comparison.** An unaligned audit counts as `unaligned`, never as a match. An
  aligned audit is compared only if the book is SYNCED with `last_update_id == L`; otherwise it is
  `skipped`. The compare sorts the venue's levels and checks the top 100 a side, price and size
  as exact `Decimal` values (so `"1.5"` equals `"1.50000000"`). If the venue sent fewer than 100
  levels on a side, that was its whole side, so the book must not have more. On a mismatch the
  book counts it, logs an error naming the first differing level, and drops to RESYNCING.
- **Back-channel.** A crossed book or an audit mismatch shouldn't wait up to 30 minutes for the
  next refresh. The book service publishes
  `{"v":1, "type":"resync_request", venue, product_id, reason, requested_at_ns}` to
  `ctl.resync_requests` (1 partition, `retention.ms` 1 hour), keyed by product. It sends one
  request per product until that book is SYNCED again; a failed send is logged and may be retried
  by the next failure. **It sends none while the book is catching up**: a failure replayed from
  history was already served, and the snapshot that fixed it follows in the log, so asking again
  would only resubscribe a healthy stream. The ingestor reads the topic from its end when it
  starts (an old request is never replayed) and runs its normal single-product resync: a resync
  marker, then a resubscribe through the shared rate limiter. Requests are deduplicated per
  product: one for a product already waiting for a snapshot or a resubscribe is a `duplicate`, and
  one for another venue or pair is `unknown`. Only the ingestor ever talks to the exchange.

**Audit and back-channel metrics:**

- book (port 9102): `simex_book_audits_total{outcome=match|mismatch|skipped|unaligned, reason}`,
  where `reason` is how the ingestor placed the audit (`aligned`, `inside_diff`, `stream_past`,
  `timeout`, `resync`, `superseded`); match, mismatch and skipped always have `reason="aligned"`;
  `simex_book_resync_requests_total{reason=audit_mismatch|crossed}`, and
  `simex_book_resyncs_total{reason=audit_mismatch}`;
- ingestor (port 9101, labels `venue` and `product_id`): `simex_ingest_audits_total{outcome}`,
  with `requested`, `skipped_not_synced`, `aligned`, and `unaligned_<reason>` (for example
  `unaligned_inside_diff`);
  `simex_ingest_resync_requests_total{outcome=accepted|duplicate|unknown}`; and
  `simex_ingest_resyncs_total{method="resubscribe", trigger="book_request"}`.

**The week-1 audit gate:** **0 mismatches, and at least 10 aligned audits per product per hour.**
At the default interval each product is asked 30 times an hour and about 64% align, so a healthy
product gets about 19. Fewer than 10 means the audit itself has stopped working (requests not
sent, books not synced, or answers no longer aligning), which a mismatch count of 0 would hide.
Both halves are Prometheus alerts in `infra/prometheus/rules/book-audit.yml`, with a `promtool`
test that CI runs: `BookAuditMismatch` (critical, any mismatch in the last hour) and
`BookAuditsNotAligning` (warning, under 10 aligned in the last hour for an hour; a product with
none at all counts as 0, and the hour of `for` keeps a fresh start from firing). There is no
Alertmanager yet; the alerts show on Prometheus's Alerts page.

**Metrics** (port 9102, labels `venue` and `product_id`):

- `simex_book_state{state}`, `simex_book_levels{side}`, `simex_book_crossed_total`
- `simex_book_updates_total{outcome}` and
  `simex_book_resyncs_total{reason=marker|crossed|audit_mismatch}`
- `simex_book_apply_seconds` and `simex_book_ingest_to_book_seconds`
- `simex_book_seconds_since_snapshot`, `simex_book_seconds_since_update`,
  `simex_book_seconds_since_synced`
- `simex_book_catching_up`, `simex_book_valkey_errors_total`,
  `simex_book_replayed_from_log_start_total`

## Consequences

- The book is correct only as long as its input is. The library trusts the ingestor's markers and
  flags, and the audit is what proves the result against the exchange.
- A crossed book or a failed audit recovers within one resubscribe (tens of milliseconds plus the
  rate limiter), not at the next refresh. If the request can't be sent, the next refresh still
  recovers it.
- An audit proves the top 100 levels a side at one instant, every 2 minutes per product, and
  only about two in three can be compared. A mismatch deeper than 100 levels, or one that heals
  itself between audits, goes unseen.
- Readers get one consistent JSON per product per read, with no partial hashes. A dead service
  shows up as a missing key within 2 s.
- Valkey holds only the top N levels. Anything that needs the full book (the matching engine)
  embeds the library rather than reading Valkey.
- Restarting the service rebuilds every book from Kafka in seconds. No book state is persisted.

## Measured

Live validation of #47 on 2026-10-15 from a Windows laptop against the local Docker stack, with
the Gemini ingestor running:

| Run | Result |
| --- | --- |
| 30 min, book service restarted at about 15 min | 0 crossed books, 0 Valkey errors, 0 re-reads from the log start. Restart: keys missing for the 10 s the service was down, then `EMPTY (catching up)` to `SYNCED` in about 5 s, reading `md.level2 from 3600 s ago: p0 10853..41012, p1 0..0, p2 19931..77109`. Scheduled refreshes dropped BTC (at 1,178 s) and ETH (at 1,778 s) for about 0.5 s each. Apply about 65 us per event. |
| `SIMEX_INGEST_DROP_EVERY=500`, 5 min | 17 injected gaps on the ingestor, 17 marker resyncs on the books (BTC 7, ETH 9, SOL 1). Outside SYNCED: mean about 280 ms, max 329 ms. 0 crossed. |

**Bug found and fixed.** In that run, `catching_up` flapped from SYNCED to `SYNCED (catching up)`
for about 0.5 s at a time long after startup: SOL 20 times in 30 minutes, BTC and ETH once or
twice. That lowered SOL's usability to 98.6%. The cause was the first version's rule, "catching
up while the product's queue is non-empty". In steady state the publisher could run while a
record was queued but not yet applied. SOL was hit most because it is published last, after the
awaits for the BTC and ETH writes, which give the dispatcher time to queue new records.
`catching_up` is now the latch described above. A test reproduces the flap on the old rule. A
40-second steady-state run on a local Kafka and Redis, polling every 20 ms, saw 27 to 30
`catching_up=true` reads per busy book with the old rule and 0 with the latch.

**Audit, forced mismatch (#46).** Against a local Kafka and Redis, with the real ingestor and book
service and the scripted fake Gemini (`INGEST_AUDIT_SECONDS=3`), one corrupted depth answer for
ETH gave one book mismatch, one request on `ctl.resync_requests`, one ETH-only resubscribe, and
ETH SYNCED again 60 ms after the corrupted answer; the following ETH audits matched. Every
other audit in the run matched and all were aligned. A restarted book service replayed four
earlier mismatches from `md.level2` and, as designed, sent no request for them (the first version
did, which caused a needless resubscribe).

**Audit, live (#46).** 30 minutes against live Gemini on 2026-10-15, from a cloud container with
a local Kafka and Redis, default settings (`INGEST_AUDIT_SECONDS=120`):

| | BTC | ETH | SOL | Total |
| --- | --- | --- | --- | --- |
| Audits | 15 | 15 | 15 | **45** |
| Aligned | 10 | 9 | 10 | **29 (64%)** |
| Match / mismatch | 10 / 0 | 9 / 0 | 10 / 0 | **29 / 0** |
| Unaligned | 5 | 6 | 5 | **16, all `inside_diff`** |

Reading every audit back from `md.level2`: for each unaligned one, `L` was not the `u` of any
diff of the product, and exactly one diff had `u > L`, the one straddling it. The answer arrived
10 to 90 ms before that diff, was held, and the diff released it. (The run predates the
`inside_diff` label and recorded them as `stream_past`; the new rule classifies all 16 as
`inside_diff`.) This is Gemini answering `depth` at the moment it is asked while the stream
batches every 100 ms; it is not a fault in the book or the ingestor. In the same run the books
were usable 7130/7130 polls each, with 0 crossed, 0 Valkey errors, 0 gaps and 0 resync requests.
