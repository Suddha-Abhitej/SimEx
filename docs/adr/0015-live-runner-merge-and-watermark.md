# 0015. Live exchange runner: merge by received time, release to a watermark

- **Status:** Accepted
- **Date:** 2026-10-19

## Context

The fill engine (ADR 0014) has so far run only on replayed recordings, on the market-data clock:
every record in `(received_at_ns, topic, partition, offset)` order, Kafka offset order within
each stream (`simex.recorder.replay`). Running it live raises two problems that replay never
has:

- **Two topics, delivered independently.** `md.level2` and `md.trades` reach the consumer on
  their own schedules, so a record received earlier can be consumed later. Applied in arrival
  order, the live engine would see a different sequence than a replay of the same hour.
- **An order must meet the book of its effective time.** An order stamped with wall time T takes
  effect at T plus the anti-cheat delay. If the engine lets it take effect while records received
  before then are still in Kafka, it meets an older book. That is latency arbitrage: a bot on a
  faster feed sees those records first and trades against the stale book, which is what the
  delay (ADR 0005, #10) exists to stop. So the order a live engine applies things in is the
  delay's correctness, not a detail.

Throughput is not the risk: the book service already applies `md.level2` live at about 65 us an
event. What is new is the trades merge and the hold-back.

## Decision

**`python -m simex.exchange`** (`make exchange`, metrics on :9104) consumes `md.level2` and
`md.trades` and drives one `PairEngine` per pair. No order entry yet; that comes with the MCP
server.

**The merge** (`simex.exchange.merge`). Per pair, each topic's records are held in offset order
and released in replay's order, `(received_at_ns, topic, partition, offset)`, but only strictly
below the pair's **watermark**: the smallest lower bound, over the pair's topics, on the received
time of any record not yet consumed. Per partition:

- **Behind** (records still to read): the received time of the last record consumed from it.
  The ingestor stamps `received_at_ns` and produces in receive order to one idempotent producer
  (`acks=all`, which keeps a partition's order across retries), so later offsets in a partition
  were received no earlier. The runner counts any record that breaks this
  (`simex_exchange_out_of_order_records`) and logs it with its type and how far below it fell.
  The live run found one source: audits (below, Measured), fixed in the ingestor.
- **Caught up** (read to the end at a fetch): the wall time just before that fetch minus the
  **hold-back** H. H covers the ingestor-to-consumer delay: the producer's 5 ms linger, the
  broker, and the consumer's fetch wait (20 ms, so a caught-up observation is never older than
  that).
- Bounds only rise. Before a pair's partition on a topic is known, its bound is the lowest over
  that topic's partitions (two products can share a partition, and the topics map them
  differently).

A record exactly at the watermark waits: another, not yet consumed, could share its time. After
each fetch the engine is advanced to the watermark minus 1 ns, so an order takes effect at T only
once every record received before T has been applied, and none received after.

**The engine's advance doesn't depend on how often it is called.** Live advances at each
watermark, replay at each event. `PairEngine.advance` now takes each due order and cancel in
time order, at its own effective time, after the depletion and matcher expiries due by then
(ADR 0014's Implementation section is updated to match). The first version processed everything
due at the call's time, so the same orders met more or less depletion, and were stamped with
different times, depending on the cadence. The live/replay equality tests found it.

**Late records.** A record received at or before a time already cleared, or sorting before one
already released, is applied at once, so the book stays right, and counted
(`simex_exchange_late_events`, `simex_exchange_late_seconds`; the `ExchangeLateRecords` alert).
At the chosen H the count must stay 0. Every record is applied exactly once, late or not.

**H = 100 ms by default** (`EXCHANGE_MERGE_HOLDBACK_MS`). `simex_exchange_delivery_seconds`
(consume time minus received time) is what H has to cover. It counts steady state only: records
from a partition caught up at this fetch and the one before. The fetch that ends a catch-up
carries backlog records whose "delay" is the backlog's age, so the catch-up after every start is
left out, and the histogram's count is below `simex_exchange_events`. (The first version counted
that fetch: on trades, a small topic whose whole backlog fits in it, 28.5% of the observations
read over 5 s; on level2 only the last batch of the backlog did.)

**The delay invariant** is that an order reaches the engine before the watermark passes its
effective time; otherwise it would take effect against a later book. The first version said "H
below the order delay", which is not it: the runner stamps an order when it arrives, and the
watermark is always at least H behind the wall clock, so an order effective at arrival + 200 ms
is never already cleared, whatever H is. What can break the invariant is transport: an order
stamped elsewhere that reaches the engine more than the delay plus the watermark lag later.
`PairEngine.submit` now refuses such an order (`rejected_on_effect:arrived_after_effective`)
instead of applying it to the wrong book. The watermark lag is H plus the fetch's wait: the
runner waits `EXCHANGE_FETCH_MAX_WAIT_MS` (20) per fetch, so about 120 to 130 ms. Every order
takes effect, in wall terms, about the delay plus that lag after it was submitted.

**Cold start**, as the book service does it (ADR 0009): both topics from two snapshot-refresh
intervals ago, so every partition starts behind and the backlog merges exactly. The end offsets
are read *before* the time lookup. Read after, a partition with nothing in the window whose first
record landed between the two queries would start past that record; the live check against a
real Kafka found exactly that. The consumer's reset policy is `earliest`: if a position ever falls
out of range (retention deleted records the runner hadn't read), it goes to the log start and
loses only what Kafka already deleted, where aiokafka's default (`latest`) would silently skip
everything up to the end.

**The determinism check.** Per pair and UTC hour, the runner writes how many records it released
and a SHA-256 over their `(topic, partition, offset)` in release order
(`data/exchange/merge-digests.jsonl`). An hour is complete only if the runner released records
from before and after it. `tools/merge_replay_check.py` recomputes both from the recordings in
replay order and compares every complete hour.

**Pairs are released one after another per fetch**, so the global order of fills across pairs
differs from replay's; within each pair it is identical. That's safe because pairs are
independent: funds are reserved at submission (ADR 0014), so a fill on one pair can't change
what happens on another.

## Consequences

- A live engine applies exactly replay's sequence, checked hour by hour against the recordings.
  So live fills are reproducible from the recordings, and the anti-cheat delay means what it
  says: an order meets the book of its effective time.
- Every order takes effect about H plus the fetch wait after its effective time in wall terms.
  This is latency in the acknowledgement, not in the price the order gets.
- **For order entry (the MCP server), a known gap.** What an order meets *at submission* (the
  gate, the funds reserved against a market order's band edge, whether a cancel finds the order
  still open) is checked against state as of the watermark, about H behind the wall clock, and
  against the order states on the market clock. Replay, which submits at the order's own time,
  can decide those checks differently. Taking effect is exact. Submission isn't yet: order entry
  must either record each submission with the watermark it saw, or move the book-dependent
  checks to effect time. The tests compare outcomes (fills per pair, final order records without
  the cancel-request fields, balances), not the raw event log, whose interleaving of submissions
  with effects follows the wall clock.
- A pair with no snapshot in the cold-start window waits for the next live one (at most 30
  minutes). The book service re-reads from the log start in that case; the runner doesn't yet.
- The book service's cold start reads the time lookup before the end offsets, so it has the same
  race; it is fixed separately.

## Measured

**Against a local Kafka** (scratchpad broker, 3 partitions per topic; a synthetic market
produced live through the ingestor's own idempotent producer, ~55 records a second across three
pairs, with limit orders submitted on the wall clock):

- Hold-back 100 ms, 60 s: all 3,291 records released, **0 late**, 0 out of order; digests equal
  to replay order; fills, final orders and balances equal to a replay of the same records and
  orders (244 fills).
- Delivery (consume time minus received time): 98% under 10 ms, all under 50 ms.
- Hold-back 1 ms: 1 late record, detected and counted, as it should be.

**Live on Gemini, first run** (2026-10-19, Windows laptop, about 2.5 hours with one restart):

- `tools/merge_replay_check.py`: PASS. The 19:00 UTC hour was complete and matched on all three
  pairs (26,007 / 26,702 / 16,530 records, live equal to recorded); 3 complete pair-hours, 6
  partial ones skipped.
- 162,841 level2 and 2,119 trade records released.
- **87 out-of-order records** (about 0.05%). Cause: the ingestor publishes an audit (ADR 0012)
  after holding it up to 2 s for the stream to reach its update ID, stamped with the depth
  response's receive time, so it lands behind diffs received after it. Not a Kafka reorder: the
  producer is idempotent with `acks=all`. Fixed in the ingestor (audits stamped when published).
  Within each stream, recordings and the runner both keep Kafka order, so existing recordings
  replay as live consumers saw them; only an audit's cross-stream position, which changes no
  book, followed the earlier stamp.
- **14 late records** (6 / 5 / 3 by pair), none in the matching hour. An out-of-order audit is late
  whenever the watermark has passed its stamp by the time it is consumed. The runner now logs
  each late record with its type and whether it came while catching up.
- **Watermark lag 0.21 s on every pair**: H plus the 100 ms fetch wait the first version used
  (now 20 ms), plus processing. Above the order delay, which is harmless: see the delay invariant.
- Delivery over 5 s: 604 of 2,119 trades and 1,832 of 114,452 level2 observations. That was the
  catch-up fetches, now left out (see H).

A second run, after the ingestor fix and these changes, is to be recorded here.
