# 0008. Gap resync: in-band markers on md.level2, gap records on md.gaps

- **Status:** Proposed
- **Date:** 2026-10-15

## Context

The ingestor detects gaps in Coinbase's `sequence_num` (ADR 0007) but only logged them. A lost
level2 update leaves every downstream order book silently wrong, and lost trades skew anything
computed from them. Gaps must be made visible and recoverable for every consumer.

Two facts shape the design:

- **What `sequence_num` counts is disputed.** Coinbase's WebSocket overview says sequence numbers
  are "increasing integer values for each product". Its AsyncAPI spec puts `sequence_num` on
  every message envelope. Our live run (120s, 2,349 messages, one tracker across level2,
  market_trades and heartbeats for three products) saw zero gaps; a per-product counter would
  have produced hundreds of false ones. Two independent captures in nautilus_trader issue #5100
  saw the same single per-connection counter. **We keep per-connection tracking.** A gap
  therefore says some message was lost, not which product or channel it belonged to.
- **How to get a fresh snapshot.** A new connection always starts level2 with a `snapshot`
  event. The docs do not say that unsubscribing and resubscribing on the same connection sends
  a new one; third-party testing says it does, and that a duplicate subscribe does not. Level2
  `new_quantity` values are absolute, so a snapshot fully replaces a book.

## Decision

- **Markers in-band on md.level2.** On a gap, before the message that revealed it, publish one
  `{"type": "resync"}` envelope per subscribed product, keyed by product_id, so it lands in
  order on that product's partition. Consumers discard the book and wait for the next snapshot.
  The envelope becomes version 2 with a `type` field (`data` or `resync`); consumers must skip
  types they don't know.
- **Keep publishing during the resync.** Level2 data envelopes carry `awaiting_snapshot`, true
  between a product's marker and its next snapshot. The recorder keeps the raw feed complete;
  book builders ignore flagged updates.
- **Resubscribe first, reconnect as fallback.** Unsubscribe and resubscribe level2 for all pairs
  on the same connection. If any product has no snapshot within `INGEST_RESYNC_TIMEOUT_SECONDS`
  (10s), reconnect. If live runs show the fallback is what actually recovers, switch to
  reconnect-only.
- **Rate limit:** at most one resubscribe per `INGEST_RESYNC_MIN_INTERVAL_SECONDS` (5s); later
  gaps are deferred, not dropped, and counted. A gap while a resubscribe is in flight needs no
  new one unless some book had already recovered.
- **Every gap is recorded on md.gaps** (keyed `gap`): products and channels possibly affected
  (all subscribed, since the counter is per connection), expected and received sequence, missing
  count, the exchange and receive times of the last message before and the first message after,
  the resync id and action, and whether the fault injector caused it. Disconnects are recorded
  too (`reason: disconnect`), since anything sent while disconnected is lost. Lost trades cannot
  be recovered; the record is how the evaluator and recorder know a window is incomplete.
- **Disconnects invalidate books** with the same markers; the new connection's snapshots recover.
- **Alarm on semantics drift:** more than `INGEST_GAP_ALERT_PER_MINUTE` (10) real gaps per minute
  logs an error, and `simex_ingest_feed_gaps_last_minute` exposes the rate for alerting.
- **Fault injector:** `SIMEX_INGEST_DROP_EVERY=N` drops every Nth received message, off by
  default and logged loudly when on. Real gaps are rare (level2 guarantees delivery), so this is
  how tests and live runs exercise the resync path. Injected gaps are labelled and never trip the
  alarm.

## Consequences

- Book builders get a simple rule: on `resync`, drop the book; rebuild from the next snapshot.
- Every gap costs a resubscribe of all three pairs' snapshots (several MB on Kafka), because a
  per-connection counter cannot say which product lost data. The rate limit bounds this.
- Downstream code must tolerate v1 and v2 envelopes and unknown `type` values.
- `simex_ingest_time_since_last_snapshot_seconds` and `simex_ingest_awaiting_snapshot` give stale
  data guards a per-product signal.
- If Coinbase really moves to per-product counters, we would see false gaps and constant resyncs;
  the alarm is there to catch that.
- Whether resubscribe yields a snapshot rests on third-party evidence until the live injector run
  confirms it.
