# 0009. Book cold start: staggered snapshot refresh and a time-based seek

- **Status:** Accepted
- **Date:** 2026-10-15

## Context

The order book builder (issue #5) rebuilds each pair's book from `md.level2` (ADR 0007), and a
book can only start from a snapshot. Coinbase sends a level2 snapshot only on subscribe or after a
resync (ADR 0008), and real gaps are rare, so on a healthy connection the only snapshot on the
topic can be the one from when the ingestor connected, days ago. That causes two problems:

- **Retention deletes it.** When the snapshot's segment ages out, a book builder that starts
  later finds nothing to start from and stays empty until the next random gap.
- **Replay is unbounded.** Before then, starting from that snapshot means replaying days of
  updates.

We considered three alternatives:

- **Search `md.level2` backwards for each product's last snapshot.** It has both problems above.
- **Periodic book checkpoints to a compacted topic.** They only help while the book service keeps
  running, so a fresh deploy, a wiped volume or a long outage hits the same wall. They also add a
  second source of truth (book state plus offset) and a serialized book format to version.
- **Having the book service ask the ingestor for a snapshot on start.** This needs a control
  channel into the ingestor, and every restart of a consumer would disturb the feed.

## Decision

- **The ingestor refreshes snapshots on a schedule.** Every `INGEST_SNAPSHOT_REFRESH_SECONDS`
  (1800 s by default; 0 turns it off), each product gets a fresh level2 snapshot. Refreshes run
  one product at a time, `interval / number of pairs` apart (10 minutes for three pairs), so at
  most one book is ever resyncing.
- **Each slot refreshes the product with the oldest snapshot.** A slot whose oldest snapshot is
  younger than the stagger is skipped, for example right after a gap resync refreshed everything.
- **A refresh is a resync of one product.** It publishes a resync marker with
  `reason: "refresh"` for that product only, then unsubscribes and resubscribes level2 for that
  product only, on the same connection. The marker comes first, so updates still in flight are
  flagged `awaiting_snapshot`. Books treat it like any marker: drop the book and wait for the
  snapshot. During the roughly 1 s gap updates are not flowing, and the marker makes the book
  honestly not ready instead of silently stale.
- **Refreshes give way to real resyncs.** They share the resync rate limiter with gap resyncs and
  wait while one is in flight. A gap or disconnect during a refresh takes over, with markers for
  every product. A refresh whose snapshot doesn't arrive within `INGEST_RESYNC_TIMEOUT_SECONDS`
  falls back to a reconnect, as a gap resync does.
- **Explicit retention on `md.level2`.** The ingestor applies `retention.ms`
  (`INGEST_LEVEL2_RETENTION_HOURS`, 24 h by default) and `retention.bytes` per partition
  (`INGEST_LEVEL2_RETENTION_BYTES`, 2 GiB by default) at every start, to new and existing topics.
  It logs an error if the time retention is under 2x the refresh interval or refresh is off. The
  book service makes the same check at startup.
- **The book service seeks by time; it has no checkpoints or committed offsets.** On start it
  assigns the `md.level2` partitions and seeks to `now - 2 x refresh interval` with
  `offsets_for_times`, clamped to the log start. Then it consumes forward. No special replay mode
  is needed: an EMPTY book skips updates until its snapshot, and each snapshot replaces the book.
  If a product reaches the head still EMPTY (the ingestor was down, say), the service re-reads
  that partition from the log start once, then waits for a live snapshot.
- **Metrics:** `simex_ingest_snapshot_refresh_duration_seconds{product_id}` (a histogram) and
  `simex_ingest_last_snapshot_refresh_duration_seconds{product_id}` give the measured recovery
  time per product. `simex_ingest_snapshot_refreshes_total{product_id,outcome}` counts
  completed, timed-out and superseded refreshes. `simex_ingest_resyncs_total{trigger="refresh"}`
  counts refreshes started.

## Consequences

- A restart replays at most about one hour of `md.level2`, from a snapshot that is guaranteed to
  be retained, whatever has happened to other consumers.
- Each book is not ready for about 1 s every 30 minutes, which is about 0.05% of the time and
  never two books at once. The matching engine must treat a non-SYNCED book as "not ready",
  which it has to do for gaps anyway.
- No silent book error can outlive the next refresh, at most 30 minutes.
- Each refresh writes one product's snapshot (several MB for a deep book) to `md.level2`, three
  per 30 minutes. That is small next to the update stream.
- The ingestor's `md.level2` configs are set with a non-incremental update, so other dynamic
  settings on that topic are reset at startup. The topic's configs belong to the ingestor.
- `retention.bytes` cannot be checked against time from config alone. If update volume ever
  pushed one partition past 2 GiB in an hour, size retention could delete the window. At current
  volumes that is far off. The book service can detect it, because the time-based seek then lands
  on the log start.
- Per-product resubscribe relies on Coinbase sending a snapshot when one product is
  resubscribed, as it does for all products (ADR 0008). The live run below confirms it.
- Checkpoints remain an option if measured restart time ever justifies them.
- **To measure:** snapshot size and level2 volume from `tools/feed_probe.py`, and refresh
  recovery time per product from a live run. These go here once measured.
