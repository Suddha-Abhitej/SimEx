# 0013. Recorder: hourly zstd Parquet, exactly once, local retention

- **Status:** Accepted
- **Date:** 2026-10-15

## Context

Issue #6 keeps the raw feed so the same order book, matching and scoring code can run on saved
data, and so a window with a gap can be found and excluded. The issue first said "Parquet on S3,
Iceberg tables"; the plan has since moved storage to the Oracle server's local disk, about 30
days there with a nightly copy to the home server (ADR 0006). What has to hold:

- **Nothing lost, nothing written twice**, across crashes, restarts, a group whose committed
  offsets expired, and Kafka deleting records the recorder never read.
- **Replay is exact.** The bytes Kafka held go back through `simex.marketdata.decode`, so prices
  stay strings (ADR 0004) and the replayed book is the live book.
- **Completeness is visible.** Gaps, resync markers and audits are in-band on md.level2 or on
  md.gaps (ADR 0008, ADR 0012), so recording them in order lets a replay tell which windows are
  incomplete.
- **Live venue only, never committed.** md.* carries Gemini; `research.coinbase.*` is never
  recorded (ADR 0010). Recordings never enter the repository.
- **Small:** 2 cores and 12 GB shared with Kafka and everything else; Windows laptops in
  development.

## Decision

**Consumer.** `python -m simex.recorder` reads exactly `md.level2`, `md.trades` and `md.gaps` (by
name, never a pattern), with its own consumer group `simex-recorder`, auto-commit off and
`auto_offset_reset=earliest`. It assigns every partition itself (one process, no rebalance to
lose buffered rows). A record whose envelope venue isn't the live venue is skipped and counted.

**What a row holds.** `value` is the Kafka record value byte for byte: the whole envelope with the
venue's message spliced in. Header fields are copied into columns for queries and windows:
`received_at_ns`, `exchange_ts_ns`, `venue`, `channel`, `product_id`, `type`
(data, resync, audit, gap, recorder_gap), `snapshot`, `update_first`, `update_last`,
`awaiting_snapshot`, `update_id`, `aligned` and `reason` (audits), plus `kafka_topic`, `partition`,
`offset`, `kafka_ts_ms` and `key`. The header is read without parsing a snapshot's payload. Each
file's Parquet footer also records the schema version, topic, partition, first and last offset,
row count, venue, channel, product and hour.

**Layout: one file per product, channel and UTC hour**, by received time (gap records by
detection time, under `product=all`):

```
data/recordings/venue=gemini/channel=level2|market_trades|gaps/date=YYYY-MM-DD/hour=HH/
    product=BTC-USD/part-<topic>-p<partition>-<first offset>-<last offset>.parquet
```

zstd level 3. While the hour is open the file is a `.tmp`, written one row group every
`RECORDER_ROW_GROUP_SECONDS` (300) or `RECORDER_ROW_GROUP_BYTES` (8 MB), so memory holds at most
one row group per open file. It closes when a record from the next hour arrives for its product,
or `RECORDER_HOUR_GRACE_SECONDS` (60) past the hour once the partition's stream has moved past it
(another product's records) or, for a silent product, once the partition is caught up and the
wall clock says the hour is over (trades can be silent for minutes). Never by the wall clock
alone: catching up on a backlog, every old hour is over, and closing after each fetch would split
an hour into many files. A file is one contiguous run of one partition's offsets for its product; it only rolls
forward, so a clock stepping back can't split a run. A clean shutdown closes open files early, so
an hour can have a second part; the offsets in the names keep them apart. `data/` and
`*.parquet` are gitignored, and a test fails if any is ever tracked.

**Exactly once.**

1. Publish a file: close the writer, fsync the `.tmp`, `os.replace` it to its final name (atomic,
   and the same call on Windows), fsync the directory. Windows can't fsync a directory, so there
   it is skipped (logged once at debug) and NTFS's journal covers the rename.
2. Only then commit. A partition's commit is the lowest offset still in an open file on that
   partition (two products can share one: live, ETH and SOL hash to the same
   partition), or the position if none is open.
3. The files are the source of truth, not the commit. At startup the recorder deletes every
   `.tmp` (never committed, so Kafka still has the records), reads each topic's `retention.ms`
   and scans the file names back that far plus a day (all of them for -1 or unknown), takes the
   highest recorded offset per (topic, partition, product), and skips any record at or below it.
4. A partition with no committed offset (new group, or offsets expired after
   `offsets.retention.minutes`, 7 days by default) starts at the log start, and step 3 drops what
   is already on disk.
5. If the committed offset is below the log start, Kafka deleted records nobody recorded: the
   recorder logs an error, counts `simex_recorder_lost_offsets_total` (an upper bound, since
   other products' records in that range may be on disk) and writes a `recorder_gap` row under
   `channel=gaps`, so a replay knows the window is incomplete.

What a crash leaves, step by step:

| Dies | On disk | After restart |
| --- | --- | --- |
| mid-file | a `.tmp` (or nothing yet) | deleted; its records re-read from Kafka (at most about an hour) |
| after the fsync, before the rename | a complete `.tmp` | the same |
| after the rename, before the directory fsync or the commit | the published file | records re-read and skipped by the scan |
| after the commit | the published file | nothing to redo |

A power loss before the directory fsync can lose the rename, but then the offsets weren't
committed either, so the records are read again. Up to an hour of data lives only in Kafka at any
time; md.level2 keeps 24 hours (ADR 0009).

**Retention.** Every hour, date directories older than `RECORDER_RETENTION_DAYS` (30) are deleted;
today and yesterday never are. Cleanup never stops recording: a directory that can't be deleted
(on Windows, a file held open by a DuckDB query or an Explorer preview raises `PermissionError`)
is logged, counted in `simex_recorder_retention_errors_total` and retried on the next pass, and
the other expired directories are still deleted. The nightly copy to the home server is a later, separate script.
The hook is ready: with `RECORDER_REQUIRE_ARCHIVED=1` an expired day is deleted only once that
script has written `date=.../_ARCHIVED`. Records older than the retention horizon are not
recorded at all, so a long re-read can't recreate a deleted day.

**Replay.** `simex.recorder.replay.read(root, start, end, products=, channels=)` yields
`RecordedEnvelope`s: the exact `value` plus topic, partition, offset and the header columns.
Within a product and channel it is offset order, the order live consumers saw; streams are merged
by received time. Files are read a batch at a time. `scripts/replay_check.py` (logic in
`simex.recorder.check`) replays a window through the unchanged decoder and `OrderBook` and checks
that every book is SYNCED after its first snapshot outside resync windows, that no aligned audit
mismatches or is skipped outside a resync, that the audit rows and the replayed audits agree, and
how many GB/day the window takes.

**Metrics** (port 9103): `simex_recorder_records_total{channel,product_id,type}`,
`simex_recorder_lag_records{topic,partition}`, `simex_recorder_lag_seconds{channel}`,
`simex_recorder_seconds_since_last_write` (row group or file), `..._seconds_since_last_file`,
`..._files_total`, `..._bytes_total`, `..._open_files`, `..._duplicates_skipped_total`,
`..._skipped_total{reason=foreign_venue|unrecordable|past_retention}`,
`..._lost_offsets_total`, `..._retention_deleted_days_total`, `..._retention_errors_total`.

**Alert** (`infra/prometheus/rules/recorder.yml`, with a promtool test in CI): `RecordingLagging`
when md.level2's lag is over 120 s **and** md.level2 has unread records, for 5 minutes. Only
level2 counts: market_trades can be silent for minutes. And unread records are required: if the
ingestor stops, lag grows with nothing waiting, which is the ingestor's alert.

**Plain Parquet now, Iceberg later.** Iceberg adds a catalog and snapshot metadata to keep
consistent, which one writer on one disk doesn't need yet. The hive-style layout and stable schema
can be registered in an Iceberg table later with PyIceberg's `add_files`, without rewriting a
file. DuckDB reads the files directly today (README).

**Sizing.** Measured at 28 msg/s (ADR 0011): about 2.4 M messages and 2.06 GB/day of raw JSON,
plus about 0.6 GB/day of envelope headers. Gzip took the raw feed to 0.29 GB/day, so the estimate
is 0.4 to 0.6 GB/day with zstd, 12 to 18 GB for 30 days on the 150 GB data disk; the first live
hour replaces the estimate. Memory: one row group per open file (at most about nine files open)
plus pyarrow and aiokafka; a synthetic catch-up measured about 85 MB above the interpreter. The
container limit is **384 MB**, checked against the live run.

## Consequences

- A replay sees what the live consumers saw, byte for byte, and gaps and resyncs are in the data,
  so incomplete windows can be found rather than guessed.
- Up to an hour can be only in Kafka; a crash costs a re-read, never data, as long as the
  recorder is back within Kafka's retention (24 h for md.level2).
- Startup scans file names, not file contents: thousands of names a day, so it is fast. A file
  someone renames or edits by hand would confuse the scan.
- `value` repeats the header that the columns also hold; it compresses well and keeps replay
  exact, and dropping it later would need a schema version 2.
- One file per hour means up to an hour before a recording can be read. Replaying the current
  hour needs Kafka.
- The archive copy, Iceberg and the home server's playground are later work; nothing here blocks
  them.

## Measured

Before live validation, against a local Kafka with a synthetic 1.4-hour feed (15,331 records):
`kill -9` mid-hour, restart, `SIGTERM`, then the consumer group deleted and new records added.
Every stage ended with each Kafka record on disk exactly once and byte-identical (15,336 of
15,336, 0 duplicates); the deleted group's restart re-read and skipped all 15,331 by the file
scan. `replay_check` on that recording: PASS, all books SYNCED, every aligned audit matched.

**Bug found and fixed before review.** The first version closed a file once the wall clock passed
its hour plus the grace. Catching up on a backlog, that closed the file after every fetch: a test
that records the whole synthetic feed in batches of 200 got 216 files instead of 19. Files now
close on the stream's position, or on the wall clock only when the partition is caught up; the
same test gets 19, and so did a second real-Kafka run with `kill -9` and a restart.

**Live validation, 2026-10-16** (Gemini, Windows laptop). The recorder started with a new group and
caught up on the 36 hours Kafka still held in about 80 seconds, writing one file per product,
channel and hour. Then it recorded 17:00 to 18:00 UTC live:

- Each hour-17 file covers a contiguous offset range, and its rows add up to that range exactly
  (87,919 rows, 6 files); duplicates on disk 0, lost offsets 0.
- `replay_check`: PASS. All three books SYNCED from their first snapshot on, 0 crossed; 90 audits,
  every audit row equal to its replay, 34 matched, 0 mismatched.
- Disk: 23.0 MB for the hour, **0.553 GB/day**, inside the estimate (about 17 GB for 30 days).
- Memory: peak **258 MB** working set during the 36-hour catch-up, 41 MB steady. The 384 MB
  limit holds, with the catch-up as the case that sets it.
- `simex_recorder_lag_seconds{channel="gaps"}` reads hours when no gap has happened, because it is
  the age of the channel's last record. That's expected; `RecordingLagging` uses level2 only.
