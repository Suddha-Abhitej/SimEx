# Restore cost and the default-startup cliff (#107)

The primary question is **at what lifetime order count does default recovery stop starting?**
`SyncLedger.load_account` calls `_call` with its 500 ms hard timeout, once. On timeout it cancels,
settles and raises; there is no retry at this boundary. `record_fill`'s three attempts do not
apply. `AccountWriter.restore` propagates the failure, so the exchange cannot start. The timeout
bounds the load call (four queries and returned-state construction), not the later construction
of `Order` and idempotency maps. A successful total restore can exceed 500 ms.

This tool measures the existing contract; it changes no production code. Historical keys are
needed forever under [ADR 0014](adr/0014-order-model-and-fill-model.md): identical requests must
return their original order and different requests must be refused, including after completion
and restart. Measurement does not choose a cache, eviction policy or expiry. See
[#107](https://github.com/Suddha-Abhitej/SimEx/issues/107) and
[ADR 0017](adr/0017-event-sourced-ledger.md).

## Reproduce safely

Use Linux and a **separate Postgres 16 instance and volume** for the large seed, not the trading
instance. Supply a maintenance DSN selecting the neutral `postgres` database explicitly, via a
named environment variable. The tool never reads `.env` or `EXCHANGE_LEDGER_DSN`. For example,
after setting that variable privately for the disposable instance:

```sh
python tools/measure_restore_cost.py \
  --maintenance-dsn-env RESTORE_BENCH_MAINTENANCE_DSN \
  --rss-budget-gib 2 --seed-disk-budget-gib 64 \
  --orders 1000 10000 100000 1000000 --unfilled-comparison \
  --json /tmp/restore-cost.json
```

Check the database volume's free space yourself before declaring `--seed-disk-budget-gib`;
this is an explicit operator budget, not a remote-filesystem probe. The tool also checks local
free disk and conservatively reserves 32 KiB/order plus 1 GiB for seeding/index/WAL headroom;
that reservation is a safety allowance, not a measured row size. Memory headroom is checked
before seeding. The RSS budget must fit available memory plus a 256 MiB safety margin.

Only a random `simex_restore_<uuid4 hex>` database is created. Maintenance connections explicitly
select and verify `postgres`. Seed and worker paths refuse any other database-name pattern;
`simex` and `simex_test` cannot match. Immediately before cleanup a literal assertion checks the
exact pattern, followed by `DROP DATABASE IF EXISTS ... WITH (FORCE)` in `finally`. An already
absent database is harmless. A role without CREATE DATABASE gets a clear skip, not a failed
measurement. No existing database is dropped. Stopping the parent with an uncatchable kill can
still leave its generated database; inspect and remove only that owned generated name manually.

A fresh executable process runs each of three default-timeout trials and three **120-second
extended diagnostic** trials per size. These diagnostic successes are never evidence of
successful default startup. A separate fresh worker performs the untimed byte pass. The
watchdog kills/reaps workers exceeding their RSS budget, low-memory margin or wall budget
(10 seconds for default, 150 seconds otherwise). Censored points remain explicit; they are not
counted as timeout evidence or successes. `timeout` and `ledger_halted` mean the default restore
failed; other error/censor statuses remain distinct. Partial reports have `complete: false`.
JSON writes are atomic; inspect completeness and every status before interpreting a table.

## Workload and measurements

At each size, 16 orders remain pending with real reservations and 16 are rejected. The rest are
terminal, with one synthetic taker fill each in the main profile. A separate 100k comparison has
terminal unfilled/cancelled orders. Keys have fixed width and rationales are 128 ASCII bytes.
Domain orders, fills, fees and postings use production helpers; bounded COPY batches seed the
same ledger schema outside timing. Counts and exact double-entry conservation are checked.
Small synthetic seeds are compared to the real adapter in tests. Measurements verify restored
order/key counts and original-key replay/different-request refusal, including rejected orders.

Each timing worker connects directly, with no relay or instrumentation changes in production.
A delegating probe measures the real `load_account` and retains counts only. Total restore wall
time uses `perf_counter_ns`; construction/projection time is its difference from load time.
Returned row counts cover orders, reservations, balance aggregates and the sequence scalar;
these are **returned rows**, not all postings scanned to produce the aggregates. Seed counts
also report the stored tables, so the two are distinguishable.

Baseline RSS is sampled from `/proc` after imports, connection setup and garbage collection.
Post-restore RSS is sampled after another collection; the difference includes allocator effects
and is not a deep object-size census. Peak RSS (`ru_maxrss`) is separate: raw rows and restored
objects can coexist during startup. Fresh workers avoid inheriting seed allocations.

The byte pass forwards opaque PostgreSQL traffic over loopback and counts server-to-client
payload bytes **after authentication, around restore only**. It retains no packets or rows.
Counts include protocol framing (and TLS records when present), exclude authentication and
TCP/IP headers, and are neither table sizes nor Python/JSON size estimates. Relay timings are
never published. An incomplete byte pass is unavailable, never zero.

Results are seeded, with uncontrolled caches and co-load; no cache flush or cold-cache claim.
Report all three statuses and ranges/medians, not a p99 from three runs. Adjacent-point marginal
costs preserve noise and nonlinearities; no linear fit is forced. Memory and the default-timeout
cliff are different limits. The two-core, 12 GiB Oracle target shares resources with Kafka,
Postgres, Valkey, Prometheus and Grafana; an x86 devcontainer threshold is not an ARM64 capacity
measurement, and the shared two-core target should reach the startup cliff sooner.

Only counts and system measurements belong in this document, tool output or issue comments
([ADR 0010](adr/0010-personal-use-and-gemini-market-data.md)). No prices, amounts, individual
order/fill identifiers or timestamps, rationale text, feed data, tokens, DSNs or row dumps.

## Measured results

Results are added after committing the measurement driver, so its exact revision identifies the
code used in the local sweep. No ARM64 measurement is available.
