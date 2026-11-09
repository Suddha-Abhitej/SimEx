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

## Measured 2026-11-09: default startup fails in the 25k–75k bracket

**At 25,000 lifetime orders all three default attempts started. At 50,000 one timed out; at
75,000 and 100,000 all three timed out.** This is the synthetic filled-history profile below,
on the **x86 Linux devcontainer, not Oracle ARM64**. The first sampled failure is 50k; all
attempts failed by 75k. This is a startup availability limit, even though retained memory is
only hundreds of MiB at those sizes. The shared two-core target should reach the cliff sooner;
its actual threshold remains unmeasured.

| Lifetime orders | Default successes | Repetitions 1 / 2 / 3 |
| ---: | ---: | --- |
| 1,000 | 3/3 | ok / ok / ok |
| 10,000 | 3/3 | ok / ok / ok |
| 25,000 | 3/3 | ok / ok / ok |
| 50,000 | 2/3 | timeout / ok / ok |
| 75,000 | 0/3 | timeout / timeout / timeout |
| 100,000 | 0/3 | timeout / timeout / timeout |
| 1,000,000 | 0/3 | timeout / timeout / timeout |

Each repetition is a fresh executable process. Grouping by repetition position gives these
observed brackets; it does not establish a universal exact cutoff or a failure probability:

| Repetition | Largest sampled default success | First sampled timeout failure |
| ---: | ---: | ---: |
| 1 | 25,000 | 50,000 |
| 2 | 50,000 | 75,000 |
| 3 | 50,000 | 75,000 |

The combined observed onset bracket is **more than 25k through 75k orders**, with a mixed
result at 50k. Successful total restore wall time can exceed 500 ms: it is the load call that
has the timeout. A diagnostic success never upgrades a default failure. All raw aggregate
repetitions, including failed/censored runs, are in the
[measurement JSON](measurements/restore-cost-2026-11-09.json).

Assuming an initially empty account and this same workload, the bracket implies the following
time **until startup can fail**, not a memory-budget forecast. These daily rates are illustrative
assumptions, not observed bot traffic; existing history shortens the time:

These rates count new persisted order rows, excluding idempotent replays and local skips.

| Assumed new orders/day | Startup-failure onset bracket |
| ---: | --- |
| 100 | More than 250 through 750 days (roughly 8–25 months) |
| 1,000 | More than 25 through 75 days |
| 10,000 | More than 2.5 through 7.5 days |

### Conditions

- Driver commit: [`858bb5d`](https://github.com/Suddha-Abhitej/SimEx/commit/858bb5d261727dd5100b7d2b2c9a31f3e30d07db), committed before the sweep.
- Linux devcontainer on an x86 laptop under WSL2; AMD Ryzen 9 8945HS, 16 visible logical CPUs.
- Python 3.12.11; Postgres 16.10 (Debian 16.10-1.pgdg13+1), separate instance/port/volume, **1 GiB memory cap and no container swap allowance**.
- Visible RAM 7.385 GiB; available at sweep start 2.281 GiB. Devcontainer cgroup memory/CPU limits reported `max` / `max 100000`.
- Worker RSS cap 2 GiB, low-memory floor 256 MiB; declared seed disk budget 64 GiB; volume free space checked at about 941 GiB.
- `perf_counter` nominal resolution 1.000000000 ns; smallest observed step 50 ns. Durations are monotonic, with no absolute feed-lag claim.
- Kafka, the normal Postgres, Valkey, Prometheus and the editor remained running. No test suite ran concurrently. Postgres autovacuum and caches were uncontrolled; seeded data is not a cold-cache benchmark.
- No Oracle ARM64 run, storage-medium benchmark or CPU-isolated run was made. Do not copy this count as an Oracle capacity guarantee.

### Extended diagnostic, never successful default startup

The following 120-second-timeout runs expose cost after default recovery has already failed.
Times and RSS deltas are median (min–max) across successful diagnostics only; the last column
is the largest observed startup peak. All three statuses remain explicit. No p99 is inferred.

| Orders | Diagnostic statuses | Load ms | Total restore ms | Construction ms | RSS delta MiB | Max peak MiB |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | ok / ok / ok | 15.082 (14.226–15.106) | 22.089 (20.465–22.691) | 6.983 (6.238–7.610) | 3.000 (3.000–3.000) | 38.508 |
| 10,000 | ok / ok / ok | 75.574 (74.866–77.644) | 159.274 (144.924–163.032) | 84.408 (69.350–85.388) | 29.566 (29.520–29.703) | 64.605 |
| 25,000 | ok / ok / ok | 188.018 (183.138–200.432) | 420.789 (397.640–439.168) | 232.771 (214.502–238.737) | 78.422 (78.414–78.465) | 111.922 |
| 50,000 | ok / ok / ok | 378.379 (368.180–394.114) | 910.503 (876.960–917.549) | 523.435 (508.780–532.124) | 154.469 (154.441–154.484) | 189.242 |
| 75,000 | ok / ok / ok | 581.829 (572.511–647.766) | 1442.968 (1438.938–1499.526) | 861.139 (851.760–866.427) | 228.156 (227.941–229.062) | 265.152 |
| 100,000 | ok / ok / ok | 791.944 (765.557–1208.635) | 2109.270 (1967.067–2750.584) | 1317.326 (1201.511–1541.949) | 307.082 (306.906–307.098) | 345.422 |
| 1,000,000 | rss_limit / rss_limit / rss_limit | unavailable | unavailable | unavailable | unavailable | unavailable |

All three million-order diagnostics and its byte pass hit the 2 GiB RSS budget. There are
zero completed diagnostic observations at that size; neither cost nor response bytes are
extrapolated. The seed counts and three default timeouts are known. This censoring does not
establish an Oracle memory threshold.

### Returned rows and actual response bytes

Counts below come from completed byte passes. A sequence is a scalar result in addition to the
row sets; stored posting counts are shown separately so they are not confused with returned
balance aggregate rows. Incomplete passes remain unavailable.

| Seed orders | Seed fills / postings | Returned orders / reservations / balances | Sequence scalar | Server response bytes | Byte-pass status |
| ---: | ---: | --- | ---: | ---: | --- |
| 1,000 | 968 / 4,842 | 1000 / 16 / 2 | 969 | 409590 | ok |
| 10,000 | 9,968 / 49,842 | 10000 / 16 / 2 | 9969 | 4072591 | ok |
| 25,000 | 24,968 / 124,842 | 25000 / 16 / 2 | 24969 | 10177591 | ok |
| 50,000 | 49,968 / 249,842 | 50000 / 16 / 2 | 49969 | 20352591 | ok |
| 75,000 | 74,968 / 374,842 | 75000 / 16 / 2 | 74969 | 30527591 | ok |
| 100,000 | 99,968 / 499,842 | 100000 / 16 / 2 | 99969 | 40702592 | ok |
| 1,000,000 | 999,968 / 4,999,842 | unavailable / unavailable / unavailable | unavailable | unavailable | rss_limit |

### Adjacent-point marginal costs

These are differences of diagnostic medians and complete byte counts, divided by additional
orders. They are descriptive local slopes, with allocator/cache effects, not a fitted model.
Negative noise would be retained; missing/censored measurements are not extrapolated.

| Orders interval | Load ns/additional order | Restore ns/additional order | RSS bytes/additional order | Response bytes/additional order |
| --- | ---: | ---: | ---: | ---: |
| 1,000–10,000 | 6721.360 | 15242.734 | 3095.211 | 407.000 |
| 10,000–25,000 | 7496.240 | 17434.357 | 3415.245 | 407.000 |
| 25,000–50,000 | 7614.477 | 19588.582 | 3189.637 | 407.000 |
| 50,000–75,000 | 8137.998 | 21298.592 | 3090.678 | 407.000 |
| 75,000–100,000 | 8404.581 | 26652.074 | 3310.387 | 407.000 |
| 100,000–1,000,000 | unavailable | unavailable | unavailable | unavailable |

### 100k terminal-unfilled control

Both profiles retain the same 16 active and 16 rejected orders. The control replaces the other
99,968 filled orders with cancelled/unfilled ones, leaving only the seed postings. It separates
order fetch/map work from the posting fold; it is not a second capacity claim.
The control also changes status, filled fields and wire widths. Its timing difference is not
a measurement of posting-fold time alone.

| Profile | Default successes | Default statuses | Diagnostic load ms | Diagnostic total ms | RSS delta MiB | Response bytes |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| filled | 0/3 | timeout / timeout / timeout | 791.944 (765.557–1208.635) | 2109.270 (1967.067–2750.584) | 307.082 (306.906–307.098) | 40702592 |
| unfilled | 0/3 | timeout / timeout / timeout | 719.813 (671.481–722.622) | 1838.424 (1707.908–1873.498) | 297.797 (297.719–297.895) | 40202697 |

### What this implies

The default query deadline is the first demonstrated availability limit. Memory is secondary:
at 100k the filled profile retains about 307 MiB more RSS, yet default startup already fails.
Construction also adds substantial time after the timed load. A memory-only cache bound would
not by itself answer the timed recovery problem. Future design work must preserve durable
historical idempotency while bounding the state fetched and constructed at startup, and must
measure the balance/posting fold as well. This experiment chooses no cache bound, expiry,
purge or recovery change. #107 stays open.

## Reproduce safely

Use Linux and a **separate Postgres 16 instance and volume** for the large seed, not the trading
instance. Supply a maintenance DSN selecting the neutral `postgres` database explicitly, via a
named environment variable. The tool never reads `.env` or `EXCHANGE_LEDGER_DSN`. For example,
after setting that variable privately for the disposable instance:

```sh
python tools/measure_restore_cost.py \
  --maintenance-dsn-env RESTORE_BENCH_MAINTENANCE_DSN \
  --rss-budget-gib 2 --seed-disk-budget-gib 64 \
  --orders 1000 10000 25000 50000 75000 100000 1000000 --unfilled-comparison \
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
measurement. No pre-existing database is dropped. Stopping the parent with an uncatchable kill can
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


## Upgrade

ADR [0027](adr/0027-live-order-recovery-and-durable-idempotency.md) bounds durable order recovery.
The original measurements above describe eager recovery before that fix. Recovery now retains
live orders only, uses durable historical key lookup, and has a five-second per-account load
budget; ordinary ledger calls remain at 500 ms. The account posting fold remains linear.

Migration 009 creates a partial index. It changes no columns or audit history and is transactional.
Index creation scans history and may exceed the migration runner's existing five-second allowance
on a large ledger. Stop the exchange writer and prebuild it with an owner maintenance connection
before starting the new version when necessary:

```sh
psql "$EXCHANGE_LEDGER_DSN" -v ON_ERROR_STOP=1 <<'SQL'
CREATE INDEX IF NOT EXISTS orders_live_account_idx ON orders (account_id, order_id)
WHERE status NOT IN ('filled', 'cancelled', 'expired', 'rejected');
SQL
```

Use the ledger's existing schema/search path. Do not change the predicate or wrap CONCURRENTLY
inside the migration runner. A failed transactional build rolls back; verify a pre-existing index
has the expected definition. The subsequent `IF NOT EXISTS` migration records its version without
rebuilding the prebuilt index. Do not put credentials in committed commands or reports.

The current harness reports five-second `default`, 500 ms `compatibility`, and 120-second
`diagnostic` attempts separately. Diagnostic success is never successful default startup. The
success criterion, set before measuring, is three successful default attempts at 1M lifetime
orders with 16 live orders. If the posting fold binds earlier, publish that limit and carry it
into a successor issue; do not conceal it with another timeout increase.
