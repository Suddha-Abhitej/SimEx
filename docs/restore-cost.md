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


## Bounded recovery measurement — 2026-11-09

The criterion was recorded before measurement in ADR 0027 and on #107: three default attempts at
1M lifetime orders, 16 live orders, within the five-second load budget. **Met.**
The default loads took **267.618–665.132 ms** (median 273.776 ms), with a maximum observed
startup worker peak of **41.543 MiB** and **8,971 PostgreSQL response bytes**. All three original
500 ms compatibility attempts timed out at 1M. The live HTTP capacity rerun was unavailable;
that limitation is reported separately below.
This is the **x86 Linux devcontainer on the Ryzen 9 8945HS laptop, not Oracle ARM64**. The shared
two-core target has less expected headroom; its cutoff remains unmeasured.

Driver/recovery revision: `fa71161da0a6717c333a92d5d822e89c9187b3dd`.
The [complete aggregate JSON](measurements/bounded-restore-2026-11-09.json) preserves every fresh
worker result, including the original 500 ms compatibility mode. PostgreSQL is pinned to 16.10
on a separate Docker instance/volume/port with a 1 GiB memory cap and no container swap. The
worker cap is 2 GiB, low-memory floor 256 MiB, seed disk budget 64 GiB. Normal Kafka, Postgres,
Valkey, Prometheus and the editor remained running; no test suite or other benchmark ran during
the timed sweep. Two insertion-statistics reads checked progress during seeding only. Caches,
autovacuum and co-load were uncontrolled; this is seeded data, not a cold-cache benchmark.
CPU/RAM, timer resolution, smallest observed step and software versions are in the JSON.

The same filled-history points and 100k unfilled control as the earlier baseline were used.
Each order carries 128 bytes of synthetic rationale. Three fresh executable workers run per
mode, with a separate untimed byte pass. Historical same/different-request probes run after
timing/RSS and byte capture; no historical object is retained to make the checks pass. Terminal
histories remain in SQL. Live orders and keys restored are **16 each** at every main point.

Times below are milliseconds. Load is median (min–max) over the three five-second default runs;
total restore and statement durations are medians. Statement time includes the client round
trip/decoding, not only server CPU. RSS delta is the process delta, not shallow object size.

| Orders | 500 ms states | 5 s states | Load ms (min–max) | Total restore ms | Live fetch ms | Posting fold ms | RSS delta bytes | Response bytes | Index migration ms |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | ok / ok / ok | ok / ok / ok | 7.183 (6.388–7.365) | 7.452 | 2.228 | 2.287 | 0 | 8,969 | 22.385 |
| 10,000 | ok / ok / ok | ok / ok / ok | 13.031 (12.776–13.908) | 13.216 | 2.173 | 8.370 | 131,072 | 8,969 | 22.096 |
| 25,000 | ok / ok / ok | ok / ok / ok | 22.934 (21.277–23.026) | 23.130 | 2.426 | 17.420 | 131,072 | 8,969 | 27.332 |
| 50,000 | ok / ok / ok | ok / ok / ok | 27.552 (24.436–27.810) | 27.871 | 2.371 | 22.515 | 131,072 | 8,969 | 69.505 |
| 75,000 | ok / ok / ok | ok / ok / ok | 34.473 (29.843–35.755) | 34.717 | 2.728 | 29.102 | 131,072 | 8,969 | 37.474 |
| 100,000 | ok / ok / ok | ok / ok / ok | 41.443 (37.469–44.463) | 41.688 | 2.578 | 36.511 | 0 | 8,969 | 37.608 |
| 1,000,000 | timeout / timeout / timeout | ok / ok / ok | 273.776 (267.618–665.132) | 273.967 | 2.665 | 268.358 | 131,072 | 8,971 | 1147.996 |

Modes ran in order: compatibility, default, diagnostic. At 1M the first default load took
665.132 ms and the later two took about 268–274 ms. Those later sub-500 ms observations do not
change the three earlier compatibility timeouts. Cache and co-load effects were uncontrolled;
this is not an exact or universal 500 ms failure probability/cutoff.

The one-time upgrade was measured after dropping the index and migration-009 marker from the
synthetic seed, then applying the real transactional migration. Its duration is reported
separately; it is not part of recovery. The owner prebuild path remains documented above for
larger/slower deployments. No global migration or ordinary-operation timeout was increased.

All three 120-second diagnostic runs also completed at every point; they do not change any
default or compatibility verdict. Maximum observed million-order startup peak RSS was **41.543 MiB** in default mode;
the earlier eager-recovery million-order diagnostics were censored by the 2 GiB cap.

The 100k terminal-unfilled control returned 8902 bytes, restored 16 live orders,
and passed all three default attempts (load median 6.046 ms;
posting-fold median 1.622 ms). It has seed postings only. This separates
the remaining posting work from fetching historical orders; no fitted linear forecast is made.

### Submission-read cost

Each default worker additionally times 100 operations/case after recovery measurements. These
are writer/read microbenchmarks on seeded, uncontrolled database caches, **not HTTP throughput**.
The first missing-key query on a fresh connection includes statement preparation; it is not a
cold-storage test. The table shows the median of three worker medians and the range of their
nearest-rank sample p99 values, not a pooled p99 or an acceptance ceiling. No writes/admissions
occur in these replay/miss probes; balances, sequence and retained live counts are checked.

| Million-order case | Samples/worker | Median of medians ms | Worker sample p99 range ms |
| --- | ---: | ---: | ---: |
| Indexed unused-key lookup | 100 | 0.560 | 0.711–0.827 |
| Resident live replay | 100 | 0.003 | 0.009–0.030 |
| Terminal replay with hydration | 100 | 0.612 | 0.820–1.082 |
| Historical key conflict | 100 | 0.630 | 0.910–1.029 |

Every genuinely new durable submission pays the indexed lookup before admission. Existing live
replays avoid it. The 6.969 ms historical commit median is not read latency; the earlier 44–47
accepted/s figure remains tied to its own workload and conditions. Partial-index maintenance
also has a write cost. See the separately labelled load-protocol outcome below rather than
inferring a new capacity figure from this microbenchmark.

### Remaining limits

The order-map problem is removed in durable mode: terminal objects/keys are not recovered or
accumulated. The posting fold remains history-dependent, live-order memory still scales with
the active set, and the bounded event journal/caller-held responses consume separate memory.
There is no claim of constant-time recovery or an Oracle threshold. No-DSN development mode
still retains its lifetime history because it has no durable source for old keys. A larger
timeout was not used to turn an observed failing five-second point green.


### Rationale and live-set sensitivity

[Separate sensitivity runs](measurements/restore-sensitivity-2026-11-09.json), revision
`1fbce61a9834f71adcc0ee2e2fc284ae1cd16e2d`, used the same database limits, fresh-worker watchdog,
three default attempts and untimed byte pass. They followed the main sweep without concurrent
tests or load. The long rationale is deterministic synthetic ASCII (8,192 characters/bytes),
not model output or recorded data. The larger live set is a different workload, not a substitute
for the 16-live-order success criterion.

| Profile | Lifetime / live orders | Rationale bytes/order | Default successes | Load median ms | RSS delta median bytes | Response bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| long_rationale | 1,000 / 16 | 8,192 | 3/3 | 8.096 | 262,144 | 137,993 |
| many_live | 10,000 / 1,024 | 128 | 3/3 | 21.483 | 3,801,088 | 493,819 |

Live state and its rationale still cost memory and response bytes. These observations do not
establish an unbounded live-order capacity, a Unicode-text memory bound, or an ARM64 limit.
Reproduce with the same explicit neutral maintenance DSN environment variable and budgets:

```sh
python tools/measure_restore_sensitivity.py --maintenance-dsn-env RESTORE_MAINTENANCE \
  --rss-budget-gib 2 --seed-disk-budget-gib 64 --json /tmp/restore-sensitivity.json
```


### Live load-protocol outcome: unavailable

The unchanged `tools/load_test.py` (#24) protocol was attempted after the recovery/sensitivity
runs, using a fresh generated `simex_load_107_*` database on the separate instance, its own
account and ephemeral ports. It could not obtain a fresh book from the normal Kafka source
within the 20-second readiness window. **No load phase ran and no new HTTP acceptance ceiling
or commit-latency sample is claimed.** No ingestor was started or gate relaxed to produce a
number. The generated database was dropped in `finally`; the normal trading ledger was unused.

| Measurement | Current result |
| --- | --- |
| Durable HTTP acceptance capacity / change versus 44–47/s | **Unavailable**: no fresh book; the earlier result retains its own conditions |
| Commit latency versus the earlier 6.969 ms median | **Unavailable** in this attempted live run |
| Absolute feed lag | **Unavailable**: no completed run and host clock synchronization unverified |

The [attempt metadata](measurements/order-entry-load-2026-11-09.json) records that limitation.
The measured 0.560 ms unused-key lookup is additional serialized work, not a new throughput
measurement. Its interaction with partial-index writes and feed processing remains to be
remeasured with a fresh live feed, under the [existing isolated load protocol](load-test.md).
Do not infer that throughput is unchanged, or replace the earlier measured table with an estimate.
