# Measuring the private API

[load_test.py](../tools/load_test.py) uses the same synchronous Python SDK as a bot. It launches
its own exchange process, ledger writer, private API and loopback metrics endpoint. It consumes
the normal Gemini Kafka topics. Only the ingestor connects to the venue; the driver never does.
Everything uses simulated money. No model, new dependency, or public endpoint is involved.

## Isolate the destination first

Never use the trading ledger or an existing API endpoint. Create an **empty, dedicated database**
whose name starts with `simex_load_`, and choose a fresh `load_` account:

```sh
docker compose exec postgres createdb -U simex simex_load_20261106
python tools/load_test.py \
  --dsn 'postgresql://simex:<password>@127.0.0.1:5432/simex_load_20261106' \
  --account load_20261106 \
  --conditions 'Linux devcontainer on x86 laptop; local Docker Postgres; describe CPU/RAM/storage' \
  --json /tmp/simex-load-summary.json
```

Replace the password placeholder locally; never commit it or a DSN. Start the normal Gemini
ingestor against the same Kafka first. `--kafka` and `--ingest-metrics` select its local bootstrap
address and metrics URL. The default metrics URL is `http://127.0.0.1:9101/metrics`.

The driver does not read `.env`, infer a DSN, or attach to an arbitrary API. It rejects `simex`,
`simex_test`, other database names, URL options that could redirect a connection, and existing
account/ledger history, including in other schemas. Checks precede migrations and deposits.
A database-scoped advisory lock prevents two load runtimes using the same destination. Ports and
API credentials are private to the new process. It disables baseline agents and leaves existing
services/accounts alone. It never resets or deletes history: retain the load database for local
inspection, and use another empty database for another run.

## Read the sweep, not a single throughput number

Defaults sweep 10/25/50/75/100 offered orders/s with 1/4/8 concurrent SDK connections to **one
account**, not multiple tenants. Each point has a 3-second server warm-up, 15-second measured
window and 3-second drain after warm-up and measurement. SDK objects are constructed outside the
window; their first HTTP connection is included. Workers each own a client. Idle connection
setup is not a claim about matching throughput. The runtime/account are shared across phases,
so history, position and fee effects accumulate; warm-up intentions are excluded from reported
order cohorts but are real simulated orders. Use explicit `--rates`, `--clients`, `--seconds`,
`--warmup-seconds`, `--drain-seconds` and `--maker-seconds` to reproduce/refine a sweep.

The workload is minimum-size BTC market buys by default (`--quantity 0.00001`). Its finite bounds,
seed and explicit quantity/notional/position limits are recorded. It never changes safety or fill
policy to reach a target, tops up the account, or resizes a refused intention. A run is limited to
50,000 offered sweep intentions. Offered slots with every worker busy are **not sent**; they are
counted separately from exchange refusals. A client-limited plateau is not the server ceiling.

The report separates offered intentions, HTTP dispatches/retries, accepted/refused/uncertain
responses, and committed order/fill identities. SDK transport retries keep the same idempotency
key; an HTTP refusal is never retried. Refusals are grouped by stable reason prefix, alongside
accepted throughput. After draining, the runtime stops; a read-only SQL snapshot then resolves
every measured client ID, including transport-uncertain ones. An accepted acknowledgement or
observed fill absent from the ledger invalidates the measurement. An early validation refusal
legitimately has no orders row. Final status can differ from an acceptance acknowledgement because
the gate is checked again at effect time.

Only acknowledgements completed within the measurement window **and** confirmed in SQL contribute
to its durable acceptance rate. Orders with committed fills emitted within that window have a
separate rate. Drain completions are not credited back to the window. Cohort fill counts and
latencies include later drain fills and always show sample counts. Stream gaps, unexpected closes
and halts abort rather than silently resume; fills are acknowledged only after recording them.

## Keep the clocks and latency definitions separate

- **Tick-to-committed-fill:** ingestor receive timestamp of the causal market record to fill
  emission after its ledger commit. A tool-only observer verifies the maker fill's timestamp
  matches the record being applied. Taker effects occurring while a tick is processed still
  are not tick-triggered fills. A final 60-second passive limit-order observation can yield no
  maker fill; then the row is `n=0 / unavailable`. No print or benchmark is manufactured.
- **Submit-to-committed-fill:** order creation to that same emission, with maker/taker samples
  separated. It includes the anti-cheat delay and watermark hold-back: around 450 ms before
  added load at the accepted defaults, not the ledger's roughly 20 ms historical operation.
- **SDK observation:** request start to receiving/applying the fill, also including client,
  HTTP and WebSocket scheduling. It is not matching-only latency.
- **Ledger phases:** exact samples from the existing measurement calls, selected by observation
  time within each measured window. Phase names retain the adapter's existing meaning; commit
  is cumulative from transaction start. Nearest-rank p99 is a sample statistic, not histogram
  interpolation or a guarantee about the unsampled tail.

The observer lives entirely in [load_test_runtime.py](../tools/load_test_runtime.py); application
code and matching/settlement behavior are unchanged. Its timing/storage overhead is part of the
experiment. Finite in-memory samples and order history make this a bounded measurement, not an
unattended load service.

Run on **Linux**. Metadata records nominal clock resolution and the smallest observed positive
performance-clock read step; neither proves absolute clock accuracy. Windows latency rows are
explicitly marked unreliable at the historical 15.625 ms timer granularity
([#75](https://github.com/Suddha-Abhitej/SimEx/issues/75)). A devcontainer shares its host clock.
`timedatectl` must report synchronization before absolute feed lag is published; missing/unverified
status or negative lag makes that row unavailable
([#74](https://github.com/Suddha-Abhitej/SimEx/issues/74)). Same-host relative intervals do not
require knowing the constant offset from the venue. Negative intervals are unavailable.

The sampler reads feed-lag, watermark/delivery, gate, late-event, ledger and reconciliation metrics
before/during/after load. Returned ranges are sampled gauges, not per-message latency quantiles;
counter ranges are not themselves event deltas when multiple label series are involved. Exact
ledger samples and the refusal breakdown accompany them. Scrape failures are counted. Absolute
feed lag stays unavailable when clock synchronization cannot be verified.

The first run also found an existing gate limitation: the ten-minute excess-lag baseline remained
unestablished. The engine trims samples to at most ten minutes but requires their span to reach
ten minutes, normally requiring an exact endpoint match. A separate synthetic reproduction over
33.6 minutes retained a 599.94-second span and never established the baseline. The measured runs
therefore exercise book/catch-up, receive-age and absolute-lag safeguards, **not** a mature
rolling-median excess-lag safeguard ([#113](https://github.com/Suddha-Abhitej/SimEx/issues/113)).
This report records the defect; the load-test change does
not alter the trading path or shorten its accepted baseline threshold.

The hypothesis from ADR 0017 is about **50 serialized operations/s** at roughly 20 ms/operation,
not 50 accepted-and-filled orders/s: each order performs multiple durable operations. Compare the
measured operation cost, refusal mix and load curve before calling a knee. x86 laptop results do
not establish Oracle ARM64 capacity. The [README metrics](../README.md#measured-system-metrics)
record actual conditions/date/commit; no performance threshold runs in CI.

Output is aggregate system measurements only (ADR 0010): no prices, feed payloads, individual fill
timestamps/IDs, agent results or return calculations. Keep the optional JSON local. Do not turn
the measurement into a public market-data or scoring endpoint.
