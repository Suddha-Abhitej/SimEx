# 0023. Private REST API and resumable fill notifications

- **Status:** Accepted
- **Date:** 2026-11-05

## Context

Issue #14 lets the owner connect scripts and traders without an MCP client. A second implementation
of order validation or portfolio accounting would disagree with MCP. An endpoint running in a
threadpool would also violate the account writer's order-state ownership: its balance lock does
not protect order submission. Fill notifications must follow commit and must never imply that a
client has complete history after a buffer eviction or writer restart.

The owner approved the proposal on #14, then required random writer identities, retention measured
in time, conversion of the runner's existing event consumer, and visible halt transitions.

## Decision

Extract `ExchangeTools`, its JSON conversion and existing `RiskLimits` to `simex.exchange.tools`
in a separate first commit. The unchanged Decimal P&L helper moves to `simex.exchange.pnl`.
Compatibility re-exports preserve existing MCP callers. This removes FastMCP from the import path
of the shared operations. REST is a thin FastAPI transport over those same account-bound methods;
both transports receive the same owner tools instance in `LiveRuntimeGroup`.

Expose the specified quote, features, order placement, two delayed cancellation forms, open orders,
portfolio and fill history routes under `/v1`, plus `/v1/stream/fills` as a WebSocket. Every endpoint
is `async def`, and order mutation runs synchronously on the exchange event loop. No executor,
background submit queue, cache, optimistic acknowledgement or second exchange loop is introduced.
Request JSON passes through to the tools parser; money must be strings there. Responses are the
original JSON-safe dicts, with no Pydantic response models or Decimal-to-float conversion. A body
containing `account_id` is a transport error, never an account selector. Integer query parsing and
malformed JSON have explicit transport errors; domain validation stays in the shared tools.

One reason-to-status table preserves the complete original refusal body. Unknown reasons default
to HTTP 400 and a warning; dynamic gate reasons use their stable prefix only for status lookup.
Clients act on `reason`, not the status. `/v1/health` reports process liveness only, even when trading
is halted. It has no market, account or halt detail. Schema and interactive documentation routes
are disabled.

`EXCHANGE_API_PORT` absent means disabled. Enabling it requires `EXCHANGE_API_TOKEN` and the existing
ledger DSN; missing configuration fails startup before metrics or the Kafka retry loop. HTTP and
WebSocket entry require `Authorization: Bearer <token>`, compared with `hmac.compare_digest`.
The single secret resolves only to `EXCHANGE_ACCOUNT_ID`; no user table or account creation is
added. Credentials are not logged. Bind to literal `127.0.0.1`, with no host option. HTTP access
logging and trusted proxy headers are disabled. Use an owner-controlled SSH tunnel or private
access layer for remote access, consistent with ADR 0010.

The API is a supervised uvicorn task beside MCP, the runner and reconciliation, using their same
writer, engines and ledger. A bind failure fails the group. Cancellation closes sockets and client
tasks before the ledger closes; a Kafka failure rebuilds the whole group. Both transports may run
together on different ports. The process owns signal handling.

### Event sequence, replay and failure visibility

Retain writer notifications in `deque(maxlen=EXCHANGE_EVENT_BUFFER_SIZE)`. A separate monotonically
increasing counter supplies every event's sequence, independent of retained length. Every writer
instance has `stream_id = uuid.uuid4().hex`: counters and low-resolution times cannot distinguish
a cold restart reliably. `_emit` remains after successful ledger commit for fills; the ledger and
fill settlement path are unchanged.

The first frame is `hello`, carrying `stream_id`, the current sequence and initial writer/account
halt flags. A live-only connection starts at that sequence. `from_seq=N&stream_id=ID` replays
events strictly after N, then uses the same cursor for live delivery. A positive cursor requires
the matching writer ID. A missing ID produces `stream_id_required`; a supplied different ID produces
`stream_changed`. A cursor ahead of the writer or an evicted cursor also produces an explicit
`gap` with the cause and `/v1/fills`, then closes with code 1008. `from_seq=0`
requests all retained history only when the beginning is still available. Omitting a cursor starts
live delivery. A `hello` sequence is the current head, not proof that replay has been delivered.

Only fill events for the credential's account are sent, with their original fill dict. Other
accounts' fills and account halts are filtered out, but the cursor advances over them. Heartbeats
include a timestamp, writer ID and fully scanned cursor, so quiet accounts can checkpoint progress.
Halt and recovery transitions use explicit `halt` frames with `scope` (`account` or `writer`) and
`halted` true/false. An already-halted live join receives the current halt immediately. This reports
account/writer halts, not market-data gate state.

Each client has a cursor and a bounded snapshot of retained events, with coalesced wakeups; no
unbounded per-client fill queue is added. A timed-out send closes with code 1013 and instructions
to reconnect using `stream_id` and `from_seq`. Eviction during live delivery also gaps explicitly.
The journal alone has a small lock for numbering, snapshots and wakeups, because reconciliation
already invokes account halt methods from its existing worker thread. This does not make order
submission thread-safe or permit it from a threadpool.
If scheduling a wakeup fails because its loop has closed, drop that watcher and continue retaining
the event and notifying other clients. Notification failure must not propagate into submission,
committed fill settlement, or the original ledger exception while a writer halts.

The runner also consumes by sequence, replacing its positional slice and length cursor. If it
falls behind, it counts the number of lost events in
`simex_exchange_writer_event_gaps_total`, logs the gap, and continues counting retained rejections
exactly once. Lost rejections cannot be reconstructed from the notification buffer; their loss is
visible. Existing determinism tests still compare the complete serialized event sequence, with
explicit assertions that their test histories were not evicted. The random writer ID is outside
that deterministic event payload.
The fill-accuracy replay tool also consumes by sequence, but aborts with an explicit invalid-results
error if eviction outruns it. It cannot continue a measurement with missing events.

### Provisional operational settings and retention measurement

The owner approved initial settings of 10,000 events, a 15-second heartbeat and a 10-second send
timeout. These are configurable resource/liveness choices, **not measured market constants**.
The buffer contains every lifecycle event, fill and halt transition across all accounts. It is
not a 10,000-fill history and has no guaranteed retention duration.

On 2026-11-05, `tools/measure_api_event_retention.py --orders-per-second 1000` submitted 12,000
synthetic valid-shaped orders to a closed gate, observing 12,000 rejections and zero fills over
11.999742 seconds: **1,000.021 events/second**. The 10,000 retained events spanned **0.166659 minutes**
(about ten seconds); capacity divided by the observed rate gives 0.166663 minutes. This deliberately
rejection-heavy experiment demonstrates that retention can vanish with no trading whatsoever.
It excludes Postgres and is neither a throughput benchmark nor a production event-rate measurement.
The tool can be rerun at an intended cadence; actual retention changes with aggregate event rate.
For example, capacity / observed events per minute gives the estimated absence window. Measure
again on the live deployment; the new API has not been deployed as part of this change.

For comparison, the owner-observed 14-hour baseline run produced roughly **150 writer events**,
including order lifecycle events, not just its 48 fills: about **0.003 events/second**. At that
observed average, 10,000 events correspond to roughly **56,000 minutes (39 days, about 5.6 weeks)**
of retention (`10000 / 150 * 14 hours`). This is an estimate from that run's approximate total,
not a newly measured stream or a guarantee about bursts or future cadence. The observed quiet-run
estimate and the synthetic rejection-heavy result therefore span about ten seconds to several
weeks; neither is a universal bound, and faster event rates can retain less than ten seconds.

## Consequences

One trading boundary preserves MCP/REST refusal and accounting agreement. Synchronous ledger work
still blocks all exchange-loop requests: ADR 0017 measured about 22 ms per fill acknowledgement
on the development Windows host, with timer-resolution and ARM64 caveats. This API makes no new
latency claim and does not hide that blocking behavior.

The stream is a bounded notification mechanism, not durable history. Clients deduplicate by fill
ID and reconcile on gaps; `/v1/fills` preserves the existing latest-1,000 maximum, not unlimited
historical pagination. Longer missing intervals require an explicit portfolio resnapshot or an
offline ledger reconciliation; they must not be labelled complete after reading a truncated list.
Cross-restart replay requires ledger history rather than pretending an old numeric cursor is valid.

The separate account-risk work in PR #91 is not included. Existing tools enforce the limits on
this branch; the transport adds no competing risk implementation. Matching, gate, anti-cheat,
fill production, evaluator and P&L conventions stay intact. No hard rule changes are needed.
