# Private REST API and fills stream

The API is for the owner only (ADR 0010). Start the existing exchange process; it owns the ledger,
writer, matching engines and both optional transports. There is no separate API runtime.

Set `EXCHANGE_API_PORT` to an unused local port, `EXCHANGE_API_TOKEN` to your private secret and
`EXCHANGE_LEDGER_DSN` to the existing ledger. Keep the token in the local environment, never in git.
Explicit `EXCHANGE_MAX_QUANTITY_<PAIR>`, `EXCHANGE_MAX_NOTIONAL_<PAIR>` and
`EXCHANGE_MAX_POSITION_<PAIR>` limits must be set. The writer enforces them for both transports;
the former MCP quantity/notional names remain compatibility aliases (ADR 0022).
An absent API port disables it; an enabled API without a token or ledger refuses startup.
It always binds `127.0.0.1`. Remote clients need your private access layer or SSH tunnel.

Send `Authorization: Bearer <your secret>` on HTTP and WebSocket requests. The credential selects
the configured owner account. Do not send `account_id`; order bodies containing it are refused.
Do not put the token in URLs. `/v1/health` alone works without authentication and reports only
`{"ok": true}` while the process is up, including while trading is halted.

| Method | Route | Shared operation |
| --- | --- | --- |
| GET | `/v1/quote/{product_id}?depth=5` | `quote` |
| GET | `/v1/features/{product_id}?depth=5` | `features` |
| POST | `/v1/orders` | `place_order` |
| DELETE | `/v1/orders/{order_id}` | `cancel(order_id=...)` |
| DELETE | `/v1/orders/by-client-id/{client_order_id}` | `cancel(client_order_id=...)` |
| GET | `/v1/orders` | `open_orders` |
| GET | `/v1/portfolio` | `portfolio` |
| GET | `/v1/fills?limit=100` | `history` |
| WebSocket | `/v1/stream/fills` | committed writer notifications |

An order body, using synthetic example prices:

```json
{
  "client_order_id": "example-1",
  "product_id": "BTC-USD",
  "side": "buy",
  "type": "limit",
  "quantity": "0.12345678",
  "limit_price": "100.00",
  "reasoning": "Synthetic connection example"
}
```

Money is a JSON string in both directions. Market orders omit `limit_price`. Use a stable
`client_order_id` when retrying: the existing idempotency rules apply. Cancellation acknowledges a
**request**; the anti-cheat delay still applies, and fills before cancellation takes effect stand.
Responses are the same dicts as MCP, including full portfolio P&L. Act on `ok` and `reason`.
HTTP statuses provide a convenience: 400 for validation, 404 for unknown resources, 409 for account,
writer or ledger halts, and 503 for unavailable books/backends. Unknown refusal reasons remain
structured HTTP 400 responses rather than turning into server errors.
Position-limit refusals include the writer's `limit`, `attempted` and other `reject_details`,
also retained inside the rejected order. `risk_limits_missing` is HTTP 503 and means server
misconfiguration; the live runtime refuses to start without its risk policy.

## Connect, checkpoint and resume

The first frame is always `hello` after successful authentication:

```json
{"type":"hello","stream_id":"<UUID hex>","seq":42,"writer_halted":false,"account_halted":false}
```

No cursor means start live at the hello sequence. To resume within that writer's retained buffer,
use `/v1/stream/fills?from_seq=42&stream_id=<saved UUID>`. Only events strictly after 42 are replayed.
`from_seq=0` requests replay from the start, provided nothing has been evicted. A positive cursor
without its writer ID is refused with an explicit gap. A new writer after a Kafka rebuild or
process restart has a fresh random ID; an old cursor cannot silently select its unrelated events.

Fill frames have exactly `type`, `seq` and the original `fill` dict. Only your account's fills are
sent. Monetary fields inside the fill stay strings. Save the writer ID from hello immediately,
and checkpoint the sequence of each fill only after applying it. Deduplicate by `fill_id` when
reconciling or reconnecting. The hello sequence is the current head, so during replay it is **not**
a delivery checkpoint. The first replayed fill can have a smaller sequence than hello.

Heartbeats contain `type: heartbeat`, integer `ts_ns`, `stream_id` and the fully scanned `seq`.
They allow a quiet account to checkpoint over non-fill events and other accounts' activity without
revealing those payloads. Writer/account changes push `type: halt`, `scope: writer` or `account`,
`halted: true` or `false`, and `seq`. Initial halt state is also in hello; an already-halted live
join receives an immediate halt frame. Halt recovery is explicit even when there are no fills.
Market-data gate state is still represented by the shared order refusal reasons, not these frames.

An expired, invalidated or future cursor produces `type: gap`, a `reason` (`events_evicted`,
`stream_id_required`, `stream_changed` or `cursor_ahead`), the current writer ID/buffer boundaries and
`history: /v1/fills`, then closes with code 1008. Reconcile committed fills and current portfolio;
never treat a gap as a complete stream. History defaults to 100 and accepts at most 1,000 fills.
If that cannot cover the missing interval, a portfolio resnapshot or offline ledger reconciliation
is necessary. The endpoint does not offer unlimited history pagination.

## Capacity and liveness

`EXCHANGE_EVENT_BUFFER_SIZE=10000`, `EXCHANGE_API_HEARTBEAT_SECONDS=15` and
`EXCHANGE_API_SEND_TIMEOUT_SECONDS=10` are provisional, configurable positive settings. **Every**
writer notification consumes retention, including rejected orders; it is not a fill-only buffer.
The rejection-heavy synthetic experiment in [ADR 0023](adr/0023-private-rest-and-fill-stream.md)
observed about 1,000 events/second and only **0.166659 minutes** of retention despite zero fills.
That is an explicit synthetic load, not a prediction of your live traffic. At another observed
rate, estimated retention in minutes is buffer capacity divided by events per minute.
The owner's observed 14-hour baseline run had roughly 150 total writer events (about 0.003/second),
implying roughly 39 days at that average rate. This estimate and the ten-second synthetic example
illustrate the range; bursts, restarts and a different cadence can shorten either absence window.

A blocked send times out and closes with code 1013, with instructions to reconnect using
`from_seq` and `stream_id`. A client that falls behind eviction gets a gap. The runner's rejection
metric also uses a sequence cursor; `simex_exchange_writer_event_gaps_total` counts events evicted
before it could observe them, so missing metric history is visible.

Ledger writes synchronously block the exchange loop. Existing measurements were roughly 22 ms
per fill acknowledgement, with host/timer limitations recorded in ADR 0017. Requests queue behind
that work. An acknowledgement does not precede its persistence, and this transport adds no
background submit queue.
