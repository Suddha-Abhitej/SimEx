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
  "time_in_force": "GTC",
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

## Time in force

`time_in_force` is optional and defaults to `GTC`. Only the exact strings `GTC`, `IOC`, `FOK`
are accepted; an explicit null or unknown value returns structured `bad_field` detail.
The SDK and MCP `place_order` accept the same optional argument. It is part of the immutable
request: changing only time in force while reusing a client ID returns `idempotency_key_reused`.

- **Limit GTC:** cross as taker, then rest the remainder until cancelled, as before.
- **IOC:** take eligible liquidity once, then immediately cancel the remainder. A zero-fill
  IOC is also `cancelled`; a completely filled IOC is `filled`.
- **FOK:** fill the whole quantity at once or become `rejected` with `fok_not_fillable` and no
  fills. The current book, limit/accepted band and prior simulated depletion determine eligibility.
- **Market GTC:** preserves existing band-bounded immediate behavior and expires any remainder.
  GTC never makes a market order rest. Explicit market IOC cancels its remainder instead;
  market FOK requires full quantity inside the existing band.

All policies keep the anti-cheat delay. IOC/FOK are accepted as durable `pending` orders with
reservations until their effective time, then finish terminal without entering the maker queue.
Explicit IOC/FOK fill groups, terminal snapshots and reservation release commit atomically.
Pending orders recover with the same effective time and reservation; terminal orders remain only
in durable history. See [ADR 0028](adr/0028-time-in-force-and-atomic-immediate-execution.md).

**Poll outcomes explicitly:** the initial POST normally returns acceptance before execution.
A FOK effect rejection or zero-fill IOC generates **no fill-stream event**; the stream still
carries fills, halts and heartbeats only. Repeat the identical `place_order` body with its original
`client_order_id` to retrieve the current order without readmission. `open_orders` lists only
live orders, so disappearance alone does not explain an outcome. The HTTP 400 mapping for
`fok_not_fillable` applies to that later replay/lookup, not the already returned acceptance.

## Actionable order refusals

`reason` is the stable machine-readable contract. Details are separate top-level fields, shared
by REST and MCP. A recorded rejection also retains them in `order.reject_details`, so an
idempotent retry or recovery preserves the original refusal even after balances, fees or limits
change. Validation before acceptance has no recorded order. A malformed payload still returns
`bad_payload`; a reasonless parsing failure falls back to `bad_order` without exposing internals.

| Reason | Details a client can expect |
| --- | --- |
| `below_min_size` | `field`, `rule`, `product_id`, `minimum`, `attempted` quantity, and human-readable `detail` |
| `bad_quantity` | `field: quantity`, `rule` requiring a positive multiple of the size increment, `increment`, `attempted`, `product_id`, `detail` |
| `bad_price` | `field: limit_price`, `rule` requiring a positive multiple of the price tick, `increment`, `attempted`, `product_id`, `detail` |
| `bad_client_order_id` | `field`, `rule` and `detail`: 1–64 characters from `A-Z a-z 0-9 . _ : -` |
| `bad_decimal` / `bad_decimal_or_product` | `field`, `rule` and `detail` requiring a finite decimal string; no echo of the malformed input |
| `bad_field` | `field`, `rule` and `detail`: permitted side/type/time-in-force values, or that market orders have no limit price |
| `unknown_product` | `field: product_id`, `rule` and `detail` requiring a configured product; HTTP 404 |
| `reasoning_required` / `bad_reasoning` | `field`, `rule` and `detail` explaining the non-empty string / 8,192-character bound |
| `quantity_limit` | Existing `rule`, `product_id`, `limit`, `attempted` quantity |
| `notional_limit` | Existing `product_id` and configured `limit`; limit orders also report their requested `attempted` notional. Market-order omissions and remedy are described below. |
| `position_limit` | Existing `rule`, `product_id`, signed `attempted` endpoint, `limit`, `minimum`, `maximum`, `side` |
| `insufficient_funds` | `rule`, `product_id`, `asset`, `available`, and `required` where safe; available excludes existing reservations |
| `stale_level2:...` / `feed_lagging:...` | `rule`, `product_id`, `detail` advising the caller to wait for fresh level2 / recovered feed lag; the entire reason suffix is unchanged; HTTP 503 |
| `idempotency_key_reused` | `field`, `rule`, `detail` explaining that the ID already names a different order |
| `fok_not_fillable` | `field: time_in_force`, `rule`, `detail`: full immediate quantity required; reduce quantity or select IOC/GTC. No book prices or available venue size. Effect rejection, HTTP 400 on a later replay/lookup. |

The grid reasons remain `bad_price` and `bad_quantity`, including for off-grid input. The exchange
never rounds or repairs an order. All monetary detail fields are Decimal strings on the wire;
the Python SDK decodes `limit`, `attempted`, `minimum`, `maximum`, `increment`, `required` and
`available` to `Decimal`, including inside `reject_details`. Malformed fields get explanatory
rules rather than raw-value or exception-text echoes. Other existing refusal reasons may have no
additional fields; clients must tolerate absent and new fields and reasons.

For a **limit buy**, `required` includes the caller's limit notional plus the higher maker/taker
fee, rounded up as in reservation. For a **sell**, it is the base quantity. The **market buy**
requirement depends on the live book's band price: publishing that amount would disclose a price
by division. Its `required` is deliberately omitted, and its rule names the available quote
balance as the funding limit, explains the band-bounded notional plus fee, and advises reducing
quantity or using a limit order to know its notional exactly.

A **market notional-limit** refusal likewise omits `attempted`. Its rule names the configured
notional cap and explains the same remedy. The notional cap itself excludes fees; the reservation
also needs fee headroom. Neither omission changes when an order is refused. No book price, band
price, midpoint, best bid/ask, other-account state, token or DSN is added to refusal details.
Market preflight failures keep `no_band_price`; they do not change into a different gate reason.
Recovered market-notional refusals from before this change also omit the old book-derived
`attempted` value and carry the remedy. Historical ledger rows are not rewritten; this protects
the recovered order and response, so direct exports of old ledger data still need private handling.

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
