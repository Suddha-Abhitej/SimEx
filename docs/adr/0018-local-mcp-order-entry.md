# 0018. Local MCP order entry

- **Status:** Accepted
- **Date:** 2026-11-04

## Context

Issue #13 adds the first interface through which an untrusted language model can submit simulated
orders. The interface must expose useful market and portfolio state without creating a path to a
real venue, bypassing the exchange's gate and timing rules, or accepting unbounded model input.

## Decision

Add a small MCP server using the Python MCP SDK. It exposes `quote`, `place_order`, `portfolio`,
`features`, `cancel`, `open_orders`, and `history`, and is constructed with one account ID, an `AccountWriter`, a per-product
`PairEngine` map, and explicit per-product quantity and notional limits. The account ID is not a
tool argument, so this server cannot create a second account.

The server binds to `127.0.0.1` for Streamable HTTP. It imports exchange books and writers only;
it has no venue adapter, network exchange client, credential, or order-placement dependency. The
only order path is `AccountWriter.submit`, followed by the corresponding `PairEngine.submit`.
That preserves the existing gate, anti-cheat delay, market-band, reservation, and idempotency
rules. Decimal inputs must be strings and all Decimal outputs are serialized as strings.

Before calling the writer, `place_order` enforces the configured product whitelist, instrument
grids, maximum quantity, and maximum notional. Gate rejection, a halted account, ledger halt,
malformed input, and idempotency conflicts return a structured refusal with a machine-readable
reason; no refusal is a silent success. A repeated request with the same client order ID and
identical payload returns the existing order from `AccountWriter`.

The demo is a local private run. Its recording is written below `data/`, which is gitignored and
must not be committed because it contains Gemini-derived prices and results.

`features` computes only instantaneous Decimal book values (mid, spread and depth imbalance);
event-time features remain the responsibility of #18. `cancel` calls the writer's delayed
`request_cancel` and reports a request rather than an immediate cancellation. `open_orders`
delegates to the writer. `history` reads committed fills from the ledger, including the order's
reasoning string, so it remains available after restart.

Every order stores a bounded reasoning string in the `orders.reasoning` column (migration 007).
The value is opaque untrusted model text: it is passed as a query parameter and is never
interpolated into SQL, log format strings or shell context.

The MCP server and reconciliation job run in the same supervised runtime group as the exchange
runner. A Kafka outage tears down the whole group, including the ledger connection, so the MCP
endpoint disappears until a new group has restored the account and rebuilt all projections.
Reconciliation is registered against that new writer, never an old runner. The MCP HTTP bind is
retried briefly after teardown to tolerate socket release and immediate port reuse; the server
still binds only to localhost. A bind failure after retries fails the group and therefore also
stops the runner and reconciliation job.

## Demo

The demo ran on 2026-11-04 against the live Gemini feed: a language model called `quote`,
`place_order` and `portfolio` over the localhost MCP server and placed 16 orders, of which 10
filled. Sells filled 5 of 5, buys 5 of 10, because the market rose during the session; a buy limit
only fills if the market comes to it, which is the fill model behaving as designed. Fees matched
the ADR 0014 schedule exactly, with $806.22 of notional at 120 bps producing a $9.67 fee.
Reconciliation reported zero mismatches at exact Decimal equality throughout. The transcript is
private under `data/` and is not committed (ADR 0010).

## Consequences

The MCP surface is deliberately narrow and private, and model input cannot exceed the limits
provided by the host. A caller must supply a live exchange runner and explicit risk limits, so the
server is not a standalone public service. Authentication beyond localhost binding is deferred to
the API work after #15. The group lifecycle adds restart latency and intentionally makes order entry
unavailable during recovery from a Kafka outage.
