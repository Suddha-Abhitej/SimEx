# 0007. Market-data envelope and Kafka topics

- **Status:** Proposed
- **Date:** 2026-10-15

## Context

The ingestor is the only component that talks to Coinbase (CLAUDE.md). Everything downstream
(order book builder, recorder, features, replay) reads market data from Kafka, so the topic layout
and message format are a contract many components will depend on. They need to keep per-pair
ordering for the one-writer-per-pair matching engine, keep prices exact (ADR 0004), carry what is
needed to measure lag and detect gaps, and let the recorder store exactly what Coinbase sent.

## Decision

- **One topic per channel:** `md.level2`, `md.trades`, `md.heartbeats`, 3 partitions each,
  `max.message.bytes` 16 MiB because a level2 snapshot for a deep book is several MB of JSON.
- **Keyed by `product_id`**, so each pair's messages stay in order on one partition. Heartbeats
  are keyed `heartbeat` and so share one partition.
- **One Kafka record per WebSocket message**, not per event or trade. Messages normally carry a
  single product; if one carries several, the unchanged message is published once per product,
  keyed by that product, so no pair's partition misses data.
- **Envelope:** a JSON object with metadata and the original WebSocket text spliced in byte for
  byte under `payload`. The payload is never parsed and re-serialized, so prices and sizes stay
  the exact strings Coinbase sent and no value ever passes through a float.

  ```json
  {"v":1,"received_at_ns":1792026986880917441,"exchange_ts_ns":1792026986880577123,
   "channel":"market_trades","product_id":"BTC-USD","sequence_num":1456,"payload":{...}}
  ```

  `received_at_ns` is local UTC wall-clock time in epoch nanoseconds, `exchange_ts_ns` is the
  message's Coinbase `timestamp` parsed exactly to nanoseconds (null if missing), `channel` is the
  subscription name (`level2`, not Coinbase's `l2_data`), `product_id` is null for heartbeats,
  `sequence_num` is copied from the message. `v` versions the envelope.
- **Only market data is published.** `subscriptions` acknowledgements and `error` messages are
  logged and counted, not sent to Kafka. Gaps are detected and reported, not repaired (issue #4).
- **Plain JSON, no Schema Registry yet.** The plan adds Schema Registry later; `v` leaves room to
  migrate.

## Consequences

- Downstream code gets ordered per-pair streams and can rebuild books from `md.level2` alone.
- The recorder can store `payload` as-is and replay can feed it back through the same code.
- JSON is verbose: level2 dominates Kafka disk use and retention must be sized from
  `tools/feed_probe.py` measurements. Compression can be turned on later without changing the
  format.
- Nanosecond integers exceed JavaScript's safe integer range; a web client must not read these
  fields as plain numbers (the web site reads from the API, not from Kafka).
- With three pairs and three partitions, key hashing can put two pairs on one partition. That is
  harmless for ordering but uneven for load.
- A message carrying several products is stored more than once; consumers must use the
  envelope's `product_id` and ignore other products' events in the payload.
- Gap detection assumes `sequence_num` counts every message on the connection across all
  channels, which is how `tools/feed_probe.py` treats it. A live run must confirm this before
  issue #4 builds resync on it.
