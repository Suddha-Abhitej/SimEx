# Connect your first Python bot

SimEx is a private simulated exchange on live Gemini market data. Your bot runs outside SimEx.
This is a connection demonstration, not a trading idea. Nothing places real orders or moves real
money. Keep market data and results private ([ADR 0010](adr/0010-personal-use-and-gemini-market-data.md)).

## Local setup

Install Python 3.12+ and Docker Compose, then install from your checkout. There is no package on
an index. The `dev` extra includes the Postgres driver needed by the exchange.

```bash
git clone https://github.com/Suddha-Abhitej/SimEx.git
cd SimEx
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env
```

Edit your private `.env`, preserving the infrastructure settings and replacing the settings below.
Replace the token placeholder locally; never commit the token or `.env`. Enable only BTC for this
demo so only BTC limits are needed. These caps are an explicit demo policy, not a recommendation.

```dotenv
SIMEX_VENUE=gemini
SIMEX_PAIRS=BTC-USD
EXCHANGE_API_PORT=8770
EXCHANGE_API_TOKEN='<choose-your-own-private-token>'
EXCHANGE_LEDGER_DSN=postgresql://simex:change-me@127.0.0.1:5432/simex
EXCHANGE_ACCOUNT_ID=owner
EXCHANGE_INITIAL_USD=100000
EXCHANGE_MAX_QUANTITY_BTC_USD=0.001
EXCHANGE_MAX_NOTIONAL_BTC_USD=100
EXCHANGE_MAX_POSITION_BTC_USD=0.001
```

The DSN matches the example Postgres credentials; adjust it if you changed them. Use `simex` for
this trading ledger and a separate `simex_test` for tests. Exchange startup applies migrations
and seeds the account once, without depositing again after a restart. The runtime requires the
token, ledger and all three limits for **every enabled pair**. Keeping the default ETH/SOL pairs
also requires explicit `EXCHANGE_MAX_QUANTITY_ETH_USD`, `EXCHANGE_MAX_NOTIONAL_ETH_USD`,
`EXCHANGE_MAX_POSITION_ETH_USD`, `EXCHANGE_MAX_QUANTITY_SOL_USD`,
`EXCHANGE_MAX_NOTIONAL_SOL_USD`, `EXCHANGE_MAX_POSITION_SOL_USD`.

In each terminal below activate the environment and load **your own trusted** `.env`. The services
also read `.env` themselves, but the bot needs the token/port in its environment. Bash:

```bash
source .venv/bin/activate
set -a
source .env
set +a
```

On Windows use `.venv\Scripts\Activate.ps1` and set the same settings as `$env:NAME='value'` in
each PowerShell terminal instead of using Bash's `source`.

```bash
docker compose up -d --wait
```

Leave the ingestor running in one terminal, then the exchange in a second:

```bash
python -m simex.ingest
```

```bash
python -m simex.exchange
```

The exchange builds its own books from Kafka; a separate book/recorder process is not needed for
this demo. Wait for catch-up and book/gate readiness. Health means process liveness only. The API
binds only `127.0.0.1`; remote bots need your private tunnel/access layer. Tokens belong in
Authorization headers, never URLs.

## See your own fill

In a third terminal with the same environment:

```bash
python examples/hello_bot.py
```

It prints a quote, connects the stream before submission, submits a **minimum-size BTC market buy
(`0.00001`)**, then prints its first fill, fee and portfolio. Response money is `Decimal`.
It deliberately crosses the spread: the entry-tier taker fee is **120 bps (1.2% of notional)**,
rounded up to the fee increment. At a synthetic price of $100,000 the notional is $1 and the fee
is $0.012; that is an illustration, not a promised live price. All costs are simulated.

A ready live book normally demonstrates the fill in seconds, including the anti-cheat delay and
hold-back. Acceptance does not guarantee a full fill: market orders are band-limited IOC orders,
may partially fill, and expire any remainder. A refusal prints the server's reason; correct the
setup and rerun when ready. If an accepted order produces no fill, inspect `client.open_orders()`,
`client.fills()` and `client.portfolio()` before placing another. Ctrl-C closes the stream cleanly;
it does not undo an accepted order. Each example run intentionally uses a new key and can buy again
until your exposure cap refuses it.

For a resting-limit variant use `type="limit"` with a Decimal/string `limit_price` on the BTC price
grid well below the current mid for a buy. It may wait indefinitely or never fill; book movement
alone does not invent maker fills ([ADR 0014](adr/0014-order-model-and-fill-model.md)).

## HTTP client

```python
from decimal import Decimal
from simex.client import APIError, Client

with Client("http://127.0.0.1:8770", token="<your-private-token>") as client:
    try:
        result = client.place_order(
            client_order_id="my-persisted-decision-1",
            product_id="BTC-USD",
            side="buy",
            type="limit",
            quantity=Decimal("0.00001"),
            limit_price="100.00",
            reasoning="Synthetic connection example",
        )
    except APIError as error:
        print(error.reason, error.status, error.body)
```

`quantity` and `limit_price` accept Decimal or strings. A float fails before sending, naming the
field and suggesting `Decimal("0.1")` or `"0.1"`. Strings pass through unchanged; the server owns
grids, funds, risk and validation. Known monetary response fields decode structurally to Decimal,
including JSON numeric fractions without an intermediate float. IDs/reasoning stay text, sequences
and timestamps stay integers, null marks stay `None`, and unknown fields remain available.
`APIError` carries the exact server reason, status, full decoded body and original `raw_body` bytes.
Unknown reasons do not need a new SDK release.

Persist your stable `client_order_id` with the decision before submitting. The SDK serializes once,
then by default retries once on a connection error or timeout with **identical bytes and key**.
Configure `Client(..., order_retries=0)` to disable retries. HTTP answers, including 400, 409 and 503,
are never retried. An exhausted transport error leaves the outcome **uncertain**: use the original
key to resolve it with the server, not a newly generated key.

`cancel(order_id)` and `cancel_by_client_id(client_order_id)` return the server's `status: requested`,
with no automatic cancellation retry. Fills during the anti-cheat cancel delay stand. Other methods
are `quote`, `features`, `open_orders`, `portfolio`, and `fills(limit=100)`; results preserve the
server dictionary structure.

## Apply, acknowledge and resume

`client.stream()` connects immediately and saves `stream.announced_stream_id` and `stream.hello`
before returning. Without a saved checkpoint it starts **live at hello**, even if old history was
evicted. Use `Checkpoint(seq=0)` explicitly for replay from the writer's beginning; that gaps if the
beginning was evicted. Live-only is never selected automatically as recovery from a gap.

Inside an open `Client` context, supply your application functions:

```python
from simex.client import Checkpoint, StreamGap

checkpoint = Checkpoint()  # Or restore stream_id, seq and applied_fill_ids from your own store.
try:
    with client.stream(checkpoint) as stream:
        for frame in stream:
            if frame["type"] == "fill":
                apply_to_my_state(frame["fill"])
                stream.ack(frame)  # AFTER applying it.
            elif frame["type"] == "halt":
                handle_halt(frame["scope"], frame["halted"])
except StreamGap as gap:
    print(gap.reason, gap.body)
    recovery = client.reconcile()  # Read the limitations below before adopting a new baseline.
```

Frames deliver freely. You may apply a batch transaction and acknowledge fills in any order. The
cursor advances through contiguous acknowledged deliveries: acking 5 and 7 without 6 leaves it at 5.
A reconnect replays 6 and deduplicates applied 7 by `fill_id`. Filtered/non-fill sequence holes are
safe to pass; a later heartbeat cannot jump an unapplied fill. During replay hello is the current
head, **not** a delivery checkpoint: a replayed fill may have a smaller seq. Resume sends both saved
`from_seq` and `stream_id`. A new peer's announced ID never relabels an old cursor before a gap.

Checkpoint state is in memory. Persist `stream_id`, `seq` and `applied_fill_ids` with application state
for crash recovery. Exactly-once application across crashes requires atomic persistence together,
or an application idempotent by fill ID. The SDK cannot transact over your external store. Applied
IDs are pruned as the contiguous cursor passes them; only out-of-order acknowledgements beyond
the cursor remain. Outstanding tracking grows with unacknowledged fills. Use one active iterator
per checkpoint, on one thread. Reopening is explicit; there is no reconnect loop.

The SDK raises on every disconnect, including `websockets.exceptions.ConnectionClosedOK` for a
clean server close, rather than silently ending a bot's fill loop. Acknowledged progress is retained.
The example catches `ConnectionClosed`, reports the close reason and exits with a nonzero status
without a traceback. Closing the example does not undo an accepted order; inspect its outcome
before submitting a new decision.

Hello flags, heartbeats and halt/recovery frames are visible. Check hello's initial halt flags before
submitting. Halts describe account/writer state, not market-data gate state. **Unknown frame types
pass through unchanged**, with JSON numeric fractions decoded to Decimal, and do not themselves
advance the checkpoint. A default branch may ignore them rather than crashing on a future server.

All four gap reasons raise `StreamGap`: `events_evicted`, `stream_id_required`, `stream_changed`,
`cursor_ahead`. The iterator closes and preserves the checkpoint instead of silently resetting it.
`reconcile()` returns `{"fills": <fills response>, "portfolio": <portfolio response>}` from **two
separate non-atomic reads**. History is capped at **1,000 fills**, without pagination, and may not
cover the interval. The helper claims no completeness, acknowledges nothing and resets/restarts
nothing. Explicitly apply recovered fills, adopt a new portfolio baseline, or reconcile offline
against the ledger. A fresh live subscription is a **new baseline**, not continuous history.
Fills can occur between reads and a new subscription: this is not an atomic snapshot/stream handoff.

## Score your bot

Start the recorder with the same environment **before** running the bot, in its own terminal:

```bash
python -m simex.recorder
```

After trading, score the committed fill history against private Gemini marks:

```bash
python -m simex.evaluator
python -m simex.evaluator --account owner --account dca --lookback-seconds 7200
```

The DSN comes from `EXCHANGE_LEDGER_DSN` (or explicit `--dsn`); recordings default to
`data/recordings`. Use actual account IDs that have fills: the second command assumes a DCA baseline
was configured and traded. By default every non-system account with fills is reported; each is
compared only with the other selected accounts. One selected account prints metrics without a
comparison. Starting balances come from recorded seed postings, not an assumed deposit.

The recorder publishes completed hours, so the current partial hour may not yet be on disk.
The CLI prints progress before replay, which can take minutes, and reports mark/fill coverage.
`--lookback-seconds` warms up the book **before** the evaluation window; it does not shorten the fill
history. If recordings lag by more than one evaluation period, the end is clamped with a notice
naming the limiting product. Use `--account` to exclude accounts exposed to that product, or wait
for more recordings. A first-mark warning suggests a longer look-back to find an earlier snapshot.
Missing recordings cannot be reconstructed by increasing the look-back.

Reports print to stdout only and contain private Gemini-derived results; do not publish or upload
them. [ADR 0020](adr/0020-evaluator-price-series-and-significance.md) defines the unchanged marks and
statistics: one session ranks agents by exposure, not skill, and Sharpe cannot distinguish accounts
merely scaling exposure to the same product.

## Find out whether your bot helps

Use the [baseline agents (ADR 0019)](adr/0019-baseline-agents.md) and
[evaluator (ADR 0020)](adr/0020-evaluator-price-series-and-significance.md) to compare with cash
and simple policies after costs. One session ranks agents by exposure, not skill. A connection and
one profitable fill do not establish an edge. See the [private API contract](private-api.md) for
the full wire protocol.
