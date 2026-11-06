"""External bot template: simulated money only. No model key; keep output private."""

import os
from collections import Counter, defaultdict
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from time import perf_counter
from uuid import uuid4

from websockets.exceptions import ConnectionClosed

from simex.client import APIError, Client


def required_env(*names: str) -> list[str]:
    """Exit with the fix rather than a KeyError: a missing variable is the usual first error."""
    try:
        return [os.environ[name] for name in names]
    except KeyError as missing:
        raise SystemExit(
            f"Set {missing.args[0]} in this terminal before running the bot. This example reads "
            "the environment directly rather than the exchange's .env, because your bot runs "
            "outside SimEx. See docs/quickstart.md."
        ) from None


# Replace decide: build context from state, call your chosen model client, parse its response.
# Model output is untrusted: validate its shape before submitting (Decimal/string money only).
def decide(state: dict) -> dict | None:
    """Return an order intent, or None to do nothing this tick."""
    # Connection demo, not a strategy: buy once with two-sided quotes and no open order.
    if state["session"]["accepted"] or state["open_orders"]:
        return None
    if not state["quote"]["bids"] or not state["quote"]["asks"]:
        return None
    # Deliberate taker: default fee 120 bps (1.2%); this is all simulated (ADR 0014).
    return dict(
        product_id=state["product_id"],
        side="buy",
        type="market",
        quantity=Decimal("0.00001"),
        reasoning="Two-sided quote and no open order; one tiny connection demonstration",
    )


def validated(intent, product):
    required = {"product_id", "side", "type", "quantity", "reasoning"}
    if not isinstance(intent, dict) or not required <= intent.keys():
        raise ValueError("Intent needs product_id, side, type, quantity and reasoning")
    if intent.keys() - required - {"limit_price"}:
        raise ValueError("Only limit_price is optional; no extra intent fields")
    if intent["product_id"] != product:
        raise ValueError("Intent must name the configured product")
    if intent["side"] not in ("buy", "sell") or intent["type"] not in ("market", "limit"):
        raise ValueError("Intent needs buy/sell and market/limit")
    if (intent["type"] == "limit") != (intent.get("limit_price") is not None):
        raise ValueError("Only limit orders require a limit_price")
    if not isinstance(intent["reasoning"], str) or not intent["reasoning"].strip():
        raise ValueError("reasoning must contain the decision's own stated rationale")
    result = dict(intent)
    money = ("quantity", "limit_price") if intent["type"] == "limit" else ("quantity",)
    for field in money:
        if not isinstance(result[field], (Decimal, str)):
            raise ValueError(f'{field} must be Decimal or str, e.g. Decimal("0.1") or "0.1"')
        result[field] = Decimal(result[field])
        if not result[field].is_finite() or result[field] <= 0:
            raise ValueError(f"{field} must be finite and positive")
    return result


def refused(error, refusals):
    details = {k: v for k, v in error.body.items() if k not in {"ok", "reason", "order"}}
    print("Refused:", error.reason, details)
    refusals.append({"reason": error.reason, **details})
    code = error.reason.split(":", 1)[0]
    # Halts mean an exchange problem: stop, do not retry.
    if code in {"account_halted", "writer_halted", "ledger_halted"}:
        raise SystemExit(f"Stopped: {code}; inspect the exchange")
    # stale_level2/feed_lagging: wait until the next tick, never hammer the gate.
    # insufficient_funds: available is shown; market buys omit required for band-price privacy.
    return code


def run(client, product="BTC-USD", interval_seconds=60):
    counts = Counter(ticks=0, intents=0, submitted=0, accepted=0, fills=0, skips=0)
    refusals, fees, quantity, due = [], defaultdict(Decimal), Decimal(0), 0
    try:
        with client.stream() as stream:
            if stream.hello["writer_halted"] or stream.hello["account_halted"]:
                raise SystemExit("Stopped: account/writer halted before the first order")
            for frame in stream:  # StreamGap propagates; client.reconcile() is capped/non-atomic.
                if frame["type"] == "halt" and frame["halted"]:
                    raise SystemExit(f"Stopped: {frame['scope']} halted")
                if frame["type"] == "fill":
                    fill = frame["fill"]
                    if fill["product_id"] == product:
                        counts["fills"] += 1
                        quantity += fill["quantity"]
                        fees[fill["fee_asset"]] += fill["fee"]
                        print("Fill:", fill)
                    stream.ack(frame)  # AFTER applying; other products deliberately ignored.
                if perf_counter() < due:
                    continue
                # Ticks can be a heartbeat late; a short cadence made a modest rule aggressive here.
                due = perf_counter() + interval_seconds
                counts["ticks"] += 1
                try:
                    orders = client.open_orders()["orders"]
                    state = dict(  # Separate reads, not an atomic snapshot; history is context.
                        product_id=product,
                        quote=client.quote(product),
                        features=client.features(product),
                        portfolio=client.portfolio(),
                        open_orders=[o for o in orders if o["product_id"] == product],
                        recent_fills=client.fills()["fills"],
                        refusals=list(refusals),
                        session=dict(counts),
                    )
                    intent = decide(state)
                    if intent is None:
                        counts["skips"] += 1
                        continue
                    counts["intents"] += 1
                    try:
                        intent = validated(intent, product)
                    except (ValueError, TypeError, InvalidOperation) as error:
                        print("Local skip:", error)
                        counts["skips"] += 1
                        continue
                    for attempt in range(2):
                        key = uuid4().hex
                        print("Submitting:", key, intent)  # Resolve this key if transport fails.
                        counts["submitted"] += 1
                        try:
                            # Preserve reasoning verbatim: this is the record read after a bad run.
                            client.place_order(client_order_id=key, **intent)
                            counts["accepted"] += 1
                            break
                        except APIError as error:
                            code = refused(error, refusals)
                            step = error.body.get("increment")
                            if attempt == 0 and code == "bad_quantity" and step:
                                old = intent["quantity"]
                                rounded = (old / step).to_integral_value(rounding=ROUND_DOWN) * step
                                print("Rounded DOWN:", old, "->", rounded)
                                minimum = error.body.get("minimum", Decimal(0))
                                if 0 < rounded < old and rounded >= minimum:
                                    intent = {**intent, "quantity": rounded}
                                    continue  # Corrected intent, fresh key; no third attempt.
                            # below_min_size: keep the minimum as feedback; never inflate.
                            counts["skips"] += 1
                            break
                except APIError as error:
                    refused(error, refusals)
                    counts["skips"] += 1
    except KeyboardInterrupt:
        print("Stopped by Ctrl-C")
    except ConnectionClosed as error:
        raise SystemExit(f"Stream disconnected: {error}") from None
    finally:
        print(f"Summary (simulated, observed {product}):", dict(counts))
        print("Quantity filled:", quantity, "Fees:", {asset: str(n) for asset, n in fees.items()})
        print("Refusals:", dict(Counter(r["reason"].split(":", 1)[0] for r in refusals)))
        print("Accepted orders may fill after exit. Score with: python -m simex.evaluator")


if __name__ == "__main__":
    interval = int(os.environ.get("BOT_INTERVAL_SECONDS", "60"))
    if interval <= 0:
        raise SystemExit("BOT_INTERVAL_SECONDS must be positive")
    port, token = required_env("EXCHANGE_API_PORT", "EXCHANGE_API_TOKEN")
    with Client(f"http://127.0.0.1:{port}", token) as client:
        run(client, os.environ.get("BOT_PRODUCT", "BTC-USD"), interval)
