"""Ledger-only execution aggregates and their private text representation."""

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal

NS = 1_000_000_000
ZERO = Decimal(0)
BPS = Decimal(10000)
STATUSES = ("pending", "open", "partially_filled", "filled", "cancelled", "expired", "rejected")
REFUSAL_COVERAGE = (
    "Persisted order refusals only; request-validation refusals are visible only to the client "
    "that made them. Other early refusals, including reused-key errors, are also not counted."
)
DECISION_COVERAGE = (
    "Persisted orders only: decide() returning None and intents rejected by local or request "
    "validation never reach this log."
)
MARKET_LABEL = (
    "Market band usage: signed bps to the worst permitted price; negative = unused band "
    "headroom, not a slippage win or evidence of skill"
)
MISSING_BANDS = (
    "Market orders completed before the band-retention fix have no recorded benchmark. "
    "Missing benchmarks are excluded and never reconstructed."
)
COHORT = (
    "Orders selected by creation time, with all their committed fills through this snapshot. "
    "Pre-window orders filling later are excluded; statuses are current committed outcomes."
)


def timestamp(ns):
    seconds, fraction = divmod(ns, NS)
    return datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S") + f".{fraction:09d}Z"


def number(value):
    if value is None:
        return "unavailable"
    if isinstance(value, Decimal):
        text = format(value, "f")
        return text.rstrip("0").rstrip(".") if "." in text else text
    return str(value)


def encoded(value):
    return json.dumps(value, default=number, ensure_ascii=False, allow_nan=False)


def ratio(numerator, denominator, scale=BPS):
    return numerator / denominator * scale if denominator else None


def distribution(values, *, centre="mean"):
    values = sorted(values)
    middle = len(values) // 2
    if not values:
        value = None
    elif centre == "mean":
        value = sum(values, ZERO) / len(values)
    elif len(values) % 2:
        value = values[middle]
    else:
        value = (values[middle - 1] + values[middle]) / 2
    return {"count": len(values), centre: value, "worst": max(values) if values else None}


def account_report(account, orders, fills, decisions):
    by_id = {order["order_id"]: order for order in orders}
    newest = sorted(orders, key=lambda order: (-order["created_at_ns"], order["order_id"]))
    counts = Counter(order["status"] for order in orders)
    reasons = defaultdict(list)
    for order in newest:
        if order["status"] == "rejected":
            reasons[(order["reject_reason"] or "unavailable").split(":", 1)[0]].append(order)
    refusals = []
    for reason, rejected in sorted(reasons.items()):
        example = rejected[0]
        refusals.append(
            dict(
                reason=reason,
                count=len(rejected),
                example=dict(
                    order_id=example["order_id"],
                    product_id=example["request"]["product_id"],
                    reason=example["reject_reason"],
                    details=example["reject_details"] or None,
                ),
            )
        )
    notionals, fees, quote_fees, foreign = {}, defaultdict(Decimal), defaultdict(Decimal), Counter()
    quotes = {}
    quality = defaultdict(lambda: {"market": [], "limit": []})
    for order in orders:
        product = order["request"]["product_id"]
        quote = product.rsplit("-", 1)[1] if "-" in product else None
        quotes[order["order_id"]] = quote
        if quote is not None:
            totals = notionals.setdefault(quote, dict(filled=ZERO, maker=ZERO, taker=ZERO))
            for key, field in (
                ("filled", "filled_notional"),
                ("maker", "maker_notional"),
                ("taker", "taker_notional"),
            ):
                totals[key] += order[field]
        quality[product][order["request"]["type"]].append(order)
    first_fill, liquidity, causes = {}, Counter(maker=0, taker=0), Counter()
    for fill in fills:
        order_id, asset = fill["order_id"], fill["fee_asset"]
        fees[asset] += fill["fee"]
        quote = quotes[order_id]
        if asset == quote:
            quote_fees[quote] += fill["fee"]
        else:
            foreign[quote] += 1
        liquidity[fill["liquidity"]] += 1
        causes[fill["cause"]] += 1
        first_fill[order_id] = min(first_fill.get(order_id, fill["ts_ns"]), fill["ts_ns"])
    for quote, totals in notionals.items():
        totals["maker_share_pct"] = ratio(totals["maker"], totals["filled"], Decimal(100))
        totals["taker_share_pct"] = ratio(totals["taker"], totals["filled"], Decimal(100))
        totals["effective_fee_bps"] = (
            None if foreign[quote] else ratio(quote_fees[quote], totals["filled"])
        )
        totals["unconverted_fee_fills"] = foreign[quote]
    prices = {}
    for product, kinds in sorted(quality.items()):
        prices[product] = {}
        for kind, selected in kinds.items():
            values, missing, invalid, unfilled = [], 0, 0, 0
            for order in selected:
                if not order["filled_quantity"]:
                    unfilled += 1
                    continue
                reference = (
                    order["market_band_price"]
                    if kind == "market"
                    else order["request"].get("limit_price")
                )
                if reference is None:
                    missing += 1
                    continue
                reference = Decimal(reference)
                if reference <= 0:
                    invalid += 1
                    continue
                vwap = order["filled_notional"] / order["filled_quantity"]
                sign = 1 if order["request"]["side"] == "buy" else -1
                values.append((vwap / reference - 1) * BPS * sign)
            prices[product][kind] = {
                **distribution(values),
                "excluded_missing_benchmark": missing,
                "excluded_invalid_benchmark": invalid,
                "unfilled_orders": unfilled,
            }
    waits = [
        Decimal(ts - by_id[order_id]["effective_at_ns"]) / NS for order_id, ts in first_fill.items()
    ]
    delay = [Decimal(order["effective_at_ns"] - order["created_at_ns"]) / NS for order in orders]
    log = [
        dict(
            order_id=order["order_id"],
            created_at_ns=order["created_at_ns"],
            intent={key: value for key, value in order["request"].items() if key != "reasoning"},
            outcome=order["status"],
            reasoning=order["reasoning"],
            reject_reason=order["reject_reason"],
            reject_details=order["reject_details"] or None,
        )
        for order in newest[:decisions]
    ]
    return dict(
        account_id=account,
        order_count=len(orders),
        statuses={status: counts[status] for status in STATUSES},
        still_open=sum(counts[status] for status in ("pending", "open", "partially_filled")),
        refusal_count=sum(group["count"] for group in refusals),
        refusal_groups=refusals,
        liquidity_counts=dict(liquidity),
        fill_causes=dict(sorted(causes.items())),
        notional_by_quote=dict(sorted(notionals.items())),
        fees_by_asset=dict(sorted(fees.items())),
        execution_bound_bps=prices,
        scheduled_delay_seconds=distribution(delay, centre="median"),
        time_to_first_fill_seconds=distribution(waits, centre="median"),
        decisions=log,
    )


def render_report(report):
    window, refs = report["window"], report["fee_references"]
    start = "all history" if window["since_ns"] is None else timestamp(window["since_ns"])
    lines = [
        "Private execution report (simulated money)",
        f"Order creation window: {start} through {timestamp(window['snapshot_ns'])}",
        COHORT,
    ]
    for account in report["accounts"]:
        lines += [
            f"\n--- Account: {account['account_id']} ---",
            f"Orders: {account['order_count']}; outcomes: {encoded(account['statuses'])}",
            f"Still open (pending/open/partially_filled): {account['still_open']}",
            f"Refusals ({account['refusal_count']}): {REFUSAL_COVERAGE}",
        ]
        for group in account["refusal_groups"]:
            unavailable = " (details unavailable)" if group["example"]["details"] is None else ""
            lines.append(
                f"  {group['reason']}: {group['count']}; "
                f"example: {encoded(group['example'])}{unavailable}"
            )
        lines += [
            f"Maker/taker fill counts: {encoded(account['liquidity_counts'])}",
            f"Fill causes: {encoded(account['fill_causes'])}",
        ]
        for quote, totals in account["notional_by_quote"].items():
            lines.append(
                f"Notional ({quote}): filled={number(totals['filled'])}; "
                f"maker={number(totals['maker'])} ({number(totals['maker_share_pct'])}%); "
                f"taker={number(totals['taker'])} ({number(totals['taker_share_pct'])}%)"
            )
        lines += [
            f"Fees by asset: {encoded(account['fees_by_asset'])}",
            f"Current configured fee references: maker={number(refs['maker_bps'])} bps; "
            f"taker={number(refs['taker_bps'])} bps (not recovered historical settings).",
            "Upward per-order fee rounding can put small-order effective rates above nominal.",
        ]
        for quote, totals in account["notional_by_quote"].items():
            lines.append(
                f"Effective fee ({quote}): {number(totals['effective_fee_bps'])} bps; "
                f"unconverted fee fills={totals['unconverted_fee_fills']}"
            )
        lines += ["Execution price bounds:", MARKET_LABEL + ".", MISSING_BANDS]
        for product, kinds in account["execution_bound_bps"].items():
            for kind, stats in kinds.items():
                label = "market band usage" if kind == "market" else "limit bound distance"
                lines.append(
                    f"  {product} {label} (bps): n={stats['count']}; mean={number(stats['mean'])}; "
                    f"worst={number(stats['worst'])}; "
                    f"excluded missing benchmark={stats['excluded_missing_benchmark']}; "
                    f"excluded invalid benchmark={stats['excluded_invalid_benchmark']}; "
                    f"unfilled={stats['unfilled_orders']}"
                )
        for key, label in (
            ("scheduled_delay_seconds", "Scheduled anti-cheat delay"),
            ("time_to_first_fill_seconds", "Time to first fill after effect"),
        ):
            stats = account[key]
            lines.append(
                f"{label} (seconds): n={stats['count']}; median={number(stats['median'])}; "
                f"worst={number(stats['worst'])}"
            )
        lines.append("Scheduled delay is a configured deadline difference, not processing latency.")
        if report["decisions_limit"]:
            lines.append(f"Decision log (last {report['decisions_limit']}): {DECISION_COVERAGE}")
            for decision in account["decisions"]:
                lines.append(
                    f"  {timestamp(decision['created_at_ns'])} {decision['order_id']} "
                    f"intent={encoded(decision['intent'])}; outcome={decision['outcome']}; "
                    f"reason={decision['reject_reason']}; "
                    f"details={encoded(decision['reject_details'])}; "
                    f"reasoning: {decision['reasoning']}"
                )
    lines.append("For performance: python -m simex.evaluator")
    return "\n".join(lines)
