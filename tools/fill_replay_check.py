"""Replay recordings through the fill simulator with probe orders, and check it (#8, ADR 0014).

Every --every seconds of market time, per pair, while the book is SYNCED: cancel the previous
passive probes, then place a minimum-size limit buy at the best bid and sell at the best ask, and
a minimum-size market buy and sell. The same stream runs under all three cancel attributions.

Checks (FAIL exits 1): money conserved and no negative balance; no fill while unexplained (every
maker fill has a cause). Reports, per pair and mode: probes, passive fill rate, maker fills by
cause (book_cross share flagged above 50%), taker fills, band binds, depletion expiries, prints
ignored while not SYNCED, rejections on effect, and the share of print quantity no shrink
explains. Counts only; no prices (ADR 0010).

Usage:
    python tools/fill_replay_check.py --start 2026-10-16T12:00 --hours 1
    python tools/fill_replay_check.py --root data/recordings --start ... --hours 6 --every 30
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    CancelAttribution,
    EngineSettings,
    FeeSchedule,
    GateDecision,
    PairEngine,
)
from simex.exchange.accounts import SYSTEM_ACCOUNTS
from simex.marketdata import decode
from simex.recorder.replay import read

SECOND = 1_000_000_000
DELAY_NS = 200_000_000
FUNDS = {"USD": "100000000", "BTC": "10000", "ETH": "100000", "SOL": "10000000"}


class BookGate:
    """Open while the pair's book is SYNCED, with its best prices (the #9 staleness rule is
    not part of this check)."""

    def __init__(self) -> None:
        self.engines: dict[str, PairEngine] = {}

    def check(self, product_id: str, now_ns: int) -> GateDecision:
        book = self.engines[product_id].book
        if not book.ready:
            return GateDecision(False, "book_not_synced")
        bid, ask = book.best_bid(), book.best_ask()
        return GateDecision(True, None, bid[0] if bid else None, ask[0] if ask else None)


class Run:
    def __init__(self, mode: CancelAttribution, products: list[str], every_ns: int) -> None:
        self.mode = mode
        self.gate = BookGate()
        self.writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), self.gate, DELAY_NS)
        for asset, amount in FUNDS.items():
            self.writer.accounts.deposit("probe", asset, Decimal(amount))
        settings = EngineSettings.from_env()
        settings = EngineSettings(
            settings.match_window_ns,
            settings.match_window_ns_by_pair,
            settings.depletion_max_age_ns,
            mode,
        )
        for product in products:
            self.gate.engines[product] = PairEngine(product, self.writer, settings)
        self.every_ns = every_ns
        self.next_probe: dict[str, int] = {}
        self.passive: dict[str, list[str]] = {p: [] for p in products}
        self.placed: Counter = Counter()
        self.ids = 0

    def on_event(self, event) -> None:
        engine = self.gate.engines.get(event.product_id)
        if engine is None:
            return
        engine.on_event(event)
        now = getattr(event, "received_at_ns", 0)
        due = self.next_probe.setdefault(event.product_id, now)
        if now >= due and engine.book.ready:
            self.next_probe[event.product_id] = now + self.every_ns
            self._probe(engine, now)

    def _probe(self, engine: PairEngine, now: int) -> None:
        product = engine.product_id
        for cid in self.passive[product]:
            self.writer.request_cancel("probe", now, client_order_id=cid)
        self.passive[product] = []
        bid, ask = engine.book.best_bid(), engine.book.best_ask()
        size = str(engine.instrument.min_size)
        orders = [("market", "buy", None), ("market", "sell", None)]
        if bid:
            orders.append(("limit", "buy", bid[0]))
        if ask:
            orders.append(("limit", "sell", ask[0]))
        for kind, side, price in orders:
            self.ids += 1
            cid = f"p{self.ids}"
            payload = {
                "client_order_id": cid,
                "product_id": product,
                "side": side,
                "type": kind,
                "quantity": size,
            }
            if price is not None:
                payload["limit_price"] = str(price)
            order = self.writer.submit("probe", payload, now)
            engine.submit(order)
            self.placed[(product, kind)] += 1
            if kind == "limit":
                self.passive[product].append(cid)

    def finish(self, end_ns: int) -> None:
        for engine in self.gate.engines.values():
            engine.advance(end_ns + 10 * SECOND)
            engine.matcher.expire(end_ns + 20 * SECOND)

    def problems(self) -> list[str]:
        out = []
        accounts = self.writer.accounts
        for asset, total in accounts.totals().items():
            if total != 0:
                out.append(f"{self.mode}: {asset} created or destroyed")
        for account, balances in accounts.balances.items():
            if account in SYSTEM_ACCOUNTS:
                continue
            for asset, balance in balances.items():
                if balance.available < 0 or balance.reserved < 0:
                    out.append(f"{self.mode}: negative {asset} balance for {account}")
        causes = {"print", "trade_through", "book_cross", "taker"}
        for event in self.writer.events:
            if event["type"] == "fill" and event["fill"]["cause"] not in causes:
                out.append(f"{self.mode}: a fill with no known cause")
                break
        return out

    def report(self, product: str) -> list[str]:
        engine = self.gate.engines[product]
        c, q = engine.stats.counts, engine.matcher.quantities
        filled = sum(
            1
            for o in self.writer.orders.values()
            if o.request.product_id == product
            and o.request.limit_price is not None
            and o.filled_quantity > 0
        )
        passive = self.placed[(product, "limit")]
        maker = {k: c[f"maker_fills:{k}"] for k in ("print", "trade_through", "book_cross")}
        total_maker = sum(maker.values())
        cross_share = maker["book_cross"] / total_maker if total_maker else 0
        unexplained = q["print_unmatched"] / q["print"] if q["print"] else Decimal(0)
        rejected = sum(v for k, v in c.items() if k.startswith("rejected_on_effect:"))
        expired = {
            k.split(":")[1]: v for k, v in sorted(c.items()) if k.startswith("depletion_expired:")
        }
        flag = "  <- RED FLAG: book_cross dominates" if cross_share > 0.5 else ""
        return [
            f"  {self.mode.value:>12}: passive {filled}/{passive} filled"
            f" ({filled / passive if passive else 0:.1%}); maker fills print {maker['print']},"
            f" trade_through {maker['trade_through']}, book_cross {maker['book_cross']}"
            f" ({cross_share:.1%}){flag}",
            f"  {'':>12}  market orders {c['market_orders']}, band binds {c['band_binds']},"
            f" taker fills {c['taker_fills']}; rejected on effect {rejected};"
            f" prints ignored (not synced) {c['prints_ignored_unsynced']};"
            f" depletion expired {expired or 0}; print qty no shrink explains {unexplained:.1%}",
        ]


def parse_time(text: str) -> datetime:
    when = datetime.fromisoformat(text)
    return when.replace(tzinfo=UTC) if when.tzinfo is None else when.astimezone(UTC)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", default="data/recordings")
    parser.add_argument("--start", required=True, help="window start, ISO time (UTC by default)")
    parser.add_argument("--hours", type=float, default=1.0)
    parser.add_argument("--every", type=float, default=60.0, help="seconds between probes")
    args = parser.parse_args()
    start = parse_time(args.start)
    end = start + timedelta(hours=args.hours)
    products = sorted(DEFAULT_INSTRUMENTS)
    runs = [Run(mode, products, int(args.every * SECOND)) for mode in CancelAttribution]
    records = 0
    for rec in read(args.root, start, end, channels=["level2", "market_trades"]):
        records += 1
        for event in decode(rec.value):
            for run in runs:
                run.on_event(event)
    if not records:
        print("no recordings in that window")
        return 1
    end_ns = int(end.timestamp() * SECOND)
    problems = []
    for run in runs:
        run.finish(end_ns)
        problems += run.problems()
    print(f"{start:%Y-%m-%d %H:%M} .. {end:%Y-%m-%d %H:%M} UTC, {records} records")
    for product in products:
        print(product)
        for run in runs:
            print("\n".join(run.report(product)))
    print("FAIL:\n  " + "\n  ".join(problems) if problems else "PASS")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
