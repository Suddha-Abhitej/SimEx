"""Replay recordings through the fill simulator with probe orders, and check it (#8, ADR 0014).

Every --every seconds of market time, per pair, while the book is SYNCED: a minimum-size limit buy
at the best bid and sell at the best ask, each cancelled --lifetime seconds later, and a
minimum-size market buy and sell. The same stream runs four ways: the three cancel attributions
under the default EXCHANGE_BOOK_CROSS=print, and proportional under `fill` (the optimistic
bracket for crossing liquidity).

FAIL (exit 1) if, in any run, money is created or destroyed or a balance goes negative; under the
default, if book-cross fills are over half of maker fills (a guard against fill-on-cross becoming
the default again); and, the hard test of maker realism (ADR 0014 §B), if the default's maker
fills show markouts more than 1 bp better on average than real prints' at the same side and
price, at any horizon with at least 20 matched fills.

Reports per pair and run: passive probes filled as maker within 10 / 60 / 300 s of taking effect
(and those that crossed as taker on arrival), maker fills by cause, crosses marked, market orders
and band binds, depletion expiries, prints before the first snapshot and during resyncs,
rejections on effect, and the share of print quantity no shrink explains. Counts and basis
points only; no prices (ADR 0010).

Usage:
    python tools/fill_replay_check.py
    python tools/fill_replay_check.py --last-hours 24 --json data/fill-accuracy/latest.json
    python tools/fill_replay_check.py --start 2026-10-16T12:00 --hours 1
    python tools/fill_replay_check.py --root data/recordings --start ... --hours 28
    python tools/fill_replay_check.py --start ... --hours 1 --blackout 15
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import json
import os
import sys
import tempfile
from collections import Counter, defaultdict, deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    CancelAttribution,
    EngineSettings,
    FeeSchedule,
    GateDecision,
    PairEngine,
    StaleDataGate,
)
from simex.exchange.accounts import SYSTEM_ACCOUNTS
from simex.exchange.engine import BookCross
from simex.marketdata import BookUpdate, Trade, TradeDeduper, decode
from simex.recorder.replay import files, read

SECOND = 1_000_000_000
DELAY_NS = 200_000_000
FUNDS = {"USD": "100000000", "BTC": "10000", "ETH": "100000", "SOL": "10000000"}
FILL_WITHIN_S = (10, 60, 300)
MARKOUT_S = (1, 10, 60)
MARKOUT_LIMIT_BPS = Decimal(1)  # ADR 0014 §B: no more than 1 bp better than real prints
MIN_MATCHED = 20  # fewer matched fills than this at a horizon: reported, not judged
BPS = Decimal(10_000)
RUNS = [
    (CancelAttribution.PROPORTIONAL, BookCross.PRINT),  # the default, judged
    (CancelAttribution.PESSIMISTIC, BookCross.PRINT),
    (CancelAttribution.OPTIMISTIC, BookCross.PRINT),
    (CancelAttribution.PROPORTIONAL, BookCross.FILL),
]


class BookGate:
    """The replay gate, with optional level2 blackouts for the live guard smoke test."""

    def __init__(self) -> None:
        self.engines: dict[str, PairEngine] = {}
        self.stale = StaleDataGate(self.engines)

    def check(self, product_id: str, now_ns: int) -> GateDecision:
        return self.stale.check(product_id, now_ns)


def mid(engine: PairEngine) -> Decimal | None:
    book = engine.book
    if not book.ready:
        return None
    bid, ask = book.best_bid(), book.best_ask()
    return (bid[0] + ask[0]) / 2 if bid and ask else None


def markout_bps(maker_side: str, price: Decimal, later_mid: Decimal) -> Decimal:
    """What the maker made by the horizon, in bps of price: positive is good for the maker."""
    move = later_mid - price if maker_side == "buy" else price - later_mid
    return move / price * BPS


class Markouts:
    """Markouts at each horizon for a stream of maker fills, measured against the book's mid
    when the horizon comes due on the market clock."""

    def __init__(self) -> None:
        # (product, horizon) -> (due, key, side, price): fills arrive in time order, so each
        # deque is in due order.
        self.due: dict[tuple[str, int], deque] = defaultdict(deque)
        self.values: dict[tuple, list[Decimal]] = defaultdict(list)  # (key, h) -> bps
        self.missed = 0  # horizon came due while the book had no mid

    def add(self, product: str, key: tuple, side: str, price: Decimal, ts: int) -> None:
        for h in MARKOUT_S:
            self.due[(product, h)].append((ts + h * SECOND, key, side, price))

    def settle(self, engine: PairEngine, now: int) -> None:
        for h in MARKOUT_S:
            pending = self.due[(engine.product_id, h)]
            while pending and pending[0][0] <= now:
                _, key, side, price = pending.popleft()
                later = mid(engine)
                if later is None:
                    self.missed += 1
                else:
                    self.values[(key, h)].append(markout_bps(side, price, later))


class Run:
    def __init__(
        self,
        mode: CancelAttribution,
        cross: BookCross,
        products: list[str],
        every_ns: int,
        lifetime_ns: int,
    ) -> None:
        self.mode, self.cross = mode, cross
        self.name = f"{mode.value}/{cross.value}"
        self.gate = BookGate()
        self.writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), self.gate, DELAY_NS)
        for asset, amount in FUNDS.items():
            self.writer.accounts.deposit("probe", asset, Decimal(amount))
        settings = dataclasses.replace(
            EngineSettings.from_env(), cancel_attribution=mode, book_cross=cross
        )
        for product in products:
            self.gate.engines[product] = PairEngine(product, self.writer, settings)
        self.every_ns, self.lifetime_ns = every_ns, lifetime_ns
        self.next_probe: dict[str, int] = {}
        self.expiring: dict[str, deque] = defaultdict(deque)  # product -> (cancel at, cid)
        self.placed: Counter = Counter()
        self.passive: dict[str, str] = {}  # order_id -> product, for passive probes
        self.first_maker_fill: dict[str, int] = {}
        self.taker_on_arrival: set[str] = set()
        self.markouts = Markouts()
        self.seen_seq = 0
        self.ids = 0
        self.closed_seconds: Counter[tuple[str, str]] = Counter()
        self._gate_seen: dict[str, tuple[int, str]] = {}

    def on_event(self, event) -> None:
        engine = self.gate.engines.get(event.product_id)
        if engine is None:
            return
        engine.on_event(event)
        now = getattr(event, "received_at_ns", 0)
        decision = self.gate.check(engine.product_id, now)
        reason = (decision.reason or "open").split(":", 1)[0]
        previous = self._gate_seen.get(engine.product_id)
        if previous is not None and previous[1] != "open":
            self.closed_seconds[(engine.product_id, previous[1])] += max(0, now - previous[0])
        self._gate_seen[engine.product_id] = (now, reason)
        self._collect_fills()
        self.markouts.settle(engine, now)
        expiring = self.expiring[engine.product_id]
        while expiring and expiring[0][0] <= now:
            _, cid = expiring.popleft()
            self.writer.request_cancel("probe", now, client_order_id=cid)
        due = self.next_probe.setdefault(event.product_id, now)
        if now >= due and engine.book.ready:
            self.next_probe[event.product_id] = now + self.every_ns
            self._probe(engine, now)

    def _collect_fills(self) -> None:
        oldest, latest, events = self.writer.events_after(self.seen_seq)
        missed = oldest - self.seen_seq - 1
        if missed > 0:
            raise RuntimeError(
                f"{self.name}: {missed} writer events evicted after sequence {self.seen_seq} "
                f"(oldest retained {oldest}); fill accuracy results invalid"
            )
        for event in events:
            if event["type"] != "fill":
                continue
            fill = event["fill"]
            order_id = fill["order_id"]
            if order_id not in self.passive:
                continue
            if fill["liquidity"] == "taker":
                self.taker_on_arrival.add(order_id)
                continue
            self.first_maker_fill.setdefault(order_id, fill["ts_ns"])
            key = ("sim", fill["product_id"], fill["side"], Decimal(fill["price"]))
            self.markouts.add(fill["product_id"], key, fill["side"], key[3], fill["ts_ns"])
        self.seen_seq = latest

    def _probe(self, engine: PairEngine, now: int) -> None:
        product = engine.product_id
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
                self.passive[order.order_id] = product
                self.expiring[product].append((now + self.lifetime_ns, cid))

    def finish(self, end_ns: int) -> None:
        for engine in self.gate.engines.values():
            engine.advance(end_ns + 10 * SECOND)
            engine.matcher.expire(end_ns + 20 * SECOND)
        self._collect_fills()
        for product, (started, reason) in self._gate_seen.items():
            if reason != "open":
                self.closed_seconds[(product, reason)] += max(0, end_ns - started)

    def money_problems(self) -> list[str]:
        out = []
        accounts = self.writer.accounts
        for asset, total in accounts.totals().items():
            if total != 0:
                out.append(f"{self.name}: {asset} created or destroyed")
        for account, balances in accounts.balances.items():
            if account in SYSTEM_ACCOUNTS:
                continue
            for asset, balance in balances.items():
                if balance.available < 0 or balance.reserved < 0:
                    out.append(f"{self.name}: negative {asset} balance for {account}")
        return out

    def maker_mix(self, product: str) -> tuple[dict[str, int], float]:
        c = self.gate.engines[product].stats.counts
        maker = {k: c[f"maker_fills:{k}"] for k in ("print", "trade_through", "book_cross")}
        total = sum(maker.values())
        return maker, (maker["book_cross"] / total if total else 0.0)

    def report(self, product: str, summary: dict | None = None) -> list[str]:
        engine = self.gate.engines[product]
        c, q = engine.stats.counts, engine.matcher.quantities
        probes = [
            o
            for o in self.writer.orders.values()
            if self.passive.get(o.order_id) == product and o.effective_at_ns
        ]
        rested = [o for o in probes if o.order_id not in self.taker_on_arrival]
        rates = []
        for seconds in FILL_WITHIN_S:
            n = sum(
                1
                for o in rested
                if o.order_id in self.first_maker_fill
                and self.first_maker_fill[o.order_id] - o.effective_at_ns <= seconds * SECOND
            )
            rates.append(f"{seconds} s {n / len(rested) if rested else 0:.1%}")
            if summary is not None:
                summary.setdefault("filled_within_seconds", {})[str(seconds)] = n
        maker, cross_share = self.maker_mix(product)
        unexplained = q["print_unmatched"] / q["print"] if q["print"] else Decimal(0)
        rejected = sum(v for k, v in c.items() if k.startswith("rejected_on_effect:"))
        expired = {
            k.split(":")[1]: v for k, v in sorted(c.items()) if k.startswith("depletion_expired:")
        }
        flag = "  <- RED FLAG: book_cross dominates" if cross_share > 0.5 else ""
        closed = (
            ", ".join(
                f"{reason} {self.closed_seconds[(pair, reason)] / SECOND:.1f}s"
                for pair, reason in self.closed_seconds
                if pair == product
            )
            or "0s"
        )
        if summary is not None:
            # Aggregate whitelist only: internal orders/markout keys contain private prices.
            summary.update(
                passive_probes=len(probes),
                rested_probes=len(rested),
                taker_on_arrival=len(probes) - len(rested),
                maker_fills=maker,
                crosses_marked=c["crosses_marked"],
                market_orders=c["market_orders"],
                band_binds=c["band_binds"],
                rejected_on_effect=rejected,
                prints_before_first_snapshot=c["prints_before_first_snapshot"],
                prints_during_resyncs=c["prints_ignored_unsynced"],
                prints_stale=c["prints_ignored_stale"],
                depletion_expired=expired,
                gate_closed_seconds={
                    reason: Decimal(duration) / SECOND
                    for (pair, reason), duration in self.closed_seconds.items()
                    if pair == product
                },
                unexplained_print_percentage=unexplained * 100,
            )
        return [
            f"  {self.name:>19}: passive filled as maker within {', '.join(rates)}"
            f" (of {len(rested)}; {len(probes) - len(rested)} crossed as taker on arrival)",
            f"  {'':>19}  maker fills print {maker['print']}, trade_through"
            f" {maker['trade_through']}, book_cross {maker['book_cross']} ({cross_share:.1%})"
            f"{flag}; crosses marked {c['crosses_marked']}",
            f"  {'':>19}  market orders {c['market_orders']}, band binds {c['band_binds']};"
            f" rejected on effect {rejected}; prints before the first snapshot"
            f" {c['prints_before_first_snapshot']}, during resyncs"
            f" {c['prints_ignored_unsynced']}, stale {c['prints_ignored_stale']}; depletion expired"
            f" {expired or 0}; gate closed {closed};"
            f" print qty no shrink explains {unexplained:.1%}",
        ]


def compare_markouts(
    run: Run, real: Markouts, product: str, summary: dict | None = None
) -> tuple[list[str], list[str]]:
    """The run's maker fills against real prints' makers at the same side and price."""
    lines, problems = [], []
    for h in MARKOUT_S:
        diffs, unmatched = [], 0
        for (key, horizon), values in run.markouts.values.items():
            if horizon != h or key[1] != product:
                continue
            reference = real.values.get((("real", *key[1:]), h))
            if not reference:
                unmatched += len(values)
                continue
            real_mean = sum(reference) / len(reference)
            diffs += [v - real_mean for v in values]
        if summary is not None:
            summary[str(h)] = {
                "matched_fills": len(diffs),
                "unmatched_fills": unmatched,
                "mean_difference_bps": None,
                "status": "no_matched_fills",
            }
        if not diffs:
            lines.append(f"{h} s: no matched fills ({unmatched} unmatched)")
            continue
        mean = sum(diffs) / len(diffs)
        verdict = ""
        status = "pass"
        if len(diffs) < MIN_MATCHED:
            verdict = " (too few to judge)"
            status = "too_few_to_judge"
        elif mean > MARKOUT_LIMIT_BPS:
            verdict = " <- better than real"
            status = "fail"
            problems.append(f"{run.name} {product}: maker markout at {h} s {mean:+.2f} bp vs real")
        lines.append(f"{h} s {mean:+.2f} bp over {len(diffs)} ({unmatched} unmatched){verdict}")
        if summary is not None:
            summary[str(h)].update(mean_difference_bps=mean, status=status)
    return lines, problems


def parse_time(text: str) -> datetime:
    when = datetime.fromisoformat(text)
    return when.replace(tzinfo=UTC) if when.tzinfo is None else when.astimezone(UTC)


class ReportError(ValueError):
    """An unavailable window or unwritable report, without a misleading partial result."""


def positive_hours(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a positive integer") from None
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def discover_window(
    root: str | Path, hours: int = 1, *, now: datetime | None = None
) -> tuple[datetime, datetime]:
    cutoff = (now or datetime.now(UTC)).astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    available = {
        part.hour
        for part in files(root, venue="gemini", products=DEFAULT_INSTRUMENTS, channels=["level2"])
        # Clean shutdown publishes open parts too; published does not mean the hour has ended.
        if part.hour < cutoff and part.product in DEFAULT_INSTRUMENTS
    }
    if not available:
        raise ReportError(
            "No complete published Gemini level2 hour is available under --root. "
            "Was the recorder running? Wait for a complete hour to be published."
        )
    end = max(available) + timedelta(hours=1)
    start = end - timedelta(hours=hours)
    missing = [
        start + timedelta(hours=i)
        for i in range(hours)
        if start + timedelta(hours=i) not in available
    ]
    if missing:
        raise ReportError(
            f"Cannot select {hours} consecutive complete hours: missing published level2 hours "
            + ", ".join(hour.isoformat() for hour in missing)
            + ". Check --root, wait for publication, or select an explicit "
            "--start / --hours window."
        )
    return start, end


def measurement_policy(runs: list[Run], blackout: float) -> dict:
    """Record the effective objects used in this experiment, not guessed environment defaults."""
    default = runs[0]
    first_engine = next(iter(default.gate.engines.values()))
    return {
        "default_cancel_attribution": first_engine.settings.cancel_attribution.value,
        "default_book_cross": first_engine.settings.book_cross.value,
        "markout_horizons_seconds": list(MARKOUT_S),
        "min_matched": MIN_MATCHED,
        "markout_limit_bps": MARKOUT_LIMIT_BPS,
        "fill_within_seconds": list(FILL_WITHIN_S),
        "probe_interval_ns": default.every_ns,
        "probe_lifetime_ns": default.lifetime_ns,
        "blackout_seconds": Decimal(str(blackout)),
        "runs": {
            run.name: {
                "maker_fee_bps": run.writer.fees.maker_bps,
                "taker_fee_bps": run.writer.fees.taker_bps,
                "fee_increment": run.writer.fees.fee_increment,
                "order_delay_ns": run.writer.delay_ns,
                "pairs": {
                    product: {
                        "market_band_bps": engine.instrument.market_band_bps,
                        "cancel_attribution": engine.settings.cancel_attribution.value,
                        "book_cross": engine.settings.book_cross.value,
                        "match_window_ns": engine.settings.window_ns(product),
                        "depletion_max_age_ns": engine.settings.depletion_max_age_ns,
                        "lag_baseline_minutes": Decimal(str(engine.settings.lag_baseline_minutes)),
                    }
                    for product, engine in run.gate.engines.items()
                },
            }
            for run in runs
        },
    }


def write_summary(path: Path, summary: dict) -> None:
    def encode(value):
        if isinstance(value, Decimal):
            return str(value)
        raise TypeError(f"Unsupported summary value: {type(value).__name__}")

    content = json.dumps(summary, default=encode, indent=2, sort_keys=True, allow_nan=False) + "\n"
    tmp = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as handle:
            tmp = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except OSError as exc:
        raise ReportError(f"Cannot write JSON summary to {path}: {exc}") from None
    finally:
        if tmp is not None:
            with contextlib.suppress(OSError):
                tmp.unlink(missing_ok=True)


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", default="data/recordings")
    window = parser.add_mutually_exclusive_group()
    window.add_argument("--start", help="window start, ISO time (UTC by default)")
    window.add_argument(
        "--last-hours", type=positive_hours, help="most recent consecutive complete hours"
    )
    parser.add_argument(
        "--hours", type=float, help="explicit window length (default with --start: 1)"
    )
    parser.add_argument("--json", type=Path, help="write an additional aggregate JSON summary")
    parser.add_argument("--every", type=float, default=60.0, help="seconds between probes")
    parser.add_argument("--lifetime", type=float, default=300.0, help="passive probe life, s")
    parser.add_argument(
        "--blackout",
        type=float,
        default=0.0,
        help="skip md.level2 for this many seconds from the window start",
    )
    args = parser.parse_args(argv)
    if args.hours is not None and args.start is None:
        parser.error("--hours requires --start; use --last-hours for discovery")
    if args.start is None:
        start, end = discover_window(args.root, args.last_hours or 1)
    else:
        try:
            start = parse_time(args.start)
        except ValueError:
            raise ReportError("Invalid --start: use an ISO date/time (UTC by default).") from None
        end = start + timedelta(hours=args.hours if args.hours is not None else 1.0)
    products = sorted(DEFAULT_INSTRUMENTS)
    every, lifetime = int(args.every * SECOND), int(args.lifetime * SECOND)
    runs = [Run(mode, cross, products, every, lifetime) for mode, cross in RUNS]
    # Real prints' makers, measured on the default run's books (every run's book is the same).
    real, dedupe, reference = Markouts(), TradeDeduper(), runs[0]
    records = 0
    blackout_until = int((start.timestamp() + args.blackout) * SECOND)
    snapshots_seen: set[str] = set()
    for rec in read(args.root, start, end, channels=["level2", "market_trades"]):
        records += 1
        for event in decode(rec.value):
            if (
                args.blackout > 0
                and event.received_at_ns < blackout_until
                and (
                    not isinstance(event, BookUpdate)
                    or not event.snapshot
                    or event.product_id in snapshots_seen
                )
                and not isinstance(event, Trade)
            ):
                continue
            if isinstance(event, BookUpdate) and event.snapshot:
                snapshots_seen.add(event.product_id)
            for run in runs:
                run.on_event(event)
            engine = reference.gate.engines.get(event.product_id)
            if engine is None:
                continue
            if (
                isinstance(event, Trade)
                and dedupe.is_new(event)
                and event.aggressor in ("buy", "sell")
                and engine.book.ready
            ):
                maker = "buy" if event.aggressor == "sell" else "sell"
                key = ("real", event.product_id, maker, event.price)
                real.add(event.product_id, key, maker, event.price, event.received_at_ns)
            real.settle(engine, getattr(event, "received_at_ns", 0))
    if not records:
        print("no recordings in that window")
        return 1
    end_ns = int(end.timestamp() * SECOND)
    problems = []
    summaries = {}
    for run in runs:
        run.finish(end_ns)
        money_problems = run.money_problems()
        problems += money_problems
        summaries[run.name] = {
            "money_problems": money_problems,
            "markouts_affect_verdict": run is runs[0],
            "pairs": {},
        }
    print(f"{start:%Y-%m-%d %H:%M} .. {end:%Y-%m-%d %H:%M} UTC, {records} records")
    default = runs[0]
    judgeable = False
    for product in products:
        print(product)
        for run in runs:
            counts = {} if args.json else None
            print("\n".join(run.report(product, counts)))
            summaries[run.name]["pairs"][product] = {"counts": counts}
        if default.maker_mix(product)[1] > 0.5:
            problems.append(f"{default.name} {product}: book_cross over half of maker fills")
        markouts = {}
        lines, markout_problems = compare_markouts(default, real, product, markouts)
        summaries[default.name]["pairs"][product]["maker_markouts"] = markouts
        judgeable |= any(item["status"] in {"pass", "fail"} for item in markouts.values())
        problems += markout_problems
        print(f"  maker markouts vs real prints ({default.name}): " + "; ".join(lines))
        if args.json:
            for run in runs[1:]:
                diagnostic = {}
                compare_markouts(run, real, product, diagnostic)
                summaries[run.name]["pairs"][product]["maker_markouts"] = diagnostic
    if not judgeable:
        print(
            "Window too short to judge: every pair has fewer than MIN_MATCHED "
            f"({MIN_MATCHED}) matched fills at every horizon. "
            "Try a longer --last-hours (24 or more)."
        )
    print("FAIL:\n  " + "\n  ".join(problems) if problems else "PASS")
    if args.json:
        write_summary(
            args.json,
            {
                "schema_version": 1,
                "window": {"start": start.isoformat(), "end": end.isoformat()},
                "record_count": records,
                "policy": measurement_policy(runs, args.blackout),
                "runs": summaries,
                "verdict": "FAIL" if problems else "PASS",
                "problems": problems,
                "insufficient_samples": not judgeable,
            },
        )
    return 1 if problems else 0


def main(argv: list[str] | None = None) -> int:
    try:
        return _main(argv)
    except (ReportError, OSError, RuntimeError) as exc:
        print(f"fill replay check: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
