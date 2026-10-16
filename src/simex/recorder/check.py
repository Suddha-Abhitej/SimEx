"""Prove a recorded window replays correctly (ADR 0013): scripts/replay_check.py and the tests.

Replays md.level2 from the recordings through the unchanged decoder and OrderBook, one book per
product starting EMPTY, and checks:

1. every product's book reaches SYNCED at its first snapshot and, after that, is SYNCED for
   every update except inside a resync window (a marker until the next snapshot), with no
   crossed book;
2. every recorded aligned audit after the book's first snapshot still matches: none mismatch,
   and none is skipped outside a resync window;
3. the recorded audit rows and the audits the decoder replays agree, in total and aligned;
4. how many bytes the window takes on disk, scaled to GB/day.

It reports counts only, never prices or sizes, so the output can be pasted anywhere (ADR 0010).
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from simex.book.orderbook import OrderBook, Outcome
from simex.marketdata import BookAudit, BookUpdate, ResyncMarker, decode

from .replay import files, read


@dataclass
class ProductReport:
    product: str
    level2: int = 0
    trades: int = 0
    first_snapshot_ns: int | None = None
    snapshots: int = 0
    markers: int = 0
    crossed: int = 0
    unsynced_updates: int = 0  # after the first snapshot, outside resync windows
    before_first_snapshot: int = 0
    in_resync: int = 0
    audit_rows: int = 0  # type == "audit" rows in the files
    audit_rows_aligned: int = 0
    audits_replayed: int = 0  # BookAudit events from decode()
    audits_replayed_aligned: int = 0
    audit_outcomes: Counter = field(default_factory=Counter)
    audit_unaligned_reasons: Counter = field(default_factory=Counter)
    audits_skipped_unexplained: int = 0
    final_state: str = "EMPTY"

    @property
    def problems(self) -> list[str]:
        found = []
        if self.first_snapshot_ns is None:
            found.append("no snapshot in the window: the book never synced")
        if self.crossed:
            found.append(f"{self.crossed} crossed book(s)")
        if self.unsynced_updates:
            found.append(f"{self.unsynced_updates} update(s) while not SYNCED outside a resync")
        if self.audit_outcomes["mismatch"]:
            found.append(f"{self.audit_outcomes['mismatch']} audit mismatch(es)")
        if self.audits_skipped_unexplained:
            found.append(f"{self.audits_skipped_unexplained} aligned audit(s) skipped")
        if self.audit_rows != self.audits_replayed:
            found.append(f"audit rows {self.audit_rows} != replayed {self.audits_replayed}")
        if self.audit_rows_aligned != self.audits_replayed_aligned:
            found.append(
                f"aligned audit rows {self.audit_rows_aligned} != replayed "
                f"{self.audits_replayed_aligned}"
            )
        return found


@dataclass
class Report:
    start_ns: int
    end_ns: int
    products: dict[str, ProductReport]
    gaps: Counter
    bytes_by_channel: dict[str, int]
    files: int

    @property
    def hours(self) -> float:
        return (self.end_ns - self.start_ns) / 3600e9

    @property
    def gb_per_day(self) -> float:
        total = sum(self.bytes_by_channel.values())
        return total / 1e9 * 24 / self.hours if self.hours > 0 else 0.0

    @property
    def ok(self) -> bool:
        return all(not p.problems for p in self.products.values())


AUDIT_NAMES = {
    Outcome.AUDIT_MATCH: "match",
    Outcome.AUDIT_MISMATCH: "mismatch",
    Outcome.AUDIT_SKIPPED: "skipped",
    Outcome.AUDIT_UNALIGNED: "unaligned",
}


def check(
    root: str | Path, start_ns: int, end_ns: int, *, venue: str = "gemini", products=None
) -> Report:
    reports: dict[str, ProductReport] = {}
    books: dict[str, OrderBook] = {}
    resyncing: set[str] = set()  # between a marker and the next snapshot
    gaps: Counter = Counter()

    def report(product: str) -> ProductReport:
        if product not in reports:
            reports[product] = ProductReport(product)
            books[product] = OrderBook(venue, product)
        return reports[product]

    for rec in read(root, start_ns, end_ns, venue=venue, products=products):
        if rec.channel == "gaps":
            gaps[rec.type] += 1
            continue
        if rec.product_id is None:
            continue
        r = report(rec.product_id)
        if rec.channel == "market_trades":
            r.trades += 1
            continue
        r.level2 += 1
        if rec.type == "audit":
            r.audit_rows += 1
            r.audit_rows_aligned += bool(rec.aligned)
        book = books[rec.product_id]
        for event in decode(rec.value):
            if event.product_id != rec.product_id:
                continue
            outcome = book.apply(event)
            if isinstance(event, BookAudit):
                r.audits_replayed += 1
                r.audits_replayed_aligned += event.aligned
                name = AUDIT_NAMES[outcome]
                r.audit_outcomes[name] += 1
                if name == "unaligned":
                    r.audit_unaligned_reasons[event.reason] += 1
                elif (
                    name == "skipped"
                    and r.first_snapshot_ns is not None
                    and rec.product_id not in resyncing
                ):
                    r.audits_skipped_unexplained += 1
            elif isinstance(event, ResyncMarker):
                r.markers += 1
                resyncing.add(rec.product_id)
            elif isinstance(event, BookUpdate):
                if outcome is Outcome.SNAPSHOT:
                    r.snapshots += 1
                    resyncing.discard(rec.product_id)
                    if r.first_snapshot_ns is None:
                        r.first_snapshot_ns = rec.received_at_ns
                elif outcome is Outcome.CROSSED:
                    r.crossed += 1
                    resyncing.add(rec.product_id)  # the live book asks for a resync
                elif outcome in (Outcome.SKIPPED_NOT_SYNCED, Outcome.SKIPPED_AWAITING):
                    if r.first_snapshot_ns is None:
                        r.before_first_snapshot += 1
                    elif rec.product_id in resyncing or event.awaiting_snapshot:
                        r.in_resync += 1
                    else:
                        r.unsynced_updates += 1
    for product, r in reports.items():
        r.final_state = books[product].state.value

    bytes_by_channel: dict[str, int] = defaultdict(int)
    window = files(root, start_ns, end_ns, venue=venue, products=products)
    for part in window:
        bytes_by_channel[part.path.parents[3].name.removeprefix("channel=")] += (
            part.path.stat().st_size
        )
    return Report(start_ns, end_ns, reports, gaps, dict(bytes_by_channel), len(window))


def format_report(report: Report) -> str:
    lines = [
        f"Window: {report.hours:.2f} h, {report.files} file(s)",
        "",
    ]
    for product, r in sorted(report.products.items()):
        first = (
            "none"
            if r.first_snapshot_ns is None
            else f"+{(r.first_snapshot_ns - report.start_ns) / 1e9:.0f} s"
        )
        lines += [
            f"{product}: {'OK' if not r.problems else 'FAIL'}",
            f"  level2 {r.level2}, trades {r.trades}; first snapshot {first}, snapshots "
            f"{r.snapshots}, markers {r.markers}; final state {r.final_state}",
            f"  not SYNCED: before first snapshot {r.before_first_snapshot}, in resync "
            f"{r.in_resync}, otherwise {r.unsynced_updates}; crossed {r.crossed}",
            f"  audits: rows {r.audit_rows} (aligned {r.audit_rows_aligned}), replayed "
            f"{r.audits_replayed} (aligned {r.audits_replayed_aligned}); "
            + ", ".join(f"{k} {r.audit_outcomes[k]}" for k in AUDIT_NAMES.values())
            + (
                "; unaligned by reason: "
                + ", ".join(f"{k} {v}" for k, v in sorted(r.audit_unaligned_reasons.items()))
                if r.audit_unaligned_reasons
                else ""
            ),
        ]
        lines += [f"  problem: {problem}" for problem in r.problems]
    gaps = ", ".join(f"{k} {v}" for k, v in sorted(report.gaps.items())) or "none"
    sizes = ", ".join(f"{k} {v / 1e6:.1f} MB" for k, v in sorted(report.bytes_by_channel.items()))
    lines += [
        "",
        f"Gap records in the window: {gaps}",
        f"On disk: {sizes or 'nothing'}; {report.gb_per_day:.3f} GB/day at this rate",
        "",
        "PASS" if report.ok else "FAIL",
    ]
    return "\n".join(lines)
