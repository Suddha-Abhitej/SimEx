"""Synthetic single-fill versus IOC/FOK group commits, in a generated simex_test schema.

No trading ledger, recordings or venue connection. Output contains system metrics only.
Example: SIMEX_TEST_POSTGRES_DSN=... python tools/measure_tif_commit.py --samples 100
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import re
import subprocess
import time
from contextlib import closing
from datetime import UTC, datetime
from decimal import Decimal as D
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import asyncpg

from simex.exchange import DEFAULT_INSTRUMENTS, AccountWriter, FeeSchedule, GateDecision, PairEngine
from simex.ledger import LedgerMetrics, SyncLedger
from simex.marketdata import BookUpdate

P = "BTC-USD"


def schema_dsn(dsn: str, schema: str) -> str:
    parts = urlsplit(dsn)
    if parts.scheme not in {"postgres", "postgresql"} or parts.path != "/simex_test":
        raise ValueError("requires an explicit simex_test DSN; never a trading ledger")
    assert re.fullmatch(r"tif_commit_[0-9a-f]{32}", schema)
    query = [(key, value) for key, value in parse_qsl(parts.query) if key != "options"]
    query.append(("options", f"-csearch_path={schema}"))
    return urlunsplit(parts._replace(query=urlencode(query)))


async def schema_ddl(dsn: str, schema: str, *, drop: bool = False) -> None:
    assert re.fullmatch(r"tif_commit_[0-9a-f]{32}", schema)
    schema_dsn(dsn, schema)  # Validate the database before either DDL operation.
    connection = await asyncpg.connect(dsn)
    try:
        operation = "DROP SCHEMA IF EXISTS" if drop else "CREATE SCHEMA"
        await connection.execute(f'{operation} "{schema}"' + (" CASCADE" if drop else ""))
    finally:
        await connection.close()


class Timings(LedgerMetrics):
    def __init__(self):
        super().__init__()
        self.commits = []

    def observe(self, phase, elapsed_ns):
        super().observe(phase, elapsed_ns)
        if phase == "commit":
            self.commits.append(elapsed_ns)


class Gate:
    def check(self, product_id, now_ns):
        return GateDecision(True, None, D("100"), D("100.01"))


def distribution(values: list[int]) -> dict:
    ordered = sorted(values)
    n = len(ordered)
    median = D(ordered[n // 2]) if n % 2 else D(ordered[n // 2 - 1] + ordered[n // 2]) / 2
    return {
        "count": n,
        "median_ms": format(median / 1_000_000, ".3f"),
        "sample_p99_ms": format(D(ordered[(99 * n + 99) // 100 - 1]) / 1_000_000, ".3f"),
    }


def measure(dsn: str, samples: int) -> dict:
    if samples < 1:
        raise ValueError("samples must be positive")
    schema = "tif_commit_" + uuid4().hex
    isolated = schema_dsn(dsn, schema)
    asyncio.run(schema_ddl(dsn, schema))
    try:
        metrics = Timings()
        with closing(SyncLedger(isolated, metrics=metrics)) as ledger:
            ledger.migrate()
            results = {}
            for tif, levels in (("GTC", 1), ("GTC", 3), ("IOC", 3), ("FOK", 3)):
                account = f"synthetic-{tif}-{levels}"
                writer = AccountWriter(
                    DEFAULT_INSTRUMENTS, FeeSchedule(), Gate(), 200_000_000, ledger=ledger
                )
                writer.deposit(account, "USD", D("1000000"), f"seed:{account}")
                engine = PairEngine(P, writer)
                elapsed = []
                for index in range(samples + 10):
                    if index == 10:
                        metrics.commits.clear()
                    now = 1_792_036_800_000_000_000 + index * 1_000_000_000
                    engine.on_event(
                        BookUpdate(
                            "gemini",
                            P,
                            True,
                            ((D("100"), D("10")),),
                            tuple((D("100.01") + D("0.01") * k, D("1")) for k in range(levels)),
                            (index, index),
                            False,
                            now,
                            now,
                        )
                    )
                    order = writer.submit(
                        account,
                        {
                            "client_order_id": f"sample-{index}",
                            "product_id": P,
                            "side": "buy",
                            "type": "limit",
                            "quantity": str(levels + (tif == "IOC")),
                            "limit_price": "100.03",
                            "time_in_force": tif,
                            "reasoning": "Synthetic commit-boundary measurement",
                        },
                        now,
                    )
                    engine.submit(order)
                    started = time.perf_counter_ns()
                    engine.advance(order.effective_at_ns)
                    duration = time.perf_counter_ns() - started
                    assert order.status.terminal and order.fill_count == levels
                    assert not writer.accounts.reservations
                    if index >= 10:
                        elapsed.append(duration)
                results[f"{tif}_{levels}_levels"] = {
                    "transaction_through_commit": distribution(metrics.commits),
                    "execution_through_ack": distribution(elapsed),
                }
            return results
    finally:
        asyncio.run(schema_ddl(dsn, schema, drop=True))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=100)
    args = parser.parse_args()
    dsn = os.environ.get("SIMEX_TEST_POSTGRES_DSN", "")
    try:
        results = measure(dsn, args.samples)
    except ValueError as exc:
        parser.error(str(exc))
    previous = time.perf_counter_ns()
    smallest_step = None
    for _ in range(10_000):
        current = time.perf_counter_ns()
        step = current - previous
        if step > 0:
            smallest_step = step if smallest_step is None else min(smallest_step, step)
        previous = current
    print(
        json.dumps(
            {
                "date": datetime.now(UTC).date().isoformat(),
                "host": platform.platform(),
                "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                "tracked_dirty": bool(
                    subprocess.check_output(["git", "diff", "--name-only"], text=True)
                ),
                "warmup_orders_per_case": 10,
                "perf_counter_nominal_resolution_ns": str(
                    D(str(time.get_clock_info("perf_counter").resolution)) * 1_000_000_000
                ),
                "smallest_observed_clock_step_ns": smallest_step,
                "results": results,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
