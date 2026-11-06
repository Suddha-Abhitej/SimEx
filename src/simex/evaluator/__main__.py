# Reports contain Gemini-derived prices and results: stdout only, never files/uploads (ADR 0010).
"""Evaluate committed accounts against private recorder marks: python -m simex.evaluator."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from simex.env import load_environment

from . import (
    EvaluationConfig,
    FillRecord,
    PriceSeries,
    evaluate,
    marks_from_recordings,
    render_report,
)
from .core import NANOSECONDS_PER_SECOND

SYSTEM_ACCOUNTS = ("_market", "_fees", "_external")


class EvaluationError(Exception):
    """An operational failure that should reach the caller without a traceback or credentials."""


def _nonnegative(value: str) -> int:
    try:
        result = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a nonnegative integer") from None
    if result < 0:
        raise argparse.ArgumentTypeError("must be a nonnegative integer")
    return result


async def _read_ledger(dsn: str) -> tuple[list[FillRecord], dict[str, dict[str, Decimal]]]:
    try:
        import asyncpg
    except ImportError:
        raise EvaluationError('Install the Postgres driver with pip install -e ".[dev]".') from None

    try:
        connection = await asyncpg.connect(dsn)
        try:
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                rows = await connection.fetch(
                    "SELECT account_id, product_id, side, price, quantity, fee, fee_asset, ts_ns "
                    "FROM fills WHERE NOT (account_id = ANY($1::text[])) "
                    "ORDER BY ts_ns, fill_id",
                    list(SYSTEM_ACCOUNTS),
                )
                seeds = await connection.fetch(
                    "SELECT account_id, asset, SUM(amount) AS amount FROM ledger_postings "
                    "WHERE transaction_id LIKE 'seed:%' "
                    "AND NOT (account_id = ANY($1::text[])) GROUP BY account_id, asset",
                    list(SYSTEM_ACCOUNTS),
                )
        finally:
            await connection.close()
        balances: dict[str, dict[str, Decimal]] = defaultdict(dict)
        for row in seeds:
            balances[row["account_id"]][row["asset"]] = row["amount"]
        return [FillRecord.from_row(row) for row in rows], dict(balances)
    except Exception:
        # Driver errors can echo connection strings. Keep credentials out of the diagnostic.
        raise EvaluationError(
            "Could not read committed fills and seeds; check --dsn / EXCHANGE_LEDGER_DSN "
            "and the existing ledger schema."
        ) from None


def _timestamp(ns: int) -> str:
    seconds, fraction = divmod(ns, NANOSECONDS_PER_SECOND)
    return datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S") + f".{fraction:09d}Z"


def _seconds(ns: int) -> str:
    return str(Decimal(ns) / NANOSECONDS_PER_SECOND)


def _evaluate(args: argparse.Namespace, fills: list[FillRecord], seeds: dict) -> None:
    available = {fill.account_id for fill in fills}
    if not available:
        raise EvaluationError("No fills in the ledger for non-system accounts; run a bot first.")
    accounts = sorted(set(args.account) if args.account else available)
    invalid = set(accounts) - available
    if invalid:
        raise EvaluationError(
            "Select non-system accounts with fills; unavailable: " + ", ".join(sorted(invalid))
        )
    fills = [fill for fill in fills if fill.account_id in accounts]
    products = {fill.product_id for fill in fills}
    for account in accounts:
        if account not in seeds:
            print(
                f"Warning: no seed postings for {account}; using empty starting balances, "
                "not an assumed deposit."
            )
        for asset, amount in seeds.get(account, {}).items():
            if asset != "USD" and amount != 0:
                products.add(f"{asset}-USD")
    products.update(f"{fill.fee_asset}-USD" for fill in fills if fill.fee_asset != "USD")
    products = sorted(products)
    config = EvaluationConfig()
    period_ns = config.period_seconds * NANOSECONDS_PER_SECOND
    start_ns = min(fill.ts_ns for fill in fills) // period_ns * period_ns
    last_fill = max(fill.ts_ns for fill in fills)
    final_boundary = (last_fill + period_ns - 1) // period_ns * period_ns
    # _metrics uses range(start_ns, end_ns, P) and filters fills with ts_ns < end_ns.
    # The +1ns sentinel includes both the final sample and a fill exactly on that boundary.
    end_ns = final_boundary + 1
    replay_start = start_ns - args.lookback_seconds * NANOSECONDS_PER_SECOND
    print("Accounts: " + ", ".join(accounts))
    print("Products: " + ", ".join(products))
    print(
        f"Requested window: {_timestamp(start_ns)} through {_timestamp(final_boundary)} "
        f"({_seconds(final_boundary - start_ns)} seconds); fills: {len(fills)}"
    )
    print(
        f"Replaying Gemini marks from {_timestamp(replay_start)} through "
        f"{_timestamp(final_boundary)}; this may take several minutes.",
        flush=True,
    )
    try:
        marks = marks_from_recordings(args.recordings, replay_start, end_ns, products)
    except Exception:
        raise EvaluationError(
            "Could not replay Gemini recordings; check the recordings directory and readable "
            "published Parquet files. Was the recorder running?"
        ) from None
    print(f"Marks: {len(marks)}")
    if not marks:
        raise EvaluationError(
            "No Gemini marks in the recordings for this window. Was the recorder running? "
            "Wait for a completed hour or increase --lookback-seconds for an earlier snapshot."
        )
    by_product = defaultdict(list)
    for mark in marks:
        by_product[mark.product_id].append(mark)
    prices = PriceSeries(marks)
    for product in products:
        series = by_product[product]
        if not series:
            raise EvaluationError(f"No Gemini marks for {product}. Was the recorder running?")
        first, last = min(mark.ts_ns for mark in series), max(mark.ts_ns for mark in series)
        print(f"  {product}: {len(series)} marks, {_timestamp(first)} through {_timestamp(last)}")
        first_fill = min(
            (fill.ts_ns for fill in fills if fill.product_id == product), default=start_ns
        )
        if first > first_fill:
            print(
                f"Warning: first {product} mark is after the first fill/window start; increase "
                "--lookback-seconds to replay an earlier recorded snapshot. Start stays fixed "
                "to preserve earlier fills and inventory."
            )
        elif prices.mark_at(product, start_ns, max_age_ns=period_ns) is None:
            print(
                f"Warning: no fresh {product} mark at the first boundary; increase "
                "--lookback-seconds and check for an earlier recorded snapshot."
            )
    limiting_product = min(
        products, key=lambda product: max(mark.ts_ns for mark in by_product[product])
    )
    last_mark = max(mark.ts_ns for mark in by_product[limiting_product])
    gap_ns = final_boundary - last_mark
    if gap_ns > period_ns:
        covered_boundary = start_ns + (last_mark - start_ns) // period_ns * period_ns
        print(
            f"Clamping end: {limiting_product} marks end {_seconds(gap_ns)} seconds "
            f"({Decimal(gap_ns) / (60 * NANOSECONDS_PER_SECOND)} minutes) "
            f"before the requested final boundary: "
            f"{_timestamp(final_boundary)} -> {_timestamp(covered_boundary)}."
        )
        print("Use --account to narrow to accounts unaffected by the limiting product.")
        # Keep the same exclusive-end convention when a last mark is itself a sample boundary.
        end_ns = covered_boundary + 1
    included = [fill for fill in fills if start_ns <= fill.ts_ns < end_ns]
    if end_ns <= start_ns or not included:
        raise EvaluationError(
            "Published marks do not overlap the filled window. Was the recorder running?"
        )
    last_sample = start_ns + (end_ns - 1 - start_ns) // period_ns * period_ns
    print(
        f"Evaluation window: {_timestamp(start_ns)} through {_timestamp(last_sample)} "
        f"({_seconds(last_sample - start_ns)} seconds); fills: {len(included)} included, "
        f"{len(fills) - len(included)} excluded by the end."
    )
    grouped = {
        account: [fill for fill in fills if fill.account_id == account] for account in accounts
    }
    for account in accounts:
        result = evaluate(
            account,
            grouped[account],
            prices,
            seeds.get(account, {}),
            start_ns=start_ns,
            end_ns=end_ns,
            baselines={
                other: (grouped[other], seeds.get(other, {}))
                for other in accounts
                if other != account
            },
        )
        print("\n---\n" + render_report(result), flush=True)


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", help="ledger DSN (default: EXCHANGE_LEDGER_DSN)")
    parser.add_argument("--recordings", type=Path, default=Path("data/recordings"))
    parser.add_argument("--lookback-seconds", type=_nonnegative, default=3600)
    parser.add_argument(
        "--account", action="append", help="account to evaluate; repeat to select several"
    )
    args = parser.parse_args(argv)
    load_environment(logging.getLogger("simex.evaluator"))
    dsn = args.dsn if args.dsn is not None else os.environ.get("EXCHANGE_LEDGER_DSN", "")
    try:
        if not dsn.strip():
            raise EvaluationError(
                "Set --dsn or EXCHANGE_LEDGER_DSN to the ledger connection string."
            )
        if not args.recordings.is_dir():
            raise EvaluationError(
                "Recordings directory does not exist or is not a directory. "
                "Was the recorder running? Check --recordings."
            )
        fills, seeds = asyncio.run(_read_ledger(dsn))
        _evaluate(args, fills, seeds)
    except EvaluationError as error:
        print(f"evaluator: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("evaluator: interrupted during evaluation.", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
