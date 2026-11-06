# Prices and execution figures are private (ADR 0010): stdout or an explicit local JSON file only.
"""Inspect execution/friction with python -m simex.report; simex.evaluator owns performance."""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import tempfile
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from simex.env import load_environment
from simex.exchange.fees import FeeSchedule

from .core import (
    COHORT,
    DECISION_COVERAGE,
    MARKET_LABEL,
    MISSING_BANDS,
    NS,
    REFUSAL_COVERAGE,
    account_report,
    encoded,
    render_report,
)

SYSTEM_ACCOUNTS = ("_market", "_fees", "_external")
ORDER_WINDOW = (
    "o.account_id = ANY($1::text[]) AND "
    "($2::bigint IS NULL OR o.created_at_ns BETWEEN $2 AND $3::bigint)"
)


class ReportError(Exception):
    """An operational failure safe to show without a traceback."""


def nonnegative(value):
    try:
        result = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("must be a nonnegative integer") from None
    if result < 0:
        raise argparse.ArgumentTypeError("must be a nonnegative integer")
    return result


def positive(value):
    result = nonnegative(value)
    if not result:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return result


def since(value):
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})",
        value,
    )
    try:
        if match is None:
            raise ValueError
        base, fraction, zone = match.groups()
        instant = datetime.fromisoformat(base + zone.replace("Z", "+00:00"))
        delta = instant.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
        return (delta.days * 86400 + delta.seconds) * NS + int((fraction or "").ljust(9, "0"))
    except ValueError:
        raise argparse.ArgumentTypeError(
            "use an ISO-8601 timestamp with an explicit timezone and at most 9 fractional digits"
        ) from None


async def read_ledger(dsn, requested=None, *, since_ns=None, hours=None):
    try:
        import asyncpg
    except ImportError:
        raise ReportError('Install the Postgres driver with pip install -e ".[dev]".') from None
    try:
        connection = await asyncpg.connect(dsn)
    except Exception:
        # Only connection errors hide their cause, since those can echo credentials.
        raise ReportError(
            "Could not connect to the ledger; check --dsn / EXCHANGE_LEDGER_DSN."
        ) from None
    try:
        try:
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                snapshot_ns = await connection.fetchval(
                    "SELECT (extract(epoch FROM transaction_timestamp()) * 1000000000)::bigint"
                )
                start_ns = snapshot_ns - hours * 3600 * NS if hours is not None else since_ns
                inventory = await connection.fetch(
                    "SELECT account_id, bool_or(has_order) AS has_orders FROM ("
                    "SELECT account_id, true AS has_order FROM orders UNION ALL "
                    "SELECT account_id, false FROM fills UNION ALL "
                    "SELECT account_id, false FROM ledger_postings UNION ALL "
                    "SELECT account_id, false FROM account_reservations) AS account_refs "
                    "WHERE NOT (account_id = ANY($1::text[])) GROUP BY account_id",
                    list(SYSTEM_ACCOUNTS),
                )
                existing = {row["account_id"] for row in inventory}
                if requested and set(requested) & set(SYSTEM_ACCOUNTS):
                    raise ReportError("System accounts cannot be selected.")
                unknown = set(requested or ()) - existing
                if unknown:
                    raise ReportError("Unknown account(s): " + ", ".join(sorted(unknown)))
                selected = sorted(
                    set(requested)
                    if requested
                    else {row["account_id"] for row in inventory if row["has_orders"]}
                )
                if not selected:
                    raise ReportError(
                        "No orders in the ledger for non-system accounts; run a bot first."
                    )
                rows = await connection.fetch(
                    "SELECT o.order_id, o.account_id, o.request, o.status, o.created_at_ns, "
                    "o.effective_at_ns, o.filled_quantity, o.filled_notional, o.maker_notional, "
                    "o.taker_notional, o.market_band_price, o.reasoning, o.reject_reason, "
                    f"o.reject_details FROM orders o WHERE {ORDER_WINDOW} "
                    "ORDER BY o.created_at_ns, o.order_id",
                    selected,
                    start_ns,
                    snapshot_ns,
                )
                missing = set(selected) - {row["account_id"] for row in rows}
                if missing:
                    raise ReportError(
                        "No orders in the requested window for account(s): "
                        + ", ".join(sorted(missing))
                    )
                fills = await connection.fetch(
                    "SELECT f.order_id, f.account_id, f.product_id, f.price, f.quantity, "
                    "f.liquidity, f.cause, f.fee, f.fee_asset, f.ts_ns FROM fills f "
                    "JOIN orders o ON o.order_id=f.order_id AND o.account_id=f.account_id "
                    f"WHERE {ORDER_WINDOW} ORDER BY f.ts_ns, f.fill_id",
                    selected,
                    start_ns,
                    snapshot_ns,
                )
        finally:
            await connection.close()
    except ReportError:
        raise
    except Exception as error:
        raise ReportError(
            f"Could not read committed orders and fills: {type(error).__name__}: {error}"
        ) from None
    orders = []
    try:
        for row in rows:
            order = dict(row)
            for key in ("request", "reject_details"):
                if isinstance(order[key], str):
                    order[key] = json.loads(order[key], parse_float=Decimal)
            orders.append(order)
    except (ValueError, TypeError) as error:
        raise ReportError(f"Could not decode order data: {type(error).__name__}: {error}") from None
    return dict(
        snapshot_ns=snapshot_ns,
        since_ns=start_ns,
        accounts=selected,
        orders=orders,
        fills=[dict(fill) for fill in fills],
    )


def build_report(snapshot, decisions, fees):
    return dict(
        schema_version=1,
        window={key: snapshot[key] for key in ("since_ns", "snapshot_ns")},
        conventions=dict(
            cohort=COHORT,
            refusals=REFUSAL_COVERAGE,
            decisions=DECISION_COVERAGE,
            market_band=MARKET_LABEL,
            missing_benchmarks=MISSING_BANDS,
        ),
        fee_references=dict(
            maker_bps=fees.maker_bps,
            taker_bps=fees.taker_bps,
            fee_increment=fees.fee_increment,
            source="current_configuration",
            historical=False,
        ),
        decisions_limit=decisions,
        accounts=[
            account_report(
                account,
                [row for row in snapshot["orders"] if row["account_id"] == account],
                [row for row in snapshot["fills"] if row["account_id"] == account],
                decisions,
            )
            for account in snapshot["accounts"]
        ],
    )


def write_json(path, content):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=".simex-report-", delete=False
        ) as output:
            temporary = Path(output.name)
            output.write(content + "\n")
        os.replace(temporary, path)  # Closed first, including on Windows.
    except OSError as error:
        raise ReportError(f"Could not write local JSON: {type(error).__name__}: {error}") from None
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def run(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", help="ledger DSN (default: EXCHANGE_LEDGER_DSN)")
    parser.add_argument("--account", action="append", help="repeat to select non-system accounts")
    window = parser.add_mutually_exclusive_group()
    window.add_argument(
        "--since", type=since, help="inclusive order creation timestamp with timezone"
    )
    window.add_argument("--hours", type=positive, help="trailing whole hours of order creation")
    parser.add_argument("--decisions", type=nonnegative, default=20)
    parser.add_argument("--json", type=Path, help="explicit private local JSON output")
    args = parser.parse_args(argv)
    try:
        try:
            logging.basicConfig(
                level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                format="%(asctime)s %(levelname)s %(name)s: %(message)s",
            )
        except ValueError as error:
            raise ReportError(f"Invalid LOG_LEVEL: {error}") from None
        load_environment(logging.getLogger("simex.report"))
        dsn = args.dsn if args.dsn is not None else os.environ.get("EXCHANGE_LEDGER_DSN", "")
        if not dsn.strip():
            raise ReportError("Set --dsn or EXCHANGE_LEDGER_DSN to the ledger connection string.")
        try:
            fees = FeeSchedule.from_env()
            if not fees.maker_bps.is_finite() or not fees.taker_bps.is_finite():
                raise ValueError("fee references must be finite")
        except (ValueError, InvalidOperation) as error:
            raise ReportError(
                f"Invalid current fee references: {type(error).__name__}: {error}"
            ) from None
        snapshot = asyncio.run(
            read_ledger(dsn, args.account, since_ns=args.since, hours=args.hours)
        )
        result = build_report(snapshot, args.decisions, fees)
        text, content = render_report(result), encoded(result)
        if args.json is not None:
            write_json(args.json, content)
        print(text)
    except ReportError as error:
        print(f"report: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("report: interrupted.", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
