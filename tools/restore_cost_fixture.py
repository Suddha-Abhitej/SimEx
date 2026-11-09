"""Synthetic ledger histories and cluster-scope safety for measure_restore_cost.

No trading DSN fallback. Only generated simex_restore_<uuid4 hex> databases are disposable.
All domain values below are hand-written, never recorded market data.
"""

from __future__ import annotations

import json
import re
import shutil
import uuid
from contextlib import asynccontextmanager
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg

from simex.exchange import DEFAULT_INSTRUMENTS, FeeSchedule, Liquidity
from simex.exchange.accounts import Reservation, required_reservation
from simex.exchange.orders import FillCause, Order, OrderRequest, order_id_for
from simex.ledger import LedgerAdapter, fill_postings

ACCOUNT = "restore_measurement"
PRODUCT = "BTC-USD"
ACTIVE = REJECTED = 16
RATIONALE = "Synthetic restore measurement; not a trading decision. ".ljust(128, "x")
INSTRUMENT = DEFAULT_INSTRUMENTS[PRODUCT]
FEES = FeeSchedule()
QUANTITY, PRICE = Decimal("0.1"), Decimal("100")
TS = 1_792_036_800_000_000_000
ORDER_COLUMNS = (
    "order_id",
    "account_id",
    "client_order_id",
    "request",
    "status",
    "reasoning",
    "created_at_ns",
    "effective_at_ns",
    "filled_quantity",
    "filled_notional",
    "maker_notional",
    "taker_notional",
    "fees",
    "fill_count",
    "reject_reason",
    "cancel_requested_at_ns",
    "cancel_effective_at_ns",
    "closed_at_ns",
    "market_band_price",
    "reject_details",
)
FILL_COLUMNS = (
    "fill_id",
    "order_id",
    "account_id",
    "product_id",
    "side",
    "price",
    "quantity",
    "liquidity",
    "cause",
    "fee",
    "fee_asset",
    "ts_ns",
    "source_key",
)
POSTING_COLUMNS = (
    "posting_id",
    "transaction_id",
    "fill_id",
    "transaction_sequence",
    "account_id",
    "asset",
    "amount",
    "entry_type",
)


class MeasurementSkipped(Exception):
    pass


def database_dsn(dsn: str, database: str) -> str:
    url = urlsplit(dsn)
    query = urlencode([(k, v) for k, v in parse_qsl(url.query) if k not in {"database", "dbname"}])
    return urlunsplit(url._replace(path=f"/{database}", query=query))


def validate_maintenance(dsn: str) -> None:
    url = urlsplit(dsn)
    if (
        url.scheme not in {"postgres", "postgresql"}
        or url.path != "/postgres"
        or any(v != "postgres" for k, v in parse_qsl(url.query) if k in {"database", "dbname"})
    ):
        raise ValueError("maintenance DSN must explicitly select the neutral postgres database")
    if any(k in {"host", "port", "service"} for k, _ in parse_qsl(url.query)):
        raise ValueError("use the URL authority for host and port, not query overrides")


def validate_target(dsn: str) -> None:
    url = urlsplit(dsn)
    if not re.fullmatch(r"/simex_restore_[0-9a-f]{32}", url.path) or any(
        k in {"database", "dbname", "host", "port", "service"} for k, _ in parse_qsl(url.query)
    ):
        raise ValueError("worker and seed require a generated disposable restore database")


async def drop_database(connection, name: str) -> None:
    # Literal guard immediately before cluster DDL; simex and simex_test can never match.
    assert re.fullmatch(r"simex_restore_[0-9a-f]{32}", name), "unsafe database drop"
    await connection.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@asynccontextmanager
async def disposable_database(dsn: str):
    validate_maintenance(dsn)
    name = f"simex_restore_{uuid.uuid4().hex}"
    maintenance = await asyncpg.connect(dsn, database="postgres")
    try:
        assert await maintenance.fetchval("SELECT current_database()") == "postgres"
        try:
            await maintenance.execute(f'CREATE DATABASE "{name}"')
        except asyncpg.InsufficientPrivilegeError:
            raise MeasurementSkipped("skipped: role needs CREATE DATABASE privilege") from None
        try:
            yield database_dsn(dsn, name)
        finally:
            await drop_database(maintenance, name)
    finally:
        await maintenance.close()


def intention(index: int) -> OrderRequest:
    return OrderRequest.parse(
        {
            "client_order_id": f"restore{index:012d}",
            "product_id": PRODUCT,
            "side": "buy",
            "type": "limit",
            "quantity": str(QUANTITY),
            "limit_price": str(PRICE),
            "reasoning": RATIONALE,
        }
    )


def example(index: int, profile: str):
    request = intention(index)
    order = Order(
        order_id_for(ACCOUNT, request.client_order_id), ACCOUNT, request, TS, TS + 200_000_000
    )
    fill = reservation = None
    if index < ACTIVE:
        asset, required = required_reservation(request.side, QUANTITY, PRICE, INSTRUMENT, FEES)
        reservation = Reservation(ACCOUNT, asset, required)
    elif index < ACTIVE + REJECTED:
        order.reject("book_not_synced", TS)
    elif profile == "filled":
        order.activate()
        fill = order.record_fill(
            PRICE,
            QUANTITY,
            Liquidity.TAKER,
            FillCause.TAKER,
            order.effective_at_ns,
            FEES,
            INSTRUMENT,
        )
    elif profile == "unfilled":
        order.cancel(order.effective_at_ns)
    else:
        raise ValueError("unknown synthetic profile")
    return order, reservation, fill


def order_row(order: Order) -> tuple:
    return (
        order.order_id,
        ACCOUNT,
        order.request.client_order_id,
        json.dumps(order.request.to_dict()),
        order.status.value,
        order.request.reasoning,
        order.created_at_ns,
        order.effective_at_ns,
        order.filled_quantity,
        order.filled_notional,
        order.maker_notional,
        order.taker_notional,
        order.fees,
        order.fill_count,
        order.reject_reason,
        order.cancel_requested_at_ns,
        order.cancel_effective_at_ns,
        order.closed_at_ns,
        None,
        json.dumps(order.reject_details),
    )


def check_headroom(count: int, available_disk: int) -> None:
    # Conservative reservation for rows, indexes, WAL and COPY work, not a measured row size.
    if available_disk < count * 32768 + 1024**3:
        raise MeasurementSkipped("skipped: insufficient seed disk headroom")


async def seed(dsn: str, count: int, profile: str, *, batch_size: int = 1000) -> dict:
    validate_target(dsn)
    if count < ACTIVE + REJECTED or batch_size < 1 or profile not in {"filled", "unfilled"}:
        raise ValueError("need at least 32 orders, a positive batch size and a known profile")
    check_headroom(count, shutil.disk_usage(Path.cwd()).free)
    connection = await asyncpg.connect(dsn)
    try:
        adapter = LedgerAdapter(connection)
        await adapter.migrate()
        assert await connection.fetchval("SELECT count(*) FROM orders") == 0
        # Enough synthetic cash for every buy and pending reservation; never exported.
        await adapter.record_deposit(ACCOUNT, "USD", Decimal(count + 1) * PRICE, "synthetic-seed")
        for first in range(0, count, batch_size):
            orders, reservations, fills, postings, transactions, events, outbox = (
                [] for _ in range(7)
            )
            for index in range(first, min(first + batch_size, count)):
                order, reservation, fill = example(index, profile)
                orders.append(order_row(order))
                if reservation:
                    reservations.append(
                        (order.order_id, ACCOUNT, reservation.asset, reservation.remaining)
                    )
                if fill:
                    key, sequence = f"fill:{fill.fill_id}", index - ACTIVE - REJECTED + 2
                    payload = json.dumps(fill.to_dict())
                    transactions.append((key, sequence))
                    fills.append(
                        (
                            fill.fill_id,
                            order.order_id,
                            ACCOUNT,
                            PRODUCT,
                            fill.side.value,
                            fill.price,
                            fill.quantity,
                            fill.liquidity.value,
                            fill.cause.value,
                            fill.fee,
                            fill.fee_asset,
                            fill.ts_ns,
                            key,
                        )
                    )
                    for posting in fill_postings(fill, INSTRUMENT):
                        postings.append(
                            (
                                uuid.uuid4(),
                                key,
                                fill.fill_id,
                                sequence,
                                posting.account_id,
                                posting.asset,
                                posting.amount,
                                posting.entry_type,
                            )
                        )
                    events.append(
                        (uuid.uuid4(), key, "fill", order.order_id, 1, "fill", fill.ts_ns, payload)
                    )
                    outbox.append((uuid.uuid4(), key, "fill", payload))
            async with connection.transaction():
                for table, columns, rows in (
                    ("orders", ORDER_COLUMNS, orders),
                    (
                        "account_reservations",
                        ("order_id", "account_id", "asset", "remaining"),
                        reservations,
                    ),
                    ("ledger_transactions", ("transaction_id", "sequence"), transactions),
                    ("fills", FILL_COLUMNS, fills),
                    ("ledger_postings", POSTING_COLUMNS, postings),
                    (
                        "ledger_events",
                        (
                            "event_id",
                            "event_key",
                            "aggregate_type",
                            "aggregate_id",
                            "aggregate_seq",
                            "event_type",
                            "occurred_at_ns",
                            "payload",
                        ),
                        events,
                    ),
                    ("outbox", ("outbox_id", "event_key", "event_type", "payload"), outbox),
                ):
                    if rows:
                        await connection.copy_records_to_table(table, records=rows, columns=columns)
        await connection.execute(
            "SELECT setval(pg_get_serial_sequence('ledger_transactions','sequence'), "
            "(SELECT max(sequence) FROM ledger_transactions))"
        )
        expected_fills = count - ACTIVE - REJECTED if profile == "filled" else 0
        counts = {}
        for table, expected in (
            ("orders", count),
            ("account_reservations", ACTIVE),
            ("fills", expected_fills),
            ("ledger_postings", 2 + 5 * expected_fills),
            ("ledger_events", expected_fills),
            ("outbox", expected_fills),
            ("ledger_transactions", expected_fills + 1),
        ):
            actual = await connection.fetchval(f"SELECT count(*) FROM {table}")
            assert actual == expected, f"synthetic {table} count mismatch"
            counts[table] = actual
        assert not await connection.fetch(
            "SELECT asset FROM ledger_postings GROUP BY asset HAVING SUM(amount) <> 0"
        )
        assert not await connection.fetch(
            "SELECT transaction_id, asset FROM ledger_postings "
            "GROUP BY transaction_id, asset HAVING SUM(amount) <> 0"
        )
        # Refresh statistics; no cache flush. All published runs are seeded/warm, never called cold.
        await connection.execute("ANALYZE")
        return counts
    finally:
        await connection.close()
