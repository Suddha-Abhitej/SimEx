import asyncio
import os
import uuid
from collections import defaultdict
from decimal import Decimal
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import pytest
from hypothesis import HealthCheck, given, seed, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    GateDecision,
    Liquidity,
    OrderStatus,
    Side,
)
from simex.exchange.accounts import EXTERNAL, FEES, MARKET
from simex.exchange.orders import FillCause
from simex.ledger import SyncLedger, fill_postings
from simex.ledger.migration_runner import apply_migrations

PRODUCTS = tuple(DEFAULT_INSTRUMENTS)
ACCOUNTS = ("alice", "bob", "carol")
T0 = 1_800_000_000_000_000_000
DELAY = 200_000_000


class OpenGate:
    def check(self, product_id, now_ns):
        return GateDecision(True, None, Decimal("100"), Decimal("100.01"))


def _quantity(instrument, multiplier):
    return instrument.min_size * Decimal(multiplier)


def _price(instrument, multiplier):
    return instrument.price_tick * Decimal(multiplier)


def _payload(instrument, side, order_type, quantity, price, client_order_id):
    payload = {
        "client_order_id": client_order_id,
        "product_id": instrument.product_id,
        "side": side.value,
        "type": order_type,
        "quantity": str(quantity),
    }
    if order_type == "limit":
        payload["limit_price"] = str(price)
    return payload


class LedgerStateMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), DELAY)
        self.orders = {}
        self.fills = []
        self.baseline = {}
        for account in ACCOUNTS:
            for asset, amount in (
                ("USD", Decimal("100")),
                ("BTC", Decimal("1000000")),
                ("ETH", Decimal("1000000")),
                ("SOL", Decimal("1000000")),
            ):
                self.writer.deposit(account, asset, amount, f"deposit:{account}:{asset}")
        self.baseline = {asset: Decimal(0) for asset in ("USD", "BTC", "ETH", "SOL")}
        assert self.writer.accounts.totals() == self.baseline

    @initialize()
    def initialized(self):
        pass

    @rule(
        account=st.sampled_from(ACCOUNTS),
        product=st.sampled_from(PRODUCTS),
        side=st.sampled_from((Side.BUY, Side.SELL)),
        multiplier=st.integers(min_value=1, max_value=1000),
        price_multiplier=st.integers(min_value=1, max_value=1000),
    )
    def submit_limit(self, account, product, side, multiplier, price_multiplier):
        instrument = DEFAULT_INSTRUMENTS[product]
        quantity = _quantity(instrument, multiplier)
        price = _price(instrument, price_multiplier)
        order = self.writer.submit(
            account,
            _payload(
                instrument,
                side,
                "limit",
                quantity,
                price,
                f"order-{len(self.orders)}",
            ),
            T0 + len(self.orders),
        )
        assert self.writer.accounts.totals() == self.baseline
        self.orders[order.order_id] = order

    @rule(
        account=st.sampled_from(ACCOUNTS),
        product=st.sampled_from(PRODUCTS),
        side=st.sampled_from((Side.BUY, Side.SELL)),
        multiplier=st.integers(min_value=1, max_value=1000),
    )
    def submit_market(self, account, product, side, multiplier):
        instrument = DEFAULT_INSTRUMENTS[product]
        quantity = _quantity(instrument, multiplier)
        order = self.writer.submit(
            account,
            _payload(
                instrument,
                side,
                "market",
                quantity,
                _price(instrument, 1),
                f"market-{len(self.orders)}",
            ),
            T0 + len(self.orders),
        )
        assert self.writer.accounts.totals() == self.baseline
        self.orders[order.order_id] = order

    @rule(order_index=st.integers(min_value=0, max_value=20))
    def activate(self, order_index):
        open_orders = [
            order
            for order in self.orders.values()
            if order.status is OrderStatus.PENDING and order.request.type.value == "limit"
        ]
        if not open_orders:
            return
        order = open_orders[order_index % len(open_orders)]
        before = self.writer.accounts.totals()
        self.writer.activate(order.order_id, T0 + order_index)
        assert self.writer.accounts.totals() == before

    @rule(order_index=st.integers(min_value=0, max_value=20))
    def cancel(self, order_index):
        open_orders = [order for order in self.orders.values() if not order.status.terminal]
        if not open_orders:
            return
        order = open_orders[order_index % len(open_orders)]
        before = self.writer.accounts.totals()
        self.writer.cancel_now(order.order_id, T0 + order_index)
        assert self.writer.accounts.totals() == before

    @rule(order_index=st.integers(min_value=0, max_value=20))
    def expire(self, order_index):
        open_orders = [
            order
            for order in self.orders.values()
            if not order.status.terminal and order.request.type.value == "market"
        ]
        if not open_orders:
            return
        order = open_orders[order_index % len(open_orders)]
        before = self.writer.accounts.totals()
        self.writer.expire(order.order_id, T0 + order_index)
        assert self.writer.accounts.totals() == before

    @rule(order_index=st.integers(min_value=0, max_value=20))
    def fill(self, order_index):
        open_orders = [
            order
            for order in self.orders.values()
            if order.status
            in (
                OrderStatus.PENDING,
                OrderStatus.OPEN,
                OrderStatus.PARTIALLY_FILLED,
            )
            and not order.status.terminal
        ]
        if not open_orders:
            return
        order = open_orders[order_index % len(open_orders)]
        instrument = DEFAULT_INSTRUMENTS[order.product_id]
        quantity = min(
            order.remaining,
            _quantity(instrument, 1),
        )
        if quantity <= 0:
            return
        price = order.request.limit_price or self.writer.band_prices[order.order_id]
        fill = self.writer.record_fill(
            order.order_id,
            price,
            quantity,
            Liquidity.TAKER,
            FillCause.TAKER,
            T0 + len(self.fills),
        )
        self.fills.append((fill, instrument))

    @invariant()
    def double_entry(self):
        for fill, instrument in self.fills:
            totals = defaultdict(Decimal)
            for posting in fill_postings(fill, instrument):
                totals[posting.asset] += posting.amount
            assert all(amount == Decimal(0) for amount in totals.values())

    @invariant()
    def conservation(self):
        assert self.writer.accounts.totals() == self.baseline

    @invariant()
    def reservations_are_safe(self):
        for account, assets in self.writer.accounts.balances.items():
            if account in (MARKET, FEES, EXTERNAL):
                continue
            for balance in assets.values():
                assert balance.available >= 0
                assert balance.reserved >= 0

    def teardown(self):
        assert self.writer.accounts.totals() == self.baseline


TestLedgerStateMachine = LedgerStateMachine.TestCase
TestLedgerStateMachine.settings = settings(
    max_examples=40,
    stateful_step_count=20,
    derandomize=True,
    suppress_health_check=[HealthCheck.too_slow],
)


def postgres_dsn():
    dsn = os.environ.get("SIMEX_TEST_POSTGRES_DSN")
    if not dsn:
        if os.environ.get("SIMEX_REQUIRE_POSTGRES_TESTS") == "1":
            raise RuntimeError("SIMEX_TEST_POSTGRES_DSN is required in this test job")
        pytest.skip("set SIMEX_TEST_POSTGRES_DSN for ledger property tests")
    return dsn


async def _postgres_setup(dsn, schema):
    connection = await asyncpg.connect(dsn)
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    await connection.execute(f'SET search_path TO "{schema}"')
    await apply_migrations(connection)
    expected = {"ledger_postings", "reconciliation_state"}
    resolved = {
        row["tablename"]
        for row in await connection.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = $1", schema
        )
    }
    assert expected <= resolved
    for table in expected:
        assert (
            await connection.fetchval(
                "SELECT n.nspname || '.' || c.relname "
                "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE c.oid = to_regclass($1)",
                f"{schema}.{table}",
            )
            == f"{schema}.{table}"
        )
    public_ledger_tables = await connection.fetchval(
        "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' "
        "AND tablename = ANY($1::text[])",
        list(expected),
    )
    assert public_ledger_tables == 0
    await connection.close()


def _database_dsn(dsn, database):
    parts = urlsplit(dsn)
    return urlunsplit((parts.scheme, parts.netloc, f"/{database}", parts.query, parts.fragment))


async def _create_database(dsn, database):
    connection = await asyncpg.connect(dsn)
    await connection.execute(f'CREATE DATABASE "{database}"')
    await connection.close()


async def _drop_database(dsn, database):
    connection = await asyncpg.connect(dsn)
    await connection.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
    await connection.close()


class PropertySyncLedger(SyncLedger):
    def fold_balances(self, account_id, marker):
        return self._call(lambda adapter: adapter.fold_balances(account_id, marker))

    def count_fills(self, account_id):
        return self._call(
            lambda adapter: adapter.connection.fetchval(
                "SELECT count(*) FROM fills WHERE account_id = $1", account_id
            )
        )


@st.composite
def fill_sequences(draw, example_id):
    count = draw(st.integers(min_value=1, max_value=20))
    fills = []
    instrument = DEFAULT_INSTRUMENTS["BTC-USD"]
    for ordinal in range(count):
        quantity = _quantity(instrument, draw(st.integers(1, 1000)))
        price = _price(instrument, draw(st.integers(1, 1000)))
        fills.append(
            {
                "fill_id": f"{example_id}-order-{ordinal}-{ordinal}",
                "order_id": f"{example_id}-order-{ordinal}",
                "account_id": f"{example_id}-account",
                "product_id": instrument.product_id,
                "side": Side.BUY,
                "price": price,
                "quantity": quantity,
                "liquidity": Liquidity.TAKER,
                "cause": FillCause.TAKER,
                "fee": (price * quantity * Decimal("0.012")).quantize(Decimal("0.00000001")),
                "fee_asset": "USD",
                "ts_ns": T0 + ordinal,
            }
        )
    return fills


def _make_fill(data):
    from simex.exchange import Fill

    return Fill(**data)


@seed(12012)
@settings(max_examples=8, deadline=None, derandomize=True)
@given(st.data())
def test_postgres_fold_matches_writer_projection_and_replay_is_exactly_once(data):
    postgres_dsn_value = postgres_dsn()
    example_id = uuid.uuid4().hex
    schema = f"ledger_{example_id}"
    database = f"simex_prop_{example_id}"
    asyncio.run(_create_database(postgres_dsn_value, database))
    dsn = _database_dsn(postgres_dsn_value, database)
    asyncio.run(_postgres_setup(dsn, schema))
    if "?" in dsn:
        dsn += f"&options=-csearch_path%3D{schema}"
    else:
        dsn += f"?options=-csearch_path%3D{schema}"
    ledger = PropertySyncLedger(dsn)
    fills = data.draw(fill_sequences(example_id))
    writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), DELAY, ledger=ledger)
    try:
        writer.deposit(f"{example_id}-account", "USD", Decimal("1000000"), "deposit")
        for item in fills:
            fill = _make_fill(item)
            order = writer.submit(
                fill.account_id,
                _payload(
                    DEFAULT_INSTRUMENTS["BTC-USD"],
                    Side.BUY,
                    "limit",
                    fill.quantity,
                    fill.price,
                    fill.order_id,
                ),
                fill.ts_ns,
            )
            writer.activate(order.order_id, fill.ts_ns)
            actual = writer.record_fill(
                order.order_id,
                fill.price,
                fill.quantity,
                fill.liquidity,
                fill.cause,
                fill.ts_ns,
            )
            projection, marker = writer.balance_snapshot(fill.account_id)
            assert ledger.fold_balances(fill.account_id, marker) == projection
            ledger.record_fill(actual, DEFAULT_INSTRUMENTS["BTC-USD"], order, None)
        assert ledger.count_fills(f"{example_id}-account") == len(fills)
    finally:
        ledger.close()
        asyncio.run(_drop_database(postgres_dsn_value, database))
