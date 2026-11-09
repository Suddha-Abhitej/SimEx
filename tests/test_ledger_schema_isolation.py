"""Exercise schema isolation beside synthetic public tables in a disposable database only."""

import asyncio
import re
import uuid
from contextlib import asynccontextmanager
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg
import pytest
import test_ledger as ledger_tests
import test_ledger_properties as property_tests
from test_ledger import connection_or_skip, fill
from test_ledger_properties import (
    _create_schema,
    _drop_schema,
    _postgres_setup,
    _schema_dsn,
    postgres_dsn,
)

from simex.exchange import DEFAULT_INSTRUMENTS
from simex.ledger import LedgerAdapter, SyncLedger


async def _drop_database(maintenance, name):
    # Cluster-scope cleanup must never accept simex, simex_test or any arbitrary name.
    assert re.fullmatch(r"simex_isolation_[0-9a-f]{32}", name), "unsafe database drop"
    await maintenance.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


@asynccontextmanager
async def disposable_database(dsn):
    name = f"simex_isolation_{uuid.uuid4().hex}"
    # The explicit keyword overrides the source DSN's database, including query parameters.
    maintenance = await asyncpg.connect(dsn, database="postgres")
    try:
        assert await maintenance.fetchval("SELECT current_database()") == "postgres"
        try:
            await maintenance.execute(f'CREATE DATABASE "{name}"')
        except asyncpg.InsufficientPrivilegeError:
            pytest.skip(
                "ledger isolation regression requires a role with CREATE DATABASE privilege"
            )
        try:
            url = urlsplit(dsn)
            query = urlencode(
                [
                    (key, value)
                    for key, value in parse_qsl(url.query)
                    if key not in {"database", "dbname"}
                ]
            )
            yield urlunsplit(url._replace(path=f"/{name}", query=query))
        finally:
            await _drop_database(maintenance, name)
    finally:
        await maintenance.close()


async def _public_snapshot(dsn):
    connection = await asyncpg.connect(dsn)
    try:
        return {
            table: await connection.fetch(f'SELECT * FROM public."{table}" ORDER BY 1')
            for table in (
                "account_reservations",
                "schema_migrations",
                "fills",
                "ledger_transactions",
                "ledger_postings",
                "ledger_events",
                "orders",
                "outbox",
                "reconciliation_state",
                "reconciliation_violations",
            )
        }
    finally:
        await connection.close()


@pytest.mark.parametrize("helper", ["ledger", "properties"])
def test_ledger_helpers_isolate_unqualified_sql_from_public(helper, monkeypatch):
    async def run():
        async with disposable_database(postgres_dsn()) as dsn:
            connection = await asyncpg.connect(dsn)
            try:
                # This database belongs to the fixture; never seed the source DSN's public schema.
                assert (
                    await connection.fetchval("SELECT current_database()") == urlsplit(dsn).path[1:]
                )
                await connection.execute("SET search_path TO public")
                adapter = LedgerAdapter(connection)
                await adapter.migrate()
                await adapter.record_fill(fill("public-1"), DEFAULT_INSTRUMENTS["BTC-USD"])
            finally:
                await connection.close()
            public_before = await _public_snapshot(dsn)
            assert len(public_before["fills"]) == 1
            assert len(public_before["ledger_postings"]) == 5

            isolated = None
            schema = f"ledger_{uuid.uuid4().hex}"
            if helper == "ledger":
                monkeypatch.setenv("SIMEX_TEST_POSTGRES_DSN", dsn)
                isolated = await connection_or_skip()
                scoped_dsn, schema = isolated.dsn, isolated._schema
            else:
                await _create_schema(dsn, schema)
                scoped_dsn = _schema_dsn(dsn, schema)
            try:
                if helper == "properties":
                    await _postgres_setup(scoped_dsn, schema)
                ledger = SyncLedger(scoped_dsn)
                try:
                    # Probe the actual worker connection, not only the async setup connection.
                    assert (
                        ledger._call(
                            lambda adapter: adapter.connection.fetchval("SELECT current_schema()")
                        )
                        == schema
                    )
                    assert (
                        ledger._call(
                            lambda adapter: adapter.connection.fetchval(
                                "SELECT count(*) FROM fills"
                            )
                        )
                        == 0
                    )
                    ledger.record_fill(
                        fill("isolated-1"), DEFAULT_INSTRUMENTS["BTC-USD"], None, None
                    )
                    ledger.record_fill(
                        fill("isolated-1"), DEFAULT_INSTRUMENTS["BTC-USD"], None, None
                    )
                    assert (
                        ledger._call(
                            lambda adapter: adapter.connection.fetchval(
                                "SELECT count(*) FROM fills"
                            )
                        )
                        == 1
                    )
                    assert (
                        ledger._call(
                            lambda adapter: adapter.connection.fetchval(
                                "SELECT count(*) FROM ledger_postings"
                            )
                        )
                        == 5
                    )
                    totals = ledger._call(
                        lambda adapter: adapter.connection.fetch(
                            "SELECT asset, SUM(amount) AS total FROM ledger_postings GROUP BY asset"
                        )
                    )
                    assert {row["asset"]: row["total"] for row in totals} == {"BTC": 0, "USD": 0}
                finally:
                    ledger.close()
                assert await _public_snapshot(dsn) == public_before
            finally:
                if isolated is not None:
                    await isolated.close()
                else:
                    await _drop_schema(dsn, schema)

    asyncio.run(run())


@pytest.mark.parametrize("helper", ["ledger", "properties"])
@pytest.mark.parametrize(
    "misroute", ["search_path", "extra_search_path", "fills", "schema_migrations"]
)
def test_ledger_helpers_reject_wrong_unqualified_namespace(helper, misroute, monkeypatch):
    module = ledger_tests if helper == "ledger" else property_tests
    migrate = module.apply_migrations
    connections = []

    async def misroute_after_migration(connection):
        await migrate(connection)
        connections.append(connection)
        if misroute == "search_path":
            await connection.execute("SET search_path TO pg_catalog")
        elif misroute == "extra_search_path":
            schema = await connection.fetchval("SELECT current_schema()")
            await connection.execute(f'SET search_path TO "{schema}", pg_catalog')
        else:
            # Implicit pg_temp resolution can shadow a table even with a correct search_path.
            await connection.execute(f'CREATE TEMP TABLE "{misroute}" (sentinel integer)')

    monkeypatch.setattr(module, "apply_migrations", misroute_after_migration)

    async def run():
        dsn = postgres_dsn()
        if helper == "ledger":
            isolated = None
            try:
                with pytest.raises(AssertionError):
                    isolated = await connection_or_skip()
            finally:
                if isolated is not None:
                    await isolated.close()
        else:
            schema = f"ledger_{uuid.uuid4().hex}"
            await _create_schema(dsn, schema)
            try:
                with pytest.raises(AssertionError):
                    await _postgres_setup(_schema_dsn(dsn, schema), schema)
            finally:
                await _drop_schema(dsn, schema)
        assert len(connections) == 1
        assert connections[0].is_closed()

    asyncio.run(run())


def test_disposable_database_cleanup_tolerates_already_absent_database():
    async def run():
        source_dsn = postgres_dsn()
        async with disposable_database(source_dsn) as dsn:
            maintenance = await asyncpg.connect(source_dsn, database="postgres")
            try:
                assert await maintenance.fetchval("SELECT current_database()") == "postgres"
                await _drop_database(maintenance, urlsplit(dsn).path[1:])
            finally:
                await maintenance.close()
        # The context's finally issues the same guarded IF EXISTS drop a second time.

    asyncio.run(run())


class MaintenanceConnection:
    def __init__(self, *, denied=False):
        self.denied = denied
        self.statements = []
        self.closed = False

    async def fetchval(self, query):
        assert query == "SELECT current_database()"
        return "postgres"

    async def execute(self, query):
        self.statements.append(query)
        if self.denied and query.startswith("CREATE DATABASE"):
            raise asyncpg.InsufficientPrivilegeError("permission denied to create database")

    async def close(self):
        self.closed = True


@pytest.mark.parametrize(
    "name",
    [
        "simex",
        "simex_test",
        "postgres",
        "simex_isolation_fixed",
        "simex_isolation_" + "a" * 33,
        "simex_isolation_" + "a" * 32 + '"; DROP DATABASE simex; --',
    ],
)
def test_disposable_database_drop_refuses_unsafe_names(name):
    maintenance = MaintenanceConnection()
    with pytest.raises(AssertionError, match="unsafe database drop"):
        asyncio.run(_drop_database(maintenance, name))
    assert maintenance.statements == []


def test_disposable_database_uses_postgres_and_cleans_up_after_failure(monkeypatch):
    maintenance = MaintenanceConnection()

    async def connect(dsn, *, database):
        assert dsn == "postgresql://localhost/simex_test?database=simex&sslmode=prefer"
        assert database == "postgres"
        return maintenance

    monkeypatch.setattr(asyncpg, "connect", connect)

    async def run():
        with pytest.raises(RuntimeError, match="mid-test failure"):
            async with disposable_database(
                "postgresql://localhost/simex_test?database=simex&sslmode=prefer"
            ) as dsn:
                name = urlsplit(dsn).path[1:]
                assert re.fullmatch(r"simex_isolation_[0-9a-f]{32}", name)
                assert uuid.UUID(hex=name.removeprefix("simex_isolation_")).version == 4
                assert parse_qsl(urlsplit(dsn).query) == [("sslmode", "prefer")]
                # Cleanup is also safe when an earlier action has already removed the database.
                await _drop_database(maintenance, name)
                raise RuntimeError("mid-test failure")

    asyncio.run(run())
    create, first_drop, final_drop = maintenance.statements
    assert create.startswith('CREATE DATABASE "simex_isolation_')
    assert (
        first_drop
        == final_drop
        == create.replace("CREATE DATABASE", "DROP DATABASE IF EXISTS") + " WITH (FORCE)"
    )
    assert maintenance.closed


def test_disposable_database_skips_without_create_privilege(monkeypatch):
    maintenance = MaintenanceConnection(denied=True)

    async def connect(dsn, *, database):
        assert database == "postgres"
        return maintenance

    monkeypatch.setattr(asyncpg, "connect", connect)

    async def run():
        with pytest.raises(pytest.skip.Exception, match="CREATE DATABASE privilege"):
            async with disposable_database("postgresql://localhost/simex_test"):
                pytest.fail("must not yield a database without CREATE DATABASE privilege")

    asyncio.run(run())
    assert len(maintenance.statements) == 1
    assert maintenance.closed
