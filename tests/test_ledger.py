import asyncio
import os
import threading
import uuid
from decimal import Decimal
from types import SimpleNamespace

import asyncpg
import pytest

from simex.exchange import DEFAULT_INSTRUMENTS, Fill, FillCause, Liquidity, Side
from simex.ledger import (
    LedgerAdapter,
    LedgerHalted,
    LedgerIntegrityError,
    LedgerMetrics,
    SyncLedger,
)
from simex.ledger.migration_runner import apply_migrations


def fill(fill_id="order-1", side=Side.BUY):
    order_id = fill_id.rsplit("-", 1)[0]
    return Fill(
        fill_id=fill_id,
        order_id=order_id,
        account_id="alice",
        product_id="BTC-USD",
        side=side,
        price=Decimal("100.00"),
        quantity=Decimal("0.1"),
        liquidity=Liquidity.TAKER,
        cause=FillCause.TAKER,
        fee=Decimal("0.12"),
        fee_asset="USD",
        ts_ns=1_792_036_800_000_000_000,
    )


async def connection_or_skip():
    dsn = os.environ.get("SIMEX_TEST_POSTGRES_DSN")
    if not dsn:
        if os.environ.get("SIMEX_REQUIRE_POSTGRES_TESTS") == "1":
            raise RuntimeError("SIMEX_TEST_POSTGRES_DSN is required in this test job")
        pytest.skip("set SIMEX_TEST_POSTGRES_DSN for ledger integration tests")
    try:
        connection = await asyncpg.connect(dsn)
    except Exception as exc:
        if os.environ.get("SIMEX_REQUIRE_POSTGRES_TESTS") == "1":
            raise RuntimeError(f"Postgres unavailable: {exc}") from exc
        pytest.skip(f"Postgres unavailable: {exc}")

    schema = f"ledger_test_{uuid.uuid4().hex}"
    try:
        await connection.execute(f'CREATE SCHEMA "{schema}"')
        await connection.execute(f'SET search_path TO "{schema}"')
        await apply_migrations(connection)
        expected = {
            "account_reservations",
            "fills",
            "ledger_events",
            "ledger_postings",
            "ledger_transactions",
            "orders",
            "outbox",
            "reconciliation_state",
            "reconciliation_violations",
            "schema_migrations",
        }
        assert expected <= {
            row["tablename"]
            for row in await connection.fetch(
                "SELECT tablename FROM pg_tables WHERE schemaname = $1", schema
            )
        }
        for table in ("ledger_postings", "reconciliation_state"):
            assert (
                await connection.fetchval(
                    "SELECT n.nspname || '.' || c.relname "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.oid = to_regclass($1)",
                    f"{schema}.{table}",
                )
                == f"{schema}.{table}"
            )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_tables WHERE schemaname = 'public' "
                "AND tablename = ANY($1::text[])",
                list(expected),
            )
            == 0
        )
        return IsolatedConnection(connection, dsn, schema)
    except Exception:
        await connection.close()
        admin = await asyncpg.connect(dsn)
        try:
            await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await admin.close()
        raise


class IsolatedConnection:
    def __init__(self, connection, dsn, schema):
        self._connection = connection
        self.dsn = f"{dsn}{'&' if '?' in dsn else '?'}options=-csearch_path%3D{schema}"
        self._dsn = dsn
        self._schema = schema

    def __getattr__(self, name):
        return getattr(self._connection, name)

    async def close(self):
        await self._connection.close()
        admin = await asyncpg.connect(self._dsn)
        try:
            await admin.execute(f'DROP SCHEMA IF EXISTS "{self._schema}" CASCADE')
        finally:
            await admin.close()


def run(coro):
    return asyncio.run(coro)


def test_sync_ledger_timeout_halts_when_cancellation_does_not_settle():
    class SlowAdapter:
        def __init__(self):
            self.in_flight = False
            self.calls = 0

        async def record_fill(self, *_args):
            self.calls += 1
            if self.in_flight:
                raise asyncpg.InterfaceError("another operation is in progress")
            self.in_flight = True
            try:
                try:
                    await asyncio.sleep(0.2)
                except asyncio.CancelledError:
                    await asyncio.sleep(0.2)
            finally:
                self.in_flight = False

    async def scenario():
        ledger = SyncLedger.__new__(SyncLedger)
        ledger.metrics = LedgerMetrics()
        ledger.hard_timeout_seconds = 0.01
        ledger.cancellation_grace_seconds = 0.01
        ledger._loop = asyncio.new_event_loop()
        ledger._adapter = SlowAdapter()
        thread = threading.Thread(
            target=lambda: (asyncio.set_event_loop(ledger._loop), ledger._loop.run_forever())
        )
        thread.start()
        try:
            with pytest.raises(LedgerHalted, match="cancellation did not settle"):
                ledger.record_fill(
                    SimpleNamespace(),
                    DEFAULT_INSTRUMENTS["BTC-USD"],
                    SimpleNamespace(),
                    None,
                )
            assert ledger.metrics.timeouts._value.get() == 1
            assert ledger.metrics.halts._value.get() == 1
        finally:
            ledger._loop.call_soon_threadsafe(ledger._loop.stop)
            thread.join()
            ledger._loop.close()

    run(scenario())


def test_sync_ledger_timeout_retry_is_idempotent_after_commit(monkeypatch):
    async def scenario():
        probe = await connection_or_skip()
        dsn = probe.dsn
        # The controlled result below injects the timeout; default deadlines do not order it.
        ledger = SyncLedger(dsn, metrics=LedgerMetrics())
        committed = threading.Event()
        settled = threading.Event()
        watchdog_seconds = 10  # A deadlock fails the test; this never selects a retry.
        attempts, sequences, stages, tasks, futures = [], [], [], [], []
        injected = False
        original = ledger._adapter.record_fill
        submit = asyncio.run_coroutine_threadsafe

        async def hold_first_acknowledgement(*args, **kwargs):
            tasks.append(asyncio.current_task())
            attempts.append(args[0].fill_id)
            assert len(attempts) <= 2, "unexpected additional retry"
            if len(attempts) == 2:
                assert settled.is_set(), "retry started before cancellation was settled"
                assert futures[0].done()
                stages.append("retry")
            sequence = await original(*args, **kwargs)
            sequences.append(sequence)
            if len(attempts) == 1:
                stages.append("committed")
                committed.set()
                try:
                    await asyncio.Future()  # Commit is real; only its acknowledgement is held.
                except asyncio.CancelledError:
                    stages.append("cancelled")
            return sequence

        class ControlledResult:
            def __init__(self, future, first):
                self.future = future
                self.first = first

            def result(self, timeout=None):
                nonlocal injected
                if self.first and not injected:
                    assert committed.wait(watchdog_seconds), "first fill did not commit"
                    assert not self.future.done(), "first acknowledgement was not held"
                    injected = True
                    stages.append("timeout")
                    raise TimeoutError("deliberately lost the first committed acknowledgement")
                try:
                    # Completion, not the caller's deadline, drives settlement and retry.
                    sequence = self.future.result(timeout=watchdog_seconds)
                except TimeoutError as exc:
                    raise AssertionError("ledger test completion watchdog expired") from exc
                if self.first:
                    settled.set()
                    stages.append("settled")
                else:
                    stages.append("acknowledged")
                return sequence

            def cancel(self):
                return self.future.cancel()

        def controlled_submit(coro, loop):
            future = submit(coro, loop)
            first = not futures
            futures.append(future)
            return ControlledResult(future, first)

        ledger._adapter.record_fill = hold_first_acknowledgement
        current = fill(f"test-sync-timeout-{os.getpid()}-1")
        try:
            with monkeypatch.context() as patch:
                patch.setattr(asyncio, "run_coroutine_threadsafe", controlled_submit)
                sequence = ledger.record_fill(current, DEFAULT_INSTRUMENTS["BTC-USD"], None, None)
            assert attempts == [current.fill_id, current.fill_id]
            assert sequences == [sequence, sequence]
            assert stages == [
                "committed",
                "timeout",
                "cancelled",
                "settled",
                "retry",
                "acknowledged",
            ]
            assert ledger.metrics.timeouts._value.get() == 1
            assert ledger.metrics.halts._value.get() == 0
            conn = await asyncpg.connect(dsn)
            try:
                for table, count in (
                    ("fills", 1),
                    ("ledger_postings", 5),
                    ("ledger_transactions", 1),
                    ("ledger_events", 1),
                    ("outbox", 1),
                ):
                    assert await conn.fetchval(f"SELECT count(*) FROM {table}") == count
                assert await conn.fetchval("SELECT fill_id FROM fills") == current.fill_id
                assert (
                    await conn.fetchval(
                        "SELECT sequence FROM ledger_transactions WHERE transaction_id = $1",
                        f"fill:{current.fill_id}",
                    )
                    == sequence
                )
                for table in ("ledger_events", "outbox"):
                    assert await conn.fetchval(f"SELECT event_key FROM {table}") == (
                        f"fill:{current.fill_id}"
                    )
                postings = await conn.fetch("SELECT account_id, asset, amount FROM ledger_postings")
                assert {(p["account_id"], p["asset"]): p["amount"] for p in postings} == {
                    ("alice", "USD"): Decimal("-10.12"),
                    ("alice", "BTC"): Decimal("0.1"),
                    ("_market", "USD"): Decimal("10"),
                    ("_market", "BTC"): Decimal("-0.1"),
                    ("_fees", "USD"): Decimal("0.12"),
                }
                totals = await conn.fetch(
                    "SELECT asset, SUM(amount) AS total FROM ledger_postings GROUP BY asset"
                )
                assert {p["asset"]: p["total"] for p in totals} == {
                    "USD": Decimal(0),
                    "BTC": Decimal(0),
                }
            finally:
                await conn.close()
        finally:

            async def drain():
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

            try:
                submit(drain(), ledger._loop).result(timeout=watchdog_seconds)
            finally:
                try:
                    ledger.close()
                finally:
                    await probe.close()

    run(scenario())


def test_sync_ledger_connection_error_halts_with_original_cause():
    class BrokenAdapter:
        async def record_fill(self, *_args):
            raise asyncpg.InterfaceError("another operation is in progress")

    async def scenario():
        ledger = SyncLedger.__new__(SyncLedger)
        ledger.metrics = LedgerMetrics()
        ledger.hard_timeout_seconds = 0.01
        ledger.cancellation_grace_seconds = 0.01
        ledger._loop = asyncio.new_event_loop()
        ledger._adapter = BrokenAdapter()
        thread = threading.Thread(
            target=lambda: (asyncio.set_event_loop(ledger._loop), ledger._loop.run_forever())
        )
        thread.start()
        try:
            with pytest.raises(LedgerHalted) as caught:
                ledger.record_fill(
                    SimpleNamespace(),
                    DEFAULT_INSTRUMENTS["BTC-USD"],
                    SimpleNamespace(),
                    None,
                )
            assert isinstance(caught.value.__cause__, asyncpg.InterfaceError)
            assert ledger.metrics.halts._value.get() == 1
        finally:
            ledger._loop.call_soon_threadsafe(ledger._loop.stop)
            thread.join()
            ledger._loop.close()

    run(scenario())


def test_fill_is_idempotent_and_conflicts_are_hard_errors():
    async def scenario():
        conn = await connection_or_skip()
        try:
            metrics = LedgerMetrics()
            adapter = LedgerAdapter(conn, metrics=metrics)
            await adapter.migrate()
            current = fill(f"test-idempotent-{os.getpid()}-1")
            await adapter.record_fill(current, DEFAULT_INSTRUMENTS["BTC-USD"])
            await adapter.record_fill(current, DEFAULT_INSTRUMENTS["BTC-USD"])
            changed = fill(current.fill_id)
            changed = Fill(**{**changed.__dict__, "fee": Decimal("0.13")})
            with pytest.raises(LedgerIntegrityError):
                await adapter.record_fill(changed, DEFAULT_INSTRUMENTS["BTC-USD"])
            assert metrics.conflicts._value.get() == 1
            for phase in (
                "transaction_begin",
                "fill_insert",
                "postings",
                "event",
                "outbox",
                "commit",
            ):
                assert (
                    metrics.registry.get_sample_value(
                        "simex_ledger_latency_seconds_count", {"phase": phase}
                    )
                    > 0
                )
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM fills WHERE fill_id = $1", current.fill_id
                )
                == 1
            )
        finally:
            await conn.close()

    run(scenario())


@pytest.mark.parametrize(
    "boundary", ["before_fill", "after_fill", "after_postings", "after_event", "after_outbox"]
)
def test_crash_injection_at_each_write_boundary_retries_atomically(boundary):
    async def scenario():
        conn = await connection_or_skip()
        calls = 0

        async def fail_once(name):
            nonlocal calls
            if name == boundary and calls == 0:
                calls += 1
                raise RuntimeError("injected process crash")

        try:
            adapter = LedgerAdapter(conn, failure_hook=fail_once)
            await adapter.migrate()
            current = fill(f"test-crash-{boundary}-{os.getpid()}-1")
            await adapter.record_fill(current, DEFAULT_INSTRUMENTS["BTC-USD"])
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM ledger_postings WHERE fill_id = $1", current.fill_id
                )
                == 5
            )
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM outbox WHERE event_key = $1", f"fill:{current.fill_id}"
                )
                == 1
            )
        finally:
            await conn.close()

    run(scenario())


def test_persistent_ledger_failure_halts_without_acknowledging_a_fill():
    async def scenario():
        conn = await connection_or_skip()

        async def always_fail(_name):
            raise RuntimeError("database unavailable")

        try:
            adapter = LedgerAdapter(conn, retries=2, backoff_seconds=0, failure_hook=always_fail)
            await adapter.migrate()
            current = fill(f"test-halt-{os.getpid()}-1")
            with pytest.raises(LedgerHalted):
                await adapter.record_fill(current, DEFAULT_INSTRUMENTS["BTC-USD"])
            assert adapter.halted
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM fills WHERE fill_id = $1", current.fill_id
                )
                == 0
            )
        finally:
            await conn.close()

    run(scenario())


def test_projection_fold_and_override_are_durable():
    async def scenario():
        conn = await connection_or_skip()
        try:
            adapter = LedgerAdapter(conn)
            await adapter.migrate()
            current = fill(f"test-reconcile-{os.getpid()}-1")
            current = Fill(**{**current.__dict__, "account_id": f"reconcile-{os.getpid()}"})
            await adapter.record_fill(current, DEFAULT_INSTRUMENTS["BTC-USD"])
            totals = await adapter.fold_balances()
            assert totals[(current.account_id, "USD")] == -(current.notional + current.fee)
            assert not await adapter.double_entry_violations()
            await adapter.write_reconciliation_override(
                account_id=current.account_id,
                asset="USD",
                actor="owner",
                reason="investigating",
                created_at_ns=10,
                expires_at_ns=20,
                event_key=f"override:{os.getpid()}",
            )
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM ledger_events "
                    "WHERE event_type = 'reconciliation_override'"
                )
                >= 1
            )
        finally:
            await conn.close()

    run(scenario())


def test_double_entry_violation_remains_unresolved_across_scans():
    async def scenario():
        conn = await connection_or_skip()
        try:
            adapter = LedgerAdapter(conn)
            await adapter.migrate()
            current = fill(f"test-violation-{uuid.uuid4().hex}-1")
            await adapter.record_fill(current, DEFAULT_INSTRUMENTS["BTC-USD"])
            await conn.execute(
                "UPDATE ledger_postings SET amount = amount + 1 WHERE fill_id = $1 "
                "AND entry_type = 'agent_payment'",
                current.fill_id,
            )

            first = await adapter.double_entry_violations()
            second = await adapter.double_entry_violations()

            assert first == second
            assert len(first) == 1
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM reconciliation_violations WHERE transaction_id = $1",
                    f"fill:{current.fill_id}",
                )
                == 1
            )
        finally:
            await conn.close()

    run(scenario())
