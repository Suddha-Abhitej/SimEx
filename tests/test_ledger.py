import asyncio
import os
import threading
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
        return await asyncpg.connect(dsn)
    except Exception as exc:
        if os.environ.get("SIMEX_REQUIRE_POSTGRES_TESTS") == "1":
            raise RuntimeError(f"Postgres unavailable: {exc}") from exc
        pytest.skip(f"Postgres unavailable: {exc}")


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


def test_sync_ledger_timeout_retry_is_idempotent_after_commit():
    async def scenario():
        dsn = os.environ.get("SIMEX_TEST_POSTGRES_DSN")
        probe = await connection_or_skip()
        await probe.close()
        ledger = SyncLedger(
            dsn,
            hard_timeout_seconds=0.01,
            cancellation_grace_seconds=0.2,
            metrics=LedgerMetrics(),
        )
        calls = 0
        original = ledger._adapter.record_fill

        async def delayed_record_fill(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                try:
                    await asyncio.sleep(0.05)
                except asyncio.CancelledError:
                    await asyncio.sleep(0.01)
            return await original(*args, **kwargs)

        ledger._adapter.record_fill = delayed_record_fill
        current = fill(f"test-sync-timeout-{os.getpid()}-1")
        try:
            ledger.record_fill(current, DEFAULT_INSTRUMENTS["BTC-USD"], None, None)
            assert calls == 2
            conn = await asyncpg.connect(dsn)
            try:
                assert (
                    await conn.fetchval(
                        "SELECT count(*) FROM fills WHERE fill_id = $1", current.fill_id
                    )
                    == 1
                )
                assert (
                    await conn.fetchval(
                        "SELECT count(*) FROM ledger_postings WHERE fill_id = $1",
                        current.fill_id,
                    )
                    == 5
                )
            finally:
                await conn.close()
        finally:
            ledger.close()

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
