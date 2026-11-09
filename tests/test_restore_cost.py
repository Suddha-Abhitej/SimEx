"""Safety and measurement contracts on hand-written histories, never live recordings."""

import asyncio
import importlib.util
import json
import os
import re
import socket
import sys
import threading
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

import asyncpg
import pytest

from simex.ledger import LedgerAdapter
from simex.ledger.sync import HARD_TIMEOUT_SECONDS

TOOLS = Path(__file__).resolve().parent.parent / "tools"
spec = importlib.util.spec_from_file_location("restore_driver", TOOLS / "measure_restore_cost.py")
driver = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(TOOLS))
try:
    spec.loader.exec_module(driver)
finally:
    sys.path.remove(str(TOOLS))
fixture = sys.modules["restore_cost_fixture"]


@pytest.fixture
def maintenance_dsn():
    dsn = os.environ.get("SIMEX_TEST_POSTGRES_DSN")
    if not dsn:
        if os.environ.get("SIMEX_REQUIRE_POSTGRES_TESTS") == "1":
            pytest.fail("SIMEX_TEST_POSTGRES_DSN required")
        pytest.skip("set SIMEX_TEST_POSTGRES_DSN for restore measurement tests")
    return fixture.database_dsn(dsn, "postgres")


@pytest.mark.parametrize(
    "name",
    [
        "simex",
        "simex_test",
        "postgres",
        "simex_restore_fixed",
        "simex_restore_" + "a" * 33,
        "simex_restore_" + "a" * 32 + '"; DROP DATABASE simex; --',
    ],
)
def test_drop_refuses_every_non_generated_name(name):
    class NeverDDL:
        async def execute(self, query):
            pytest.fail("unsafe DDL executed")

    with pytest.raises(AssertionError, match="unsafe database drop"):
        asyncio.run(fixture.drop_database(NeverDDL(), name))


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://localhost/simex",
        "postgresql://localhost/simex_test",
        "postgresql://localhost/",
        "postgresql://localhost/postgres?dbname=simex",
        "postgresql://localhost/postgres?host=elsewhere",
    ],
)
def test_maintenance_must_be_explicit_neutral_database(dsn):
    with pytest.raises(ValueError):
        fixture.validate_maintenance(dsn)


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://localhost/simex",
        "postgresql://localhost/simex_test",
        "postgresql://localhost/simex_restore_fixed",
        "postgresql://localhost/simex_restore_" + "a" * 32 + "?dbname=simex",
    ],
)
def test_seed_and_worker_refuse_non_disposable_targets(dsn):
    with pytest.raises(ValueError):
        asyncio.run(fixture.seed(dsn, 36, "filled"))
    with pytest.raises(ValueError):
        driver.run_worker(dsn, 36, "default", driver.GIB)


class Maintenance:
    def __init__(self, denied=False):
        self.queries = []
        self.denied = denied
        self.closed = False

    async def fetchval(self, query):
        return "postgres"

    async def execute(self, query):
        self.queries.append(query)
        if self.denied and query.startswith("CREATE DATABASE"):
            raise asyncpg.InsufficientPrivilegeError()

    async def close(self):
        self.closed = True


@pytest.mark.parametrize("fails", [False, True])
def test_disposable_cleanup_is_finally_guarded_unique_and_tolerates_absence(monkeypatch, fails):
    connections = []

    async def connect(dsn, **kwargs):
        assert kwargs == {"database": "postgres"}
        connection = Maintenance()
        connections.append(connection)
        return connection

    monkeypatch.setattr(fixture.asyncpg, "connect", connect)

    async def scenario():
        names = []
        for _ in range(2):
            try:
                async with fixture.disposable_database("postgresql://localhost/postgres") as dsn:
                    name = urlsplit(dsn).path.removeprefix("/")
                    assert re.fullmatch(r"simex_restore_[0-9a-f]{32}", name)
                    assert UUID(hex=name.removeprefix("simex_restore_")).version == 4
                    names.append(name)
                    if fails:
                        raise RuntimeError("mid-test failure")
            except RuntimeError:
                assert fails
        assert names[0] != names[1]

    asyncio.run(scenario())
    for c in connections:
        assert c.closed
        assert c.queries[-1].startswith('DROP DATABASE IF EXISTS "simex_restore_')
        assert c.queries[-1].endswith('" WITH (FORCE)')


def test_create_privilege_skip_is_clear_and_closes_maintenance(monkeypatch):
    connection = Maintenance(denied=True)

    async def connect(*args, **kwargs):
        return connection

    monkeypatch.setattr(fixture.asyncpg, "connect", connect)

    async def scenario():
        async with fixture.disposable_database("postgresql://localhost/postgres"):
            pytest.fail("must not seed without CREATE DATABASE privilege")

    with pytest.raises(fixture.MeasurementSkipped, match="skipped: role needs CREATE DATABASE"):
        asyncio.run(scenario())
    assert connection.closed
    assert len(connection.queries) == 1


def test_disk_headroom_guard():
    with pytest.raises(fixture.MeasurementSkipped, match="disk headroom"):
        fixture.check_headroom(1000000, driver.GIB)
    fixture.check_headroom(1000, 2 * driver.GIB)


def test_watchdogs_and_incomplete_results_are_not_successes():
    assert driver.watchdog_reason(0, 11, driver.GIB, 10, 10) == "rss_limit"
    assert driver.watchdog_reason(11, 1, driver.GIB, 10, 10) == "wall_limit"
    assert driver.watchdog_reason(0, 1, 1, 10, 10) == "memory_pressure"
    assert driver.watchdog_reason(0, 1, driver.GIB, 10, 10) is None
    empty = driver.summarize([{"status": "rss_limit"}, {"status": "timeout"}, {"status": "error"}])
    assert empty["successes"] == 0
    assert empty["restore_ns"] is None
    points = [
        {"orders": 1000, "default": {"runs": [{"status": "ok"}] * 3}},
        {
            "orders": 10000,
            "default": {
                "runs": [{"status": "timeout"}, {"status": "ok"}, {"status": "wall_limit"}]
            },
        },
    ]
    bracket = driver.startup_bracket(points)["per_run"]
    assert bracket[0] == {"largest_pass": 1000, "first_timeout_failure": 10000}
    assert bracket[1] == {"largest_pass": 10000, "first_timeout_failure": None}
    assert bracket[2] == {"largest_pass": 1000, "first_timeout_failure": None}


@pytest.mark.parametrize(
    "extra",
    [
        {"price": "100"},
        {"price": 100},
        {"dsn": "secret"},
        {"orders_rows": 1.1},
        {"orders_rows": "100"},
        {"status": "secret"},
    ],
)
def test_worker_aggregate_whitelist(extra):
    with pytest.raises(ValueError):
        driver.validate_result({"status": "ok", "checked_keys": 3} | extra)


def test_same_key_and_reuse_guards_are_actually_checked():
    class BrokenWriter:
        orders = {"order": object()}
        by_key = {(fixture.ACCOUNT, fixture.intention(0).client_order_id): "order"}

        def submit(self, *args):
            return self.orders["order"]

    with pytest.raises(AssertionError, match="different request"):
        driver.verify_keys(BrokenWriter(), 36)


@pytest.mark.skipif(sys.platform != "linux", reason="Linux /proc measurement")
@pytest.mark.parametrize("profile", ["filled", "unfilled"])
def test_small_copy_seed_matches_real_adapter_and_actual_recovery(
    maintenance_dsn, monkeypatch, profile
):
    async def scenario():
        try:
            async with fixture.disposable_database(maintenance_dsn) as dsn:
                counts = await fixture.seed(dsn, 36, profile, batch_size=7)
                assert counts["orders"] == 36
                assert counts["fills"] == (4 if profile == "filled" else 0)
                connection = await asyncpg.connect(dsn)
                try:
                    copied = await LedgerAdapter(connection).load_account(fixture.ACCOUNT)
                    await connection.execute("CREATE SCHEMA reference")
                    await connection.execute("SET search_path TO reference")
                    adapter = LedgerAdapter(connection)
                    await adapter.migrate()
                    await adapter.record_deposit(
                        fixture.ACCOUNT,
                        "USD",
                        fixture.Decimal(37) * fixture.PRICE,
                        "synthetic-seed",
                    )
                    for index in range(36):
                        order, reservation, fill = fixture.example(index, profile)
                        if fill:
                            await adapter.record_fill(fill, fixture.INSTRUMENT, order)
                        else:
                            await adapter.save_order(order, reservation)
                    reference = await adapter.load_account(fixture.ACCOUNT)
                    assert sorted(copied.orders, key=lambda r: r["order_id"]) == sorted(
                        reference.orders, key=lambda r: r["order_id"]
                    )
                    assert sorted(copied.reservations, key=lambda r: r["order_id"]) == sorted(
                        reference.reservations, key=lambda r: r["order_id"]
                    )
                    assert sorted(copied.balances, key=lambda r: r["asset"]) == sorted(
                        reference.balances, key=lambda r: r["asset"]
                    )
                    assert copied.latest_sequence == reference.latest_sequence
                finally:
                    await connection.close()
                monkeypatch.setenv(driver.WORKER_DSN, dsn)
                result = await asyncio.to_thread(driver.worker, {"orders": 36, "mode": "default"})
                assert result["status"] == "ok"
                assert (
                    result["orders_rows"]
                    == result["restored_orders"]
                    == result["restored_keys"]
                    == 36
                )
                assert result["reservations_rows"] == 16
                assert result["balances_rows"] == (2 if profile == "filled" else 1)
                assert result["checked_keys"] == 3
                assert result["rss_before_bytes"] > 0
                assert result["rss_after_bytes"] > 0
                assert result["peak_rss_bytes"] >= result["rss_after_bytes"]
                assert result["construct_ns"] == result["restore_ns"] - result["load_ns"]
        except fixture.MeasurementSkipped as exc:
            pytest.skip(str(exc))

    asyncio.run(scenario())


@pytest.mark.skipif(sys.platform != "linux", reason="Linux /proc measurement")
def test_cli_end_to_end_fresh_workers_and_untimed_response_bytes(
    maintenance_dsn, tmp_path, monkeypatch
):
    monkeypatch.setenv("RESTORE_TEST_MAINTENANCE", maintenance_dsn)
    # Test hardware may have less spare memory than the local published sweep; headroom is
    # independently tested above. The worker still has its real RSS/memory/wall watchdogs.
    actual = driver.memory_info
    monkeypatch.setattr(driver, "memory_info", lambda: actual() | {"MemAvailable": 3 * driver.GIB})
    output = tmp_path / "summary.json"
    code = driver.main(
        [
            "--maintenance-dsn-env",
            "RESTORE_TEST_MAINTENANCE",
            "--rss-budget-gib",
            "1",
            "--seed-disk-budget-gib",
            "2",
            "--orders",
            "36",
            "--json",
            str(output),
        ]
    )
    if code == 0 and not output.exists():
        pytest.skip("restore measurement skipped without CREATE DATABASE privilege")
    assert code == 0
    report = json.loads(output.read_text())
    point = report["points"][0]
    assert point["default"]["successes"] == point["diagnostic"]["successes"] == 3
    assert all(run["restored_orders"] == 36 for run in point["default"]["runs"])
    assert point["bytes"]["status"] == "ok"
    assert point["bytes"]["response_bytes"] > 0
    assert "restore_ns" not in point["bytes"]
    assert report["policy"]["default_timeout_ns"] == int(HARD_TIMEOUT_SECONDS * 10**9)
    assert report["policy"]["diagnostic_timeout_seconds"] == 120
    assert report["policy"]["rationale_bytes"] == 128
    assert all(
        word not in output.read_text()
        for word in ("postgresql:", "fill_id", "limit_price", "synthetic-seed")
    )


def test_relay_counts_only_server_payload_and_can_exclude_authentication():
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)

    def receive(peer, size):
        data = bytearray()
        while len(data) < size:
            chunk = peer.recv(size - len(data))
            assert chunk, "connection closed before the complete payload"
            data.extend(chunk)
        return bytes(data)

    def server():
        with listener, listener.accept()[0] as peer:
            peer.sendall(b"synthetic-auth")
            assert receive(peer, len(b"request")) == b"request"
            peer.sendall(b"synthetic-response")

    thread = threading.Thread(target=server)
    thread.start()
    relay = driver.CountingRelay(f"postgresql://127.0.0.1:{listener.getsockname()[1]}/postgres")
    try:
        with socket.create_connection(("127.0.0.1", relay.port)) as client:
            assert receive(client, len(b"synthetic-auth")) == b"synthetic-auth"
            baseline = relay.received
            client.sendall(b"request")
            assert receive(client, len(b"synthetic-response")) == b"synthetic-response"
            assert relay.received - baseline == len(b"synthetic-response")
    finally:
        relay.close()
        thread.join(5)
    assert not thread.is_alive()


@pytest.mark.skipif(sys.platform != "linux", reason="Linux worker measurement")
@pytest.mark.parametrize(
    "mode,expected", [("default", {}), ("diagnostic", {"hard_timeout_seconds": 120})]
)
def test_timeout_is_one_load_attempt_and_preserves_the_production_default(
    monkeypatch, mode, expected
):
    calls = []
    closed = []

    class TimedOut:
        def __init__(self, dsn, **kwargs):
            assert kwargs == expected

        def load_account(self, account):
            calls.append(account)
            raise TimeoutError()

        def close(self):
            closed.append(True)

    monkeypatch.setenv(driver.WORKER_DSN, "postgresql://localhost/simex_restore_" + "a" * 32)
    monkeypatch.setattr(driver, "SyncLedger", TimedOut)
    result = driver.worker({"orders": 36, "mode": mode})
    assert result["status"] == "timeout"
    assert calls == [fixture.ACCOUNT]
    assert closed == [True]
    assert result["construct_ns"] is None
    assert driver.summarize([result])["successes"] == 0


def test_marginals_keep_noise_and_missing_measurements_honest():
    first = {
        "orders": 1000,
        "diagnostic": {
            "restore_ns": {"median": 1000},
            "load_ns": {"median": 900},
            "rss_delta_bytes": {"median": 800},
        },
        "bytes": {"response_bytes": 1000},
    }
    second = {
        "orders": 2000,
        "diagnostic": {
            "restore_ns": {"median": 900},
            "load_ns": {"median": 850},
            "rss_delta_bytes": None,
        },
        "bytes": {"status": "rss_limit"},
    }
    assert driver.marginal(first, second) == {
        "restore_ns_per_order": "-0.1",
        "load_ns_per_order": "-0.05",
        "rss_delta_bytes_per_order": None,
        "response_bytes_per_order": None,
    }


@pytest.mark.skipif(sys.platform != "linux", reason="Linux worker measurement")
def test_byte_worker_excludes_connection_auth_and_never_reports_relay_timings(monkeypatch):
    from types import SimpleNamespace

    relay = SimpleNamespace(received=0, failed=False, port=1234, close=lambda: None)

    class Ledger:
        def __init__(self, *args, **kwargs):
            relay.received = 100  # Connection/authentication payload.

        def load_account(self, account):
            relay.received += 200
            return SimpleNamespace(
                orders=(None,) * 36, reservations=(None,) * 16, balances=(None,), latest_sequence=1
            )

        def close(self):
            pass

    class Writer:
        def __init__(self, *args, ledger, **kwargs):
            self.ledger = ledger
            self.orders = dict.fromkeys(range(36))
            self.by_key = dict.fromkeys(range(36))
            self.accounts = SimpleNamespace(reservations=dict.fromkeys(range(16)))

        def restore(self, account):
            self.ledger.load_account(account)

    monkeypatch.setenv(driver.WORKER_DSN, "postgresql://localhost/simex_restore_" + "a" * 32)
    monkeypatch.setattr(driver, "CountingRelay", lambda dsn: relay)
    monkeypatch.setattr(driver, "SyncLedger", Ledger)
    monkeypatch.setattr(driver, "AccountWriter", Writer)
    monkeypatch.setattr(driver, "verify_keys", lambda *args: 3)
    result = driver.worker({"orders": 36, "mode": "bytes"})
    assert result["status"] == "ok"
    assert result["response_bytes"] == 200
    assert "restore_ns" not in result
    assert "rss_delta_bytes" not in result


@pytest.mark.skipif(sys.platform != "linux", reason="Linux watchdog")
def test_watchdog_censors_and_reaps_a_controlled_worker(monkeypatch):
    import io

    class Controlled:
        pid = os.getpid()
        returncode = None
        polls = 0
        killed = reaped = False
        stdin = io.BytesIO()

        def poll(self):
            self.polls += 1
            assert self.polls < 5, "watchdog failed to terminate the worker"
            return self.returncode

        def kill(self):
            self.killed = True
            self.returncode = -9

        def wait(self):
            assert self.killed
            self.reaped = True
            return self.returncode

    child = Controlled()
    monkeypatch.setattr(driver.subprocess, "Popen", lambda *args, **kwargs: child)
    monkeypatch.setattr(driver, "watchdog_reason", lambda *args: "rss_limit")
    result = driver.run_worker(
        "postgresql://127.0.0.1:1/simex_restore_" + "a" * 32, 36, "default", 1
    )
    assert result == {"status": "rss_limit"}
    assert child.killed and child.reaped


def test_post_exit_peak_budget_is_still_enforced(monkeypatch):
    import io

    class Finished:
        returncode = 0

        def __init__(self, *args, **kwargs):
            self.stdin = io.BytesIO()
            kwargs["stdout"].write(
                json.dumps({"status": "ok", "checked_keys": 3, "peak_rss_bytes": 1000}).encode()
            )

        def poll(self):
            return 0

        def wait(self):
            return 0

    monkeypatch.setattr(driver.subprocess, "Popen", Finished)
    result = driver.run_worker(
        "postgresql://localhost/simex_restore_" + "a" * 32, 36, "default", 10
    )
    assert result == {"status": "rss_limit"}


def test_finally_drop_tolerates_database_already_removed(maintenance_dsn):
    async def scenario():
        try:
            async with fixture.disposable_database(maintenance_dsn) as dsn:
                maintenance = await asyncpg.connect(maintenance_dsn, database="postgres")
                try:
                    await fixture.drop_database(maintenance, urlsplit(dsn).path.removeprefix("/"))
                finally:
                    await maintenance.close()
        except fixture.MeasurementSkipped as exc:
            pytest.skip(str(exc))

    asyncio.run(scenario())
