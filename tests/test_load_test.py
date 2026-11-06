"""The load driver over the real API and synthetic fills; no recorded market data."""

import asyncio
import importlib.util
import json
import sys
import time
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import httpx
import pytest
from test_api import TOKEN, no_floats
from test_api_stream import emit_fill, stream_tools
from test_client import serving
from test_exchange_runner import level2, trade
from test_mcp_ledger import OpenGate, _dsn_or_skip

from simex.client import StreamGap
from simex.exchange.merge import Arrival
from simex.exchange.risk import RiskLimits
from simex.exchange.runner_config import RunnerSettings
from simex.exchange.runner_metrics import RunnerMetrics
from simex.exchange.tools import ExchangeTools
from simex.ledger import SyncLedger
from simex.marketdata import decode

TOOLS = Path(__file__).resolve().parent.parent / "tools"
spec = importlib.util.spec_from_file_location("load_driver", TOOLS / "load_test.py")
driver = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(TOOLS))
try:
    spec.loader.exec_module(driver)
finally:
    sys.path.remove(str(TOOLS))
runtime = sys.modules["load_test_runtime"]
P = "BTC-USD"


@pytest.fixture
def load_dsn():
    base = _dsn_or_skip()
    name = "simex_load_test_" + uuid4().hex
    url = urlsplit(base)
    dsn = urlunsplit((url.scheme, url.netloc, "/" + name, "", ""))

    async def manage(create):
        connection = await asyncpg.connect(base)
        try:
            if create:
                await connection.execute(f'CREATE DATABASE "{name}"')
            else:
                await connection.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
        finally:
            await connection.close()

    asyncio.run(manage(True))
    try:
        yield dsn
    finally:
        asyncio.run(manage(False))


def measured_tools(dsn=None):
    ledger = SyncLedger(dsn) if dsn else None
    if ledger:
        ledger.migrate()
    observations = runtime.Observations()
    limits = RiskLimits({P: D("1")}, {P: D("1000")}, {P: D("10")})
    runner = runtime.MeasuredRunner(
        RunnerSettings(pairs=(P,), account_id="load_synthetic", digest_path=""),
        SimpleNamespace(),
        RunnerMetrics(),
        ledger=ledger,
        risk_limits=limits,
        observations=observations,
    )
    runner.writer.gate = OpenGate()
    runner.writer.deposit("load_synthetic", "USD", D("100000"), "seed:load_synthetic:USD:v1")
    t = time.time_ns() - 1_000_000_000
    for event in decode(level2(P, t, [("99", "10")], [("101", "10")], snapshot=True)):
        runner.engines[P].on_event(event)
    return (
        runner,
        observations,
        ExchangeTools("load_synthetic", runner.writer, runner.engines, limits),
    )


def intent(key="synthetic", **changes):
    return dict(
        client_order_id=key,
        product_id=P,
        side="buy",
        type="market",
        quantity="0.00001",
        reasoning="hand-written load fixture",
        **changes,
    )


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://localhost/simex",
        "postgresql://localhost/simex_test",
        "postgresql://localhost/other",
        "postgresql://localhost/simex_load_",
        "postgresql://localhost/simex_load_ok?database=simex",
        "postgresql://localhost/simex_load_ok?options=-csearch_path=other",
        "postgresql://localhost/simex_load_ok#simex",
        "dbname=simex_load_ok",
        "malformed",
    ],
)
def test_unsafe_destinations_are_rejected_before_connect(dsn, monkeypatch):
    async def forbidden(*args, **kwargs):
        pytest.fail("unsafe destination was connected")

    monkeypatch.setattr(runtime, "connect", forbidden)
    with pytest.raises(ValueError):
        asyncio.run(runtime.check_empty_database(dsn, "load_test"))


@pytest.mark.parametrize("account", ["owner", "_fees", "load_", "load_a;DROP", "load_" + "x" * 49])
def test_only_explicit_fresh_load_accounts_are_allowed(account):
    with pytest.raises(ValueError, match="load_.*account"):
        runtime.validate_destination("postgresql://localhost/simex_load_safe", account)


def test_existing_history_in_another_schema_is_refused_without_mutation(load_dsn):
    async def check():
        await runtime.check_empty_database(load_dsn, "load_test")
        connection = await asyncpg.connect(load_dsn)
        try:
            await connection.execute(
                "CREATE SCHEMA private; CREATE TABLE private.history (account_id text)"
            )
            await connection.execute("INSERT INTO private.history VALUES ('unrelated')")
            with pytest.raises(ValueError, match="account history"):
                await runtime.check_empty_database(load_dsn, "load_test")
            assert (
                await connection.fetchval("SELECT account_id FROM private.history") == "unrelated"
            )
            assert await connection.fetchval("SELECT to_regclass('public.orders')") is None
        finally:
            await connection.close()

    asyncio.run(check())


def test_exact_statistics_do_not_interpolate_or_fabricate_empty_samples():
    assert driver.distribution([]) == {"count": 0, "median_ms": None, "p99_ms": None}
    assert driver.distribution([1_000_000, 3_000_000, 2_000_000, 10_000_000]) == {
        "count": 4,
        "median_ms": 2.5,
        "p99_ms": 10.0,
    }


@pytest.mark.parametrize("lost_responses", [1, 2])
def test_retry_counts_dispatches_but_does_not_create_a_second_committed_order(
    load_dsn, lost_responses
):
    runner, _, tools = measured_tools(load_dsn)
    try:
        with serving(tools) as api, driver.CountingClient(api.url, TOKEN) as client:

            def lose_first(response):
                if client.attempts <= lost_responses:
                    response.close()
                    raise httpx.ReadTimeout("lost after commit", request=response.request)

            client._http.event_hooks["response"] = [lose_first]
            result = driver.submit(client, intent())
            assert result["attempts"] == 2
            assert result["outcome"] == ("accepted" if lost_responses == 1 else "uncertain")
            bodies = [row[2] for row in api.probe.requests]
            assert len(bodies) == 2 and bodies[0] == bodies[1]
            for body in bodies:
                no_floats(json.loads(body))
        orders, fills = asyncio.run(
            runtime.committed_orders(load_dsn, "load_synthetic", ["synthetic"])
        )
        assert len(orders) == 1 and not fills
    finally:
        runner.writer.ledger.close()


def test_real_http_refusals_are_counted_and_never_retried():
    with serving() as api, driver.CountingClient(api.url, TOKEN) as client:
        result = driver.submit(client, {**intent(), "quantity": "2"})
        assert result["outcome"] == "refused" and result["reason"] == "quantity_limit"
        assert result["attempts"] == 1
        assert len(api.probe.requests) == 1


def test_phase_real_api_fills_resolve_to_postgres_and_stream(load_dsn):
    runner, observations, tools = measured_tools(load_dsn)

    def fill_due():
        pending = [o for o in runner.writer.orders.values() if o.status.value == "pending"]
        if pending:
            runner.engines[P].advance(max(o.effective_at_ns for o in pending))

    try:
        with serving(tools, after_order=fill_due) as api:
            receiver = driver.FillReceiver(api.url, TOKEN)
            try:
                phase = driver.run_phase(
                    api.url, TOKEN, P, "0.00001001", 5, 2, 0.6, "measured", receiver
                )
                deadline = time.perf_counter() + 3
                while len(receiver.received) < len(phase["results"]):
                    assert time.perf_counter() < deadline
                    time.sleep(0.01)
                orders, fills = asyncio.run(
                    runtime.committed_orders(
                        load_dsn, "load_synthetic", [r["client_order_id"] for r in phase["results"]]
                    )
                )
                report = driver.summarise(
                    phase, orders, fills, observations.snapshot(), receiver.received, [], False
                )
                assert phase["offered"] == len(phase["results"]) + phase["client_busy"]
                assert (
                    report["committed_orders"] == report["committed_fills"] == len(phase["results"])
                )
                assert report["committed_fills_not_observed"] == 0
                assert report["tick_to_committed_fill"]["count"] == 0
                assert report["sdk_submit_to_fill_observation"]["count"] == len(phase["results"])
                for _, path, body in api.probe.requests:
                    if path == "/v1/orders":
                        no_floats(json.loads(body))
            finally:
                receiver.close()
    finally:
        runner.writer.ledger.close()


def test_only_a_causal_maker_tick_produces_tick_latency(monkeypatch):
    runner, observations, tools = measured_tools()
    engine = runner.engines[P]
    now = time.time_ns()
    order = runner.writer.submit(
        "load_synthetic",
        {**intent(), "type": "limit", "quantity": "0.1", "limit_price": "100"},
        now,
    )
    engine.submit(order)
    engine.advance(order.effective_at_ns)
    received = order.effective_at_ns + 100_000_000
    monkeypatch.setattr(runtime.time, "time_ns", lambda: received + 3_000_000)
    value = trade(P, received, "100", "0.1", True)
    runner._apply(Arrival("md.trades", 0, 0, received, P, value), late=False)
    [fill] = observations.snapshot()["fills"]
    assert fill["tick_to_fill_ns"] == 3_000_000
    assert fill["submit_to_fill_ns"] == 303_000_000
    assert runner.causal_tick is None


@pytest.mark.parametrize("kind", ["gap", "halt"])
def test_real_stream_gap_or_halt_stops_driver_instead_of_silent_continuation(kind):
    with serving(stream_tools(size=3)) as api:
        receiver = driver.FillReceiver(api.url, TOKEN)
        try:

            def provoke():
                if kind == "halt":
                    api.tools.writer.halt_account("owner")
                else:
                    for index in range(20):
                        emit_fill(api.tools.writer, fill_id=f"synthetic-{index}")

            api.call(provoke)
            deadline = time.perf_counter() + 3
            while receiver.error is None:
                assert time.perf_counter() < deadline
                time.sleep(0.01)
            assert receiver.error == ("StreamGap" if kind == "gap" else "ValueError")
            with pytest.raises(ValueError, match="no continuous result"):
                driver.run_phase(api.url, TOKEN, P, "0.00001", 1, 1, 0.1, "stopped", receiver)
            assert not api.probe.requests
        finally:
            receiver.close()


def test_summary_refuses_unproven_ack_and_resolves_uncertain_ids():
    phase = dict(
        rate=2,
        clients=1,
        seconds=1,
        offered=2,
        client_busy=0,
        started_ns=0,
        ended_ns=1_000_000_000,
        requests_drained_ns=1_000_000_000,
        results=[
            dict(
                client_order_id="accepted-but-absent",
                outcome="accepted",
                reason=None,
                attempts=1,
                started_ns=0,
                finished_ns=100,
            ),
            dict(
                client_order_id="uncertain",
                outcome="uncertain",
                reason=None,
                attempts=2,
                started_ns=0,
                finished_ns=100,
            ),
        ],
    )
    orders = [
        dict(client_order_id="uncertain", order_id="durable", status="pending", reject_reason=None)
    ]
    with pytest.raises(ValueError, match="accepted order missing"):
        driver.summarise(phase, orders, [], {"fills": [], "ledger": {}}, {}, [], False)
    phase["results"] = phase["results"][1:]
    report = driver.summarise(
        phase, orders, [], {"fills": [], "ledger": {"commit": [(1, 3_000_000)]}}, {}, [], False
    )
    assert report["durable_accepted_acks_per_second"] == 0
    assert report["committed_orders"] == 1 and report["missing_order_rows"] == 0
    assert report["http_dispatches"] == 2 and report["retries"] == 1
    assert report["http_outcomes"] == {"uncertain": 1}
    assert report["ledger_phase_latency"]["commit"]["median_ms"] == 3


@pytest.mark.parametrize("verified,lag", [(False, 0.1), (True, -0.7)])
def test_unverified_or_negative_absolute_feed_lag_is_unavailable(verified, lag):
    phase = dict(
        rate=1,
        clients=1,
        seconds=1,
        offered=0,
        client_busy=0,
        results=[],
        started_ns=0,
        ended_ns=100,
        requests_drained_ns=100,
    )
    report = driver.summarise(
        phase,
        [],
        [],
        {"fills": [], "ledger": {}},
        {},
        [(50, {"simex_ingest_last_feed_lag_seconds": [lag]})],
        verified,
    )
    value = report["metric_snapshots"]["simex_ingest_last_feed_lag_seconds"]
    assert value["minimum"] is value["maximum"] is None
    assert "unavailable" in value


def test_a_second_runtime_cannot_share_the_load_database(load_dsn):
    async def check():
        async with runtime.exclusive_destination(load_dsn, "load_first"):
            with pytest.raises(ValueError, match="already owns"):
                async with runtime.exclusive_destination(load_dsn, "load_second"):
                    pytest.fail("second writer was allowed")

    asyncio.run(check())


def test_event_only_history_is_not_mistaken_for_an_empty_database(load_dsn):
    async def check():
        connection = await asyncpg.connect(load_dsn)
        try:
            await connection.execute("CREATE TABLE ledger_events (aggregate_id text)")
            await connection.execute("INSERT INTO ledger_events VALUES ('unrelated')")
            with pytest.raises(ValueError, match="ledger history"):
                await runtime.check_empty_database(load_dsn, "load_test")
        finally:
            await connection.close()

    asyncio.run(check())


def test_drain_completions_are_not_credited_to_the_measurement_window():
    phase = dict(
        rate=1,
        clients=1,
        seconds=1,
        offered=1,
        client_busy=0,
        started_ns=0,
        ended_ns=1_000_000_000,
        requests_drained_ns=1_100_000_000,
        results=[
            dict(
                client_order_id="late",
                outcome="accepted",
                reason=None,
                attempts=1,
                started_ns=0,
                finished_ns=1_100_000_000,
            )
        ],
    )
    orders = [
        dict(client_order_id="late", order_id="committed", status="filled", reject_reason=None)
    ]
    fills = [dict(order_id="committed", fill_id="f")]
    observed = dict(
        client_order_id="late",
        order_id="committed",
        fill_id="f",
        observed_ns=1_200_000_000,
        submit_to_fill_ns=1_200_000_000,
        tick_to_fill_ns=None,
        maker_without_causal_tick=False,
    )
    observations = {
        "fills": [observed],
        "ledger": {"commit": [(500_000_000, 1_000_000), (2_000_000_000, 99_000_000)]},
    }
    result = driver.summarise(phase, orders, fills, observations, {"f": 1_300_000_000}, [], False)
    assert result["durable_accepted_acks_per_second"] == 0
    assert result["orders_with_committed_fill_per_second"] == 0
    assert result["committed_fills"] == 1
    assert result["request_drain_tail_seconds"] == 0.1
    assert result["ledger_phase_latency"]["commit"] == {"count": 1, "median_ms": 1.0, "p99_ms": 1.0}
    with pytest.raises(ValueError, match="observed fill missing"):
        driver.summarise(phase, orders, [], observations, {}, [], False)


def test_negative_intervals_are_unavailable_instead_of_fast_latencies():
    result = driver.distribution([-1, 1_000_000])
    assert result["count"] == 2 and result["median_ms"] is result["p99_ms"] is None
    assert "unavailable" in result


def test_taker_effect_during_a_tick_is_not_mislabelled_as_tick_triggered():
    runner, observations, _ = measured_tools()
    now = time.time_ns()
    order = runner.writer.submit("load_synthetic", intent(), now)
    runner.engines[P].submit(order)
    received = order.effective_at_ns
    value = level2(P, received, [("99", "10")], [("101", "10")], snapshot=True)
    runner._apply(Arrival("md.level2", 0, 1, received, P, value), late=False)
    [fill] = observations.snapshot()["fills"]
    assert fill["liquidity"] == "taker" and fill["tick_to_fill_ns"] is None


def test_clock_sync_is_never_assumed_when_status_command_is_unavailable(monkeypatch):
    def missing(*args, **kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(driver.subprocess, "run", missing)
    assert driver.clock_status()["verified"] is False


def test_verified_system_clock_status_is_recorded(monkeypatch):
    monkeypatch.setattr(
        driver.subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=0, stdout="yes\n")
    )
    assert driver.clock_status() == {"verified": True, "source": "timedatectl NTPSynchronized=yes"}


def test_shutdown_does_not_suppress_a_gap_and_before_after_metrics_stay_sanitized():
    receiver = driver.FillReceiver.__new__(driver.FillReceiver)
    receiver.stopping, receiver.error = True, None

    def gap():
        raise StreamGap({"reason": "events_evicted"})
        yield  # Iterator interface, raising on its first read.

    receiver.stream = gap()
    receiver.run()
    assert receiver.error == "StreamGap"
    for samples in (
        [(0, {"simex_ingest_last_feed_lag_seconds": [0.1]})],
        [(1, {"simex_ingest_last_feed_lag_seconds": [-0.7]})],
    ):
        result = driver.metric_ranges(samples, False)
        value = result["simex_ingest_last_feed_lag_seconds"]
        assert value["minimum"] is value["maximum"] is None
        assert "unavailable" in value
