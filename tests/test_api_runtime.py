import asyncio
import json
import socket

import httpx
import pytest
import websockets
from test_api import HEADERS, TOKEN, api_tools
from test_api_stream import emit_fill
from test_exchange_runtime import FakeLedger, FakeReconciliation, FakeRunner, FakeSource, FakeStore
from test_mcp import order_payload

from simex.api import run_server_async
from simex.exchange.runner_config import RunnerSettings
from simex.exchange.runner_metrics import RunnerMetrics
from simex.exchange.runtime import LiveRuntimeGroup


def test_runtime_shares_tools_and_rebuilds_both_transports_on_kafka_failure(monkeypatch):
    import simex.exchange.runtime as runtime

    monkeypatch.setattr(runtime, "SyncLedger", FakeLedger)
    monkeypatch.setattr(runtime, "ReconciliationJob", FakeReconciliation)
    monkeypatch.setattr(FakeRunner, "calls", 0)
    first_tools = api_tools()
    sources = []
    mcp_tools, api = [], []
    started = asyncio.Event()

    def source_factory():
        source = FakeSource()
        sources.append(source)
        return source

    async def mcp_server(tools, *, port):
        mcp_tools.append(tools)
        await asyncio.Future()

    async def api_server(tools, **kwargs):
        api.append((tools, kwargs))
        started.set()
        await asyncio.Future()

    async def scenario():
        settings = RunnerSettings(
            pairs=("BTC-USD",),
            ledger_dsn="fake",
            mcp_port=8765,
            api_port=8770,
        )
        groups = []
        for index in range(2):
            started.clear()
            group = LiveRuntimeGroup(
                settings,
                RunnerMetrics(),
                source_factory,
                risk_limits=first_tools.limits,
                ledger_metrics=None,
                server_runner=mcp_server,
                api_runner=api_server,
                api_token=TOKEN,
                runner_factory=FakeRunner,
                store_factory=FakeStore,
            )
            groups.append(group)
            if index == 0:
                with pytest.raises(RuntimeError, match="source failed"):
                    await group.run()
                assert not group.tasks
            else:
                task = asyncio.create_task(group.run())
                try:
                    await asyncio.wait_for(started.wait(), 5)
                    assert api[-1][0] is mcp_tools[-1] is group.tools
                    assert group.tools_by_account["owner"] is group.tools
                    assert group.tools.writer is group.runner.writer
                    assert group.tools.writer is not groups[0].runner.writer
                    assert group.tools.writer.stream_id != groups[0].runner.writer.stream_id
                    assert api[-1][1] == {
                        "port": 8770,
                        "token": TOKEN,
                        "heartbeat_seconds": 15.0,
                        "send_timeout_seconds": 10.0,
                    }
                    assert {task.get_name() for task in group.tasks} >= {
                        "api-server",
                        "mcp-server",
                        "exchange-runner",
                        "reconciliation",
                    }
                    assert group.tools.place_order(order_payload("after-restart"))["ok"]
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                assert not group.tasks
        assert all(source.stopped for source in sources)

    asyncio.run(scenario())


def test_api_task_failure_cancels_sibling_tasks_and_stops_source(monkeypatch):
    import simex.exchange.runtime as runtime

    monkeypatch.setattr(runtime, "SyncLedger", FakeLedger)
    monkeypatch.setattr(runtime, "ReconciliationJob", FakeReconciliation)
    stopped = []

    async def idle(self):
        try:
            await asyncio.Future()
        finally:
            stopped.append("runner")

    async def mcp_server(*args, **kwargs):
        try:
            await asyncio.Future()
        finally:
            stopped.append("mcp")

    async def broken_api(*args, **kwargs):
        raise OSError("synthetic bind failure")

    monkeypatch.setattr(FakeRunner, "run", idle)
    source = FakeSource()
    group = LiveRuntimeGroup(
        RunnerSettings(ledger_dsn="fake", mcp_port=8765, api_port=8770),
        RunnerMetrics(),
        lambda: source,
        risk_limits=api_tools().limits,
        server_runner=mcp_server,
        api_runner=broken_api,
        api_token=TOKEN,
        runner_factory=FakeRunner,
        store_factory=FakeStore,
    )
    with pytest.raises(OSError, match="synthetic bind failure"):
        asyncio.run(group.run())
    assert source.stopped
    assert sorted(stopped) == ["mcp", "runner"]
    assert not group.tasks


def test_missing_api_token_fails_before_metrics_or_retry_loop(monkeypatch):
    import simex.exchange.__main__ as main

    monkeypatch.setattr(main, "load_environment", lambda log: None)
    monkeypatch.setenv("EXCHANGE_API_PORT", "8770")
    monkeypatch.setenv("EXCHANGE_LEDGER_DSN", "synthetic")
    monkeypatch.delenv("EXCHANGE_API_TOKEN", raising=False)

    def unexpected(*args, **kwargs):
        raise AssertionError("startup reached side effects without a token")

    monkeypatch.setattr(main, "start_http_server", unexpected)
    with pytest.raises(ValueError, match="EXCHANGE_API_TOKEN"):
        asyncio.run(main.main())


def test_uvicorn_serves_http_and_live_websocket_on_one_loop_and_closes_on_cancel():
    async def scenario():
        tools = api_tools()
        # The production configuration cannot use port 0; tests reserve a free local port.
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        task = asyncio.create_task(
            run_server_async(
                tools,
                port=port,
                token=TOKEN,
                heartbeat_seconds=0.02,
                send_timeout_seconds=1,
            )
        )
        try:
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port}", trust_env=False
            ) as client:
                async with asyncio.timeout(5):
                    while True:
                        if task.done():
                            task.result()
                        try:
                            response = await client.get("/v1/health")
                            if response.status_code == 200:
                                break
                        except httpx.ConnectError:
                            pass
                        await asyncio.sleep(0.01)
                assert response.json() == {"ok": True}
                async with websockets.connect(
                    f"ws://127.0.0.1:{port}/v1/stream/fills",
                    additional_headers=HEADERS,
                ) as stream:
                    assert json.loads(await stream.recv())["type"] == "hello"
                    response = await client.post(
                        "/v1/orders", headers=HEADERS, json=order_payload()
                    )
                    assert response.status_code == 200
                    emit_fill(tools.writer)
                    async with asyncio.timeout(2):
                        while (frame := json.loads(await stream.recv()))["type"] != "fill":
                            assert frame["type"] == "heartbeat"
                    assert frame["fill"]["quantity"] == "0.12345678"
                    task.cancel()
                    await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 3)
                    await asyncio.wait_for(stream.wait_closed(), 2)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert not tools.writer._journal._watchers
        # Successful immediate rebind proves the supervised socket was released.
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", port))

    asyncio.run(scenario())


def test_bind_failure_raises_to_supervisor_instead_of_exiting_process():
    async def scenario():
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            with pytest.raises(OSError):
                await run_server_async(api_tools(), port=listener.getsockname()[1], token=TOKEN)

    asyncio.run(scenario())


def test_transport_binding_is_fixed_and_has_no_proxy_or_access_logging(monkeypatch):
    import simex.api.server as server

    captured = {}

    class Listener:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def setsockopt(self, *args):
            pass

        def bind(self, address):
            captured["address"] = address

        def listen(self, backlog):
            pass

    class Server:
        started = False

        def __init__(self, config):
            captured["config"] = config

        async def serve(self, *, sockets):
            assert len(sockets) == 1

    async def scenario():
        # Patch only after asyncio has created its own socketpair.
        with monkeypatch.context() as patch:
            patch.setattr(server.socket, "socket", lambda *args: Listener())
            patch.setattr(server, "_SupervisedServer", Server)
            await server.run_server_async(api_tools(), port=8770, token=TOKEN)

    asyncio.run(scenario())
    assert captured["address"] == ("127.0.0.1", 8770)
    assert captured["config"].host == "127.0.0.1"
    assert not captured["config"].access_log
    assert not captured["config"].proxy_headers
