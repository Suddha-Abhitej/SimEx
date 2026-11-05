import asyncio
from contextlib import closing
from decimal import Decimal
from functools import partial
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from test_api import HEADERS, TOKEN, api_tools, no_floats
from test_mcp import OpenGate, order_payload
from test_portfolio_pnl import live_tools, trade
from test_portfolio_pnl import pnl_dsn as pnl_dsn

from simex.api import create_app
from simex.api.stream import stream_fills
from simex.exchange import DEFAULT_INSTRUMENTS, AccountWriter, FeeSchedule, FillCause, Liquidity
from simex.exchange.tools import ExchangeTools
from simex.ledger import SyncLedger


def emit_fill(writer, account="owner", fill_id="synthetic-fill"):
    # Hand-written notification fixture, deliberately including high-precision monetary strings.
    writer._emit(
        "fill",
        fill={
            "account_id": account,
            "fill_id": fill_id,
            "price": "12345.67",
            "quantity": "0.12345678",
            "fee": "0.00000001",
        },
    )


def stream_tools(size=10000):
    tools = api_tools()
    writer = AccountWriter(
        DEFAULT_INSTRUMENTS,
        FeeSchedule(),
        OpenGate(),
        0,
        ledger=tools.writer.ledger,
        event_buffer_size=size,
    )
    return ExchangeTools("owner", writer, {}, tools.limits)


def app_for(tools):
    return create_app(tools, token=TOKEN, heartbeat_seconds=0.02, send_timeout_seconds=1)


def receive_type(socket, expected):
    # Bounded by heartbeat frames: a missing fill or halt fails instead of hanging the test.
    for _ in range(10):
        frame = socket.receive_json()
        if frame["type"] == expected:
            return frame
        assert frame["type"] == "heartbeat", frame
    raise AssertionError(f"no {expected} arrived before ten heartbeats")


@pytest.mark.parametrize("authorization", [None, "", "Basic wrong", "Bearer", "Bearer wrong"])
def test_websocket_requires_bearer_auth(authorization):
    headers = {} if authorization is None else {"Authorization": authorization}
    with TestClient(app_for(stream_tools())) as client:
        with pytest.raises(WebSocketDisconnect) as error:
            with client.websocket_connect("/v1/stream/fills", headers=headers):
                pass
        assert error.value.code == 1008
        assert error.value.reason == "unauthorized"


def test_hello_precedes_live_fill_and_heartbeat_contains_safe_cursor():
    tools = stream_tools()
    with TestClient(app_for(tools)) as client:
        with client.websocket_connect("/v1/stream/fills", headers=HEADERS) as socket:
            hello = socket.receive_json()
            assert hello["type"] == "hello"
            assert hello["seq"] == 0
            assert UUID(hex=hello["stream_id"]).version == 4
            assert not hello["writer_halted"]
            assert not hello["account_halted"]
            client.portal.call(emit_fill, tools.writer)
            fill = receive_type(socket, "fill")
            assert fill == {"type": "fill", "seq": 1, "fill": tools.writer.events[-1]["fill"]}
            no_floats(fill)
            heartbeat = receive_type(socket, "heartbeat")
            assert heartbeat["stream_id"] == hello["stream_id"]
            assert heartbeat["seq"] == 1
            assert isinstance(heartbeat["ts_ns"], int)
    assert not tools.writer._journal._watchers


def test_replay_uses_exclusive_sequence_and_filters_another_accounts_fills_and_halts():
    tools = stream_tools()
    emit_fill(tools.writer, fill_id="before")
    emit_fill(tools.writer, "other", "private-other-fill")
    tools.writer.halt_account("other")
    emit_fill(tools.writer, fill_id="after")
    url = f"/v1/stream/fills?from_seq=1&stream_id={tools.writer.stream_id}"
    with TestClient(app_for(tools)) as client:
        with client.websocket_connect(url, headers=HEADERS) as socket:
            assert socket.receive_json()["type"] == "hello"
            fill = receive_type(socket, "fill")
            assert fill["fill"]["fill_id"] == "after"
            assert fill["seq"] == 4
            client.portal.call(emit_fill, tools.writer, "other", "also-private")
            heartbeat = receive_type(socket, "heartbeat")
            assert heartbeat["seq"] == 5


@pytest.mark.parametrize("cursor", ["0", "1"])
def test_evicted_cursor_sends_explicit_gap_then_closes(cursor):
    tools = stream_tools(3)
    for index in range(8):
        emit_fill(tools.writer, fill_id=str(index))
    assert len(tools.writer.events) == 3
    with TestClient(app_for(tools)) as client:
        with client.websocket_connect(
            f"/v1/stream/fills?from_seq={cursor}&stream_id={tools.writer.stream_id}",
            headers=HEADERS,
        ) as socket:
            assert socket.receive_json()["type"] == "hello"
            gap = socket.receive_json()
            assert gap == {
                "type": "gap",
                "reason": "events_evicted",
                "stream_id": tools.writer.stream_id,
                "from_seq": int(cursor),
                "oldest_seq": 6,
                "seq": 8,
                "history": "/v1/fills",
            }
            with pytest.raises(WebSocketDisconnect) as error:
                socket.receive_json()
            assert error.value.code == 1008
            assert "GET /v1/fills" in error.value.reason


def test_eviction_of_order_lifecycle_events_also_invalidates_replay():
    tools = stream_tools(2)
    for index in range(5):
        tools.writer._emit("order_rejected", order={"client_order_id": str(index)})
    with TestClient(app_for(tools)) as client:
        with client.websocket_connect("/v1/stream/fills?from_seq=0", headers=HEADERS) as socket:
            assert socket.receive_json()["type"] == "hello"
            assert socket.receive_json()["reason"] == "events_evicted"


@pytest.mark.parametrize("identity", ["missing", "old", "matching-ahead"])
def test_resume_rejects_writer_rebuilds_missing_identity_and_future_cursors(identity):
    old = stream_tools().writer
    new = stream_tools().writer
    emit_fill(new)
    tools = ExchangeTools("owner", new, {}, stream_tools().limits)
    query = "from_seq=1"
    expected = "stream_changed"
    if identity == "old":
        query += f"&stream_id={old.stream_id}"
    elif identity == "matching-ahead":
        query = f"from_seq=2&stream_id={new.stream_id}"
        expected = "cursor_ahead"
    with TestClient(app_for(tools)) as client:
        with client.websocket_connect(f"/v1/stream/fills?{query}", headers=HEADERS) as socket:
            assert socket.receive_json()["stream_id"] == new.stream_id
            assert socket.receive_json()["reason"] == expected


@pytest.mark.parametrize("cursor", ["-1", "1.5", "bad", "", "+1", "١", "9" * 4400])
def test_bad_stream_cursor_is_an_explicit_input_error(cursor):
    with TestClient(app_for(stream_tools())) as client:
        with client.websocket_connect(
            f"/v1/stream/fills?from_seq={cursor}", headers=HEADERS
        ) as socket:
            assert socket.receive_json()["type"] == "hello"
            assert socket.receive_json() == {"type": "error", "reason": "bad_from_seq"}


def test_replay_from_zero_and_live_only_join_have_distinct_behavior():
    tools = stream_tools()
    emit_fill(tools.writer)
    with TestClient(app_for(tools)) as client:
        with client.websocket_connect("/v1/stream/fills?from_seq=0", headers=HEADERS) as socket:
            assert socket.receive_json()["type"] == "hello"
            assert receive_type(socket, "fill")["seq"] == 1
        with client.websocket_connect("/v1/stream/fills", headers=HEADERS) as socket:
            assert socket.receive_json()["type"] == "hello"
            assert socket.receive_json()["type"] == "heartbeat"


def test_account_and_writer_halt_and_recovery_are_pushed_without_an_order():
    tools = stream_tools()
    with TestClient(app_for(tools)) as client:
        with client.websocket_connect("/v1/stream/fills", headers=HEADERS) as socket:
            socket.receive_json()
            for scope, halted, action in (
                ("account", True, partial(tools.writer.halt_account, "owner")),
                ("account", False, partial(tools.writer.lift_account_halt, "owner")),
                ("writer", True, partial(setattr, tools.writer, "halted", True)),
                ("writer", False, partial(setattr, tools.writer, "halted", False)),
            ):
                client.portal.call(action)
                frame = receive_type(socket, "halt")
                assert frame["scope"] == scope
                assert frame["halted"] == halted


def test_connect_to_already_halted_account_is_explicit_and_worker_halts_wake_stream():
    tools = stream_tools()
    tools.writer.halt_account("owner")
    with TestClient(app_for(tools)) as client:
        with client.websocket_connect("/v1/stream/fills", headers=HEADERS) as socket:
            assert socket.receive_json()["account_halted"]
            assert socket.receive_json()["halted"]
            # Reconciliation currently invokes this method from its existing worker thread.
            tools.writer.lift_account_halt("owner")
            assert receive_type(socket, "halt")["halted"] is False


def test_slow_socket_closes_with_resume_instructions_and_unregisters_watcher():
    class SlowSocket:
        def __init__(self):
            self.closed = None

        async def accept(self):
            pass

        async def send_json(self, frame):
            await asyncio.Future()

        async def close(self, *, code, reason):
            self.closed = (code, reason)

    async def scenario():
        tools = stream_tools()
        socket = SlowSocket()
        await asyncio.wait_for(
            stream_fills(
                socket,
                tools,
                from_seq=None,
                stream_id=None,
                heartbeat_seconds=0.02,
                send_timeout_seconds=0.01,
            ),
            timeout=1,
        )
        assert socket.closed[0] == 1013
        assert "from_seq" in socket.closed[1]
        assert not tools.writer._journal._watchers

    asyncio.run(scenario())


def test_live_client_that_falls_behind_gets_gap_instead_of_silent_skip():
    class Socket:
        def __init__(self, writer):
            self.writer = writer
            self.frames = []

        async def accept(self):
            pass

        async def receive(self):
            await asyncio.Future()

        async def send_json(self, frame):
            self.frames.append(frame)
            if frame["type"] == "hello":
                for index in range(5):
                    emit_fill(self.writer, fill_id=str(index))

        async def close(self, *, code, reason):
            assert code == 1008

    async def scenario():
        tools = stream_tools(2)
        socket = Socket(tools.writer)
        await asyncio.wait_for(
            stream_fills(
                socket,
                tools,
                from_seq=None,
                stream_id=None,
                heartbeat_seconds=0.02,
                send_timeout_seconds=1,
            ),
            timeout=1,
        )
        assert [frame["type"] for frame in socket.frames] == ["hello", "gap"]
        assert socket.frames[1]["reason"] == "events_evicted"

    asyncio.run(scenario())


def test_postgres_fill_is_committed_before_stream_delivery_and_portfolio_matches(pnl_dsn):
    with closing(SyncLedger(pnl_dsn)) as ledger:
        ledger.migrate()
        tools = live_tools(ledger)
        with TestClient(app_for(tools)) as client:
            with client.websocket_connect("/v1/stream/fills", headers=HEADERS) as socket:
                socket.receive_json()
                fill = client.portal.call(
                    partial(trade, tools, "buy", "100", quantity="0.12345678")
                )
                frame = receive_type(socket, "fill")
                assert frame["fill"] == fill.to_dict()
                no_floats(frame)
                assert ledger.history("owner", 100)[0]["fill_id"] == fill.fill_id
                response = client.get("/v1/portfolio", headers=HEADERS)
                assert response.json() == client.portal.call(tools.portfolio)
                assert Decimal(response.json()["pnl"]["BTC"]["average_cost_basis"]) == Decimal(
                    "100"
                )


def test_failed_commit_cannot_appear_as_a_fill_on_live_stream(monkeypatch):
    tools = api_tools()
    order = tools.writer.submit("owner", order_payload(), 1)
    tools.writer.activate(order.order_id, order.effective_at_ns)

    def failure(*args):
        raise RuntimeError("synthetic failed commit")

    monkeypatch.setattr(tools.writer.ledger, "record_fill", failure, raising=False)
    with TestClient(app_for(tools)) as client:
        with client.websocket_connect("/v1/stream/fills", headers=HEADERS) as socket:
            socket.receive_json()
            with pytest.raises(RuntimeError, match="failed commit"):
                client.portal.call(
                    partial(
                        tools.writer.record_fill,
                        order.order_id,
                        Decimal("100"),
                        Decimal("0.1"),
                        Liquidity.TAKER,
                        FillCause.TAKER,
                        order.effective_at_ns,
                    )
                )
            assert receive_type(socket, "halt")["halted"]
            assert socket.receive_json()["type"] == "heartbeat"
            assert not [event for event in tools.writer.events if event["type"] == "fill"]
