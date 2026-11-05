"""SDK contract tests over real HTTP and sync WebSockets, never mocked responses."""

import asyncio
import copy
import json
import runpy
import socket
import threading
import time
from contextlib import closing, contextmanager
from decimal import Decimal as D
from functools import partial
from types import SimpleNamespace

import httpx
import pytest
import uvicorn
from test_api import TOKEN, api_tools, no_floats
from test_api_stream import emit_fill, stream_tools
from test_portfolio_pnl import live_tools, trade
from test_portfolio_pnl import pnl_dsn as pnl_dsn
from websockets.exceptions import ConnectionClosedOK

from simex.api import create_app
from simex.api.server import _SupervisedServer
from simex.client import APIError, Checkpoint, Client, StreamGap
from simex.ledger import SyncLedger
from simex.marketdata import BookUpdate


class WireProbe:
    def __init__(self, app, *, future_frame=False, after_order=None, close_stream=False):
        self.app = app
        self.requests = []
        self.streams = 0
        self.future_frame = future_frame
        self.after_order = after_order
        self.close_stream = close_stream

    async def __call__(self, scope, receive, send):
        if scope["type"] == "websocket":
            self.streams += 1
        request_index = None
        if scope["type"] == "http" and scope["path"] != "/v1/health":
            request_index = len(self.requests)
            self.requests.append((scope["method"], scope["path"], b""))
        body = bytearray()

        async def read():
            message = await receive()
            if message["type"] == "http.request":
                body.extend(message.get("body", b""))
                if not message.get("more_body", False) and request_index is not None:
                    self.requests[request_index] = (scope["method"], scope["path"], bytes(body))
            return message

        async def write(message):
            await send(message)
            if (
                self.close_stream
                and message["type"] == "websocket.send"
                and json.loads(message["text"])["type"] == "hello"
            ):
                await send(
                    {
                        "type": "websocket.close",
                        "code": 1000,
                        "reason": "synthetic clean server close",
                    }
                )
            if (
                self.future_frame
                and message["type"] == "websocket.send"
                and json.loads(message["text"])["type"] == "hello"
            ):
                await send(
                    {
                        "type": "websocket.send",
                        "text": '{"type":"future_notice","id":"123","fraction":0.125,"seq":999}',
                    }
                )

        await self.app(scope, read, write)
        if (
            self.after_order is not None
            and scope["type"] == "http"
            and scope["method"] == "POST"
            and scope["path"] == "/v1/orders"
        ):
            self.after_order()


@contextmanager
def serving(tools=None, *, future_frame=False, after_order=None, close_stream=False):
    tools = tools if tools is not None else api_tools()
    probe = WireProbe(
        create_app(tools, token=TOKEN, heartbeat_seconds=0.02, send_timeout_seconds=1),
        future_frame=future_frame,
        after_order=after_order,
        close_stream=close_stream,
    )
    server = _SupervisedServer(
        uvicorn.Config(
            probe,
            lifespan="off",
            access_log=False,
            log_level="warning",
            timeout_graceful_shutdown=1,
        )
    )
    loop = None
    errors = []
    ready = threading.Event()

    async def run(listener):
        nonlocal loop
        loop = asyncio.get_running_loop()
        ready.set()
        await server.serve(sockets=[listener])

    def target(listener):
        try:
            asyncio.run(run(listener))
        except BaseException as error:
            errors.append(error)

    def call(function, *args, **kwargs):
        async def invoke():
            return function(*args, **kwargs)

        return asyncio.run_coroutine_threadsafe(invoke(), loop).result(timeout=5)

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        worker = threading.Thread(target=target, args=(listener,), daemon=True)
        worker.start()
        try:
            assert ready.wait(5)
            deadline = time.perf_counter() + 5
            with httpx.Client(trust_env=False, timeout=0.2) as health:
                while True:
                    if errors:
                        raise errors[0]
                    try:
                        if health.get(url + "/v1/health").status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    assert time.perf_counter() < deadline, "test API did not start"
                    time.sleep(0.01)
            yield SimpleNamespace(url=url, tools=tools, probe=probe, call=call)
        finally:
            if loop is not None and worker.is_alive():
                loop.call_soon_threadsafe(setattr, server, "should_exit", True)
            worker.join(5)
            assert not worker.is_alive(), "test API did not stop"
            assert not errors


def place(client, **changes):
    return client.place_order(
        **{
            "client_order_id": "synthetic-client-order",
            "product_id": "BTC-USD",
            "side": "buy",
            "type": "limit",
            "quantity": D("0.12345678"),
            "limit_price": "99.99",
            "reasoning": "12345",
            **changes,
        }
    )


def receive(stream, kind):
    for _ in range(20):
        frame = next(stream)
        if frame["type"] == kind:
            return frame
        assert frame["type"] == "heartbeat", frame
    raise AssertionError(f"no {kind} after twenty heartbeats")


@pytest.mark.parametrize("field", ["quantity", "limit_price"])
@pytest.mark.parametrize("value", [0.1, 0.3 - 0.2])
def test_float_rejected_before_request(field, value):
    with serving() as api, Client(api.url, TOKEN) as client:
        with pytest.raises(TypeError) as caught:
            place(client, **{field: value})
        assert field in str(caught.value)
        assert 'Decimal("0.1") or "0.1"' in str(caught.value)
        assert not api.probe.requests


@pytest.mark.parametrize("quantity", [D("0.12345678"), "0.12345678"])
def test_eight_decimal_round_trip_and_structural_conversion(quantity):
    with serving() as api, Client(api.url, TOKEN) as client:
        result = place(client, quantity=quantity, limit_price=D("99.99"))
        assert result["order"]["quantity"] == D("0.12345678")
        assert result["order"]["reasoning"] == "12345"
        assert isinstance(result["order"]["created_at_ns"], int)
        assert result["order"]["average_price"] is None
        sent = json.loads(api.probe.requests[0][2])
        assert sent["quantity"] == "0.12345678"
        no_floats(sent)
        no_floats(result)
        assert client.quote("BTC-USD")["asks"] == [[D("101"), D("1")]]
        assert client.features("BTC-USD")["mid"] == D("100.5")
        assert client.open_orders()["orders"][0]["quantity"] == D("0.12345678")
        snapshot = client.portfolio()
        assert snapshot["balances"]["USD"] == D("1000")
        assert snapshot["pnl"]["BTC"]["mark"] == D("100.5")
        no_floats(snapshot)


@pytest.mark.parametrize(
    "failure", [httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError]
)
def test_lost_order_response_retries_same_bytes_and_creates_only_one_order(failure):
    with serving() as api, Client(api.url, TOKEN) as client:
        attempts = 0

        def lose_first(response):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                response.close()
                raise failure("synthetic lost response after acceptance", request=response.request)

        client._http.event_hooks["response"] = [lose_first]
        result = place(client)
        assert attempts == 2
        bodies = [request[2] for request in api.probe.requests]
        assert len(bodies) == 2 and bodies[0] == bodies[1]
        assert json.loads(bodies[0])["client_order_id"] == "synthetic-client-order"
        assert api.call(len, api.tools.writer.orders) == 1
        assert api.call(lambda: list(api.tools.writer.orders)) == [result["order"]["order_id"]]


def test_refusal_has_exact_reason_full_details_raw_body_and_is_not_retried():
    with serving() as api, Client(api.url, TOKEN, order_retries=3) as client:
        with pytest.raises(APIError) as caught:
            place(client, quantity="2")
        error = caught.value
        assert error.reason == "quantity_limit"
        assert error.status == 400
        assert error.body["limit"] == D("1")
        assert error.body["attempted"] == D("2")
        assert error.body["order"]["reject_details"]["attempted"] == D("2")
        assert json.loads(error.raw_body)["limit"] == "1"
        no_floats(error.body)
        assert len(api.probe.requests) == 1


def test_future_server_reason_and_unknown_fields_pass_through():
    tools = api_tools()
    tools.place_order = lambda payload: {
        "ok": False,
        "reason": "future_server_reason",
        "opaque": {"id": "123", "quantity": "not-a-monetary-field-here"},
    }
    with serving(tools) as api, Client(api.url, TOKEN, order_retries=3) as client:
        with pytest.raises(APIError) as caught:
            place(client)
        assert caught.value.reason == "future_server_reason"
        assert caught.value.status == 400
        assert caught.value.body == json.loads(caught.value.raw_body)
        assert caught.value.body["opaque"]["id"] == "123"
        assert len(api.probe.requests) == 1


@pytest.mark.parametrize("reason,status", [("account_halted", 409), ("risk_limits_missing", 503)])
def test_conflict_and_unavailable_answers_are_not_retried(reason, status):
    tools = api_tools()
    if reason == "account_halted":
        tools.writer.halt_account("owner")
    else:
        tools.writer.risk_limits = None
    with serving(tools) as api, Client(api.url, TOKEN, order_retries=3) as client:
        with pytest.raises(APIError) as caught:
            place(client)
        assert (caught.value.reason, caught.value.status) == (reason, status)
        assert len(api.probe.requests) == 1


def test_invalid_order_policy_is_left_to_the_real_server():
    with serving() as api, Client(api.url, TOKEN) as client:
        with pytest.raises(APIError) as caught:
            place(client, quantity="0.000000001")  # Off-grid; transport still sends it unchanged.
        assert caught.value.reason == "bad_order"
        assert json.loads(api.probe.requests[0][2])["quantity"] == "0.000000001"


@pytest.mark.parametrize("by_client_id", [False, True])
def test_cancel_is_requested_and_a_lost_response_is_never_retried(by_client_id):
    with serving() as api, Client(api.url, TOKEN, order_retries=3) as client:
        order = place(client)["order"]
        cancel = (
            partial(client.cancel_by_client_id, order["client_order_id"])
            if by_client_id
            else partial(client.cancel, order["order_id"])
        )
        result = cancel()
        assert result["status"] == "requested"
        assert result["order"]["status"] == "pending"
        assert result["order"]["cancel_effective_at_ns"] > result["order"]["cancel_requested_at_ns"]
        place(client, client_order_id="second")

        def lose(response):
            response.close()
            raise httpx.ReadTimeout("synthetic lost cancel response", request=response.request)

        client._http.event_hooks["response"] = [lose]
        with pytest.raises(httpx.ReadTimeout):
            client.cancel_by_client_id("second")
        assert sum(method == "DELETE" for method, _, _ in api.probe.requests) == 2


def test_exhausted_timeout_remains_uncertain_and_keeps_the_original_key():
    with serving() as api, Client(api.url, TOKEN) as client:

        def lose(response):
            response.close()
            raise httpx.ReadTimeout("synthetic response loss", request=response.request)

        client._http.event_hooks["response"] = [lose]
        with pytest.raises(httpx.ReadTimeout):
            place(client)
        assert len(api.probe.requests) == 2
        assert api.probe.requests[0][2] == api.probe.requests[1][2]
        assert api.call(len, api.tools.writer.orders) == 1


def test_first_subscription_is_live_even_after_retention_has_evicted_history():
    tools = stream_tools(2)
    for index in range(8):
        emit_fill(tools.writer, fill_id=str(index))
    with serving(tools) as api, Client(api.url, TOKEN) as client, client.stream() as stream:
        assert stream.announced_stream_id == tools.writer.stream_id
        assert stream.checkpoint.seq == 8
        assert next(stream)["type"] == "hello"
        assert next(stream)["type"] == "heartbeat"
        api.call(emit_fill, tools.writer, fill_id="new-live-fill")
        frame = receive(stream, "fill")
        assert frame["fill"]["fill_id"] == "new-live-fill"
        assert stream.checkpoint.seq == 8
        stream.ack(frame)
        assert stream.checkpoint.seq == 9
        assert not stream.checkpoint.applied_fill_ids


@pytest.mark.parametrize(
    "reason", ["events_evicted", "stream_id_required", "stream_changed", "cursor_ahead"]
)
def test_every_gap_is_visible_and_never_resets_or_reconnects(reason):
    tools = stream_tools(2)
    for index in range(4):
        emit_fill(tools.writer, fill_id=str(index))
    checkpoint = Checkpoint(tools.writer.stream_id, 1, {"already-applied"})
    if reason == "stream_id_required":
        checkpoint.stream_id = None
    elif reason == "stream_changed":
        checkpoint.stream_id = "previous-writer"
    elif reason == "cursor_ahead":
        checkpoint.seq = 5
    before = copy.deepcopy(checkpoint)
    with serving(tools) as api, Client(api.url, TOKEN) as client:
        with client.stream(checkpoint) as stream:
            assert stream.announced_stream_id == tools.writer.stream_id
            assert checkpoint == before
            assert next(stream)["type"] == "hello"
            with pytest.raises(StreamGap) as caught:
                next(stream)
            assert caught.value.reason == reason
            assert caught.value.body["type"] == "gap"
            assert checkpoint == before
            with pytest.raises(StopIteration):
                next(stream)
        assert api.probe.streams == 1


def test_replay_below_hello_watermark_batch_ack_and_restored_deduplication():
    tools = stream_tools()
    for _ in range(4):
        tools.writer._emit("order_accepted", order={})
    for name in ("five", "six", "seven"):
        emit_fill(tools.writer, fill_id=name)
    emit_fill(tools.writer, account="other", fill_id="filtered-eight")
    checkpoint = Checkpoint(tools.writer.stream_id, 4)
    with serving(tools) as api, Client(api.url, TOKEN) as client:
        with client.stream(checkpoint) as stream:
            assert next(stream)["seq"] == 8
            assert checkpoint.seq == 4
            five, six, seven = [receive(stream, "fill") for _ in range(3)]
            assert [five["seq"], six["seq"], seven["seq"]] == [5, 6, 7]
            assert checkpoint.seq == 4  # Receipt alone acknowledges nothing.
            stream.ack(seven)
            assert checkpoint.seq == 4
            assert checkpoint.applied_fill_ids == {"seven"}
            stream.ack(five)
            assert checkpoint.seq == 5
            assert checkpoint.applied_fill_ids == {"seven"}
            assert receive(stream, "heartbeat")["seq"] == 8
            assert checkpoint.seq == 5  # Neither hello nor heartbeat can jump six.
            restored = copy.deepcopy(checkpoint)
        with client.stream(restored) as resumed:
            assert next(resumed)["type"] == "hello"
            assert restored.seq == 5
            replayed = receive(resumed, "fill")
            assert replayed["fill"]["fill_id"] == "six"
            assert next(resumed)["type"] == "heartbeat"  # seven was already applied.
            assert restored.seq == 5
            assert restored.applied_fill_ids == {"seven"}
            resumed.ack(replayed)
            assert restored.seq == 8
            assert not restored.applied_fill_ids


def test_explicit_replay_zero_and_duplicate_unacknowledged_fill_id():
    tools = stream_tools()
    emit_fill(tools.writer, fill_id="same-id")
    emit_fill(tools.writer, fill_id="same-id")
    checkpoint = Checkpoint(seq=0)
    with serving(tools) as api, Client(api.url, TOKEN) as client:
        with client.stream(checkpoint) as stream:
            assert next(stream)["seq"] == 2
            assert checkpoint.seq == 0
            frame = next(stream)
            assert frame["seq"] == 1
            assert next(stream)["type"] == "heartbeat"
            assert checkpoint.seq == 0
            stream.ack(frame)
            assert checkpoint.seq == 2
            stream.ack(frame)  # Repeated acknowledgement is harmless.


def test_ordered_application_prunes_ids_over_repeated_batches():
    tools = stream_tools()
    with serving(tools) as api, Client(api.url, TOKEN) as client, client.stream() as stream:
        next(stream)
        for batch in range(20):
            for offset in range(3):
                api.call(emit_fill, tools.writer, fill_id=f"batch-{batch}-{offset}")
            frames = [receive(stream, "fill") for _ in range(3)]
            stream.ack(frames[2])
            assert stream.checkpoint.applied_fill_ids == {f"batch-{batch}-2"}
            stream.ack(frames[0])
            assert stream.checkpoint.applied_fill_ids == {f"batch-{batch}-2"}
            stream.ack(frames[1])
            assert stream.checkpoint.seq == frames[2]["seq"]
            assert not stream.checkpoint.applied_fill_ids


def test_clean_server_close_raises_in_sdk_and_preserves_checkpoint():
    with serving(close_stream=True) as api, Client(api.url, TOKEN) as client:
        with client.stream() as stream:
            next(stream)
            checkpoint = copy.deepcopy(stream.checkpoint)
            with pytest.raises(ConnectionClosedOK):
                next(stream)
            assert stream.checkpoint == checkpoint


def test_hello_bot_reports_clean_server_close_without_traceback(monkeypatch):
    with serving(close_stream=True) as api:
        monkeypatch.setenv("EXCHANGE_API_PORT", api.url.rsplit(":", 1)[1])
        monkeypatch.setenv("EXCHANGE_API_TOKEN", TOKEN)
        with pytest.raises(SystemExit, match="synthetic clean server close"):
            runpy.run_path("examples/hello_bot.py", run_name="__main__")


def test_halts_and_future_frame_type_reach_the_caller():
    tools = stream_tools()
    tools.writer.halt_account("owner")
    with serving(tools, future_frame=True) as api, Client(api.url, TOKEN) as client:
        with client.stream() as stream:
            hello = next(stream)
            assert hello["account_halted"]
            boundary = stream.checkpoint.seq
            unknown = next(stream)
            assert unknown == {
                "type": "future_notice",
                "id": "123",
                "fraction": D("0.125"),
                "seq": 999,
            }
            no_floats(unknown)
            assert stream.checkpoint.seq == boundary
            assert receive(stream, "halt")["scope"] == "account"
            api.call(setattr, tools.writer, "halted", True)
            assert receive(stream, "halt")["halted"]
            api.call(setattr, tools.writer, "halted", False)
            assert receive(stream, "halt")["halted"] is False


def test_committed_postgres_fill_decimal_stream_portfolio_and_reconcile(pnl_dsn):
    with closing(SyncLedger(pnl_dsn)) as ledger:
        ledger.migrate()
        tools = live_tools(ledger)
        with serving(tools) as api, Client(api.url, TOKEN) as client, client.stream() as stream:
            next(stream)
            fill = api.call(trade, tools, "buy", "100", quantity="0.12345678")
            frame = receive(stream, "fill")
            assert frame["fill"]["quantity"] == D("0.12345678")
            assert frame["fill"]["fee"] == fill.fee
            assert ledger.history("owner", 100)[0]["fill_id"] == frame["fill"]["fill_id"]
            before = copy.deepcopy(stream.checkpoint)
            reconciled = client.reconcile()
            assert reconciled["fills"]["fills"][0]["quantity"] == D("0.12345678")
            assert reconciled["portfolio"]["positions"]["BTC"] == D("0.12345678")
            assert reconciled["portfolio"]["pnl"]["BTC"]["total_fees_paid"] == fill.fee
            pnl = reconciled["portfolio"]["pnl"]["BTC"]
            assert pnl["average_cost_basis"] == D("100")
            assert pnl["mark"] == D("180")
            assert pnl["realised_pnl"] == D("0")
            assert pnl["unrealised_pnl"] == D("9.87654240")
            assert pnl["net_pnl"] == D("9.72839426")
            assert stream.checkpoint == before
            no_floats(frame)
            no_floats(reconciled)
            stream.ack(frame)
            assert stream.checkpoint.seq == frame["seq"]


def test_runnable_hello_bot_buys_minimum_size_and_prints_committed_fill(
    pnl_dsn, monkeypatch, capsys
):
    with closing(SyncLedger(pnl_dsn)) as ledger:
        ledger.migrate()
        tools = live_tools(ledger)
        tools.engines["BTC-USD"].book.apply(
            BookUpdate(
                "gemini",
                "BTC-USD",
                True,
                ((D("100"), D("1")),),
                ((D("100.01"), D("1")),),
                (3, 3),
                False,
                3,
                3,
            )
        )

        def settle():
            # Drive synthetic market time on the API's own loop, never from the SDK thread.
            order = next(iter(tools.writer.orders.values()))
            tools.engines["BTC-USD"].advance(order.effective_at_ns)

        with serving(tools, after_order=settle) as api:
            monkeypatch.setenv("EXCHANGE_API_PORT", api.url.rsplit(":", 1)[1])
            monkeypatch.setenv("EXCHANGE_API_TOKEN", TOKEN)
            runpy.run_path("examples/hello_bot.py", run_name="__main__")
            rows = ledger.history("owner", 100)
            assert len(rows) == 1
            assert D(rows[0]["quantity"]) == D("0.00001")
            assert D(rows[0]["fee"]) == D("0.00001201")
            sent = json.loads(
                next(body for method, _, body in api.probe.requests if method == "POST")
            )
            assert sent["type"] == "market"
            assert "limit_price" not in sent
        printed = capsys.readouterr().out
        assert "Quote:" in printed and "Accepted:" in printed
        assert "Fill (including fee):" in printed and "Portfolio:" in printed
        assert rows[0]["fill_id"] in printed
