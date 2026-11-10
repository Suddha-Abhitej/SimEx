"""Time-in-force decisions on handwritten books; no venue recordings."""

import asyncio
import json
from decimal import Decimal as D

import httpx
import pytest
from test_api import HEADERS, TOKEN, api_tools, no_floats
from test_client import serving
from test_exchange_engine import DELAY, T0, P, fills, started, trade
from test_order_metrics import assert_identity, count

from simex.api import create_app
from simex.client import APIError, Client
from simex.exchange import OrderError, OrderStatus
from simex.exchange.order_metrics import OrderMetrics
from simex.mcp import create_server


def payload(tif="GTC", *, key="tif", side="buy", quantity="4", price="100.02", kind="limit"):
    result = dict(
        client_order_id=key,
        product_id=P,
        side=side,
        type=kind,
        quantity=quantity,
        time_in_force=tif,
        reasoning="Synthetic TIF decision",
    )
    if kind == "limit":
        result["limit_price"] = price
    return result


def submit(engine, writer, request, now=T0):
    order = writer.submit("alice", request, now)
    engine.submit(order)
    return order


@pytest.mark.parametrize("kind", ["limit", "market"])
def test_default_gtc_preserves_explicit_gtc_execution_and_events(kind):
    histories = []
    for explicit in (False, True):
        engine, writer, _ = started()
        request = payload(kind=kind, quantity="10")
        if not explicit:
            request.pop("time_in_force")
        order = submit(engine, writer, request)
        engine.advance(T0 + DELAY)
        histories.append((order.to_dict(), tuple(writer.events), writer.balance_totals("alice")))
        assert order.status is (
            OrderStatus.PARTIALLY_FILLED if kind == "limit" else OrderStatus.EXPIRED
        )
    assert histories[0] == histories[1]


@pytest.mark.parametrize(
    "side,quantity,price,filled,usd,btc",
    [
        ("buy", "4", "100.02", "3", "999696.3494", "103"),
        ("sell", "7", "99.99", "5", "1000493.97036", "95"),
    ],
)
def test_ioc_partial_execution_cancels_remainder_and_releases_exactly(
    side, quantity, price, filled, usd, btc
):
    engine, writer, _ = started()
    order = submit(engine, writer, payload("IOC", side=side, quantity=quantity, price=price))
    engine.advance(T0 + DELAY - 1)
    assert order.status is OrderStatus.PENDING and not fills(writer)
    engine.advance(T0 + DELAY)
    assert order.status is OrderStatus.CANCELLED
    assert order.filled_quantity == D(filled)
    assert writer.accounts.balance("alice", "USD").available == D(usd)
    assert writer.accounts.balance("alice", "BTC").available == D(btc)
    assert not writer.accounts.reservations and not engine.resting and not engine.pending
    assert writer.accounts.balance("alice", "USD").reserved == 0
    assert writer.accounts.balance("alice", "BTC").reserved == 0
    engine.on_event(trade(T0 + DELAY + 1, price, "10", "sell" if side == "buy" else "buy"))
    assert order.filled_quantity == D(filled)  # No delayed maker execution.


@pytest.mark.parametrize("side,price", [("buy", "99"), ("sell", "101")])
def test_ioc_no_liquidity_is_cancelled_with_no_fee_or_reservation(side, price):
    engine, writer, _ = started()
    before = writer.balance_totals("alice")
    order = submit(engine, writer, payload("IOC", side=side, price=price))
    engine.advance(T0 + DELAY)
    assert order.status is OrderStatus.CANCELLED and order.fees == 0
    assert not fills(writer) and not engine.resting and not writer.accounts.reservations
    assert writer.balance_totals("alice") == before


@pytest.mark.parametrize("side,price", [("buy", "100.02"), ("sell", "99.99")])
def test_fok_executes_the_entire_multilevel_plan(side, price):
    engine, writer, _ = started()
    order = submit(engine, writer, payload("FOK", side=side, quantity="3", price=price))
    engine.advance(T0 + DELAY)
    assert order.status is OrderStatus.FILLED and order.filled_quantity == D("3")
    assert len(fills(writer)) == 2
    assert not engine.resting and not writer.accounts.reservations
    assert all(cause == "taker" for _, _, cause in fills(writer))


@pytest.mark.parametrize("side,quantity,price", [("buy", "4", "100.02"), ("sell", "6", "99.99")])
def test_short_fok_plan_has_no_fills_no_depletion_and_exact_release(side, quantity, price):
    engine, writer, _ = started()
    observer = OrderMetrics(writer.instruments)
    writer._order_observer = observer
    before = writer.balance_totals("alice")
    request = payload("FOK", side=side, quantity=quantity, price=price)
    order = submit(engine, writer, request)
    engine.advance(T0 + DELAY)
    assert order.status is OrderStatus.REJECTED and order.reject_reason == "fok_not_fillable"
    assert order.filled_quantity == order.fees == 0
    assert not fills(writer) and not engine.depletion and not engine.resting
    assert not writer.accounts.reservations and writer.balance_totals("alice") == before
    assert "reduce quantity" in order.reject_details["rule"]
    assert not any(
        word in json.dumps(order.reject_details)
        for word in ("100.01", "best_bid", "best_ask", "available")
    )
    assert count(observer, "accepted") == count(observer, "submitted") == 1
    assert count(observer, "effect_rejected", reason="fok_not_fillable") == 1
    assert count(observer, "arrival_rejected", reason="fok_not_fillable") == 0
    assert writer.submit("alice", request, T0 + DELAY + 1) is order
    assert count(observer, "replayed") == 1
    assert_identity(observer)


def test_fok_counts_depletion():
    engine, writer, _ = started()
    prior = submit(engine, writer, payload(key="prior", quantity="2"))
    engine.advance(T0 + DELAY)
    assert prior.filled_quantity == 2
    fok = submit(engine, writer, payload("FOK", key="fok", quantity="2"), T0 + 1)
    engine.advance(T0 + DELAY + 1)
    assert fok.status is OrderStatus.REJECTED
    assert fok.fill_count == 0  # Raw depth was 3, remaining depth was only 1.


@pytest.mark.parametrize("tif", ["IOC", "FOK"])
def test_gate_at_effect_precedes_any_immediate_execution(tif):
    engine, writer, gate = started()
    order = submit(engine, writer, payload(tif, quantity="1"))
    gate.open = False
    engine.advance(T0 + DELAY)
    assert order.reject_reason == "stale" and order.status is OrderStatus.REJECTED
    assert not fills(writer) and not writer.accounts.reservations


@pytest.mark.parametrize("tif", [None, "", "gtc", "DAY", 1, []])
def test_invalid_tif_is_a_structured_bad_field(tif):
    engine, writer, _ = started()
    with pytest.raises(OrderError) as error:
        writer.submit("alice", payload(tif), T0)
    assert error.value.reason == "bad_field"
    assert error.value.details["field"] == "time_in_force"
    assert error.value.details["rule"] == "must be one of: GTC, IOC, FOK"
    assert error.value.details["detail"]
    assert not writer.orders and not writer.accounts.reservations


@pytest.mark.parametrize("tif", ["GTC", "IOC", "FOK", "DAY"])
def test_rest_mcp_and_sdk_share_tif_policy_and_precision(tif):
    tools = api_tools()
    tools.clock_ns = lambda: T0
    request = payload(tif, quantity="0.12345678", price="99.99")

    async def transports():
        mcp = (await create_server(tools).call_tool("place_order", request))[1]
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(tools, token=TOKEN)),
            base_url="http://private",
            headers=HEADERS,
        ) as client:
            rest = await client.post("/v1/orders", json=request)
        assert rest.json() == mcp
        assert rest.status_code == (400 if tif == "DAY" else 200)
        return mcp

    result = asyncio.run(transports())
    with serving(tools) as server, Client(server.url, TOKEN) as client:
        if tif == "DAY":
            with pytest.raises(APIError) as error:
                client.place_order(**request)
            assert error.value.body == result
        else:
            response = client.place_order(**request)
            assert response["order"]["time_in_force"] == tif
            assert response["order"]["quantity"] == D("0.12345678")
            no_floats(response)
        no_floats(json.loads(server.probe.requests[-1][2]))


def test_changed_tif_reuses_no_admission_or_reservation():
    engine, writer, _ = started()
    submit(engine, writer, payload())
    reserved = writer.accounts.balance("alice", "USD").reserved
    with pytest.raises(OrderError, match="idempotency_key_reused"):
        writer.submit("alice", payload("IOC"), T0 + 1)
    assert writer.accounts.balance("alice", "USD").reserved == reserved
    assert len(writer.orders) == 1


@pytest.mark.parametrize(
    "tif,status,quantity", [("IOC", OrderStatus.CANCELLED, "7"), ("FOK", OrderStatus.REJECTED, "0")]
)
def test_market_immediate_policies_obey_the_band(tif, status, quantity):
    engine, writer, _ = started()
    order = submit(engine, writer, payload(tif, kind="market", quantity="10"))
    engine.advance(T0 + DELAY)
    assert order.status is status and order.filled_quantity == D(quantity)
    assert not writer.accounts.reservations and not engine.resting


def test_execution_applies_the_exact_plan_without_a_second_walk(monkeypatch):
    engine, writer, _ = started()
    order = submit(engine, writer, payload("IOC"))
    real = writer.record_execution

    def finalize(order_id, plan, now):
        def forbidden(*args):
            raise AssertionError("execution walked the book again")

        monkeypatch.setattr(engine, "_available", forbidden)
        return real(order_id, plan, now)

    monkeypatch.setattr(writer, "record_execution", finalize)
    engine.advance(T0 + DELAY)
    assert order.filled_quantity == 3 and order.status is OrderStatus.CANCELLED


def test_sdk_polling_reports_an_effect_refusal_after_post_accepted():
    tools = api_tools()
    tools.clock_ns = lambda: T0
    request = payload("FOK", quantity="0.12345678", price="99.99")
    with serving(tools) as server, Client(server.url, TOKEN) as client:
        accepted = client.place_order(**request)
        assert accepted["order"]["status"] == "pending"
        server.call(tools.engines[P].advance, T0 + DELAY)
        with pytest.raises(APIError) as refused:
            client.place_order(**request)  # Poll by replaying the unchanged idempotent request.
        assert refused.value.status == 400 and refused.value.reason == "fok_not_fillable"
        assert refused.value.body["order"]["status"] == "rejected"
        assert refused.value.body["rule"] == refused.value.body["order"]["reject_details"]["rule"]
        assert server.call(lambda: list(tools.writer.events))[-1]["type"] == "order_rejected"


@pytest.mark.parametrize("tif", ["IOC", "FOK"])
def test_sdk_transport_retry_preserves_tif_body_and_idempotency_key(tif):
    with serving() as server, Client(server.url, TOKEN) as client:
        attempts = 0

        def lose(response):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                response.close()
                raise httpx.ReadTimeout("synthetic lost acceptance", request=response.request)

        client._http.event_hooks["response"] = [lose]
        request = payload(tif, quantity="0.12345678", price="99.99")
        response = client.place_order(**request)
        assert response["order"]["time_in_force"] == tif
        bodies = [body for method, path, body in server.probe.requests if method == "POST"]
        assert len(bodies) == 2 and bodies[0] == bodies[1]
        assert json.loads(bodies[0])["client_order_id"] == "tif"
        assert json.loads(bodies[0])["time_in_force"] == tif
        assert server.call(lambda: len(server.tools.writer.orders)) == 1
