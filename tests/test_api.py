import asyncio
import inspect
import threading
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
from fastapi.testclient import TestClient
from test_mcp import ClosedGate, make_tools, order_payload

from simex.api import create_app, validate_api_configuration
from simex.book.orderbook import BookState
from simex.exchange.risk import RiskLimits
from simex.exchange.runner_config import RunnerSettings
from simex.ledger import LedgerHalted
from simex.mcp import create_server

TOKEN = "synthetic-private-test-token"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
ROUTES = [
    ("GET", "/v1/quote/BTC-USD"),
    ("GET", "/v1/features/BTC-USD"),
    ("POST", "/v1/orders"),
    ("DELETE", "/v1/orders/missing"),
    ("DELETE", "/v1/orders/by-client-id/missing"),
    ("GET", "/v1/orders"),
    ("GET", "/v1/portfolio"),
    ("GET", "/v1/fills"),
]


def api_tools():
    tools = make_tools()
    tools.writer.deposit("owner", "USD", Decimal("1000"), "synthetic-seed")
    tools.writer.ledger = SimpleNamespace(
        save_order=lambda *args: None,
        history=lambda account, limit: [],
        portfolio_fills=lambda account, sequence: [],
    )
    return tools


def no_floats(value):
    assert not isinstance(value, float)
    if isinstance(value, dict):
        for item in value.values():
            no_floats(item)
    elif isinstance(value, list):
        for item in value:
            no_floats(item)


@pytest.mark.parametrize("method,path", ROUTES)
@pytest.mark.parametrize(
    "authorization", [None, "", "Basic wrong", "Bearer", "Bearer wrong", f"Bearer  {TOKEN}"]
)
def test_all_account_routes_require_bearer_auth(method, path, authorization, caplog):
    headers = {} if authorization is None else {"Authorization": authorization}
    with TestClient(create_app(api_tools(), token=TOKEN)) as client:
        response = client.request(method, path, headers=headers)
        assert response.status_code == 401
        assert response.json() == {"ok": False, "reason": "unauthorized"}
        assert response.headers["www-authenticate"] == "Bearer"
    assert TOKEN not in caplog.text
    assert "Bearer wrong" not in caplog.text


def test_health_is_only_liveness_and_schema_routes_are_disabled():
    tools = api_tools()
    tools.writer.halted = True
    with TestClient(create_app(tools, token=TOKEN)) as client:
        assert client.get("/v1/health").json() == {"ok": True}
        for path in ("/openapi.json", "/docs", "/redoc"):
            assert client.get(path, headers=HEADERS).status_code == 404
        assert (
            client.get("/v1/orders", headers={"Authorization": f"bearer {TOKEN}"}).json()["reason"]
            == "writer_halted"
        )
        response = client.get(
            "/v1/orders",
            headers=[("Authorization", f"Bearer {TOKEN}"), ("Authorization", f"Bearer {TOKEN}")],
        )
        assert response.status_code == 401


def test_all_handlers_are_async_and_have_no_response_model():
    app = create_app(api_tools(), token=TOKEN)
    for route in app.routes:
        assert inspect.iscoroutinefunction(route.endpoint), route.path
        assert getattr(route, "response_model", None) is None


def test_routes_return_shared_tools_dicts_and_preserve_full_money_precision():
    tools = api_tools()
    with TestClient(create_app(tools, token=TOKEN)) as client:
        for route, expected in (
            ("/v1/quote/BTC-USD?depth=1", tools.quote("BTC-USD", 1)),
            ("/v1/features/BTC-USD?depth=1", tools.features("BTC-USD", 1)),
            ("/v1/portfolio", tools.portfolio()),
            ("/v1/fills?limit=1", tools.history(1)),
        ):
            response = client.get(route, headers=HEADERS)
            assert response.status_code == 200
            assert response.json() == expected
            no_floats(response.json())
        response = client.post(
            "/v1/orders",
            headers=HEADERS,
            json={**order_payload(), "quantity": "0.12345678", "limit_price": "99.99"},
        )
        assert response.status_code == 200
        order = response.json()["order"]
        assert order["quantity"] == "0.12345678"
        assert order["limit_price"] == "99.99"
        no_floats(response.json())
        assert client.get("/v1/orders", headers=HEADERS).json() == tools.open_orders()
        # Both cancellation forms retain the delayed-request acknowledgement.
        cancel = client.delete(f"/v1/orders/{order['order_id']}", headers=HEADERS)
        assert cancel.json()["status"] == "requested"
        assert cancel.json()["order"]["status"] == "pending"
        assert (
            client.delete(
                f"/v1/orders/by-client-id/{order['client_order_id']}", headers=HEADERS
            ).json()["reason"]
            == "cancel_already_requested"
        )
        client.post("/v1/orders", headers=HEADERS, json=order_payload("second"))
        assert (
            client.delete("/v1/orders/by-client-id/second", headers=HEADERS).json()["status"]
            == "requested"
        )


def test_rest_position_limit_refusal_includes_writer_reject_details():
    tools = api_tools()
    tools.writer.risk_limits = RiskLimits(
        tools.limits.max_quantity, tools.limits.max_notional, {"BTC-USD": Decimal("0.15")}
    )
    # The first order is still pending: the authoritative writer counts its remaining exposure.
    with TestClient(create_app(tools, token=TOKEN)) as client:
        assert (
            client.post("/v1/orders", headers=HEADERS, json=order_payload("first")).status_code
            == 200
        )
        response = client.post("/v1/orders", headers=HEADERS, json=order_payload("breach"))
    assert response.status_code == 400
    body = response.json()
    assert body["ok"] is False
    assert body["reason"] == "position_limit"
    details = {
        "rule": "max_position",
        "product_id": "BTC-USD",
        "limit": "0.15",
        "attempted": "0.2",
        "minimum": "-0.15",
        "maximum": "0.15",
        "side": "buy",
    }
    assert {key: body[key] for key in details} == details
    assert body["order"]["reject_details"] == details
    assert body["order"]["status"] == "rejected"
    no_floats(body)


def test_rest_missing_writer_risk_policy_is_a_server_misconfiguration():
    tools = api_tools()
    tools.writer.risk_limits = None
    with TestClient(create_app(tools, token=TOKEN)) as client:
        response = client.post("/v1/orders", headers=HEADERS, json=order_payload())
    assert response.status_code == 503
    assert response.json() == {"ok": False, "reason": "risk_limits_missing"}
    assert not tools.writer.orders


@pytest.mark.parametrize(
    "path,reason,status",
    [
        ("/v1/quote/UNKNOWN", "unknown_product", 404),
        ("/v1/features/UNKNOWN", "unknown_product", 404),
        ("/v1/quote/BTC-USD?depth=0", "bad_depth", 400),
        ("/v1/features/BTC-USD?depth=101", "bad_depth", 400),
        ("/v1/quote/BTC-USD?depth=abc", "bad_depth", 400),
        ("/v1/features/BTC-USD?depth=1.5", "bad_depth", 400),
        ("/v1/fills?limit=0", "bad_limit", 400),
        ("/v1/fills?limit=1001", "bad_limit", 400),
        ("/v1/fills?limit=abc", "bad_limit", 400),
    ],
)
def test_read_refusals_keep_exact_reasons(path, reason, status):
    with TestClient(create_app(api_tools(), token=TOKEN)) as client:
        response = client.get(path, headers=HEADERS)
        assert response.status_code == status
        assert response.json() == {"ok": False, "reason": reason}


@pytest.mark.parametrize(
    "patch,reason",
    [
        ({"quantity": "1.1"}, "quantity_limit"),
        ({"quantity": 0.1}, "bad_decimal_or_product"),
        ({"limit_price": 100}, "bad_decimal_or_product"),
        ({"quantity": "NaN"}, "bad_decimal"),
        ({"side": "wrong"}, "bad_field"),
        ({"product_id": "UNKNOWN"}, "unknown_product"),
        ({"quantity": "0.000000001"}, "bad_quantity"),
        ({"reasoning": ""}, "reasoning_required"),
        ({"reasoning": "x" * 8193}, "bad_reasoning"),
    ],
)
def test_bad_order_refusal_is_identical_over_rest_and_mcp(patch, reason):
    async def scenario():
        tools = api_tools()
        payload = {**order_payload(), **patch}
        mcp = create_server(tools)
        mcp_result = await mcp.call_tool("place_order", payload)
        # FastMCP exposes structured dicts alongside its text content.
        expected = mcp_result[1]
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(tools, token=TOKEN)),
            base_url="http://private",
            headers=HEADERS,
        ) as client:
            response = await client.post("/v1/orders", json=payload)
        assert response.status_code == (404 if reason == "unknown_product" else 400)
        assert response.json()["reason"] == reason
        assert expected["reason"] == reason
        assert response.json() == expected

    # MCP request-shape validation rejects numeric money before the shared method is called.
    # Its typed tool is tested separately below with a valid string request shape.
    if not isinstance(patch.get("quantity", ""), str) or not isinstance(
        patch.get("limit_price", ""), str
    ):
        tools = api_tools()
        with TestClient(create_app(tools, token=TOKEN)) as client:
            assert client.post(
                "/v1/orders", headers=HEADERS, json={**order_payload(), **patch}
            ).json() == tools.place_order({**order_payload(), **patch})
    else:
        asyncio.run(scenario())


@pytest.mark.parametrize("route", ["/v1/quote/BTC-USD", "/v1/features/BTC-USD"])
def test_book_not_ready_refuses_with_503(route):
    tools = api_tools()
    tools.engines["BTC-USD"].book.state = BookState.EMPTY
    with TestClient(create_app(tools, token=TOKEN)) as client:
        response = client.get(route, headers=HEADERS)
        assert response.status_code == 503
        assert response.json()["reason"] == "book_not_ready"


def test_halts_history_failures_and_order_conflicts(monkeypatch):
    tools = api_tools()
    with TestClient(create_app(tools, token=TOKEN)) as client:
        for path in ("/v1/orders/missing", "/v1/orders/by-client-id/missing"):
            response = client.delete(path, headers=HEADERS)
            assert response.status_code == 404
            assert response.json()["reason"] == "unknown_order"
        tools.writer.halt_account("owner")
        for method, path in ROUTES:
            if "quote" in path or "features" in path or "portfolio" in path:
                continue  # Shared tools deliberately keep these read views available.
            response = client.request(method, path, headers=HEADERS, json=order_payload())
            assert response.status_code == 409
            assert response.json()["reason"] == "account_halted"
        tools.writer.lift_account_halt("owner")
        tools.writer.halted = True
        assert client.get("/v1/fills", headers=HEADERS).json()["reason"] == "writer_halted"
        tools.writer.halted = False
        tools.writer.gate = ClosedGate()
        response = client.post(
            "/v1/orders", headers=HEADERS, json=order_payload("market", "market")
        )
        assert response.status_code == 503
        assert response.json()["reason"] == "no_band_price"

        def ledger_fail(*args):
            raise LedgerHalted("synthetic failure")

        monkeypatch.setattr(tools.writer.ledger, "history", ledger_fail)
        response = client.get("/v1/fills", headers=HEADERS)
        assert response.status_code == 409
        assert response.json()["reason"] == "ledger_halted"


@pytest.mark.parametrize(
    "reason,status",
    [
        ("unknown_product", 404),
        ("unknown_order", 404),
        ("account_halted", 409),
        ("writer_halted", 409),
        ("ledger_halted", 409),
        ("quantity_limit", 400),
        ("notional_limit", 400),
        ("bad_order", 400),
        ("book_not_ready", 503),
        ("no_band_price", 503),
        ("ledger_unavailable", 503),
        ("future_refusal", 400),
        ("stale_level2:1234", 503),
    ],
)
def test_status_table_preserves_complete_reason_and_extra_fields(
    reason, status, monkeypatch, caplog
):
    tools = api_tools()
    body = {"ok": False, "reason": reason, "limit": "0.12345678", "attempted": "0.12345679"}
    monkeypatch.setattr(tools, "open_orders", lambda: body)
    with TestClient(create_app(tools, token=TOKEN)) as client:
        response = client.get("/v1/orders", headers=HEADERS)
        assert response.status_code == status
        assert response.json() == body
    if reason == "future_refusal":
        assert "future_refusal" in caplog.text


def test_transport_rejects_account_injection_and_malformed_json_without_submitting(monkeypatch):
    tools = api_tools()

    def unexpected(payload):
        raise AssertionError("transport-invalid request reached tools")

    monkeypatch.setattr(tools, "place_order", unexpected)
    with TestClient(create_app(tools, token=TOKEN)) as client:
        response = client.post(
            "/v1/orders", headers=HEADERS, json={**order_payload(), "account_id": "another-account"}
        )
        assert response.status_code == 400
        assert response.json()["reason"] == "account_id_not_allowed"
        response = client.post("/v1/orders", headers=HEADERS, content="{")
        assert response.status_code == 400
        assert response.json()["reason"] == "bad_json"


def test_concurrent_idempotent_orders_use_only_the_request_event_loop(monkeypatch):
    async def scenario():
        tools = api_tools()
        original = tools.writer.submit
        mutations = []

        def submit(*args):
            mutations.append((threading.get_ident(), asyncio.get_running_loop()))
            return original(*args)

        monkeypatch.setattr(tools.writer, "submit", submit)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(tools, token=TOKEN)),
            base_url="http://private",
            headers=HEADERS,
        ) as client:
            results = await asyncio.gather(
                *[client.post("/v1/orders", json=order_payload()) for _ in range(20)]
            )
        assert all(response.status_code == 200 for response in results)
        assert len({response.json()["order"]["order_id"] for response in results}) == 1
        assert len(tools.writer.orders) == 1
        assert mutations == [(threading.get_ident(), asyncio.get_running_loop())] * 20

    asyncio.run(scenario())


@pytest.mark.parametrize("token", [None, "", "   "])
def test_enabled_api_fails_startup_without_token(token):
    with pytest.raises(ValueError, match="EXCHANGE_API_TOKEN"):
        validate_api_configuration(RunnerSettings(api_port=8770, ledger_dsn="fake"), token)
    validate_api_configuration(RunnerSettings(), token)


def test_api_requires_ledger_and_distinct_transport_ports():
    with pytest.raises(ValueError, match="LEDGER_DSN"):
        validate_api_configuration(RunnerSettings(api_port=8770), TOKEN)
    with pytest.raises(ValueError, match="different"):
        validate_api_configuration(
            RunnerSettings(api_port=8770, mcp_port=8770, ledger_dsn="fake"), TOKEN
        )
    with pytest.raises(ValueError, match="ledger"):
        create_app(make_tools(), token=TOKEN)


@pytest.mark.parametrize(
    "name,value",
    [
        ("EXCHANGE_API_PORT", "0"),
        ("EXCHANGE_API_PORT", "65536"),
        ("EXCHANGE_EVENT_BUFFER_SIZE", "0"),
        ("EXCHANGE_EVENT_BUFFER_SIZE", "-1"),
        ("EXCHANGE_API_HEARTBEAT_SECONDS", "0"),
        ("EXCHANGE_API_HEARTBEAT_SECONDS", "nan"),
        ("EXCHANGE_API_SEND_TIMEOUT_SECONDS", "inf"),
        ("EXCHANGE_API_SEND_TIMEOUT_SECONDS", "-1"),
    ],
)
def test_invalid_api_environment_fails_clearly(name, value):
    with pytest.raises(ValueError, match=name):
        RunnerSettings.from_env({name: value})


def test_api_environment_settings():
    settings = RunnerSettings.from_env(
        {
            "EXCHANGE_API_PORT": "8770",
            "EXCHANGE_EVENT_BUFFER_SIZE": "37",
            "EXCHANGE_API_HEARTBEAT_SECONDS": "12",
            "EXCHANGE_API_SEND_TIMEOUT_SECONDS": "8",
        }
    )
    assert settings.api_port == 8770
    assert settings.event_buffer_size == 37
    assert settings.api_heartbeat_seconds == 12
    assert settings.api_send_timeout_seconds == 8
    assert RunnerSettings.from_env({}).api_port is None
