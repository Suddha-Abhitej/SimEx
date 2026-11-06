"""Actionable refusals on handwritten inputs, never recorded market data."""

import pytest
from test_mcp import make_tools, order_payload

from simex.exchange import OrderError, OrderRequest


@pytest.mark.parametrize(
    "patch,reason,detail",
    [
        ({"quantity": "0.000005"}, "below_min_size", "0.00001"),
        ({"quantity": "0.000000001"}, "bad_quantity", "1E-8"),
        ({"limit_price": "100.005"}, "bad_price", "0.01"),
        ({"quantity": "NaN"}, "bad_decimal", "quantity"),
        ({"side": "wrong"}, "bad_field", "Side"),
        ({"client_order_id": "invalid id"}, "bad_client_order_id", "A-Z a-z 0-9 . _ : -"),
        ({"product_id": "UNKNOWN"}, "unknown_product", "product_id"),
    ],
)
def test_validation_preserves_specific_reason_and_detail(patch, reason, detail):
    tools = make_tools()
    result = tools.place_order({**order_payload(), **patch})
    assert result["ok"] is False
    assert result["reason"] == reason
    assert detail in result["detail"]
    assert tools.writer.orders == {}  # Validation still precedes acceptance/persistence.


@pytest.mark.parametrize("payload", [None, [], "not a mapping", 3])
def test_non_mapping_keeps_bad_payload(payload):
    assert make_tools().place_order(payload) == {"ok": False, "reason": "bad_payload"}


@pytest.mark.parametrize("error", [KeyError, TypeError, ValueError])
def test_reasonless_parser_exception_keeps_bad_order_fallback(monkeypatch, error):
    def broken_parser(payload):
        raise error("synthetic internals must not escape")

    monkeypatch.setattr(OrderRequest, "parse", broken_parser)
    assert make_tools().place_order(order_payload()) == {"ok": False, "reason": "bad_order"}


def test_writer_order_error_preserves_detail(monkeypatch):
    tools = make_tools()

    def refuse(*args):
        raise OrderError("idempotency_key_reused", "Use a different client_order_id")

    monkeypatch.setattr(tools.writer, "submit", refuse)
    result = tools.place_order(order_payload())
    assert result == {
        "ok": False,
        "reason": "idempotency_key_reused",
        "detail": "Use a different client_order_id",
    }
