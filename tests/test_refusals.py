"""Actionable refusals on handwritten inputs, never recorded market data."""

import asyncio
import json
from contextlib import closing
from decimal import Decimal as D
from uuid import uuid4

import pytest
from test_mcp import make_tools, order_payload
from test_mcp_ledger import _create_schema, _drop_schema, _dsn_or_skip, _schema_dsn

from simex.client.codec import order as decoded_order
from simex.exchange import (
    FeeSchedule,
    GateDecision,
    InvalidTransition,
    OrderError,
    OrderRequest,
)
from simex.exchange.risk import RiskLimits
from simex.ledger import SyncLedger


@pytest.mark.parametrize(
    "patch,reason,detail",
    [
        ({"quantity": "0.000005"}, "below_min_size", "0.00001"),
        ({"quantity": "0.000000001"}, "bad_quantity", "1E-8"),
        ({"limit_price": "100.005"}, "bad_price", "0.01"),
        ({"quantity": "NaN"}, "bad_decimal", "quantity"),
        ({"side": "wrong"}, "bad_field", "side"),
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


@pytest.mark.parametrize(
    "patch,reason,field,rule,values",
    [
        (
            {"quantity": "0.000005"},
            "below_min_size",
            "quantity",
            "minimum",
            {"minimum": D("0.00001"), "attempted": D("0.000005")},
        ),
        (
            {"quantity": "0.000000001"},
            "bad_quantity",
            "quantity",
            "size_increment",
            {"increment": D("0.00000001"), "attempted": D("0.000000001")},
        ),
        (
            {"quantity": "-1"},
            "bad_quantity",
            "quantity",
            "positive",
            {"increment": D("0.00000001"), "attempted": D("-1")},
        ),
        (
            {"limit_price": "100.005"},
            "bad_price",
            "limit_price",
            "price_tick",
            {"increment": D("0.01"), "attempted": D("100.005")},
        ),
        (
            {"limit_price": "0"},
            "bad_price",
            "limit_price",
            "positive",
            {"increment": D("0.01"), "attempted": D("0")},
        ),
    ],
)
def test_validation_details_come_from_the_validator_and_reach_direct_writer(
    patch, reason, field, rule, values
):
    tools = make_tools()
    payload = {**order_payload(), **patch}
    result = tools.place_order(payload)
    assert result["reason"] == reason and result["field"] == field
    assert rule in result["rule"] and result["product_id"] == "BTC-USD"
    for name, expected in values.items():
        assert isinstance(result[name], str) and D(result[name]) == expected
    order = tools.writer.submit("owner", payload, 0)
    assert order.reject_reason == reason
    assert {k: v for k, v in result.items() if k not in {"ok", "reason"}} == order.reject_details
    assert tools.writer.events[-1]["order"]["reject_details"] == order.reject_details


@pytest.mark.parametrize(
    "patch,reason,field,rule",
    [
        ({"side": "hold"}, "bad_field", "side", "buy, sell"),
        ({"type": "stop"}, "bad_field", "type", "market, limit"),
        ({"type": "market"}, "bad_field", "limit_price", "absent"),
        ({"client_order_id": "bad id"}, "bad_client_order_id", "client_order_id", "1 to 64"),
        ({"quantity": "half"}, "bad_decimal", "quantity", "finite decimal string"),
        ({"quantity": "Infinity"}, "bad_decimal", "quantity", "finite decimal string"),
        ({"limit_price": "NaN"}, "bad_decimal", "limit_price", "finite decimal string"),
        ({"quantity": 0.1}, "bad_decimal_or_product", "quantity", "decimal string"),
        ({"limit_price": 100}, "bad_decimal_or_product", "limit_price", "decimal string"),
        ({"reasoning": ""}, "reasoning_required", "reasoning", "non-empty"),
        ({"reasoning": "x" * 8193}, "bad_reasoning", "reasoning", "8192"),
        ({"product_id": None}, "unknown_product", "product_id", "configured product"),
        ({"product_id": "UNKNOWN"}, "unknown_product", "product_id", "configured product"),
    ],
)
def test_field_refusals_name_the_affected_field_and_permitted_rule(patch, reason, field, rule):
    result = make_tools().place_order({**order_payload(), **patch})
    assert result["reason"] == reason and result["field"] == field
    assert rule in result["rule"] and field in result["detail"]


@pytest.mark.parametrize(
    "name", ["quantity", "limit_price", "side", "type", "client_order_id", "product_id"]
)
def test_malformed_fields_do_not_echo_arbitrary_input(name):
    secret = "synthetic-secret-token postgresql://secret-dsn other-account"
    result = make_tools().place_order({**order_payload(), name: secret})
    assert not result["ok"] and result["rule"]
    assert secret not in json.dumps(result)
    for part in ("synthetic-secret-token", "secret-dsn", "other-account"):
        assert part not in json.dumps(result)


@pytest.mark.parametrize(
    "maker,taker,required", [("60", "120", "0.00101211"), ("200", "120", "0.00102011")]
)
def test_insufficient_limit_buy_reports_exact_fee_inclusive_rounded_requirement(
    maker, taker, required
):
    tools = make_tools()
    tools.writer.fees = FeeSchedule(maker_bps=D(maker), taker_bps=D(taker))
    tools.writer.deposit("owner", "USD", D("0.00100010"), "synthetic-seed")
    payload = {**order_payload(), "quantity": "0.00001", "limit_price": "100.01"}
    result = tools.place_order(payload)
    assert result["reason"] == "insufficient_funds" and result["asset"] == "USD"
    assert D(result["required"]) == D(required) > D("0.00100010")
    assert D(result["available"]) == D("0.00100010")
    assert "fee" in result["rule"]
    assert result["order"]["reject_details"]["required"] == result["required"]
    # The original refusal is immutable even when cash and fee configuration change.
    tools.writer.deposit("owner", "USD", D("1"), "later-funding")
    tools.writer.fees = FeeSchedule(maker_bps=D("0"), taker_bps=D("0"))
    assert tools.place_order(payload) == result


def test_funding_refusal_reports_available_not_reserved_cash():
    tools = make_tools()
    tools.writer.deposit("owner", "USD", D("10.12"), "seed")
    assert tools.place_order(order_payload("reserved"))["ok"]
    result = tools.place_order(order_payload("unfunded"))
    assert result["reason"] == "insufficient_funds"
    assert D(result["required"]) == D("10.12") and D(result["available"]) == 0
    assert tools.writer.accounts.balance("owner", "USD").reserved == D("10.12")


@pytest.mark.parametrize("kind", ["limit", "market"])
def test_sell_funding_details_use_only_base_requirement(kind):
    tools = make_tools()
    result = tools.place_order({**order_payload(order_type=kind), "side": "sell"})
    assert result["reason"] == "insufficient_funds" and result["asset"] == "BTC"
    assert D(result["required"]) == D("0.1") and D(result["available"]) == 0


def assert_private(body, forbidden):
    encoded = json.dumps(body)
    for value in forbidden:
        assert value not in encoded
    if isinstance(body, dict):
        assert not set(body) & {"best_bid", "best_ask", "mid", "band_price", "market_band_price"}
        for value in body.values():
            assert_private(value, forbidden)
    elif isinstance(body, list):
        for value in body:
            assert_private(value, forbidden)


@pytest.mark.parametrize("failure", ["funds", "notional"])
@pytest.mark.parametrize("side", ["buy", "sell"])
def test_market_omissions_are_private_and_still_name_limit_and_remedy(failure, side):
    tools = make_tools()

    class PricedGate:
        def check(self, product_id, now_ns):
            return GateDecision(True, None, D("54321.12"), D("54322.34"))

    tools.writer.gate = PricedGate()
    cap = "10" if failure == "notional" else "1000000"
    tools.writer.risk_limits = RiskLimits(
        {"BTC-USD": D("1")}, {"BTC-USD": D(cap)}, {"BTC-USD": D("10")}
    )
    payload = {**order_payload(order_type="market"), "side": side}
    result = tools.place_order(payload)
    assert result["reason"] == ("notional_limit" if failure == "notional" else "insufficient_funds")
    details = result["order"]["reject_details"]
    if failure == "notional":
        assert result["limit"] == cap and "attempted" not in result and "attempted" not in details
        assert f"{cap} USD" in result["rule"] and "max_notional" in result["rule"]
    elif side == "buy":
        assert "required" not in result and "required" not in details
        assert "available USD balance 0" in result["rule"]
    else:
        assert D(result["required"]) == D("0.1")
    if failure == "notional" or side == "buy":
        assert "band-bounded notional plus fee" in result["rule"]
        assert "Reduce quantity" in result["rule"] and "limit order" in result["rule"]
        assert "notional exactly" in result["rule"]
    assert_private(
        result, ("54321.12", "54322.34", "other-account", "synthetic-secret-token", "secret-dsn")
    )
    assert tools.writer.band_prices == {}


@pytest.mark.parametrize(
    "reason,remedy",
    [
        ("stale_level2:age_ns=5000000001", "fresh level2"),
        ("feed_lagging:excess_ns=2000000001", "feed lag recovers"),
    ],
)
@pytest.mark.parametrize("on_effect", [False, True])
def test_gate_reasons_keep_the_entire_suffix_and_add_remedy(reason, remedy, on_effect):
    tools = make_tools()
    if on_effect:
        tools.writer.deposit("owner", "USD", D("100"), "seed")
        order = tools.writer.submit("owner", order_payload(), 0)
        tools.writer.reject_on_effect(order.order_id, reason, order.effective_at_ns)
    else:

        class Closed:
            def check(self, product_id, now_ns):
                return GateDecision(False, reason)

        tools.writer.gate = Closed()
    result = tools.place_order(order_payload())
    assert result["reason"] == reason
    assert remedy in result["rule"] and remedy in result["detail"]
    assert result["order"]["reject_details"]["rule"] == result["rule"]


@pytest.fixture
def refusal_dsn():
    dsn = _dsn_or_skip()
    schema = f"refusals_{uuid4().hex}"
    asyncio.run(_create_schema(dsn, schema))
    try:
        yield _schema_dsn(dsn, schema)
    finally:
        asyncio.run(_drop_schema(dsn, schema))


@pytest.mark.parametrize(
    "case", ["validation", "funds", "market_funds", "market_notional", "gate", "effect"]
)
def test_original_details_and_privacy_survive_postgres_recovery(refusal_dsn, case):
    tools = make_tools()
    payload = order_payload(order_type="market" if case.startswith("market_") else "limit")
    with closing(SyncLedger(refusal_dsn)) as ledger:
        ledger.migrate()
        tools.writer.ledger = ledger
        if case == "validation":
            payload["quantity"] = "0.000005"
        elif case == "market_notional":
            tools.writer.risk_limits = RiskLimits(
                {"BTC-USD": D("1")}, {"BTC-USD": D("1")}, {"BTC-USD": D("10")}
            )
        elif case == "gate":

            class Closed:
                def check(self, product_id, now_ns):
                    return GateDecision(False, "stale_level2:age_ns=6000000000")

            tools.writer.gate = Closed()
        elif case == "effect":
            tools.writer.deposit("owner", "USD", D("100"), "seed")
        order = tools.writer.submit("owner", payload, 0)
        if case == "effect":
            tools.writer.reject_on_effect(
                order.order_id, "feed_lagging:excess_ns=3000000000", order.effective_at_ns
            )
        original = order.to_dict()
        assert original["reject_details"]
        if case == "market_notional":
            assert "attempted" not in original["reject_details"]
        if case == "market_funds":
            assert "required" not in original["reject_details"]
    with closing(SyncLedger(refusal_dsn)) as ledger:
        ledger.migrate()
        restored = make_tools()
        restored.writer.ledger = ledger
        restored.writer.restore("owner")
        restored.writer.deposit("owner", "USD", D("10000"), "later-funding")
        restored.writer.fees = FeeSchedule(maker_bps=D("0"), taker_bps=D("0"))
        again = restored.writer.submit("owner", payload, 999)
        assert again.reject_details == original["reject_details"]
        # NUMERIC restores the same money with the database's fixed Decimal scale.
        assert decoded_order(again.to_dict()) == decoded_order(original)
        if case != "validation":
            body = restored.place_order(payload)
            assert decoded_order(body["order"]) == decoded_order(original)
            assert all(body[k] == v for k, v in original["reject_details"].items())


@pytest.mark.parametrize("legacy", [False, True])
def test_market_notional_recovery_redacts_legacy_values_and_preserves_safe_details(
    refusal_dsn, legacy
):
    payload = order_payload(order_type="market")
    with closing(SyncLedger(refusal_dsn)) as ledger:
        ledger.migrate()
        tools = make_tools()
        tools.writer.ledger = ledger
        tools.writer.risk_limits = RiskLimits(
            {"BTC-USD": D("1")}, {"BTC-USD": D("1")}, {"BTC-USD": D("10")}
        )
        order = tools.writer.submit("owner", payload, 0)
        assert order.reject_reason == "notional_limit"
        # Handwritten pre-#86 persisted shape; simulate an existing private ledger row.
        if legacy:
            order.reject_details = {
                "rule": "max_notional",
                "product_id": "BTC-USD",
                "limit": "1",
                "attempted": "5432.12345",
            }
        else:
            order.reject_details["rule"] += "; original recorded wording"
        original = order.reject_details.copy()
        ledger.save_order(order, None)
    with closing(SyncLedger(refusal_dsn)) as ledger:
        ledger.migrate()
        restored = make_tools()
        restored.writer.ledger = ledger
        restored.writer.restore("owner")
        result = restored.place_order(payload)
        assert result["reason"] == "notional_limit" and result["limit"] == "1"
        assert "attempted" not in result and "attempted" not in result["order"]["reject_details"]
        assert "1 USD" in result["rule"] and "Reduce quantity" in result["rule"]
        assert "limit order" in result["rule"]
        assert "5432.12345" not in json.dumps(result)
        if not legacy:
            assert result["order"]["reject_details"] == original


def test_invalid_effect_rejection_preserves_original_details():
    tools = make_tools()
    order = tools.writer.submit("owner", {**order_payload(), "quantity": "1.1"}, 0)
    original = order.to_dict()
    assert order.reject_reason == "quantity_limit"
    with pytest.raises(InvalidTransition):
        tools.writer.reject_on_effect(order.order_id, "stale_level2:age_ns=6000000000", 1)
    assert order.to_dict() == original
