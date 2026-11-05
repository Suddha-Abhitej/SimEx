import asyncio
from contextlib import closing
from decimal import Decimal as D
from uuid import uuid4

import pytest
from test_mcp_ledger import _create_schema, _drop_schema, _dsn_or_skip, _schema_dsn

from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    FillCause,
    GateDecision,
    Liquidity,
    OrderStatus,
)
from simex.exchange.risk import RiskLimits
from simex.ledger import SyncLedger


class OpenGate:
    def check(self, product_id, now_ns):
        return GateDecision(True, None, D("100"), D("101"))


def payload(key, quantity="0.1", *, side="buy", product="BTC-USD", kind="limit", price="100"):
    request = {
        "client_order_id": key,
        "product_id": product,
        "side": side,
        "type": kind,
        "quantity": quantity,
        "reasoning": "synthetic risk test",
    }
    if kind == "limit":
        request["limit_price"] = price
    return request


def make_writer(*, quantity="1", notional="10000", position="1", ledger=None):
    products = ("BTC-USD", "ETH-USD")
    limits = RiskLimits(
        {p: D(quantity) for p in products},
        {p: D(notional) for p in products},
        {p: D(position) for p in products},
    )
    writer = AccountWriter(
        DEFAULT_INSTRUMENTS,
        FeeSchedule(),
        OpenGate(),
        200_000_000,
        risk_limits=limits,
        ledger=ledger,
    )
    for account in ("owner", "other"):
        writer.deposit(account, "USD", D("10000"), f"seed:{account}")
    return writer


def test_direct_writer_rejects_oversized_order_with_actionable_details():
    writer = make_writer(quantity="0.1")
    order = writer.submit("owner", payload("over", "0.2"), 0)
    assert order.status is OrderStatus.REJECTED
    assert order.reject_reason == "quantity_limit"
    assert order.reject_details == {
        "rule": "max_quantity",
        "product_id": "BTC-USD",
        "limit": "0.1",
        "attempted": "0.2",
    }
    assert order.order_id not in writer.accounts.reservations


@pytest.mark.parametrize("kind,attempted", [("limit", "20.0"), ("market", "20.302")])
def test_writer_enforces_notional_cap_using_the_reservation_price(kind, attempted):
    writer = make_writer(notional="10")
    order = writer.submit("owner", payload("notional", "0.2", kind=kind), 0)
    assert order.status is OrderStatus.REJECTED
    assert order.reject_reason == "notional_limit"
    assert D(order.reject_details["limit"]) == D("10")
    assert D(order.reject_details["attempted"]) == D(attempted)
    assert order.order_id not in writer.band_prices


@pytest.mark.parametrize("activate", [False, True])
def test_twenty_legal_orders_cannot_collectively_exceed_position_cap(activate):
    writer = make_writer(quantity="0.1")
    for index in range(20):
        order = writer.submit("owner", payload(f"order-{index}"), index)
        if index < 10:
            assert order.status is OrderStatus.PENDING
            if activate:
                writer.activate(order.order_id, order.effective_at_ns)
        else:
            assert order.status is OrderStatus.REJECTED
            assert order.reject_reason == "position_limit"
            assert D(order.reject_details["limit"]) == D("1")
            assert D(order.reject_details["attempted"]) == D("1.1")
    assert len(writer.open_orders("owner")) == 10


def test_reducing_sell_does_not_give_pending_buys_capacity():
    writer = make_writer()
    writer.deposit("owner", "BTC", D("1"), "existing-position")
    sell = writer.submit("owner", payload("reduce", side="sell"), 0)
    assert sell.status is OrderStatus.PENDING
    buy = writer.submit("owner", payload("no-offset"), 1)
    assert buy.reject_reason == "position_limit"
    assert D(buy.reject_details["attempted"]) == D("1.1")


def test_cancel_request_counts_until_the_order_becomes_terminal():
    writer = make_writer()
    first = writer.submit("owner", payload("first", "1"), 0)
    writer.request_cancel("owner", 1, order_id=first.order_id)
    refused = writer.submit("owner", payload("before-cancel"), 2)
    assert refused.reject_reason == "position_limit"
    writer.cancel_now(first.order_id, first.cancel_effective_at_ns)
    assert (
        writer.submit("owner", payload("after-cancel"), first.cancel_effective_at_ns).status
        is OrderStatus.PENDING
    )


def test_partial_fill_counts_position_and_only_the_remaining_order_quantity():
    from simex.exchange import FillCause, Liquidity

    writer = make_writer()
    first = writer.submit("owner", payload("first", "1"), 0)
    writer.activate(first.order_id, first.effective_at_ns)
    writer.record_fill(
        first.order_id, D("100"), D("0.4"), Liquidity.TAKER, FillCause.TAKER, first.effective_at_ns
    )
    assert first.status is OrderStatus.PARTIALLY_FILLED
    assert writer.submit("owner", payload("over"), 1).reject_reason == "position_limit"
    writer.cancel_now(first.order_id, first.effective_at_ns + 1)
    assert (
        writer.submit("owner", payload("remaining", "0.6"), first.effective_at_ns + 2).status
        is OrderStatus.PENDING
    )


def test_account_and_product_exposure_are_independent():
    writer = make_writer()
    assert writer.submit("owner", payload("btc", "1"), 0).status is OrderStatus.PENDING
    assert writer.submit("other", payload("btc", "1"), 0).status is OrderStatus.PENDING
    assert (
        writer.submit("owner", payload("eth", "1", product="ETH-USD"), 0).status
        is OrderStatus.PENDING
    )


def test_idempotent_retries_do_not_consume_capacity_twice_or_change_refusal():
    writer = make_writer()
    request = payload("first", "1")
    first = writer.submit("owner", request, 0)
    assert writer.submit("owner", request, 1) is first
    refused_request = payload("over")
    refused = writer.submit("owner", refused_request, 1)
    original = refused.to_dict()
    writer.cancel_now(first.order_id, first.effective_at_ns)
    assert writer.submit("owner", refused_request, first.effective_at_ns).to_dict() == original


def test_deposit_over_limit_is_not_bounded_but_further_accumulation_is():
    writer = make_writer()
    writer.deposit("owner", "BTC", D("2"), "deposit-over-cap")
    assert writer.accounts.balance("owner", "BTC").total == D("2")
    assert writer.submit("owner", payload("accumulate"), 0).reject_reason == "position_limit"
    assert writer.submit("owner", payload("reduce", side="sell"), 0).status is OrderStatus.PENDING


def test_position_policy_bounds_the_short_endpoint_and_allows_reduction():
    # Signed exposure policy is symmetric; existing spot funding rules remain unchanged.
    writer = make_writer()
    limits = writer.risk_limits
    assert (
        limits.position_refusal("BTC-USD", "sell", D("-1"), D("0"), D("0.1")).reason
        == "position_limit"
    )
    assert limits.position_refusal("BTC-USD", "buy", D("-1"), D("0"), D("0.1")) is None
    assert (
        limits.position_refusal("BTC-USD", "sell", D("0"), D("1"), D("0.1")).reason
        == "position_limit"
    )
    assert limits.position_refusal("BTC-USD", "sell", D("1"), D("0"), D("2")) is None


def test_writer_bounds_signed_exposure_without_credit_for_pending_reductions():
    writer = make_writer()
    # Synthetic signed snapshot exercises the risk rule; no borrowing/fill behavior is added.
    writer.accounts.balance("owner", "BTC").available = D("-1")
    reducing = writer.submit("owner", payload("reduce-short"), 0)
    assert reducing.status is OrderStatus.PENDING
    refused = writer.submit("owner", payload("increase-short", side="sell"), 1)
    assert refused.reject_reason == "position_limit"
    assert D(refused.reject_details["attempted"]) == D("-1.1")
    assert D(refused.reject_details["minimum"]) == D("-1")


def test_pending_market_orders_count_during_the_anti_cheat_delay():
    writer = make_writer()
    first = writer.submit("owner", payload("market", "1", kind="market"), 0)
    assert first.status is OrderStatus.PENDING
    assert first.effective_at_ns == 200_000_000
    assert writer.submit("owner", payload("over"), 1).reject_reason == "position_limit"


@pytest.mark.parametrize("terminal", ["expired", "rejected"])
def test_terminal_orders_stop_using_exposure_budget(terminal):
    writer = make_writer()
    first = writer.submit("owner", payload("first", "1", kind="market"), 0)
    if terminal == "expired":
        writer.expire(first.order_id, first.effective_at_ns)
    else:
        writer.reject_on_effect(first.order_id, "book_not_synced", first.effective_at_ns)
    assert (
        writer.submit("owner", payload("next", "1"), first.effective_at_ns + 1).status
        is OrderStatus.PENDING
    )


def test_filled_orders_count_as_position_until_a_sell_releases_capacity():
    writer = make_writer()
    buy = writer.submit("owner", payload("buy", "1"), 0)
    writer.activate(buy.order_id, buy.effective_at_ns)
    writer.record_fill(
        buy.order_id, D("100"), D("1"), Liquidity.TAKER, FillCause.TAKER, buy.effective_at_ns
    )
    assert (
        writer.submit("owner", payload("over"), buy.effective_at_ns + 1).reject_reason
        == "position_limit"
    )
    sell = writer.submit("owner", payload("sell", "1", side="sell"), buy.effective_at_ns + 2)
    writer.activate(sell.order_id, sell.effective_at_ns)
    writer.record_fill(
        sell.order_id, D("100"), D("1"), Liquidity.TAKER, FillCause.TAKER, sell.effective_at_ns
    )
    assert (
        writer.submit("owner", payload("again", "1"), sell.effective_at_ns + 1).status
        is OrderStatus.PENDING
    )


def test_risk_position_read_does_not_add_zero_assets_to_the_balance_projection():
    writer = make_writer()
    before = writer.balance_totals("owner")
    assert writer.submit("owner", payload("first"), 0).status is OrderStatus.PENDING
    assert writer.balance_totals("owner") == before


def test_disabled_product_is_refused_without_a_gate_lookup():
    writer = make_writer()

    class NoGate:
        def check(self, product_id, now_ns):
            raise AssertionError("disabled product reached gate")

    writer.gate = NoGate()
    refused = writer.submit("owner", payload("disabled", product="SOL-USD"), 0)
    assert refused.reject_reason == "product_not_allowed"


def test_mcp_cannot_weaken_writer_limits_or_recompute_idempotent_refusals():
    from simex.mcp import ExchangeTools

    writer = make_writer(quantity="0.1")
    facade_limits = RiskLimits(
        {"BTC-USD": D("100")}, {"BTC-USD": D("100000")}, {"BTC-USD": D("100")}
    )
    tools = ExchangeTools("owner", writer, {}, facade_limits)
    request = payload("over", "0.2")
    refused = tools.place_order(request)
    assert refused["reason"] == "quantity_limit"
    assert refused["limit"] == "0.1"
    assert refused["attempted"] == "0.2"
    assert refused["order"]["status"] == "rejected"
    writer.risk_limits = RiskLimits(
        {"BTC-USD": D("0.01")}, {"BTC-USD": D("10000")}, {"BTC-USD": D("1")}
    )
    assert tools.place_order(request) == refused


def test_mcp_will_not_accept_orders_on_an_unconfigured_writer():
    from test_mcp import make_tools

    tools = make_tools()
    tools.writer.risk_limits = None
    assert tools.place_order(payload("unconfigured")) == {
        "ok": False,
        "reason": "risk_limits_missing",
    }
    assert tools.writer.orders == {}


def test_market_risk_refusal_retries_keep_original_details_when_the_gate_changes():
    from test_mcp import ClosedGate

    from simex.mcp import ExchangeTools

    writer = make_writer(notional="10")
    tools = ExchangeTools("owner", writer, {}, writer.risk_limits)
    request = payload("market-retry", "0.2", kind="market")
    refusal = tools.place_order(request)
    assert refusal["reason"] == "notional_limit"
    assert D(refusal["attempted"]) == D("20.302")
    writer.gate = ClosedGate()
    assert tools.place_order(request) == refusal


def config():
    return {
        "EXCHANGE_MAX_QUANTITY_BTC_USD": "0.1",
        "EXCHANGE_MAX_NOTIONAL_BTC_USD": "1000",
        "EXCHANGE_MAX_POSITION_BTC_USD": "1",
    }


def test_complete_config_and_legacy_aliases_parse_the_same_policy():
    canonical = config()
    legacy = {
        name.replace("EXCHANGE_MAX_", "EXCHANGE_MCP_MAX_")
        if "POSITION" not in name
        else name: value
        for name, value in canonical.items()
    }
    assert RiskLimits.from_env(["BTC-USD"], canonical) == RiskLimits.from_env(["BTC-USD"], legacy)
    assert RiskLimits.from_env(["BTC-USD"], {**canonical, **legacy}) == RiskLimits.from_env(
        ["BTC-USD"], canonical
    )


@pytest.mark.parametrize("value", ["", "0", "-1", "NaN", "Infinity", "invalid"])
def test_position_cap_has_no_implicit_or_invalid_default(value):
    with pytest.raises(ValueError, match="EXCHANGE_MAX_POSITION_BTC_USD"):
        RiskLimits.from_env(["BTC-USD"], {**config(), "EXCHANGE_MAX_POSITION_BTC_USD": value})


def test_conflicting_config_aliases_fail_but_equal_decimal_values_do_not():
    with pytest.raises(ValueError, match="conflicting"):
        RiskLimits.from_env(["BTC-USD"], {**config(), "EXCHANGE_MCP_MAX_QUANTITY_BTC_USD": "0.2"})
    assert RiskLimits.from_env(
        ["BTC-USD"], {**config(), "EXCHANGE_MCP_MAX_QUANTITY_BTC_USD": "0.10"}
    ).max_quantity["BTC-USD"] == D("0.1")


@pytest.mark.parametrize("value", [1.0, D("0"), D("NaN")])
def test_policy_constructor_refuses_float_and_invalid_caps(value):
    with pytest.raises(ValueError):
        RiskLimits({"BTC-USD": D("1")}, {"BTC-USD": D("100")}, {"BTC-USD": value})


def test_policy_requires_the_same_products_and_freezes_the_supplied_maps():
    with pytest.raises(ValueError, match="same products"):
        RiskLimits({"BTC-USD": D("1")}, {"BTC-USD": D("100")}, {})
    supplied = {"BTC-USD": D("1")}
    limits = RiskLimits(supplied, supplied, supplied)
    supplied["BTC-USD"] = D("1000")
    assert limits.max_position["BTC-USD"] == D("1")
    with pytest.raises(TypeError):
        limits.max_position["BTC-USD"] = D("1000")


@pytest.fixture
def risk_dsn():
    dsn = _dsn_or_skip()
    schema = f"account_risk_{uuid4().hex}"
    asyncio.run(_create_schema(dsn, schema))
    try:
        yield _schema_dsn(dsn, schema)
    finally:
        asyncio.run(_drop_schema(dsn, schema))


@pytest.mark.parametrize("state", ["pending", "open", "partial", "cancel_requested"])
def test_exposure_and_original_refusal_details_survive_postgres_recovery(risk_dsn, state):
    with closing(SyncLedger(risk_dsn)) as ledger:
        ledger.migrate()
        writer = make_writer(ledger=ledger)
        first = writer.submit("owner", payload("first", "1"), 0)
        if state in ("open", "partial"):
            writer.activate(first.order_id, first.effective_at_ns)
        if state == "partial":
            writer.record_fill(
                first.order_id,
                D("100"),
                D("0.4"),
                Liquidity.TAKER,
                FillCause.TAKER,
                first.effective_at_ns,
            )
        if state == "cancel_requested":
            writer.request_cancel("owner", 1, order_id=first.order_id)
        refusal = writer.submit("owner", payload("over"), 2)
        assert refusal.reject_reason == "position_limit"
        details = refusal.reject_details.copy()
        policy = writer.risk_limits

    with closing(SyncLedger(risk_dsn)) as restarted:
        restarted.migrate()
        restored = AccountWriter(
            DEFAULT_INSTRUMENTS,
            FeeSchedule(),
            OpenGate(),
            200_000_000,
            ledger=restarted,
            risk_limits=policy,
        )
        restored.restore("owner")
        same = restored.submit("owner", payload("over"), 3)
        assert same.reject_details == details
        assert restored.submit("owner", payload("still-over"), 4).reject_reason == "position_limit"
        restored.cancel_now(first.order_id, first.effective_at_ns + 1)
        assert (
            restored.submit("owner", payload("over"), first.effective_at_ns + 2).reject_details
            == details
        )
        assert (
            restored.submit("owner", payload("new"), first.effective_at_ns + 3).status
            is OrderStatus.PENDING
        )
