from decimal import Decimal

from simex.book.orderbook import BookState
from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    GateDecision,
    OrderStatus,
    PairEngine,
)
from simex.exchange.fees import Liquidity
from simex.exchange.orders import FillCause
from simex.ledger import LedgerHalted
from simex.marketdata import BookUpdate
from simex.mcp import ExchangeTools, RiskLimits


class OpenGate:
    def check(self, product_id, now_ns):
        return GateDecision(True, None, Decimal("100"), Decimal("100.01"))


class ClosedGate:
    def check(self, product_id, now_ns):
        return GateDecision(False, "stale_level2")


class RaisingEngine:
    def __init__(self, writer):
        self.writer = writer
        self.book = PairEngine("BTC-USD", writer).book

    def submit(self, order):
        raise RuntimeError("engine unavailable")


def make_tools(gate=None, engine=None):
    limits = RiskLimits(
        {"BTC-USD": Decimal("1")}, {"BTC-USD": Decimal("1000")}, {"BTC-USD": Decimal("10")}
    )
    writer = AccountWriter(
        DEFAULT_INSTRUMENTS, FeeSchedule(), gate or OpenGate(), 200_000_000, risk_limits=limits
    )
    engine = engine or PairEngine("BTC-USD", writer)
    engine.book.apply(
        BookUpdate(
            "gemini",
            "BTC-USD",
            True,
            ((Decimal("100"), Decimal("1")),),
            ((Decimal("101"), Decimal("1")),),
            (1, 1),
            False,
            1,
            1,
        )
    )
    return ExchangeTools(
        "owner",
        writer,
        {"BTC-USD": engine},
        limits,
    )


def order_payload(order_id="demo-1", order_type="limit"):
    payload = {
        "client_order_id": order_id,
        "product_id": "BTC-USD",
        "side": "buy",
        "type": order_type,
        "quantity": "0.1",
        "reasoning": "test order",
    }
    if order_type == "limit":
        payload["limit_price"] = "100"
    return payload


def test_quote_serializes_book_decimals_as_strings():
    result = make_tools().quote("BTC-USD")

    assert result["ok"]
    assert result["bids"] == [["100", "1"]]
    assert result["asks"] == [["101", "1"]]


def test_features_are_instantaneous_decimal_book_values():
    result = make_tools().features("BTC-USD", depth=1)

    assert result == {
        "ok": True,
        "product_id": "BTC-USD",
        "depth": 1,
        "mid": "100.5",
        "spread": "1",
        "depth_imbalance": "0.000000",
    }


def test_place_order_delegates_to_writer_and_is_idempotent():
    tools = make_tools()
    tools.writer.deposit("owner", "USD", Decimal("1000"), "deposit:owner:usd")
    payload = order_payload()

    first = tools.place_order(payload)
    second = tools.place_order(payload)

    assert first["ok"]
    assert second["ok"]
    assert first["order"]["order_id"] == second["order"]["order_id"]
    assert len(tools.writer.orders) == 1


def test_place_order_requires_non_empty_reasoning_before_writer():
    tools = make_tools()
    calls = 0
    original_submit = tools.writer.submit

    def submit(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original_submit(*args, **kwargs)

    tools.writer.submit = submit
    for reasoning in ("", "   "):
        assert tools.place_order({**order_payload(reasoning), "reasoning": reasoning}) == {
            "ok": False,
            "reason": "reasoning_required",
            "field": "reasoning",
            "rule": "must be a non-empty string",
            "detail": "reasoning must be a non-empty string",
        }
    assert calls == 0
    assert tools.writer.orders == {}


def test_reasoning_is_returned_verbatim():
    tools = make_tools()
    tools.writer.deposit("owner", "USD", Decimal("1000"), "deposit:owner:usd")
    reasoning = "price=$100'; %s {opaque} \\n"

    result = tools.place_order({**order_payload(), "reasoning": reasoning})

    assert result["ok"]
    assert result["order"]["reasoning"] == reasoning


def test_cancel_is_requested_and_fill_before_effect_stands():
    tools = make_tools()
    tools.writer.deposit("owner", "USD", Decimal("1000"), "deposit:owner:usd")
    order = tools.writer.submit("owner", order_payload(), 1_000)
    tools.writer.activate(order.order_id, 1_000 + tools.writer.delay_ns)

    result = tools.cancel(order_id=order.order_id)

    assert result["ok"]
    assert result["status"] == "requested"
    tools.writer.record_fill(
        order.order_id,
        Decimal("100"),
        Decimal("0.05"),
        Liquidity.MAKER,
        FillCause.PRINT,
        1_001,
    )
    tools.writer.cancel_now(order.order_id, 1_000 + 2 * tools.writer.delay_ns)
    assert order.status is OrderStatus.CANCELLED
    assert order.filled_quantity == Decimal("0.05")


def test_cancel_refusals_are_structured():
    tools = make_tools()

    assert tools.cancel(order_id="missing") == {"ok": False, "reason": "unknown_order"}
    assert tools.cancel() == {"ok": False, "reason": "missing_order_reference"}

    tools.writer.deposit("owner", "USD", Decimal("1000"), "deposit:owner:usd")
    order = tools.writer.submit("owner", order_payload(), 1_000)
    tools.writer.cancel_now(order.order_id, 1_000 + tools.writer.delay_ns)
    assert tools.cancel(order_id=order.order_id) == {
        "ok": False,
        "reason": "order_terminal",
    }


def test_cancel_halt_refusals_are_structured():
    tools = make_tools()
    tools.writer.halt_account("owner")
    assert tools.cancel(order_id="missing") == {"ok": False, "reason": "account_halted"}

    tools = make_tools()
    tools.writer.halted = True
    assert tools.cancel(order_id="missing") == {"ok": False, "reason": "writer_halted"}

    tools = make_tools()
    tools.writer.deposit("owner", "USD", Decimal("1000"), "deposit:owner:usd")
    order = tools.writer.submit("owner", order_payload(), 1_000)

    def fail(_order):
        raise LedgerHalted("ledger unavailable")

    tools.writer._persist_order = fail
    assert tools.cancel(order_id=order.order_id) == {"ok": False, "reason": "ledger_halted"}


def test_open_orders_and_history_require_expected_backends():
    tools = make_tools()
    assert tools.open_orders() == {"ok": True, "orders": []}
    assert tools.history() == {"ok": False, "reason": "ledger_unavailable"}

    class HistoryLedger:
        def history(self, account_id, limit):
            assert account_id == "owner"
            assert limit == 1
            return [
                {
                    "fill_id": "order-1",
                    "price": Decimal("100"),
                    "quantity": Decimal("0.1"),
                    "reasoning": "opaque %s",
                }
            ]

    tools.writer.ledger = HistoryLedger()
    assert tools.history(1) == {
        "ok": True,
        "fills": [
            {
                "fill_id": "order-1",
                "price": "100",
                "quantity": "0.1",
                "reasoning": "opaque %s",
            }
        ],
    }


def test_place_order_rejects_risk_limit_and_non_string_decimal():
    tools = make_tools()

    refused = tools.place_order(
        {
            "client_order_id": "too-large",
            "product_id": "BTC-USD",
            "side": "buy",
            "type": "limit",
            "quantity": "1.1",
            "limit_price": "100",
            "reasoning": "test risk limit",
        }
    )
    assert not refused["ok"]
    assert refused["reason"] == "quantity_limit"
    assert refused["limit"] == "1"
    assert refused["attempted"] == "1.1"
    assert refused["order"]["status"] == "rejected"
    assert tools.place_order(
        {
            "client_order_id": "float",
            "product_id": "BTC-USD",
            "side": "buy",
            "type": "limit",
            "quantity": 0.1,
            "limit_price": "100",
            "reasoning": "test decimal type",
        }
    ) == {
        "ok": False,
        "reason": "bad_decimal_or_product",
        "field": "quantity",
        "rule": "must be a finite decimal string",
        "detail": "quantity must be a finite decimal string",
    }


def test_market_order_without_band_price_fails_closed():
    tools = make_tools(gate=ClosedGate())

    assert tools.place_order(order_payload("closed-market", "market")) == {
        "ok": False,
        "reason": "no_band_price",
    }


def test_place_order_refuses_halted_account():
    tools = make_tools()
    tools.writer.halt_account("owner")

    assert tools.place_order(order_payload("account-halted")) == {
        "ok": False,
        "reason": "account_halted",
    }


def test_place_order_refuses_halted_writer():
    tools = make_tools()
    tools.writer.halted = True

    assert tools.place_order(order_payload("writer-halted")) == {
        "ok": False,
        "reason": "writer_halted",
    }


def test_place_order_refuses_ledger_halt():
    tools = make_tools()

    def raise_ledger_halted(*args, **kwargs):
        raise LedgerHalted("ledger unavailable")

    tools.writer.submit = raise_ledger_halted

    assert tools.place_order(order_payload("ledger-halted")) == {
        "ok": False,
        "reason": "ledger_halted",
    }


def test_place_order_reports_engine_submit_failure_with_order_id():
    tools = make_tools()
    tools.engines["BTC-USD"] = RaisingEngine(tools.writer)
    tools.writer.deposit("owner", "USD", Decimal("1000"), "deposit:owner:usd")

    result = tools.place_order(order_payload("engine-failed"))

    assert result["ok"] is False
    assert result["reason"] == "engine_submit_failed"
    assert result["order_id"] in tools.writer.orders


def test_portfolio_serializes_balances_and_positions():
    tools = make_tools()
    tools.writer.deposit("owner", "USD", Decimal("1000"), "deposit:owner:usd")

    result = tools.portfolio()

    assert result["balances"] == {"USD": "1000"}
    assert result["positions"]["BTC"] == "0"
    assert all(isinstance(value, str) for value in result["balances"].values())
    assert tools.engines["BTC-USD"].book.state is BookState.SYNCED
