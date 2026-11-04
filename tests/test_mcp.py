from decimal import Decimal

from simex.book.orderbook import BookState
from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    GateDecision,
    PairEngine,
)
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
    writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), gate or OpenGate(), 200_000_000)
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
        RiskLimits({"BTC-USD": Decimal("1")}, {"BTC-USD": Decimal("1000")}),
    )


def order_payload(order_id="demo-1", order_type="limit"):
    payload = {
        "client_order_id": order_id,
        "product_id": "BTC-USD",
        "side": "buy",
        "type": order_type,
        "quantity": "0.1",
    }
    if order_type == "limit":
        payload["limit_price"] = "100"
    return payload


def test_quote_serializes_book_decimals_as_strings():
    result = make_tools().quote("BTC-USD")

    assert result["ok"]
    assert result["bids"] == [["100", "1"]]
    assert result["asks"] == [["101", "1"]]


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


def test_place_order_rejects_risk_limit_and_non_string_decimal():
    tools = make_tools()

    assert tools.place_order(
        {
            "client_order_id": "too-large",
            "product_id": "BTC-USD",
            "side": "buy",
            "type": "limit",
            "quantity": "1.1",
            "limit_price": "100",
        }
    ) == {"ok": False, "reason": "quantity_limit"}
    assert tools.place_order(
        {
            "client_order_id": "float",
            "product_id": "BTC-USD",
            "side": "buy",
            "type": "limit",
            "quantity": 0.1,
            "limit_price": "100",
        }
    ) == {"ok": False, "reason": "bad_decimal_or_product"}


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
