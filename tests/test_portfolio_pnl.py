from contextlib import closing
from decimal import Decimal as D
from types import SimpleNamespace
from uuid import uuid4

import pytest
from test_mcp import ClosedGate, make_tools
from test_mcp_ledger import _create_schema, _drop_schema, _dsn_or_skip, _schema_dsn

from simex.book.orderbook import BookState
from simex.evaluator import FillRecord, PriceMark, PriceSeries, evaluate
from simex.exchange import FillCause, Liquidity, PairEngine
from simex.ledger import SyncLedger
from simex.marketdata import BookUpdate

NS = 1_000_000_000


def fill(side, price, quantity="1", fee="1", product="BTC-USD"):
    return {
        "product_id": product,
        "side": side,
        "price": D(price),
        "quantity": D(quantity),
        "fee": D(fee),
        "fee_asset": "USD",
    }


BUYS = [fill("buy", "100"), fill("buy", "200", fee="2")]
PARTIAL = [*BUYS, fill("sell", "180", fee="1.8")]
CLOSED = [*PARTIAL, fill("sell", "160", fee="1.6")]
REOPENED = [*CLOSED, fill("buy", "120", fee="1.2")]


def set_mark(tools, price, product="BTC-USD"):
    book = tools.engines[product].book
    book.apply(
        BookUpdate(
            "gemini",
            product,
            True,
            ((D(price) - 1, D("10")),),
            ((D(price) + 1, D("10")),),
            (2, 2),
            False,
            2,
            2,
        )
    )


def portfolio_tools(fills, quantity):
    tools = make_tools()
    tools.writer.accounts.balance("owner", "BTC").available = D(quantity)
    tools.writer.applied_sequences["owner"] = 42

    def read(account_id, through_sequence):
        assert account_id == "owner"
        assert through_sequence == 42
        return fills

    tools.writer.ledger = SimpleNamespace(portfolio_fills=read)
    set_mark(tools, "180")
    return tools


@pytest.mark.parametrize(
    "fills,quantity,basis,realised,unrealised,fees,net",
    [
        (BUYS[:1], "1", "100", "0", "80", "1", "79"),
        (BUYS, "2", "150", "0", "60", "3", "57"),
        (PARTIAL, "1", "150", "30", "30", "4.8", "55.2"),
        (CLOSED, "0", None, "40", "0", "6.4", "33.6"),
        (REOPENED, "1", "120", "40", "60", "7.6", "92.4"),
    ],
)
def test_hand_computed_weighted_average_pnl(
    fills, quantity, basis, realised, unrealised, fees, net
):
    result = portfolio_tools(fills, quantity).portfolio()
    assert result["cost_basis_method"] == "weighted_average"
    assert result["ledger_sequence"] == 42
    pnl = result["pnl"]["BTC"]
    assert pnl["quantity"] == quantity
    assert pnl["quote_asset"] == "USD"
    assert pnl["mark"] == "180"
    assert pnl["average_cost_basis"] == basis
    for key, expected in (
        ("realised_pnl", realised),
        ("unrealised_pnl", unrealised),
        ("total_fees_paid", fees),
        ("net_pnl", net),
    ):
        assert isinstance(pnl[key], str)
        assert D(pnl[key]) == D(expected)
    assert pnl["reason"] is None


@pytest.mark.parametrize(
    "state", ["empty", "resyncing", "one_sided", "missing", "coinbase", "stale"]
)
def test_unavailable_book_mark_never_falls_back_to_a_fill_price(state):
    tools = portfolio_tools(REOPENED, "1")
    book = tools.engines["BTC-USD"].book
    if state == "empty":
        book.state = BookState.EMPTY
    elif state == "resyncing":
        book.state = BookState.RESYNCING
    elif state == "one_sided":
        book.asks.clear()
    elif state == "missing":
        tools.engines.clear()
    elif state == "coinbase":
        book.venue = "coinbase"
    else:
        tools.writer.gate = ClosedGate()

    pnl = tools.portfolio()["pnl"]["BTC"]
    assert pnl["mark"] is None
    assert pnl["unrealised_pnl"] is None
    assert pnl["net_pnl"] is None
    assert D(pnl["average_cost_basis"]) == D("120")
    assert D(pnl["realised_pnl"]) == D("40")
    assert D(pnl["total_fees_paid"]) == D("7.6")
    assert pnl["reason"] == "mark_unavailable"


@pytest.mark.parametrize(
    "fills,quantity,fees", [([], "1", "0"), (BUYS[:1], "2", "1"), ([fill("sell", "100")], "0", "1")]
)
def test_inventory_without_fill_established_basis_is_unavailable(fills, quantity, fees):
    pnl = portfolio_tools(fills, quantity).portfolio()["pnl"]["BTC"]
    for key in ("average_cost_basis", "realised_pnl", "unrealised_pnl", "net_pnl"):
        assert pnl[key] is None
    assert D(pnl["total_fees_paid"]) == D(fees)
    assert pnl["reason"] == "unknown_cost_basis"


@pytest.mark.parametrize("unavailable", ["absent", "error"])
def test_unavailable_ledger_keeps_portfolio_but_does_not_invent_pnl(unavailable):
    tools = make_tools()
    tools.writer.deposit("owner", "USD", D("1000"), "seed")
    if unavailable == "error":

        def read(account_id, through_sequence):
            raise RuntimeError("ledger unavailable")

        tools.writer.ledger = SimpleNamespace(portfolio_fills=read)
    result = tools.portfolio()
    assert result["ok"]
    assert result["balances"] == {"USD": "1000"}
    assert result["positions"]["BTC"] == "0"
    assert result["open_orders"] == []
    pnl = result["pnl"]["BTC"]
    for key in (
        "average_cost_basis",
        "realised_pnl",
        "unrealised_pnl",
        "total_fees_paid",
        "net_pnl",
    ):
        assert pnl[key] is None
    assert pnl["reason"] == "ledger_unavailable"


def test_float_in_pnl_inputs_is_rejected():
    tools = portfolio_tools([{**BUYS[0], "price": 100.0}], "1")
    with pytest.raises(TypeError, match="Decimal"):
        tools.portfolio()


def test_pnl_keeps_each_assets_cost_and_fees_separate():
    tools = portfolio_tools([BUYS[0], fill("buy", "200", "2", "4", product="ETH-USD")], "1")
    tools.writer.accounts.balance("owner", "ETH").available = D("2")
    tools.engines["ETH-USD"] = PairEngine("ETH-USD", tools.writer)
    set_mark(tools, "300", "ETH-USD")

    pnl = tools.portfolio()["pnl"]
    assert D(pnl["BTC"]["average_cost_basis"]) == D("100")
    assert D(pnl["BTC"]["total_fees_paid"]) == D("1")
    assert D(pnl["BTC"]["net_pnl"]) == D("79")
    assert D(pnl["ETH"]["average_cost_basis"]) == D("200")
    assert D(pnl["ETH"]["unrealised_pnl"]) == D("200")
    assert D(pnl["ETH"]["total_fees_paid"]) == D("4")
    assert D(pnl["ETH"]["net_pnl"]) == D("196")


def test_reserved_inventory_keeps_its_cost_basis():
    tools = portfolio_tools(BUYS[:1], "1")
    balance = tools.writer.accounts.balance("owner", "BTC")
    balance.available = D("0")
    balance.reserved = D("1")
    pnl = tools.portfolio()["pnl"]["BTC"]
    assert D(pnl["quantity"]) == D("1")
    assert D(pnl["average_cost_basis"]) == D("100")
    assert D(pnl["net_pnl"]) == D("79")


@pytest.fixture
def pnl_dsn():
    import asyncio

    dsn = _dsn_or_skip()
    schema = f"portfolio_pnl_{uuid4().hex}"
    asyncio.run(_create_schema(dsn, schema))
    try:
        yield _schema_dsn(dsn, schema)
    finally:
        asyncio.run(_drop_schema(dsn, schema))


def live_tools(ledger, *, restore=False, account="owner"):
    tools = make_tools()
    tools.account_id = account
    tools.writer.ledger = ledger
    if restore:
        tools.writer.restore(account)
    else:
        tools.writer.deposit(account, "USD", D("10000"), f"seed:{account}:USD:v1")
    set_mark(tools, "180")
    return tools


def trade(tools, side, price, *, quantity="1", ts_ns=10 * NS):
    order = tools.writer.submit(
        tools.account_id,
        {
            "client_order_id": f"synthetic-{tools.writer.event_sequence}",
            "product_id": "BTC-USD",
            "side": side,
            "type": "limit",
            "quantity": quantity,
            "limit_price": price,
            "reasoning": "hand-computed synthetic P&L test",
        },
        ts_ns - tools.writer.delay_ns,
    )
    tools.writer.activate(order.order_id, ts_ns)
    return tools.writer.record_fill(
        order.order_id, D(price), D(quantity), Liquidity.TAKER, FillCause.TAKER, ts_ns
    )


@pytest.mark.parametrize(
    "count,realised,unrealised,fees,net",
    [
        (3, "30", "30", "5.76", "54.24"),
        (4, "40", "0", "7.68", "32.32"),
        (5, "40", "60", "9.12", "90.88"),
    ],
)
def test_live_pnl_agrees_exactly_with_evaluator_and_survives_restart(
    pnl_dsn, count, realised, unrealised, fees, net
):
    with closing(SyncLedger(pnl_dsn)) as ledger:
        ledger.migrate()
        tools = live_tools(ledger)
        for index, (side, price) in enumerate(
            [("buy", "100"), ("buy", "200"), ("sell", "180"), ("sell", "160"), ("buy", "120")][
                :count
            ],
            1,
        ):
            trade(tools, side, price, ts_ns=index * 10 * NS)
        snapshot = tools.portfolio()
        rows = ledger.portfolio_fills("owner", snapshot["ledger_sequence"])
        mark = tools.engines["BTC-USD"].book.mid()
        offline = evaluate(
            "owner",
            [FillRecord.from_row(row) for row in rows],
            PriceSeries([PriceMark("BTC-USD", ts, mark) for ts in (0, 60 * NS)]),
            {"USD": D("10000")},
            start_ns=0,
            end_ns=120 * NS,
        ).account
        live = snapshot["pnl"]["BTC"]
        assert D(live["realised_pnl"]) == D(realised)
        assert D(live["unrealised_pnl"]) == D(unrealised)
        assert D(live["total_fees_paid"]) == D(fees)
        assert D(live["net_pnl"]) == D(net)
        assert (
            D(live["realised_pnl"]) + D(live["unrealised_pnl"]) - D(live["total_fees_paid"])
            == offline.final_equity - offline.initial_equity
        )
        assert D(live["net_pnl"]) == offline.final_equity - offline.initial_equity
        assert D(live["total_fees_paid"]) == offline.fees
        assert D(live["total_fees_paid"]) / D("10000") == offline.fee_drag
        assert D(live["net_pnl"]) / D("10000") == offline.return_value

    with closing(SyncLedger(pnl_dsn)) as restarted:
        restarted.migrate()
        recovered = live_tools(restarted, restore=True).portfolio()
        assert recovered["ledger_sequence"] == snapshot["ledger_sequence"]
        for key, value in live.items():
            actual = recovered["pnl"]["BTC"][key]
            if key in ("quote_asset", "reason") or value is None:
                assert actual == value
            else:
                assert D(actual) == D(value)


def test_portfolio_uses_one_sequence_bound_and_isolates_accounts(pnl_dsn, monkeypatch):
    with closing(SyncLedger(pnl_dsn)) as ledger:
        ledger.migrate()
        tools = live_tools(ledger)
        first = trade(tools, "buy", "100")
        old_balances, old_sequence = tools.writer.balance_snapshot("owner")
        second = trade(tools, "buy", "200")  # Same timestamp: committed sequence breaks the tie.
        other = live_tools(ledger, account="other")
        trade(other, "buy", "500")
        latest_sequence = other.writer.applied_sequences["other"]
        rows = ledger.portfolio_fills("owner", latest_sequence)
        assert [row["fill_id"] for row in rows] == [first.fill_id, second.fill_id]
        assert {row["account_id"] for row in rows} == {"owner"}

        # Simulate a later commit after the portfolio captured its balance/sequence snapshot.
        monkeypatch.setattr(
            tools.writer, "balance_snapshot", lambda account: (old_balances, old_sequence)
        )
        snapshot = tools.portfolio()
        assert snapshot["ledger_sequence"] == old_sequence
        pnl = snapshot["pnl"]["BTC"]
        assert D(pnl["quantity"]) == D("1")
        assert D(pnl["average_cost_basis"]) == D("100")
        assert D(pnl["total_fees_paid"]) == D("1.2")
        assert D(pnl["net_pnl"]) == D("78.8")


def test_portfolio_reads_more_than_the_history_display_limit(pnl_dsn):
    with closing(SyncLedger(pnl_dsn)) as ledger:
        ledger.migrate()
        tools = live_tools(ledger)
        order = tools.writer.submit(
            "owner",
            {
                "client_order_id": "many-fills",
                "product_id": "BTC-USD",
                "side": "buy",
                "type": "limit",
                "quantity": "0.01001",
                "limit_price": "100",
            },
            NS,
        )
        tools.writer.activate(order.order_id, order.effective_at_ns)
        for index in range(1001):
            tools.writer.record_fill(
                order.order_id,
                D("100"),
                D("0.00001"),
                Liquidity.TAKER,
                FillCause.TAKER,
                order.effective_at_ns + index,
            )
        snapshot = tools.portfolio()
        assert len(ledger.history("owner", 1000)) == 1000
        assert len(ledger.portfolio_fills("owner", snapshot["ledger_sequence"])) == 1001
        pnl = snapshot["pnl"]["BTC"]
        assert D(pnl["quantity"]) == D("0.01001")
        assert D(pnl["average_cost_basis"]) == D("100")
        assert D(pnl["total_fees_paid"]) == D("0.012012")
        assert D(pnl["net_pnl"]) == D("0.788788")


def test_committed_non_cash_deposit_has_no_invented_acquisition_cost(pnl_dsn):
    with closing(SyncLedger(pnl_dsn)) as ledger:
        ledger.migrate()
        tools = live_tools(ledger)
        tools.writer.deposit("owner", "BTC", D("1"), "unknown-acquisition")
        trade(tools, "sell", "180", quantity="0.5")
        pnl = tools.portfolio()["pnl"]["BTC"]
        assert D(pnl["quantity"]) == D("0.5")
        assert pnl["reason"] == "unknown_cost_basis"
        assert pnl["average_cost_basis"] is None
        assert pnl["realised_pnl"] is None
        assert pnl["unrealised_pnl"] is None
        assert pnl["net_pnl"] is None
        assert D(pnl["total_fees_paid"]) == D("1.08")
