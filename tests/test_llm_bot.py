"""The external template over real HTTP/WebSockets and handwritten synthetic books."""

import ast
import builtins
import importlib.util
import json
import runpy
import threading
from contextlib import closing, contextmanager
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path

import httpx
import pytest
from test_account_writer_ledger import MemoryLedger
from test_api import TOKEN, no_floats
from test_client import serving
from test_mcp import make_tools
from test_portfolio_pnl import pnl_dsn as pnl_dsn

from simex.client import Client, FillStream, StreamGap
from simex.exchange import GateDecision, OrderError, OrderRequest
from simex.exchange.events import EventJournal
from simex.ledger import LedgerHalted, SyncLedger
from simex.marketdata import BookUpdate

SCRIPT = Path(__file__).resolve().parent.parent / "examples" / "llm_bot.py"


@pytest.fixture
def bot():
    spec = importlib.util.spec_from_file_location("llm_bot", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BotLedger(MemoryLedger):
    """In-memory committed rows for real-app tests; the runnable test uses Postgres."""

    def __init__(self):
        super().__init__()
        self.sequence, self.rows = 0, []

    def record_deposit(self, *args):
        super().record_deposit(*args)
        self.sequence += 1
        return self.sequence

    def record_fill(self, fill, instrument, order, reservation, band_price=None):
        super().record_fill(fill, instrument, order, reservation, band_price)
        self.sequence += 1
        self.rows.append(
            {
                **fill.to_dict(),
                "price": fill.price,
                "quantity": fill.quantity,
                "fee": fill.fee,
                "reasoning": order.request.reasoning,
                "sequence": self.sequence,
            }
        )
        return self.sequence

    def history(self, account, limit):
        return [row for row in reversed(self.rows) if row["account_id"] == account][:limit]

    def portfolio_fills(self, account, through_sequence):
        return [
            row
            for row in self.rows
            if row["account_id"] == account and row["sequence"] <= through_sequence
        ]


def ready_tools(ledger=None):
    tools = make_tools()
    tools.writer.ledger = ledger if ledger is not None else BotLedger()
    tools.writer.deposit("owner", "USD", D("1000"), "seed:owner:USD")
    tools.engines["BTC-USD"].book.apply(
        BookUpdate(
            "gemini",
            "BTC-USD",
            True,
            ((D("100"), D("1")),),
            ((D("100.01"), D("1")),),
            (2, 2),
            False,
            2,
            2,
        )
    )
    return tools


def settle(tools, *, halt=False):
    for order in tools.writer.open_orders("owner"):
        tools.engines[order.product_id].advance(order.effective_at_ns)
    if halt and any(e["type"] == "fill" for e in tools.writer.events):
        tools.writer.halt_account("owner")


@contextmanager
def watchdog(api):
    # A broken demo must fail assertions instead of hanging on an unbounded stream read.
    timer = threading.Timer(5, lambda: api.call(setattr, api.tools.writer, "halted", True))
    timer.daemon = True
    timer.start()
    try:
        yield
    finally:
        timer.cancel()
        timer.join(6)
        assert not timer.is_alive()


def intent(quantity="0.00001", **changes):
    return {
        "product_id": "BTC-USD",
        "side": "buy",
        "type": "market",
        "quantity": quantity,
        "reasoning": "  The model's own rationale: Δ, line one\nline two.  ",
        **changes,
    }


def once(bot, monkeypatch, plan):
    states = []

    def decide(state):
        states.append(state)
        if len(states) == 1:
            return plan
        raise KeyboardInterrupt

    monkeypatch.setattr(bot, "decide", decide)
    return states


def posts(api):
    result = [json.loads(body) for method, _, body in api.probe.requests if method == "POST"]
    no_floats(result)
    return result


def summary(output):
    line = next(line for line in output.splitlines() if line.startswith("Summary (simulated"))
    return ast.literal_eval(line.split("): ", 1)[1])


def test_runnable_placeholder_prints_its_committed_fill_and_summary(pnl_dsn, monkeypatch, capsys):
    with closing(SyncLedger(pnl_dsn)) as ledger:
        ledger.migrate()
        tools = ready_tools(ledger)
        with serving(tools, after_order=lambda: settle(tools, halt=True)) as api, watchdog(api):
            monkeypatch.setenv("EXCHANGE_API_PORT", api.url.rsplit(":", 1)[1])
            monkeypatch.setenv("EXCHANGE_API_TOKEN", TOKEN)
            monkeypatch.delenv("BOT_INTERVAL_SECONDS", raising=False)
            monkeypatch.delenv("BOT_PRODUCT", raising=False)
            with pytest.raises(SystemExit, match="account halted"):
                runpy.run_path(str(SCRIPT), run_name="__main__")
            sent = posts(api)
            assert len(sent) == 1 and sent[0]["type"] == "market"
            assert D(sent[0]["quantity"]) == D("0.00001")
            rows = ledger.history("owner", 100)
            assert len(rows) == 1 and rows[0]["reasoning"] == sent[0]["reasoning"]
            assert D(rows[0]["quantity"]) == D("0.00001")
            assert D(rows[0]["fee"]) == D("0.00001201")
    output = capsys.readouterr().out
    assert rows[0]["fill_id"] in output
    assert summary(output) == dict(ticks=1, intents=1, submitted=1, accepted=1, fills=1, skips=0)
    assert "Quantity filled: 0.00001" in output and "0.00001201" in output
    assert "python -m simex.evaluator" in output and "may fill after exit" in output
    assert TOKEN not in output


def test_none_continues_without_orders_and_carries_the_observations(bot, monkeypatch, capsys):
    states = []

    def decide(state):
        states.append(state)
        if len(states) == 3:
            raise KeyboardInterrupt
        return None

    monkeypatch.setattr(bot, "decide", decide)
    with (
        serving(ready_tools(), future_frame=True) as api,
        watchdog(api),
        Client(api.url, TOKEN, timeout=2) as client,
    ):
        bot.run(client, interval_seconds=0)
        assert posts(api) == []
    assert len(states) == 3
    for state in states:
        assert {
            "quote",
            "features",
            "portfolio",
            "open_orders",
            "recent_fills",
            "refusals",
        } <= state.keys()
        assert type(state["quote"]["bids"][0][0]) is D
        assert state["product_id"] == "BTC-USD" and state["refusals"] == []
    output = capsys.readouterr().out
    assert "Stopped by Ctrl-C" in output
    assert summary(output) == dict(ticks=3, intents=0, submitted=0, accepted=0, fills=0, skips=2)


@pytest.mark.parametrize("side", ["buy", "sell"])
def test_grid_refusal_rounds_down_once_with_fresh_key_and_verbatim_rationale(
    bot, monkeypatch, capsys, side
):
    plan = intent("0.000010001", side=side)
    states = once(bot, monkeypatch, plan)
    tools = ready_tools()
    tools.writer.deposit("owner", "BTC", D("0.000010001"), "seed:owner:BTC")
    with (
        serving(tools, after_order=lambda: settle(tools)) as api,
        watchdog(api),
        Client(api.url, TOKEN, timeout=2) as client,
    ):
        bot.run(client, interval_seconds=0)
        sent = posts(api)
        assert len(sent) == 2 and sent[0]["client_order_id"] != sent[1]["client_order_id"]
        assert D(sent[0]["quantity"]) == D("0.000010001")
        assert D(sent[1]["quantity"]) == D("0.00001000")
        assert sent[0]["reasoning"] == sent[1]["reasoning"] == plan["reasoning"]
        assert (
            api.call(
                lambda: (
                    tools.writer.lookup_order(
                        "owner", client_order_id=sent[1]["client_order_id"]
                    ).fill_count
                )
            )
            == 1
        )
    assert plan["quantity"] == "0.000010001"  # The decision itself wasn't mutated.
    assert states[1]["refusals"][0]["reason"] == "bad_quantity"
    assert states[1]["refusals"][0]["increment"] == D("0.00000001")
    counts = summary(capsys.readouterr().out)
    assert counts["submitted"] == 2 and counts["accepted"] == 1


@pytest.mark.parametrize(
    "quantity,attempts", [("0.000005", 1), ("0.000000015", 2), ("0.000000001", 1)]
)
def test_below_minimum_or_zero_rounding_skips_without_inflating(
    bot, monkeypatch, capsys, quantity, attempts
):
    states = once(bot, monkeypatch, intent(quantity))
    with serving(ready_tools()) as api, watchdog(api), Client(api.url, TOKEN, timeout=2) as client:
        bot.run(client, interval_seconds=0)
        sent = posts(api)
        assert len(sent) == attempts
        assert all(D(order["quantity"]) <= D(quantity) for order in sent)
    assert len(states[1]["refusals"]) == attempts
    if D(quantity) >= D("0.00000001"):
        assert states[1]["refusals"][-1]["reason"] == "below_min_size"
        assert states[1]["refusals"][-1]["minimum"] == D("0.00001")
    counts = summary(capsys.readouterr().out)
    assert counts["accepted"] == 0 and counts["skips"] == 1


def test_reported_minimum_prevents_a_second_request_after_rounding(bot, monkeypatch, capsys):
    actual = OrderRequest.check

    def with_minimum(request, instrument):
        try:
            actual(request, instrument)
        except OrderError as error:
            error.details["minimum"] = str(instrument.min_size)
            raise

    monkeypatch.setattr(OrderRequest, "check", with_minimum)
    states = once(bot, monkeypatch, intent("0.000000015"))
    with serving(ready_tools()) as api, watchdog(api), Client(api.url, TOKEN, timeout=2) as client:
        bot.run(client, interval_seconds=0)
        assert len(posts(api)) == 1
    assert states[1]["refusals"][0]["minimum"] == D("0.00001")
    output = capsys.readouterr().out
    assert "Rounded DOWN:" in output and "minimum" in output
    assert summary(output)["skips"] == 1


def test_minimum_feedback_can_be_a_new_decision_on_the_next_tick(bot, monkeypatch, capsys):
    states = []

    def decide(state):
        states.append(state)
        if len(states) == 1:
            return intent("0.000005")
        assert state["session"]["submitted"] == 1  # No immediate inflation/retry happened.
        minimum = state["refusals"][0]["minimum"]
        return intent(minimum, reasoning="New decision: use the reported minimum on this tick")

    monkeypatch.setattr(bot, "decide", decide)
    tools = ready_tools()
    with (
        serving(tools, after_order=lambda: settle(tools, halt=True)) as api,
        watchdog(api),
        Client(api.url, TOKEN, timeout=2) as client,
    ):
        with pytest.raises(SystemExit, match="halted"):
            bot.run(client, interval_seconds=0)
        sent = posts(api)
        assert len(sent) == 2 and sent[0]["client_order_id"] != sent[1]["client_order_id"]
        assert sent[1]["reasoning"] == "New decision: use the reported minimum on this tick"
    assert len(states) == 2
    assert summary(capsys.readouterr().out)["accepted"] == 1


@pytest.mark.parametrize(
    "patch",
    [
        {"quantity": 500},
        {"quantity": 0.1},
        {"quantity": True},
        {"quantity": []},
        {"quantity": "NaN"},
        {"quantity": "Infinity"},
        {"quantity": "no"},
        {"quantity": "0"},
        {"quantity": "-1"},
        {"type": "limit", "limit_price": 100},
        {"type": "limit", "limit_price": 100.1},
        {"type": "limit", "limit_price": "NaN"},
        {"type": "limit"},
        {"limit_price": "100"},
        {"product_id": "ETH-USD"},
        {"side": "hold"},
        {"type": "stop"},
        {"reasoning": ""},
        {"reasoning": 1},
        {"client_order_id": "model-chosen-key"},
        {"account_id": "other"},
    ],
)
def test_untrusted_intent_is_a_local_skip_before_http(bot, monkeypatch, capsys, patch):
    once(bot, monkeypatch, intent(**patch))
    with serving(ready_tools()) as api, watchdog(api), Client(api.url, TOKEN, timeout=2) as client:
        bot.run(client, interval_seconds=0)
        assert posts(api) == []
    output = capsys.readouterr().out
    assert "Local skip:" in output
    counts = summary(output)
    assert counts["intents"] == 1 and counts["submitted"] == 0 and counts["skips"] == 1


@pytest.mark.parametrize("plan", [[], {}, {"quantity": "1"}])
def test_missing_or_non_mapping_intent_is_a_local_skip(bot, monkeypatch, capsys, plan):
    once(bot, monkeypatch, plan)
    with serving(ready_tools()) as api, watchdog(api), Client(api.url, TOKEN, timeout=2) as client:
        bot.run(client, interval_seconds=0)
        assert posts(api) == []
    assert "Local skip:" in capsys.readouterr().out


@pytest.mark.parametrize("scope", ["writer", "account"])
def test_hello_halt_prevents_the_first_order(bot, capsys, scope):
    tools = ready_tools()
    if scope == "writer":
        tools.writer.halted = True
    else:
        tools.writer.halt_account("owner")
    with serving(tools) as api, watchdog(api), Client(api.url, TOKEN, timeout=2) as client:
        with pytest.raises(SystemExit, match="before the first order"):
            bot.run(client)
        assert api.probe.requests == []
    assert summary(capsys.readouterr().out)["ticks"] == 0


@pytest.mark.parametrize("reason", ["account_halted", "writer_halted", "ledger_halted"])
def test_refusal_halt_stops_without_retry(bot, monkeypatch, capsys, reason):
    tools = ready_tools()
    with serving(tools) as api, watchdog(api), Client(api.url, TOKEN, timeout=2) as client:

        def decide(state):
            if reason == "account_halted":
                api.call(tools.writer.halt_account, "owner")
            elif reason == "writer_halted":
                api.call(setattr, tools.writer, "halted", True)
            else:

                def fail(*args):
                    raise LedgerHalted("synthetic commit failure")

                api.call(setattr, tools.writer.ledger, "save_order", fail)
            return intent()

        monkeypatch.setattr(bot, "decide", decide)
        with pytest.raises(SystemExit, match=reason):
            bot.run(client)
        assert len(posts(api)) == 1
    output = capsys.readouterr().out
    assert reason in output and summary(output)["accepted"] == 0


def test_ack_follows_application_and_ignores_other_product_totals(bot, monkeypatch, capsys):
    observed, acknowledged = set(), []
    actual_ack = FillStream.ack

    def printing(*args, **kwargs):
        if args and args[0] == "Fill:":
            observed.add(args[1]["fill_id"])
        builtins.print(*args, **kwargs)

    def ack(stream, frame):
        if frame["fill"]["product_id"] == "BTC-USD":
            assert frame["fill"]["fill_id"] in observed
        acknowledged.append(frame["fill"])
        actual_ack(stream, frame)

    monkeypatch.setattr(bot, "print", printing, raising=False)
    monkeypatch.setattr(FillStream, "ack", ack)
    tools = ready_tools()

    def after_order():
        settle(tools)
        tools.writer._emit(
            "fill",
            fill={
                "fill_id": "synthetic-other-product",
                "account_id": "owner",
                "product_id": "ETH-USD",
                "quantity": "5",
                "fee": "7",
                "fee_asset": "USD",
            },
        )
        tools.writer.halt_account("owner")

    with (
        serving(tools, after_order=after_order) as api,
        watchdog(api),
        Client(api.url, TOKEN, timeout=2) as client,
    ):
        with pytest.raises(SystemExit, match="account halted"):
            bot.run(client)
    assert len(acknowledged) == 2 and len(observed) == 1
    output = capsys.readouterr().out
    assert summary(output)["fills"] == 1
    assert "Quantity filled: 0.00001" in output and "0.00001201" in output


def test_gap_propagates_without_reconnect_and_still_prints_summary(bot, capsys):
    tools = ready_tools()
    tools.writer._journal = EventJournal(2)
    tools.writer.events = tools.writer._journal.events

    def evict():
        for _ in range(10):
            tools.writer._emit("synthetic")

    with (
        serving(tools, after_order=evict) as api,
        watchdog(api),
        Client(api.url, TOKEN, timeout=2) as client,
    ):
        with pytest.raises(StreamGap) as caught:
            bot.run(client)
        assert caught.value.reason == "events_evicted" and api.probe.streams == 1
    assert summary(capsys.readouterr().out)["submitted"] == 1


def test_clean_disconnect_is_reported_without_traceback(bot, monkeypatch, capsys):
    monkeypatch.setattr(bot, "decide", lambda state: None)
    with (
        serving(ready_tools(), close_stream=True) as api,
        watchdog(api),
        Client(api.url, TOKEN, timeout=2) as client,
    ):
        with pytest.raises(SystemExit, match="Stream disconnected"):
            bot.run(client)
    output = capsys.readouterr().out
    assert "Traceback" not in output and summary(output)["submitted"] == 0


def test_uncertain_transport_failure_keeps_the_logged_key_and_stops(bot, capsys):
    tools = ready_tools()
    with serving(tools) as api, watchdog(api), Client(api.url, TOKEN, timeout=2) as client:

        def lost(response):
            if response.request.method == "POST":
                raise httpx.ReadError("synthetic lost response", request=response.request)

        client._http.event_hooks["response"] = [lost]
        with pytest.raises(httpx.ReadError):
            bot.run(client)
        sent = posts(api)
        assert len(sent) == 2 and sent[0] == sent[1]  # The SDK's own same-key transport retry.
        assert api.call(len, tools.writer.orders) == 1
    output = capsys.readouterr().out
    assert sent[0]["client_order_id"] in output
    assert summary(output)["submitted"] == 1


@pytest.mark.parametrize(
    ("reason", "market"),
    [
        ("stale_level2:age_ns=5000000001", False),
        ("feed_lagging:excess_ns=2000000001", False),
        ("insufficient_funds", False),
        ("insufficient_funds", True),
    ],
)
def test_gate_and_funding_refusals_wait_for_the_next_tick(bot, monkeypatch, capsys, reason, market):
    tools = ready_tools()
    if reason == "insufficient_funds":
        tools.writer.accounts.balance("owner", "USD").available = D(0)
    else:

        class Closed:
            def check(self, product_id, now):
                return GateDecision(False, reason)

        tools.writer.gate = Closed()
    states = once(bot, monkeypatch, intent() if market else intent(type="limit", limit_price="100"))
    with serving(tools) as api, watchdog(api), Client(api.url, TOKEN, timeout=2) as client:
        bot.run(client, interval_seconds=0)
        assert len(posts(api)) == 1
    assert len(states) == 2 and states[1]["refusals"][0]["reason"] == reason
    output = capsys.readouterr().out
    assert reason in output and "rule" in output
    if reason == "insufficient_funds":
        feedback = states[1]["refusals"][0]
        assert "available" in feedback
        assert ("required" in feedback) is not market


def test_default_placeholder_does_not_retire_after_a_refusal(bot, monkeypatch, capsys):
    tools = ready_tools()
    opened = tools.writer.gate

    class Closed:
        def check(self, product_id, now):
            return GateDecision(False, "stale_level2:age_ns=6000000000")

    tools.writer.gate = Closed()
    states, original = [], bot.decide

    def decide(state):
        states.append(state)
        return original(state)

    def recover():
        tools.writer.gate = opened
        settle(tools, halt=True)

    monkeypatch.setattr(bot, "decide", decide)
    with (
        serving(tools, after_order=recover) as api,
        watchdog(api),
        Client(api.url, TOKEN, timeout=2) as client,
    ):
        with pytest.raises(SystemExit, match="halted"):
            bot.run(client, interval_seconds=0)
        assert len(posts(api)) == 2
    assert states[1]["session"]["accepted"] == 0
    assert states[1]["refusals"][0]["reason"] == "no_band_price"
    counts = summary(capsys.readouterr().out)
    assert counts["intents"] == 2 and counts["accepted"] == 1


def test_heartbeats_drain_without_ticking_before_the_interval(bot, monkeypatch, capsys):
    clock = {"now": 0, "beats": 0}
    actual = FillStream.__next__

    def on_frame(stream):
        frame = actual(stream)
        if frame["type"] == "heartbeat":
            clock["beats"] += 1
            if clock["beats"] == 2:
                assert len(states) == 1
                clock["now"] = 60
            else:
                clock["now"] = 59
        return frame

    states = once(bot, monkeypatch, None)
    monkeypatch.setattr(bot, "perf_counter", lambda: clock["now"])
    monkeypatch.setattr(FillStream, "__next__", on_frame)
    with serving(ready_tools()) as api, watchdog(api), Client(api.url, TOKEN, timeout=2) as client:
        bot.run(client)
        assert posts(api) == []
    assert clock["beats"] == 2 and len(states) == 2
    assert summary(capsys.readouterr().out)["ticks"] == 2


def test_historical_fills_inform_decisions_but_are_not_session_totals(bot, monkeypatch, capsys):
    tools = ready_tools()
    tools.place_order({**intent("0.1"), "client_order_id": "historical"})
    settle(tools)
    states, original = [], bot.decide

    def decide(state):
        states.append(state)
        return original(state)

    monkeypatch.setattr(bot, "decide", decide)
    with (
        serving(tools, after_order=lambda: settle(tools, halt=True)) as api,
        watchdog(api),
        Client(api.url, TOKEN, timeout=2) as client,
    ):
        with pytest.raises(SystemExit, match="halted"):
            bot.run(client)
        assert len(posts(api)) == 1
    assert len(states[0]["recent_fills"]) == 1
    assert states[0]["recent_fills"][0]["quantity"] == D("0.1")
    output = capsys.readouterr().out
    assert summary(output)["fills"] == 1 and "Quantity filled: 0.00001" in output


def test_a_second_grid_refusal_never_gets_a_third_attempt(bot, monkeypatch, capsys):
    tools = ready_tools()
    states = once(bot, monkeypatch, intent("0.000010019"))
    calls = []

    def change_grid():
        calls.append(True)
        if len(calls) == 1:
            instrument = tools.writer.instruments["BTC-USD"]
            tools.writer.instruments["BTC-USD"] = replace(
                instrument, size_increment=D("0.00000002")
            )

    with (
        serving(tools, after_order=change_grid) as api,
        watchdog(api),
        Client(api.url, TOKEN, timeout=2) as client,
    ):
        bot.run(client, interval_seconds=0)
        sent = posts(api)
        assert len(sent) == 2
        assert [D(order["quantity"]) for order in sent] == [D("0.000010019"), D("0.00001001")]
    assert [r["reason"] for r in states[1]["refusals"]] == ["bad_quantity", "bad_quantity"]
    assert [r["increment"] for r in states[1]["refusals"]] == [D("0.00000001"), D("0.00000002")]
    assert summary(capsys.readouterr().out)["accepted"] == 0


def test_placeholder_stops_submitting_after_acceptance(bot, monkeypatch, capsys):
    tools = ready_tools()
    states, original = [], bot.decide

    def decide(state):
        states.append(state)
        if len(states) == 3:
            raise KeyboardInterrupt
        return original(state)

    monkeypatch.setattr(bot, "decide", decide)
    with (
        serving(tools, after_order=lambda: settle(tools)) as api,
        watchdog(api),
        Client(api.url, TOKEN, timeout=2) as client,
    ):
        bot.run(client, interval_seconds=0)
        assert len(posts(api)) == 1
    assert states[1]["session"]["accepted"] == 1
    counts = summary(capsys.readouterr().out)
    assert counts["intents"] == 1 and counts["accepted"] == 1 and counts["skips"] == 1
