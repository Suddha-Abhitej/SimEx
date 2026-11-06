from decimal import Decimal

import pytest
from test_mcp import make_tools

from simex.agents import BaselineAgent, BaselineSpec
from simex.exchange import FeeSchedule, GateDecision, OrderStatus
from simex.exchange.runner_metrics import RunnerMetrics
from simex.mcp import RiskLimits


def agent(
    kind: str,
    seed: int = 7,
    *,
    metrics: RunnerMetrics | None = None,
    quote_cash: Decimal = Decimal("1000"),
) -> BaselineAgent:
    tools = make_tools()
    if quote_cash:
        tools.writer.deposit("owner", "USD", quote_cash, f"deposit:{kind}")
    return BaselineAgent(
        BaselineSpec(kind, "owner", seed),
        tools,
        cadence_seconds=1,
        dca_quote_amount=Decimal("10"),
        metrics=metrics,
    )


def metric(baseline, name, reason):
    return (
        baseline.metrics.registry.get_sample_value(
            f"simex_exchange_agent_{name}_total",
            {"account_id": baseline.spec.account_id, "reason": reason},
        )
        or 0
    )


def test_buy_and_hold_fires_once():
    baseline = agent("buy_and_hold")

    first = baseline.tick()
    second = baseline.tick()

    assert first["ok"]
    assert second["ok"]
    assert len(baseline.tools.writer.orders) == 1
    assert "buy_and_hold" in next(iter(baseline.tools.writer.orders.values())).request.reasoning


def test_dca_fires_at_each_tick():
    baseline = agent("dca")

    baseline.tick()
    baseline.tick()

    assert len(baseline.tools.writer.orders) == 2


def test_rebuilt_agent_uses_new_idempotency_keys():
    first = agent("dca")
    assert first.tick()["ok"]

    rebuilt = BaselineAgent(
        BaselineSpec("dca", "owner", 7),
        first.tools,
        cadence_seconds=1,
        dca_quote_amount=Decimal("10"),
    )
    result = rebuilt.tick()

    assert result["ok"]
    assert len(first.tools.writer.orders) == 2
    assert first.run_id != rebuilt.run_id


def test_random_is_reproducible_from_seed():
    left = agent("random", seed=42)
    right = agent("random", seed=42)
    for baseline in (left, right):
        baseline.tools.writer.deposit("owner", "BTC", Decimal("1"), "synthetic-base")

    assert left.tick()["order"]["quantity"] == right.tick()["order"]["quantity"]
    assert left.tick()["order"]["side"] == right.tick()["order"]["side"]


def test_momentum_uses_features_thresholds():
    baseline = agent("momentum")
    values = iter(
        (
            {"ok": True, "mid": "100", "depth_imbalance": "0.3"},
            {"ok": True, "mid": "101", "depth_imbalance": "0.3"},
        )
    )
    baseline.tools.features = lambda product, depth: next(values)

    assert baseline.tick()["action"] == "none"
    result = baseline.tick()

    assert result["ok"]
    assert "mid rose" in result["order"]["reasoning"]


def test_shared_limits_are_used():
    baseline = agent("dca")
    baseline.tools.limits = RiskLimits(
        {"BTC-USD": Decimal("0.000001")}, {"BTC-USD": Decimal("1")}, {"BTC-USD": Decimal("1")}
    )

    assert baseline.tick()["reason"] == "below_min_size"


def test_below_min_size_is_counted_on_every_tick_and_logged_once(caplog):
    metrics = RunnerMetrics()
    baseline = agent("dca", metrics=metrics)
    baseline.tools.limits = RiskLimits(
        {"BTC-USD": Decimal("0.000001")}, {"BTC-USD": Decimal("1")}, {"BTC-USD": Decimal("1")}
    )

    for _ in range(3):
        assert baseline.tick() == {"ok": False, "reason": "below_min_size"}

    assert (
        metrics.registry.get_sample_value(
            "simex_exchange_agent_skipped_total",
            {"account_id": "owner", "reason": "below_min_size"},
        )
        == 3
    )
    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 1
    assert all(value in messages[0] for value in ("dca", "owner", "below_min_size"))
    assert "skipped" in messages[0]
    assert metric(baseline, "refusals", "below_min_size") == 0
    assert not baseline.tools.writer.orders
    assert metrics.agent_errors.labels("dca", "owner")._value.get() == 0


@pytest.mark.parametrize(
    "reason",
    [
        "quantity_limit",
        "no_band_price",
        "book_not_ready",
        "account_halted",
        "writer_halted",
        "ledger_halted",
        "insufficient_funds",
    ],
)
def test_structured_refusals_are_counted_on_every_tick(reason, monkeypatch, caplog):
    metrics = RunnerMetrics()
    baseline = agent("dca", metrics=metrics)
    monkeypatch.setattr(
        baseline.tools, "place_order", lambda payload: {"ok": False, "reason": reason}
    )

    for _ in range(3):
        assert baseline.tick() == {"ok": False, "reason": reason}

    assert (
        metrics.registry.get_sample_value(
            "simex_exchange_agent_refusals_total", {"account_id": "owner", "reason": reason}
        )
        == 3
    )
    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 1
    assert all(value in messages[0] for value in ("dca", "owner", reason))
    assert metrics.agent_errors.labels("dca", "owner")._value.get() == 0
    assert metric(baseline, "skipped", reason) == 0


def test_refusals_log_each_reason_once_per_agent(monkeypatch, caplog):
    metrics = RunnerMetrics()
    first = agent("dca", metrics=metrics)
    second = agent("dca", metrics=metrics)
    second.spec = BaselineSpec("dca", "other", 7)
    second.tools.account_id = "other"
    second.tools.writer.deposit("other", "USD", Decimal("1000"), "synthetic-other-cash")
    for baseline in (first, second):
        reasons = iter(("quantity_limit", "writer_halted", "quantity_limit"))
        monkeypatch.setattr(
            baseline.tools,
            "place_order",
            lambda payload, reasons=reasons: {"ok": False, "reason": next(reasons)},
        )
        for _ in range(3):
            baseline.tick()

    assert len(caplog.records) == 4
    for account in ("owner", "other"):
        for reason, count in (("quantity_limit", 2), ("writer_halted", 1)):
            assert (
                metrics.registry.get_sample_value(
                    "simex_exchange_agent_refusals_total", {"account_id": account, "reason": reason}
                )
                == count
            )


def test_gate_refusal_details_do_not_create_new_reason_labels(caplog):
    class StaleGate:
        def __init__(self):
            self.age = 0

        def check(self, product_id, now_ns):
            self.age += 1
            return GateDecision(False, f"stale_level2:age_ns={self.age}")

    metrics = RunnerMetrics()
    baseline = agent("dca", metrics=metrics)
    baseline.tools.writer.gate = StaleGate()
    for _ in range(3):
        assert not baseline.tick()["ok"]

    assert (
        metrics.registry.get_sample_value(
            "simex_exchange_agent_refusals_total", {"account_id": "owner", "reason": "stale_level2"}
        )
        == 3
    )
    assert len(caplog.records) == 1
    assert "stale_level2:age_ns=1" in caplog.records[0].getMessage()


@pytest.mark.parametrize(
    "available,submits", [("0", False), ("0.09999999", False), ("0.1", True), ("0.2", True)]
)
def test_sell_uses_available_base_without_resizing(available, submits):
    baseline = agent("random")
    if Decimal(available):
        baseline.tools.writer.deposit("owner", "BTC", Decimal(available), "synthetic-base")
    result = baseline._order("sell", Decimal("0.1"), Decimal("100"), "synthetic sell")
    assert result["ok"] is submits
    assert len(baseline.tools.writer.orders) == int(submits)
    assert metric(baseline, "skipped", "insufficient_funds") == int(not submits)
    assert metric(baseline, "refusals", "insufficient_funds") == 0
    if submits:
        assert result["order"]["quantity"] == "0.1"
        assert baseline.tools.writer.accounts.balance("owner", "BTC").reserved == Decimal("0.1")
    else:
        assert result == {"ok": False, "reason": "insufficient_funds"}
        assert baseline.tools.writer.accounts.balance("owner", "BTC").available == Decimal(
            available
        )


@pytest.mark.parametrize(
    "available,submits",
    [("0", False), ("10", False), ("10.11999999", False), ("10.12", True), ("11", True)],
)
def test_buy_requires_notional_and_fee_without_resizing(available, submits):
    baseline = agent("dca", quote_cash=Decimal(available))
    result = baseline._order("buy", Decimal("0.1"), Decimal("100"), "synthetic buy")
    assert result["ok"] is submits
    assert len(baseline.tools.writer.orders) == int(submits)
    assert metric(baseline, "skipped", "insufficient_funds") == int(not submits)
    assert metric(baseline, "refusals", "insufficient_funds") == 0
    balance = baseline.tools.writer.accounts.balance("owner", "USD")
    if submits:
        assert result["order"]["quantity"] == "0.1"
        assert balance.reserved == Decimal("10.12")
    else:
        assert result == {"ok": False, "reason": "insufficient_funds"}
        assert balance.available == Decimal(available) and balance.reserved == 0


@pytest.mark.parametrize("cash,submits", [("0.001023149999", False), ("0.00102315", True)])
def test_buy_respects_upward_fee_rounding(cash, submits):
    baseline = agent("dca", quote_cash=Decimal(cash))
    # 0.00001001 * 101 = 0.00101101; fee 0.00001213212 rounds UP to 0.00001214.
    result = baseline._order("buy", Decimal("0.00001001"), Decimal("101"), "synthetic rounding")
    assert result["ok"] is submits
    assert len(baseline.tools.writer.orders) == int(submits)
    assert metric(baseline, "skipped", "insufficient_funds") == int(not submits)
    assert metric(baseline, "refusals", "insufficient_funds") == 0


def test_buy_uses_writer_fee_schedule_even_when_maker_is_higher():
    baseline = agent("dca", quote_cash=Decimal("10.12"))
    baseline.tools.writer.fees = FeeSchedule(maker_bps=Decimal("250"))
    assert baseline._order("buy", Decimal("0.1"), Decimal("100"), "synthetic fee tier") == {
        "ok": False,
        "reason": "insufficient_funds",
    }
    assert not baseline.tools.writer.orders
    assert metric(baseline, "skipped", "insufficient_funds") == 1
    assert metric(baseline, "refusals", "insufficient_funds") == 0


@pytest.mark.parametrize("opened", [False, True])
def test_reserved_cash_cannot_fund_another_buy(opened):
    baseline = agent("dca", quote_cash=Decimal("20.12"))
    # Rest at the bid so opening the order does not consume it as a taker.
    assert baseline.tools.place_order(
        {
            "client_order_id": "synthetic-existing-order",
            "product_id": "BTC-USD",
            "side": "buy",
            "type": "limit",
            "quantity": "0.1",
            "limit_price": "100",
            "reasoning": "synthetic resting order reserves cash",
        }
    )["ok"]
    order = next(iter(baseline.tools.writer.orders.values()))
    if opened:
        baseline.tools.engines["BTC-USD"].advance(order.effective_at_ns)
    assert order.status is (OrderStatus.OPEN if opened else OrderStatus.PENDING)
    balance = baseline.tools.writer.accounts.balance("owner", "USD")
    assert balance.available == Decimal("10") and balance.reserved == Decimal("10.12")
    assert baseline.tick() == {"ok": False, "reason": "insufficient_funds"}
    assert len(baseline.tools.writer.orders) == 1
    assert balance.available == Decimal("10") and balance.reserved == Decimal("10.12")
    assert metric(baseline, "skipped", "insufficient_funds") == 1
    assert metric(baseline, "refusals", "insufficient_funds") == 0


def test_buy_and_hold_funding_skip_preserves_its_shot():
    baseline = agent("buy_and_hold", quote_cash=Decimal("1"))
    assert baseline.tick() == {"ok": False, "reason": "insufficient_funds"}
    assert not baseline._fired and baseline._tick_count == 1
    assert not baseline.tools.writer.orders
    baseline.tools.writer.deposit("owner", "USD", Decimal("200"), "synthetic-later-cash")
    result = baseline.tick()
    assert result["ok"] and result["order"]["quantity"] == "1"
    assert baseline._fired and baseline._tick_count == 2
    assert baseline.tick() == {"ok": True, "action": "none"}
    assert len(baseline.tools.writer.orders) == 1
    assert metric(baseline, "skipped", "insufficient_funds") == 1
    assert metric(baseline, "refusals", "insufficient_funds") == 0


def test_buy_and_hold_existing_position_retires_the_shot():
    baseline = agent("buy_and_hold", quote_cash=Decimal(0))
    baseline.tools.writer.deposit("owner", "BTC", Decimal("0.1"), "synthetic-held-base")
    assert baseline.tick() == {"ok": True, "action": "none"}
    assert baseline._fired and not baseline.tools.writer.orders
    assert metric(baseline, "skipped", "insufficient_funds") == 0


@pytest.mark.parametrize("reason", ["stale_level2", "feed_lagging"])
def test_buy_and_hold_exchange_refusal_preserves_shot_until_accepted(reason, caplog):
    baseline = agent("buy_and_hold")
    original_gate = baseline.tools.writer.gate

    class ClosedGate:
        def check(self, product_id, now_ns):
            return GateDecision(False, reason)

    baseline.tools.writer.gate = ClosedGate()
    assert baseline.tools.quote("BTC-USD")["ok"]
    refused_ids = []
    for tick in range(2):
        result = baseline.tick()
        assert not result["ok"] and result["reason"] == reason
        assert result["order"]["status"] == "rejected"
        refused_ids.append(result["order"]["client_order_id"])
        assert not baseline._fired and baseline._tick_count == tick + 1
    baseline.tools.writer.gate = original_gate
    accepted = baseline.tick()
    assert accepted["ok"] and accepted["order"]["status"] == "pending"
    assert baseline._fired and baseline._tick_count == 3
    assert len(set(refused_ids + [accepted["order"]["client_order_id"]])) == 3
    assert baseline.tick() == {"ok": True, "action": "none"}
    assert len(baseline.tools.writer.orders) == 3
    assert metric(baseline, "refusals", reason) == 2
    assert metric(baseline, "skipped", reason) == 0
    assert len(caplog.records) == 1 and reason in caplog.records[0].getMessage()


def test_random_funding_skips_preserve_decisions_and_rng_progress(monkeypatch):
    funded = agent("random", seed=42)
    unfunded = agent("random", seed=42, quote_cash=Decimal(0))
    funded.tools.writer.deposit("owner", "BTC", Decimal("3"), "synthetic-base")
    decisions = [[], []]
    for baseline, observed in zip((funded, unfunded), decisions, strict=True):
        actual = baseline._order

        def observe(*args, actual=actual, observed=observed):
            observed.append(args)
            return actual(*args)

        monkeypatch.setattr(baseline, "_order", observe)
    for tick in range(20):
        assert funded.tick()["ok"]
        assert unfunded.tick() == {"ok": False, "reason": "insufficient_funds"}
        assert decisions[0] == decisions[1]
        assert funded.random.getstate() == unfunded.random.getstate()
        assert funded._tick_count == unfunded._tick_count == tick + 1
    assert len(funded.tools.writer.orders) == 20 and not unfunded.tools.writer.orders
    assert metric(unfunded, "skipped", "insufficient_funds") == 20
    assert metric(unfunded, "refusals", "insufficient_funds") == 0
    unfunded.tools.writer.deposit("owner", "USD", Decimal("1000"), "synthetic-later-cash")
    unfunded.tools.writer.deposit("owner", "BTC", Decimal("3"), "synthetic-later-base")
    left, right = funded.tick(), unfunded.tick()
    assert left["ok"] and right["ok"]
    assert decisions[0] == decisions[1] and len(decisions[0]) == 21
    for field in ("side", "quantity", "limit_price", "reasoning"):
        assert left["order"][field] == right["order"][field]
    assert funded.random.getstate() == unfunded.random.getstate()


def test_momentum_skip_still_updates_previous_mid_and_tick_count():
    baseline = agent("momentum")
    values = iter(
        (
            {"ok": True, "mid": "100", "depth_imbalance": "0"},
            {"ok": True, "mid": "99", "depth_imbalance": "-0.3"},
            {"ok": True, "mid": "99.5", "depth_imbalance": "0.3"},
        )
    )
    baseline.tools.features = lambda product, depth: next(values)
    assert baseline.tick()["action"] == "none"
    assert baseline.tick() == {"ok": False, "reason": "insufficient_funds"}
    assert baseline._previous_mid == Decimal("99") and baseline._tick_count == 2
    result = baseline.tick()
    assert result["ok"] and result["order"]["side"] == "buy"
    assert baseline._previous_mid == Decimal("99.5") and baseline._tick_count == 3
    assert len(baseline.tools.writer.orders) == 1


def test_local_skips_count_every_occurrence_and_log_each_reason_once(caplog):
    baseline = agent("dca", quote_cash=Decimal(0))
    for _ in range(3):
        baseline._order("buy", Decimal(0), Decimal("100"), "synthetic minimum")
        baseline._order("buy", Decimal("0.1"), Decimal("100"), "synthetic cash")
    for reason in ("below_min_size", "insufficient_funds"):
        assert metric(baseline, "skipped", reason) == 3
        assert metric(baseline, "refusals", reason) == 0
    assert len(caplog.records) == 2
    assert all("skipped" in record.getMessage() for record in caplog.records)
    assert not baseline.tools.writer.orders
