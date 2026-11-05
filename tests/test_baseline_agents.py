from decimal import Decimal

import pytest
from test_mcp import make_tools

from simex.agents import BaselineAgent, BaselineSpec
from simex.exchange import GateDecision
from simex.exchange.runner_metrics import RunnerMetrics
from simex.mcp import RiskLimits


def agent(kind: str, seed: int = 7, *, metrics: RunnerMetrics | None = None) -> BaselineAgent:
    tools = make_tools()
    tools.writer.deposit("owner", "USD", Decimal("1000"), f"deposit:{kind}")
    return BaselineAgent(
        BaselineSpec(kind, "owner", seed),
        tools,
        cadence_seconds=1,
        dca_quote_amount=Decimal("10"),
        metrics=metrics,
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
            "simex_exchange_agent_refusals_total",
            {"account_id": "owner", "reason": "below_min_size"},
        )
        == 3
    )
    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 1
    assert all(value in messages[0] for value in ("dca", "owner", "below_min_size"))
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


def test_refusals_log_each_reason_once_per_agent(monkeypatch, caplog):
    metrics = RunnerMetrics()
    first = agent("dca", metrics=metrics)
    second = agent("dca", metrics=metrics)
    second.spec = BaselineSpec("dca", "other", 7)
    second.tools.account_id = "other"
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
