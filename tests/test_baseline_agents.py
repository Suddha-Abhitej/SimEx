from decimal import Decimal

from test_mcp import make_tools

from simex.agents import BaselineAgent, BaselineSpec
from simex.mcp import RiskLimits


def agent(kind: str, seed: int = 7) -> BaselineAgent:
    tools = make_tools()
    tools.writer.deposit("owner", "USD", Decimal("1000"), f"deposit:{kind}")
    return BaselineAgent(
        BaselineSpec(kind, "owner", seed),
        tools,
        cadence_seconds=1,
        dca_quote_amount=Decimal("10"),
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
    baseline.tools.limits = RiskLimits({"BTC-USD": Decimal("0.000001")}, {"BTC-USD": Decimal("1")})

    assert baseline.tick()["reason"] == "below_min_size"
