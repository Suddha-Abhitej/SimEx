import asyncio
from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace

import pytest
from prometheus_client import CollectorRegistry, generate_latest
from test_account_writer_ledger import (
    DELAY,
    T0,
    MemoryLedger,
    OpenGate,
    P,
    make_writer,
    order_payload,
)
from test_ledger import connection_or_skip
from test_mcp import make_tools

from simex.exchange import DEFAULT_INSTRUMENTS, AccountWriter, FeeSchedule, Liquidity, OrderStatus
from simex.exchange.gate import GateDecision
from simex.exchange.order_metrics import FAMILIES, REASONS, OrderMetrics
from simex.exchange.orders import FillCause, OrderError, OrderRequest
from simex.exchange.risk import RiskLimits
from simex.exchange.runner import ExchangeRunner
from simex.exchange.runner_config import RunnerSettings
from simex.exchange.runner_metrics import RunnerMetrics
from simex.ledger import LedgerHalted, SyncLedger


def measured(ledger=None):
    registry = CollectorRegistry()
    observer = OrderMetrics(DEFAULT_INSTRUMENTS)
    registry.register(observer)
    writer = make_writer(ledger)
    writer._order_observer = observer
    return writer, observer, registry


def values(observer):
    return {
        (sample.name, tuple(sorted(sample.labels.items()))): sample.value
        for family in observer.collect()
        for sample in family.samples
    }


def count(observer, outcome, product=P, reason=None):
    name = (
        "simex_exchange_order_submit_errors_total"
        if outcome == "submit_errors"
        else f"simex_exchange_orders_{outcome}_total"
    )
    labels = {"product_id": product}
    if reason is not None:
        labels["reason"] = reason
    return values(observer).get((name, tuple(sorted(labels.items()))), 0)


def totals(observer):
    return {
        outcome: sum(
            sample.value
            for family in observer.collect()
            for sample in family.samples
            if sample.name == observer._family(outcome).name + "_total"
        )
        for outcome in FAMILIES
    }


def assert_identity(observer):
    snapshot = values(observer)
    for product in observer.products:
        accepted = snapshot[("simex_exchange_orders_accepted_total", (("product_id", product),))]
        rejected = sum(
            value
            for (name, labels), value in snapshot.items()
            if name == "simex_exchange_orders_arrival_rejected_total"
            and dict(labels)["product_id"] == product
        )
        assert (
            snapshot[("simex_exchange_orders_submitted_total", (("product_id", product),))]
            == accepted + rejected
        )


def test_completed_admissions_replays_errors_and_effect_rejection_are_distinct():
    writer, observer, _ = measured()
    accepted = writer.submit("alice", order_payload(), T0)
    assert totals(observer) == dict.fromkeys(FAMILIES, 0) | {"submitted": 1, "accepted": 1}
    assert writer.submit("alice", order_payload(), T0 + 1) is accepted
    with pytest.raises(OrderError, match="idempotency_key_reused"):
        writer.submit("alice", order_payload() | {"quantity": "0.2"}, T0 + 2)
    writer.gate = SimpleNamespace(
        check=lambda *args: GateDecision(False, "feed_lagging:excess_ns=987654321")
    )
    rejected = writer.submit("alice", order_payload("rejected"), T0 + 3)
    assert rejected.status is OrderStatus.REJECTED
    assert writer.submit("alice", order_payload("rejected"), T0 + 4) is rejected
    writer.reject_on_effect(accepted.order_id, "feed_lagging:excess_ns=987654321", T0 + DELAY)
    assert count(observer, "arrival_rejected", reason="feed_lagging") == 1
    assert count(observer, "effect_rejected", reason="feed_lagging") == 1
    assert count(observer, "submit_errors", reason="idempotency_key_reused") == 1
    assert totals(observer) == {
        "submitted": 2,
        "accepted": 1,
        "arrival_rejected": 1,
        "effect_rejected": 1,
        "replayed": 2,
        "submit_errors": 1,
    }
    assert_identity(observer)


@pytest.mark.parametrize("refusal", ["risk", "funds", "gate"])
def test_arrival_rejections_have_the_writer_denominator(refusal):
    writer, observer, _ = measured()
    expected = {"risk": "quantity_limit", "funds": "insufficient_funds", "gate": "stale_level2"}[
        refusal
    ]
    if refusal == "risk":
        writer.risk_limits = RiskLimits({P: D("0.01")}, {P: D("100")}, {P: D("100")})
    elif refusal == "funds":
        writer.accounts.balance("alice", "USD").available = D(0)
    else:
        writer.gate = SimpleNamespace(
            check=lambda *args: GateDecision(False, "stale_level2:age_ns=100")
        )
    order = writer.submit("alice", order_payload(), T0)
    assert order.status is OrderStatus.REJECTED
    assert totals(observer) == dict.fromkeys(FAMILIES, 0) | {"submitted": 1, "arrival_rejected": 1}
    assert count(observer, "arrival_rejected", reason=expected) == 1
    assert_identity(observer)


@pytest.mark.parametrize("status", ["pending", "filled", "cancelled", "expired", "rejected"])
def test_restored_and_terminal_replays_do_not_invent_admissions(status):
    ledger = MemoryLedger()
    writer, observer, _ = measured(ledger)
    payload = order_payload()
    if status == "expired":
        payload = payload | {"type": "market", "limit_price": None}
    order = writer.submit("alice", payload, T0)
    if status == "filled":
        writer.activate(order.order_id, T0 + DELAY)
        writer.record_fill(
            order.order_id, D("100"), D("0.1"), Liquidity.TAKER, FillCause.TAKER, T0 + DELAY
        )
    elif status == "cancelled":
        writer.cancel_now(order.order_id, T0 + DELAY)
    elif status == "expired":
        writer.expire(order.order_id, T0 + DELAY)
    elif status == "rejected":
        writer.reject_on_effect(order.order_id, "book_not_synced", T0 + DELAY)
    replay = writer.submit("alice", payload, T0 + DELAY + 1)
    assert replay == order
    if not order.status.terminal:
        assert replay is order
    restored = AccountWriter(
        DEFAULT_INSTRUMENTS,
        FeeSchedule(),
        OpenGate(),
        DELAY,
        ledger=ledger,
        order_observer=observer,
    )
    before = totals(observer)
    restored.restore("alice")
    assert totals(observer) == before
    replay = restored.submit("alice", payload, T0 + DELAY + 2)
    assert replay.status is order.status
    assert count(observer, "submitted") == 1
    assert count(observer, "replayed") == 2
    assert_identity(observer)


@pytest.mark.parametrize(
    "kind,reason",
    [
        ("malformed", "bad_decimal"),
        ("writer_halt", "writer_halted"),
        ("account_halt", "account_halted"),
        ("persist", "persistence_failure"),
        ("unexpected", "internal_error"),
    ],
)
def test_failed_submits_keep_the_original_exception_without_admission(kind, reason):
    writer, observer, _ = measured()
    request = order_payload()
    failure = LedgerHalted("private exception contents")
    if kind == "malformed":
        request["quantity"] = 0.1
    elif kind == "writer_halt":
        writer.halted = True
    elif kind == "account_halt":
        writer.halt_account("alice")
    else:

        def fail(*args):
            raise failure

        if kind == "persist":
            writer.ledger = SimpleNamespace(save_order=fail, load_order=lambda *a, **kw: None)
        else:
            writer.gate = SimpleNamespace(check=fail)
    with pytest.raises(Exception) as raised:
        writer.submit("alice", request, T0)
    if kind in {"persist", "unexpected"}:
        assert raised.value is failure
    assert totals(observer) == dict.fromkeys(FAMILIES, 0) | {"submit_errors": 1}
    product = P if kind in {"persist", "unexpected"} else "unknown"
    assert count(observer, "submit_errors", product=product, reason=reason) == 1
    assert writer.halted == (kind in {"writer_halt", "persist"})


def test_runner_rebuild_reuses_observer_and_journal_eviction_does_not_lose_admissions():
    metrics = RunnerMetrics()
    settings = RunnerSettings(pairs=(P,), digest_path="", event_buffer_size=3)
    for _ in range(2):
        runner = ExchangeRunner(settings, None, metrics)
        runner.writer.gate = SimpleNamespace(
            check=lambda *args: GateDecision(False, "stale_level2:age_ns=100")
        )
        assert runner.writer._order_observer is metrics.orders
        for index in range(10):
            runner.writer.submit("owner", order_payload(str(index)), T0 + index)
        runner.release()
    assert count(metrics.orders, "submitted") == 20
    assert count(metrics.orders, "arrival_rejected", reason="stale_level2") == 20
    assert metrics.rejections.labels(P, "stale_level2")._value.get() == 6
    assert metrics.writer_event_gaps._value.get() == 14
    assert_identity(metrics.orders)


def test_label_sets_are_bounded_and_export_no_user_or_market_content():
    writer, observer, registry = measured()
    sensitive = "private-account-token-dsn-price-54321"
    for index in range(50):
        request = replace(
            OrderRequest.parse(order_payload(str(index))), product_id=f"{sensitive}-{index}"
        )
        writer.submit("alice", request, T0)
        observer.observe("effect_rejected", P, f"{sensitive}-{index}:age_ns={index}")
    exposition = generate_latest(registry).decode()
    assert sensitive not in exposition
    assert "100.00" not in exposition
    assert "alice" not in exposition
    assert count(observer, "arrival_rejected", product="unknown", reason="unknown_product") == 50
    assert count(observer, "effect_rejected", reason="other") == 50
    assert len(observer._snapshot) == 2
    for family in observer.collect():
        assert family.type == "counter"
        for sample in family.samples:
            assert set(sample.labels) <= {"product_id", "reason"}
            assert sample.labels["product_id"] in {*DEFAULT_INSTRUMENTS, "unknown"}
            assert sample.labels.get("reason", "other") in REASONS


def test_early_tools_validation_is_outside_writer_coverage():
    tools = make_tools()
    observer = OrderMetrics(DEFAULT_INSTRUMENTS)
    tools.writer._order_observer = observer
    payload = order_payload() | {"quantity": "0.000001", "reasoning": "synthetic"}
    result = tools.place_order(payload)
    assert result["reason"] == "below_min_size"
    assert totals(observer) == dict.fromkeys(FAMILIES, 0)
    # Bypassing tools reaches the writer and creates a recorded rejection, even for this reason.
    tools.writer.submit("owner", payload, T0)
    assert count(observer, "arrival_rejected", reason="below_min_size") == 1


def test_lost_persistence_acknowledgement_is_not_an_admission():
    ledger = MemoryLedger()
    writer, observer, _ = measured(ledger)
    original = ledger.save_order
    failure = TimeoutError("acknowledgement lost after commit")

    def commit_then_fail(*args):
        original(*args)
        raise failure

    ledger.save_order = commit_then_fail
    with pytest.raises(TimeoutError) as raised:
        writer.submit("alice", order_payload(), T0)
    assert raised.value is failure
    assert len(ledger.orders) == 1
    assert totals(observer) == dict.fromkeys(FAMILIES, 0) | {"submit_errors": 1}
    assert count(observer, "submit_errors", reason="persistence_failure") == 1


@pytest.mark.parametrize("persistence_fails", [False, True])
def test_effect_observation_follows_successful_close_and_cannot_change_it(
    monkeypatch, persistence_fails
):
    ledger = MemoryLedger()
    writer, observer, _ = measured(ledger)
    order = writer.submit("alice", order_payload(), T0)
    failure = LedgerHalted("close persistence failed")

    def fail(*args):
        raise failure

    if persistence_fails:
        ledger.save_order = fail
        with pytest.raises(LedgerHalted) as raised:
            writer.reject_on_effect(order.order_id, "catching_up", T0 + DELAY)
        assert raised.value is failure
        assert count(observer, "effect_rejected", reason="catching_up") == 0
        assert count(observer, "submitted") == 1
    else:
        monkeypatch.setattr(observer, "_publish", fail)
        writer.reject_on_effect(order.order_id, "catching_up", T0 + DELAY)
        assert ledger.orders[order.order_id].status is OrderStatus.REJECTED
        assert observer.collect() == ()
        assert not writer.halted
    assert writer.accounts.balance("alice", "USD").reserved == 0


def test_each_scrape_uses_one_immutable_snapshot(monkeypatch):
    _, observer, _ = measured()
    observer.observe("accepted", P)
    original = observer._family
    switched = False

    def family(outcome):
        nonlocal switched
        if not switched:
            switched = True
            observer.observe("arrival_rejected", P, "quantity_limit")
        return original(outcome)

    monkeypatch.setattr(observer, "_family", family)
    first = values(observer)
    assert first[("simex_exchange_orders_submitted_total", (("product_id", P),))] == 1
    assert not any("arrival_rejected" in name for name, _ in first)
    assert count(observer, "submitted") == 2
    assert_identity(observer)


@pytest.mark.parametrize("fault", ["observe", "normalize", "publish", "export", "logging"])
def test_telemetry_failure_omits_families_warns_once_and_never_changes_trading(
    monkeypatch, caplog, fault
):
    writer, observer, registry = measured()

    def fail(*args, **kwargs):
        raise RuntimeError("private failure data")

    if fault == "observe":
        monkeypatch.setattr(observer, "observe", fail)
    elif fault == "normalize":
        monkeypatch.setattr(observer, "_labels", fail)
    elif fault == "export":
        monkeypatch.setattr(observer, "_family", fail)
        assert observer.collect() == ()
    else:
        monkeypatch.setattr(observer, "_publish", fail)
        if fault == "logging":
            monkeypatch.setattr("simex.exchange.order_metrics.log.warning", fail)
    order = writer.submit("alice", order_payload(), T0)
    assert order.status is OrderStatus.PENDING
    writer.activate(order.order_id, T0 + DELAY)
    writer.record_fill(
        order.order_id, D("100"), D("0.1"), Liquidity.TAKER, FillCause.TAKER, T0 + DELAY
    )
    assert order.status is OrderStatus.FILLED
    assert writer.balance_totals("alice") == {"USD": D("989.88"), "BTC": D("10.1")}
    assert not writer.halted
    assert writer.submit("alice", order_payload(), T0 + DELAY + 1) is order
    assert observer.collect() == ()
    assert generate_latest(registry) == b""
    assert len([r for r in caplog.records if "metrics disabled" in r.message]) == (
        0 if fault == "logging" else 1
    )
    assert "private failure data" not in caplog.text
    assert [e["type"] for e in writer.events] == ["order_accepted", "order_activated", "fill"]


def test_broken_observer_and_logger_cannot_mask_original_ledger_error(monkeypatch):
    writer, observer, _ = measured()
    failure = LedgerHalted("original persistence failure")

    def fail(*args):
        raise failure

    writer.ledger = SimpleNamespace(save_order=fail, load_order=lambda *a, **kw: None)
    monkeypatch.setattr(observer, "observe", fail)
    monkeypatch.setattr("simex.exchange.order_metrics.log.warning", fail)
    with pytest.raises(LedgerHalted) as raised:
        writer.submit("alice", order_payload(), T0)
    assert raised.value is failure
    assert writer.halted
    assert observer.collect() == ()


def test_postgres_metric_failure_preserves_exact_committed_fill_and_postings(monkeypatch):
    async def scenario():
        connection = await connection_or_skip()
        ledger = None
        try:
            ledger = SyncLedger(connection.dsn)
            writer, observer, _ = measured(ledger)

            def fail(*args):
                raise RuntimeError("metric publication failed")

            monkeypatch.setattr(observer, "_publish", fail)
            order = writer.submit("alice", order_payload(), T0)
            writer.activate(order.order_id, T0 + DELAY)
            fill = writer.record_fill(
                order.order_id, D("100"), D("0.1"), Liquidity.TAKER, FillCause.TAKER, T0 + DELAY
            )
            assert await connection.fetchval("SELECT count(*) FROM fills") == 1
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM ledger_postings WHERE fill_id=$1", fill.fill_id
                )
                == 5
            )
            assert writer.balance_totals("alice") == {"USD": D("989.88"), "BTC": D("10.1")}
            assert not writer.halted
            assert observer.collect() == ()
        finally:
            if ledger is not None:
                ledger.close()
            await connection.close()

    asyncio.run(scenario())
