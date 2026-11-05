import asyncio
import threading
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID

import pytest
from test_mcp import ClosedGate, make_tools, order_payload

from simex.exchange import DEFAULT_INSTRUMENTS, AccountWriter, FeeSchedule, FillCause, Liquidity
from simex.exchange.events import EventJournal
from simex.exchange.runner import ExchangeRunner
from simex.exchange.runner_config import RunnerSettings
from simex.exchange.runner_metrics import RunnerMetrics
from simex.ledger import LedgerHalted


def test_writer_buffer_is_bounded_and_sequence_is_independent_of_length():
    writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), ClosedGate(), 0, event_buffer_size=3)
    for index in range(10):
        writer.submit("owner", order_payload(str(index)), index)
    oldest, latest, events = writer.events_after(0)
    assert len(writer.events) == 3
    assert (oldest, latest) == (8, 10)
    assert [event["seq"] for event in events] == [8, 9, 10]
    assert UUID(hex=writer.stream_id).version == 4
    assert writer.stream_id != make_tools().writer.stream_id


def test_runner_counts_rejections_after_many_buffer_wraps_and_reports_eviction():
    metrics = RunnerMetrics()
    runner = ExchangeRunner(
        RunnerSettings(pairs=("BTC-USD",), digest_path="", event_buffer_size=3),
        None,
        metrics,
    )
    writer = runner.writer
    writer.gate = ClosedGate()
    for index in range(20):
        writer.submit("owner", order_payload(str(index)), index)
        runner.release()
    assert metrics.rejections.labels("BTC-USD", "stale_level2")._value.get() == 20
    assert metrics.writer_event_gaps._value.get() == 0
    # Eviction loses two events before the next collection; retained events still count once.
    for index in range(20, 25):
        writer.submit("owner", order_payload(str(index)), index)
    runner.release()
    runner.release()
    assert metrics.rejections.labels("BTC-USD", "stale_level2")._value.get() == 23
    assert metrics.writer_event_gaps._value.get() == 2
    for index in range(25, 30):
        writer.submit("owner", order_payload(str(index)), index)
        runner.release()
    assert metrics.rejections.labels("BTC-USD", "stale_level2")._value.get() == 28


def test_halt_transitions_are_observable_once_and_scoped():
    writer = make_tools().writer
    writer.halt_account("owner")
    writer.halt_account("owner")
    writer.lift_account_halt("owner")
    writer.lift_account_halt("owner")
    writer.halted = True
    writer.halted = True
    writer.halted = False
    assert [(event["scope"], event["halted"]) for event in writer.events] == [
        ("account", True),
        ("account", False),
        ("writer", True),
        ("writer", False),
    ]


def closed_loop_watch(journal):
    loop = asyncio.new_event_loop()

    async def register():
        context = journal.watch()
        context.__enter__()
        return context

    try:
        return loop.run_until_complete(register())
    finally:
        loop.close()


@pytest.mark.parametrize("from_worker", [False, True])
def test_closed_watcher_cannot_fail_append_or_prevent_healthy_wakeup(from_worker):
    journal = EventJournal(3)
    # Retain the context explicitly: do not rely on GC to remove the closed-loop watcher.
    stale = closed_loop_watch(journal)
    errors = []

    def append():
        try:
            journal.append("halt", scope="account", account_id="owner", halted=True)
        except Exception as exc:
            errors.append(exc)

    async def scenario():
        with journal.watch() as changed:
            if from_worker:
                thread = threading.Thread(target=append)
                thread.start()
                thread.join()
            else:
                append()
            assert errors == []
            await asyncio.wait_for(changed.wait(), 1)
            assert len(journal._watchers) == 1  # Only the healthy watcher remains.

    try:
        asyncio.run(scenario())
        assert journal.after(0) == (
            1,
            1,
            (
                {
                    "seq": 1,
                    "type": "halt",
                    "scope": "account",
                    "account_id": "owner",
                    "halted": True,
                },
            ),
        )
        assert not journal._watchers
    finally:
        stale.__exit__(None, None, None)


def test_closed_watcher_does_not_mask_ledger_failure_while_halting():
    writer = make_tools().writer
    writer.deposit("owner", "USD", Decimal("1000"), "synthetic-seed")
    failure = LedgerHalted("original ledger failure")

    def fail(*args):
        raise failure

    writer.ledger = SimpleNamespace(save_order=fail)
    stale = closed_loop_watch(writer._journal)
    try:
        with pytest.raises(LedgerHalted) as raised:
            writer.submit("owner", order_payload(), 1)
        assert raised.value is failure
        assert writer.halted
        assert writer.events[-1]["type"] == "halt"
        assert not writer._journal._watchers
    finally:
        stale.__exit__(None, None, None)


def test_closed_watcher_does_not_turn_committed_fill_into_engine_exception():
    writer = make_tools().writer
    writer.deposit("owner", "USD", Decimal("1000"), "synthetic-seed")
    committed = []

    def commit(fill, *args):
        committed.append(fill.fill_id)
        return 1

    writer.ledger = SimpleNamespace(record_fill=commit, save_order=lambda *args: None)
    order = writer.submit("owner", order_payload(), 1)
    writer.activate(order.order_id, order.effective_at_ns)
    stale = closed_loop_watch(writer._journal)
    try:
        fill = writer.record_fill(
            order.order_id,
            Decimal("100"),
            Decimal("0.1"),
            Liquidity.TAKER,
            FillCause.TAKER,
            order.effective_at_ns,
        )
        assert committed == [fill.fill_id]
        assert writer.applied_sequences["owner"] == 1
        assert writer.balance_totals("owner")["BTC"] == Decimal("0.1")
        assert writer.events[-1] == {"seq": 3, "type": "fill", "fill": fill.to_dict()}
        assert not writer._journal._watchers
    finally:
        stale.__exit__(None, None, None)
