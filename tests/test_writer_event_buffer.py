from uuid import UUID

from test_mcp import ClosedGate, make_tools, order_payload

from simex.exchange import DEFAULT_INSTRUMENTS, AccountWriter, FeeSchedule
from simex.exchange.runner import ExchangeRunner
from simex.exchange.runner_config import RunnerSettings
from simex.exchange.runner_metrics import RunnerMetrics


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
