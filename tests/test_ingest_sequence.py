from simex.ingest.sequence import SequenceAnomaly, SequenceTracker


def test_consecutive_numbers_are_fine():
    tracker = SequenceTracker()
    assert [tracker.observe(n) for n in range(5)] == [None] * 5


def test_gap_reports_missing_count():
    tracker = SequenceTracker()
    tracker.observe(10)
    anomaly = tracker.observe(14)
    assert anomaly == SequenceAnomaly("gap", expected=11, received=14)
    assert anomaly.missing == 3
    assert tracker.observe(15) is None  # tracking continues from the new number


def test_repeat_or_backwards_is_out_of_order_and_keeps_high_water_mark():
    tracker = SequenceTracker()
    tracker.observe(5)
    assert tracker.observe(5) == SequenceAnomaly("out_of_order", expected=6, received=5)
    assert tracker.observe(3).kind == "out_of_order"
    assert tracker.observe(6) is None
    assert tracker.observe(3).missing == 0


def test_reset_starts_fresh_for_a_new_connection():
    tracker = SequenceTracker()
    tracker.observe(500)
    tracker.reset()
    assert tracker.observe(0) is None
