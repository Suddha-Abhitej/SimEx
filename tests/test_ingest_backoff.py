import pytest

from simex.ingest.backoff import Backoff


def upper(lo: float, hi: float) -> float:
    return hi


def test_ceiling_doubles_up_to_the_maximum():
    backoff = Backoff(initial=1, maximum=10, rng=upper)
    assert [backoff.next_delay() for _ in range(6)] == [1, 2, 4, 8, 10, 10]


def test_full_jitter_draws_between_zero_and_ceiling():
    calls = []

    def record(lo, hi):
        calls.append((lo, hi))
        return (lo + hi) / 2

    backoff = Backoff(initial=0.5, maximum=60, rng=record)
    assert backoff.next_delay() == 0.25
    backoff.next_delay()
    assert calls == [(0.0, 0.5), (0.0, 1.0)]


def test_real_jitter_stays_in_range():
    backoff = Backoff(initial=1, maximum=8)
    for _ in range(200):
        ceiling = backoff.ceiling()
        assert 0 <= backoff.next_delay() <= ceiling <= 8


def test_reset_starts_over():
    backoff = Backoff(initial=1, maximum=60, rng=upper)
    for _ in range(5):
        backoff.next_delay()
    backoff.reset()
    assert backoff.next_delay() == 1


def test_many_attempts_do_not_overflow():
    backoff = Backoff(initial=1, maximum=60, rng=upper)
    backoff.attempt = 10_000
    assert backoff.next_delay() == 60


@pytest.mark.parametrize(("initial", "maximum"), [(0, 1), (2, 1), (-1, 5)])
def test_invalid_settings(initial, maximum):
    with pytest.raises(ValueError):
        Backoff(initial, maximum)
