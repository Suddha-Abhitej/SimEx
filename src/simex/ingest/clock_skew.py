"""Detection only: persistent negative level2 lag suggests clock disagreement (ADR 0025)."""

from dataclasses import dataclass

CONFIRM_SECONDS = 60.0
MIN_OBSERVATIONS = 3


@dataclass(frozen=True)
class NegativeLagWindow:
    started_at: float
    last_observed_at: float
    observations: int


@dataclass
class ClockSkewMonitor:
    stale_after_seconds: float
    window: NegativeLagWindow | None = None
    warned: bool = False

    def break_coverage(self) -> None:
        """An outage or unknown timestamp is not recovery; retain the warning episode latch."""
        self.window = None

    def observe(self, lag_ns: int | None, now: float) -> NegativeLagWindow | None:
        """Return the window once per episode, rearmed only by a nonnegative observation."""
        if lag_ns is None:
            self.break_coverage()
            return None
        if lag_ns >= 0:
            self.break_coverage()
            self.warned = False
            return None
        window = self.window
        if window is None or now - window.last_observed_at > self.stale_after_seconds:
            self.window = NegativeLagWindow(now, now, 1)
        else:
            self.window = NegativeLagWindow(window.started_at, now, window.observations + 1)
        if self.suspected(now) and not self.warned:
            self.warned = True
            return self.window
        return None

    def suspected(self, now: float) -> int:
        """Pure scrape callback: 0 means not currently confirmed, never proof of clock sync."""
        # Prometheus scrapes on another thread: read one immutable observation window.
        window = self.window
        return int(
            window is not None
            and window.last_observed_at - window.started_at >= CONFIRM_SECONDS
            and window.observations >= MIN_OBSERVATIONS
            and now - window.last_observed_at <= self.stale_after_seconds
        )
