"""Measure event-buffer retention under an explicitly synthetic refusal-heavy cadence.

This is an operational retention experiment, not a live-market or ledger throughput claim.
Every tick submits a valid-shaped order to a closed gate, so every retained event is a rejection
and there are no fills. Run with the intended cadence and enough events to wrap the buffer:

    python tools/measure_api_event_retention.py --orders-per-second 1000
"""

from __future__ import annotations

import argparse
import json
import time
from collections import deque

from simex.exchange import DEFAULT_INSTRUMENTS, AccountWriter, FeeSchedule, GateDecision
from simex.exchange.events import DEFAULT_EVENT_BUFFER_SIZE


class ClosedGate:
    def check(self, product_id: str, now_ns: int) -> GateDecision:
        return GateDecision(False, "stale_level2")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--orders-per-second", type=int, required=True)
    parser.add_argument("--buffer-size", type=int, default=DEFAULT_EVENT_BUFFER_SIZE)
    parser.add_argument("--events", type=int, default=12_000)
    args = parser.parse_args()
    if args.orders_per_second <= 0 or args.buffer_size <= 0 or args.events <= args.buffer_size:
        parser.error("cadence and buffer must be positive; events must exceed buffer size")
    writer = AccountWriter(
        DEFAULT_INSTRUMENTS,
        FeeSchedule(),
        ClosedGate(),
        200_000_000,
        event_buffer_size=args.buffer_size,
    )
    retained_times: deque[float] = deque(maxlen=args.buffer_size)
    started = time.perf_counter()
    for index in range(args.events):
        target = started + index / args.orders_per_second
        remaining = target - time.perf_counter()
        if remaining > 0:
            time.sleep(remaining)
        writer.submit(
            "synthetic",
            {
                "client_order_id": f"refusal-{index}",
                "product_id": "BTC-USD",
                "side": "buy",
                "type": "limit",
                "quantity": "0.00001",
                "limit_price": "100",
                "reasoning": "synthetic retention measurement",
            },
            time.perf_counter_ns(),
        )
        retained_times.append(time.perf_counter())
    elapsed = time.perf_counter() - started
    rate = writer.event_sequence / elapsed
    print(
        json.dumps(
            {
                "workload": "synthetic closed-gate orders; no ledger; not a throughput measurement",
                "events": writer.event_sequence,
                "fills": 0,
                "rejections": writer.event_sequence,
                "buffer_size": len(writer.events),
                "elapsed_seconds": elapsed,
                "observed_events_per_second": rate,
                "observed_retained_minutes": (retained_times[-1] - retained_times[0]) / 60,
                "capacity_minutes_at_observed_rate": args.buffer_size / rate / 60,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
