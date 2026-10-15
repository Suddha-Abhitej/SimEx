"""Measure the Gemini market-data feed before building on it.

Connects to Gemini's public WebSocket (no account or key) with a full-book snapshot requested
(?snapshot=-1), subscribes to {symbol}@depth@100ms and {symbol}@trade for the SimEx pairs, and
reports after N seconds:

- messages and events (price levels, trades) per second, per stream and symbol
- raw bytes per second and an extrapolated GB/day (raw JSON and gzip-compressed)
- feed lag: E vs local receive time, raw and corrected by the local clock's NTP offset
- snapshots: Gemini sends them as a depthUpdate with U == u and the full book; how they are
  detected, their size, time from SUBSCRIBE, and how the next diffs relate to them
- what U and u are: per-symbol counters, a counter shared across symbols, or timestamps.
  It prints U, u and E side by side, the distributions of u - U, U - previous u and u - E, and
  the steps between consecutive diffs of all symbols in arrival order.
- trade-ID continuity per symbol and across symbols, and trades per minute
- the shape (keys only) of every kind of frame, ping round trip and conninfo

Partway through the run it tries two ways to resync without reconnecting: a one-shot "depth"
request, and an unsubscribe and resubscribe of one symbol's depth stream.

Nothing is written to disk: per ADR 0010 captured feed data never goes in the repo. Numbers
printed are IDs, timestamps, counts and sizes; no prices or quantities.

Usage:
    pip install websockets ntplib
    python tools/gemini_probe.py --seconds 300
"""

from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field

import websockets

URL = "wss://ws.gemini.com?snapshot=-1"
DEFAULT_SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD"]
NTP_SERVER = "time.google.com"
SAMPLE_LIMIT = 5_000_000
# A U == u frame with at least this many levels is treated as a snapshot even if no SUBSCRIBE
# for its symbol is pending.
SNAPSHOT_MIN_LEVELS = 500
TABLE_ROWS = 20
FOLLOW_DIFFS = 3  # diffs reported after each snapshot or depth request


def to_seconds(value: int) -> tuple[float, str]:
    """Epoch timestamp in s, ms, us or ns (detected from its size) to seconds, and the unit."""
    for unit, scale, floor in (("ns", 1e9, 1e17), ("us", 1e6, 1e14), ("ms", 1e3, 1e11)):
        if value > floor:
            return value / scale, unit
    return float(value), "s"


def to_ns(value: int) -> int:
    for scale, floor in ((1, 10**17), (10**3, 10**14), (10**6, 10**11)):
        if value > floor:
            return value * scale
    return value * 10**9


def shape(value: object) -> object:
    """Keys and value types of a frame, without its data."""
    if isinstance(value, dict):
        return {k: shape(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [shape(value[0]), f"... {len(value)} items"] if value else []
    return type(value).__name__


def dist(values: list[int] | list[float]) -> str:
    if not values:
        return "n=0"
    ordered = sorted(values)

    def at(q: float):
        return ordered[min(len(ordered) - 1, int(len(ordered) * q))]

    return (
        f"min {ordered[0]}  p1 {at(0.01)}  p50 {at(0.5)}  p99 {at(0.99)}  max {ordered[-1]}"
        f"  (n={len(ordered)})"
    )


def clock_offset() -> float | None:
    """Seconds to add to the local clock to get true time, measured with NTP; None if unknown."""
    try:
        import ntplib
    except ImportError:
        print("WARNING: ntplib not installed (pip install ntplib); lag will not be corrected")
        return None
    try:
        response = ntplib.NTPClient().request(NTP_SERVER, version=3, timeout=5)
    except Exception as exc:  # any failure just means no correction
        print(f"WARNING: NTP query to {NTP_SERVER} failed ({exc}); lag will not be corrected")
        return None
    return response.offset


def size_of(raw: str | bytes) -> int:
    return len(raw if isinstance(raw, bytes) else raw.encode())


def levels_of(msg: dict) -> tuple[int, int]:
    bids = msg.get("b") if "b" in msg else msg.get("bids")
    asks = msg.get("a") if "a" in msg else msg.get("asks")
    return len(bids or []), len(asks or [])


@dataclass
class Follow:
    """Watches the next few diffs of a symbol after a baseline (snapshot or depth request)."""

    label: str
    baseline: int
    left: int = FOLLOW_DIFFS
    lines: list[str] = field(default_factory=list)

    def observe(self, first: int, last: int) -> None:
        self.lines.append(f"U - L = {first - self.baseline:+d}, u - L = {last - self.baseline:+d}")
        self.left -= 1


@dataclass
class Depth:
    """Everything observed on one symbol's depth stream."""

    subscribe_sent: float | None = None  # a SUBSCRIBE whose first depth frame hasn't arrived
    last_u: int | None = None
    diffs: int = 0
    before_snapshot: int = 0
    malformed: int = 0
    single_id_frames: list[str] = field(default_factory=list)  # every U == u frame
    snapshots: list[str] = field(default_factory=list)
    snapshot_bytes: int = 0
    first_frames: list[str] = field(default_factory=list)  # first depth frame after a SUBSCRIBE
    spans: list[int] = field(default_factory=list)  # u - U
    steps: list[int] = field(default_factory=list)  # U - previous u
    u_minus_e: list[int] = field(default_factory=list)  # u - E, both in ns
    step_kinds: Counter = field(default_factory=Counter)
    table: list[str] = field(default_factory=list)
    follows: list[Follow] = field(default_factory=list)


@dataclass
class Trades:
    count: int = 0
    last_id: int | None = None
    contiguous: int = 0
    gaps: int = 0
    missing: int = 0
    out_of_order: int = 0
    deltas: Counter = field(default_factory=Counter)

    def observe(self, trade_id: int) -> None:
        self.count += 1
        if self.last_id is not None:
            delta = trade_id - self.last_id
            self.deltas[delta] += 1
            if delta == 1:
                self.contiguous += 1
            elif delta > 1:
                self.gaps += 1
                self.missing += delta - 1
            else:
                self.out_of_order += 1
        if self.last_id is None or trade_id > self.last_id:
            self.last_id = trade_id

    def summary(self, minutes: float) -> str:
        top = ", ".join(f"{d:+d} x{n}" for d, n in self.deltas.most_common(4))
        return (
            f"{self.count} trades ({self.count / minutes:.1f}/min): {self.contiguous} contiguous, "
            f"{self.gaps} gaps ({self.missing} IDs skipped), {self.out_of_order} out of order"
            + (f"; top steps {top}" if top else "")
        )


class Probe:
    def __init__(self, url: str, symbols: list[str], seconds: int, resync_test: bool) -> None:
        self.url = url
        self.symbols = [s.upper() for s in symbols]
        self.seconds = seconds
        self.resync_test = resync_test
        self.elapsed = 0.0
        self.offset: float | None = None
        self.next_id = 1
        self.pending: dict[int, tuple[str, float]] = {}  # request id -> (what, sent at)
        self.responses: list[str] = []
        self.shapes: dict[str, object] = {}
        self.msgs: Counter = Counter()
        self.events: Counter = Counter()
        self.raw_bytes = 0
        self.sample = bytearray()
        self.lags: dict[str, list[float]] = defaultdict(list)
        self.time_units: Counter = Counter()
        self.depth = {s: Depth() for s in self.symbols}
        self.trades = {s: Trades() for s in self.symbols}
        self.all_trades = Trades()
        self.last_seen: dict[str, float] = {}
        self.max_silence: dict[str, float] = defaultdict(float)
        self.diff_intervals: dict[str, list[float]] = defaultdict(list)
        self.unknown = 0
        # All symbols' diffs in arrival order, to see whether IDs are shared across symbols.
        self.interleaved: list[str] = []
        self.global_last_u: int | None = None
        self.global_steps: list[int] = []

    # Requests ----------------------------------------------------------------------------------

    async def request(self, ws, method: str, params: object = None, what: str = "") -> None:
        frame: dict = {"method": method, "id": self.next_id}
        if params is not None:
            frame["params"] = params
        self.pending[self.next_id] = (what or method, time.monotonic())
        self.next_id += 1
        await ws.send(json.dumps(frame))

    # Frames ------------------------------------------------------------------------------------

    @staticmethod
    def classify(msg: dict) -> str:
        if "id" in msg and ("result" in msg or "error" in msg or "status" in msg):
            return "response"
        if "lastUpdateId" in msg and "bids" in msg and "asks" in msg:
            return "snapshot_envelope"
        if "U" in msg and "u" in msg:
            return "depth"
        if "t" in msg and "p" in msg and "q" in msg:
            return "trade"
        return "unknown"

    @staticmethod
    def symbol_of(msg: dict) -> str | None:
        symbol = msg.get("s") or msg.get("symbol")
        return symbol.upper() if isinstance(symbol, str) else None

    def on_frame(self, raw: str | bytes, received: float, mono: float) -> None:
        size = size_of(raw)
        msg = json.loads(raw)
        if not isinstance(msg, dict):
            self.unknown += 1
            return
        kind = self.classify(msg)
        if kind == "unknown":
            self.unknown += 1
            self.shapes.setdefault(f"unknown {sorted(msg)}", shape(msg))
            return
        if kind == "response":
            self.shapes.setdefault("response", shape(msg))
            self.on_response(msg, size, mono)
            return

        symbol = self.symbol_of(msg)
        if kind == "snapshot_envelope":
            self.shapes.setdefault(kind, shape(msg))
            self.responses.append(f"unexpected lastUpdateId snapshot frame for {symbol}, {size} B")
            return

        if kind == "depth" and symbol in self.depth:
            first, last = msg.get("U"), msg.get("u")
            if not (isinstance(first, int) and isinstance(last, int)):
                self.depth[symbol].malformed += 1
                return
            if self.on_depth(symbol, msg, first, last, size, mono):
                return  # a snapshot: not counted in stream throughput

        stream = f"{kind}:{symbol}"
        self.shapes.setdefault(kind, shape(msg))
        self.msgs[stream] += 1
        self.raw_bytes += size
        if len(self.sample) < SAMPLE_LIMIT:
            self.sample += raw if isinstance(raw, bytes) else raw.encode()
        if symbol in self.last_seen:
            self.max_silence[symbol] = max(self.max_silence[symbol], mono - self.last_seen[symbol])
        self.last_seen[symbol] = mono
        if isinstance(msg.get("E"), int):
            exchange_s, unit = to_seconds(msg["E"])
            self.time_units[unit] += 1
            self.lags[kind].append(received - exchange_s)

        if kind == "depth":
            self.events[stream] += sum(levels_of(msg))
            key = f"diff:{symbol}"
            if key in self.last_seen:
                self.diff_intervals[symbol].append(mono - self.last_seen[key])
            self.last_seen[key] = mono
        elif kind == "trade" and symbol in self.trades:
            self.events[stream] += 1
            if isinstance(msg.get("t"), int):
                self.trades[symbol].observe(msg["t"])
                self.all_trades.observe(msg["t"])

    def on_depth(
        self, symbol: str, msg: dict, first: int, last: int, size: int, mono: float
    ) -> bool:
        """Record a depth frame; return True if it is a snapshot."""
        d = self.depth[symbol]
        bids, asks = levels_of(msg)
        after_subscribe = d.subscribe_sent is not None
        since = (
            f"{1000 * (mono - d.subscribe_sent):.0f} ms after SUBSCRIBE" if after_subscribe else ""
        )
        if first == last:
            d.single_id_frames.append(
                f"{bids + asks} levels, {size} B" + (f", first frame {since}" if since else "")
            )
        if after_subscribe:
            d.first_frames.append(f"U {'==' if first == last else '!='} u, {bids + asks} levels")
            d.subscribe_sent = None

        is_snapshot = first == last and (after_subscribe or bids + asks >= SNAPSHOT_MIN_LEVELS)
        if is_snapshot:
            why = "first frame after SUBSCRIBE" if after_subscribe else "large U == u frame"
            d.snapshots.append(
                f"{bids} bids + {asks} asks = {bids + asks} levels, {size} B ({size / 1e3:.0f} KB)"
                f", {since or 'no SUBSCRIBE pending'}; detected as {why}; U = u = {last}"
            )
            d.snapshot_bytes += size
            d.last_u = last
            d.follows.append(Follow(f"snapshot at {last}", last))
            return True

        d.diffs += 1
        for follow in d.follows:
            if follow.left > 0:
                follow.observe(first, last)
        if first > last:
            d.malformed += 1
        if d.last_u is None:
            d.before_snapshot += 1
            return False

        step = first - d.last_u
        d.steps.append(step)
        d.spans.append(last - first)
        d.step_kinds["U == prev u + 1"] += step == 1
        d.step_kinds["U <= prev u (overlap/repeat)"] += step <= 0
        d.step_kinds["U > prev u + 1 (skip)"] += step > 1
        d.step_kinds["u < prev u (goes backwards)"] += last < d.last_u
        e_ns = to_ns(msg["E"]) if isinstance(msg.get("E"), int) else None
        if e_ns is not None:
            d.u_minus_e.append(last - e_ns)
        if len(d.table) < TABLE_ROWS:
            d.table.append(
                f"{e_ns or '-':>20} {first:>20} {last:>20} {step:>14} {last - first:>12} "
                f"{(last - e_ns) if e_ns is not None else '-':>14} {sum(levels_of(msg)):>6}"
            )
        d.last_u = max(d.last_u, last)

        if self.global_last_u is not None:
            self.global_steps.append(first - self.global_last_u)
        if len(self.interleaved) < 30:
            gstep = "" if self.global_last_u is None else f"{first - self.global_last_u:+d}"
            self.interleaved.append(f"{symbol} U {first} u {last}  U - prev u (any symbol) {gstep}")
        self.global_last_u = last
        return False

    def on_response(self, msg: dict, size: int, mono: float) -> None:
        what, sent = self.pending.pop(msg.get("id"), ("unsolicited", mono))
        rtt = (mono - sent) * 1000
        result = msg.get("result")
        if what.startswith("depth request "):
            symbol = what.removeprefix("depth request ")
            d = self.depth[symbol]
            if isinstance(result, dict) and isinstance(result.get("lastUpdateId"), int):
                bids, asks = levels_of(result)
                lid = result["lastUpdateId"]
                vs_live = (
                    ""
                    if d.last_u is None
                    else f", lastUpdateId - live last u = {lid - d.last_u:+d}"
                )
                now_ns = time.time_ns() + round((self.offset or 0) * 1e9)
                self.responses.append(
                    f"{what}: {rtt:.0f} ms, {size} B ({size / 1e3:.0f} KB), {bids} bids + {asks} "
                    f"asks = {bids + asks} levels, keys {sorted(result)}{vs_live}, "
                    f"lastUpdateId - now = {(lid - now_ns) / 1e6:+.0f} ms if read as epoch ns"
                )
                d.follows.append(Follow(f"depth request at {lid}", lid))
            else:
                self.responses.append(f"{what}: {rtt:.0f} ms, {size} B, shape {shape(msg)}")
            return
        if what == "conninfo":
            # Connection metadata, not market data: printed in full.
            self.responses.append(f"conninfo: {rtt:.0f} ms, {json.dumps(msg)}")
            return
        self.responses.append(f"{what}: {rtt:.0f} ms, shape {shape(msg)}")

    # Run ---------------------------------------------------------------------------------------

    async def run(self) -> None:
        self.offset = await asyncio.to_thread(clock_offset)
        if self.offset is not None:
            print(
                f"Local clock offset vs {NTP_SERVER}: {self.offset * 1000:+.0f} ms (true - local)"
            )
        depth = [f"{s.lower()}@depth@100ms" for s in self.symbols]
        trade = [f"{s.lower()}@trade" for s in self.symbols]
        async with websockets.connect(self.url, max_size=None, open_timeout=15) as ws:
            sent = time.monotonic()
            for symbol in self.symbols:
                self.depth[symbol].subscribe_sent = sent
            await self.request(ws, "SUBSCRIBE", depth + trade, "subscribe")
            await self.request(ws, "ping")
            await self.request(ws, "conninfo")
            symbols = ", ".join(self.symbols)
            print(f"Connected to {self.url}. Sampling {self.seconds}s of {symbols} ...")

            start = time.monotonic()
            deadline = start + self.seconds
            tests = [(0.5, "depth"), (0.75, "resubscribe")] if self.resync_test else []
            while (remaining := deadline - time.monotonic()) > 0:
                if tests and time.monotonic() - start >= tests[0][0] * self.seconds:
                    await self.run_test(ws, tests.pop(0)[1])
                wait = remaining
                if tests:
                    wait = min(wait, start + tests[0][0] * self.seconds - time.monotonic())
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(0.001, wait))
                except TimeoutError:
                    continue
                self.on_frame(raw, time.time(), time.monotonic())
                self.elapsed = time.monotonic() - start
            await self.request(ws, "LIST_SUBSCRIPTIONS", what="list_subscriptions")
            try:
                while self.pending:
                    raw = await asyncio.wait_for(ws.recv(), timeout=2)
                    self.on_frame(raw, time.time(), time.monotonic())
            except TimeoutError:
                pass

    async def run_test(self, ws, test: str) -> None:
        if test == "depth":
            symbol = self.symbols[1 % len(self.symbols)]
            await self.request(ws, "depth", {"symbol": symbol.lower()}, f"depth request {symbol}")
        else:
            symbol = self.symbols[0]
            stream = f"{symbol.lower()}@depth@100ms"
            await self.request(ws, "UNSUBSCRIBE", [stream], f"unsubscribe {symbol}")
            self.depth[symbol].subscribe_sent = time.monotonic()
            await self.request(ws, "SUBSCRIBE", [stream], f"resubscribe {symbol}")

    # Report ------------------------------------------------------------------------------------

    def report(self) -> None:
        elapsed = self.elapsed or 1.0
        print(f"\nSampled {elapsed:.0f} s.")
        print("\nPer stream (snapshots excluded):")
        for stream in sorted(self.msgs):
            print(
                f"  {stream:<14} {self.msgs[stream] / elapsed:8.2f} msg/s"
                f"  {self.events[stream] / elapsed:9.2f} events/s"
            )
        ratio = len(gzip.compress(bytes(self.sample))) / len(self.sample) if self.sample else 1.0
        raw_gb_day = self.raw_bytes / elapsed * 86_400 / 1e9
        print(f"\nRaw throughput:    {self.raw_bytes / elapsed / 1024:8.1f} KB/s")
        print(f"Raw JSON per day:  {raw_gb_day:8.2f} GB/day")
        print(f"gzip ratio:        {ratio:8.2f}")
        print(f"Compressed/day:    {raw_gb_day * ratio:8.2f} GB/day")

        print("\nFeed lag, E vs receive time:")
        print(f"  E unit detected: {dict(self.time_units) or 'no E field'}")
        if self.offset is None:
            print("  clock offset unknown: corrected lag not available")
        for kind, lags in sorted(self.lags.items()):
            ms = sorted(lag * 1000 for lag in lags)
            p99 = ms[int(len(ms) * 0.99)]
            line = f"  {kind:<6} raw  p50 {statistics.median(ms):6.0f}  p99 {p99:6.0f}"
            if self.offset is not None:
                corr = [v + self.offset * 1000 for v in ms]
                line += (
                    f"  | corrected p50 {statistics.median(corr):6.0f}"
                    f"  p99 {corr[int(len(corr) * 0.99)]:6.0f}  min {corr[0]:6.0f}"
                )
            print(f"{line} ms  (n={len(ms)})")

        for symbol, d in self.depth.items():
            print(f"\n=== {symbol} depth ===")
            intervals = self.diff_intervals.get(symbol)
            cadence = (
                f", median {statistics.median(intervals) * 1000:.0f} ms apart" if intervals else ""
            )
            print(
                f"  {d.diffs} diffs{cadence}; {d.before_snapshot} before any snapshot, "
                f"{d.malformed} with U > u"
            )
            print(f"  First depth frame after each SUBSCRIBE: {d.first_frames or 'none seen'}")
            print(f"  U == u frames ({len(d.single_id_frames)}):")
            for line in d.single_id_frames:
                print(f"    {line}")
            print(f"  Snapshots ({d.snapshot_bytes / 1e6:.2f} MB):")
            for line in d.snapshots:
                print(f"    {line}")
            for follow in d.follows:
                print(f"  Next diffs after {follow.label}: {follow.lines or 'none'}")
            print("  After the first snapshot:")
            for kind, n in d.step_kinds.items():
                print(f"    {kind:<30} {n}")
            print(f"    u - U:          {dist(d.spans)}")
            print(f"    U - previous u: {dist(d.steps)}")
            print(f"    u - E (ns):     {dist(d.u_minus_e)}")
            if d.table:
                print(f"  First {len(d.table)} diffs after the first snapshot:")
                print(
                    f"  {'E (ns)':>20} {'U':>20} {'u':>20} {'U - prev u':>14} {'u - U':>12} "
                    f"{'u - E':>14} {'levels':>6}"
                )
                for row in d.table:
                    print(f"  {row}")

        print("\n=== Are update IDs shared across symbols? (all symbols, arrival order) ===")
        for line in self.interleaved:
            print(f"  {line}")
        print(f"  U - previous u of any symbol: {dist(self.global_steps)}")
        print(f"  steps of exactly +1 across symbols: {sum(s == 1 for s in self.global_steps)}")

        print("\n=== Verdict on U/u ===")
        for symbol, d in self.depth.items():
            print(f"  {symbol}: {verdict(d)}")

        minutes = elapsed / 60
        print("\nTrade IDs:")
        for symbol, trades in self.trades.items():
            print(f"  {symbol}: {trades.summary(minutes)}")
        print(f"  all symbols, arrival order: {self.all_trades.summary(minutes)}")

        print("\nLongest silence per symbol (any stream):")
        for symbol in self.symbols:
            print(f"  {symbol}: {self.max_silence.get(symbol, float('nan')):.1f} s")

        print("\nRequests:")
        for line in self.responses:
            print(f"  {line}")
        for request_id, (what, _) in self.pending.items():
            print(f"  {what} (id {request_id}): no response")

        print(f"\nFrames of unknown kind: {self.unknown}")
        print("Frame shapes (keys and types only):")
        for kind, frame_shape in sorted(self.shapes.items()):
            print(f"  {kind}: {json.dumps(frame_shape)}")


def verdict(d: Depth) -> str:
    if not d.steps:
        return "not enough diffs after a snapshot"
    contiguous = sum(s == 1 for s in d.steps) / len(d.steps)
    if contiguous >= 0.99:
        return f"per-symbol counter: {contiguous:.1%} of diffs have U == prev u + 1"
    if d.u_minus_e and abs(statistics.median(d.u_minus_e)) < 1_000_000_000 and d.steps[0] != 1:
        return (
            f"u tracks E (median u - E {statistics.median(d.u_minus_e) / 1e6:+.1f} ms): "
            "U/u look like epoch-ns timestamps, not counters"
        )
    return f"unclear: only {contiguous:.1%} contiguous; see the distributions above"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seconds", type=int, default=300, help="sampling time (default 300)")
    parser.add_argument("--url", default=URL, help=f"WebSocket URL (default {URL})")
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS, help="Gemini symbols")
    parser.add_argument(
        "--no-resync-test",
        dest="resync_test",
        action="store_false",
        help="skip the depth-request and resubscribe tests",
    )
    args = parser.parse_args()
    probe = Probe(args.url, args.symbols, args.seconds, args.resync_test)
    try:
        asyncio.run(probe.run())
    finally:
        probe.report()


if __name__ == "__main__":
    main()
