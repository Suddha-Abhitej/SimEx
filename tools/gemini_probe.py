"""Measure the Gemini market-data feed before building on it.

Connects to Gemini's public WebSocket (no account or key) with a full-book snapshot requested
(?snapshot=-1), subscribes to {symbol}@depth@100ms and {symbol}@trade for the SimEx pairs, and
reports after N seconds:

- messages and events (price levels, trades) per second, per stream and symbol
- raw bytes per second and an extrapolated GB/day (raw JSON and gzip-compressed)
- feed lag: the E field vs local receive time (needs an NTP-synced clock; E's unit is detected)
- snapshot size per symbol and how its lastUpdateId chains to the first diff's U/u
- update-ID continuity per symbol: whether every diff's U == previous u + 1
- whether trade IDs are contiguous, per symbol and across symbols
- the shape (keys only) of every kind of frame, ping round trip and conninfo

It also tries the two ways to resync without reconnecting, partway through the run:

- unsubscribe and resubscribe one symbol's depth stream: is a fresh snapshot sent, how fast,
  and do later diffs chain onto it?
- the one-shot "depth" request: what it returns and whether live diffs chain onto it.

Nothing is written to disk: per ADR 0010 captured feed data never goes in the repo.

Usage:
    pip install websockets
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
SAMPLE_LIMIT = 5_000_000


def to_seconds(value: int) -> tuple[float, str]:
    """Epoch timestamp in s, ms, us or ns (detected from its size) to seconds, and the unit."""
    for unit, scale, floor in (("ns", 1e9, 1e17), ("us", 1e6, 1e14), ("ms", 1e3, 1e11)):
        if value > floor:
            return value / scale, unit
    return float(value), "s"


def shape(value: object) -> object:
    """Keys and value types of a frame, without its data."""
    if isinstance(value, dict):
        return {k: shape(v) for k, v in sorted(value.items())}
    if isinstance(value, list):
        return [shape(value[0]), f"... {len(value)} items"] if value else []
    return type(value).__name__


def percentile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * q))]


@dataclass
class Chain:
    """Update-ID continuity of one symbol's depth diffs."""

    last_u: int | None = None
    baseline: str = ""  # what last_u came from: "snapshot", "depth request" or "diff"
    diffs: int = 0
    contiguous: int = 0  # U == last u + 1
    overlapping: int = 0  # U <= last u < u: some IDs repeated, still usable
    stale: int = 0  # u <= last u: entirely old, skipped
    gaps: int = 0  # U > last u + 1: IDs missing
    missing_ids: int = 0
    malformed: int = 0  # U > u
    single_id: int = 0  # U == u
    before_snapshot: int = 0
    first_after_baseline: list[str] = field(default_factory=list)
    gap_examples: list[str] = field(default_factory=list)

    def rebase(self, last_update_id: int, source: str) -> None:
        self.last_u, self.baseline = last_update_id, source

    def observe(self, first: int, last: int) -> None:
        self.diffs += 1
        if first > last:
            self.malformed += 1
            return
        if first == last:
            self.single_id += 1
        if self.last_u is None:
            self.before_snapshot += 1
            return
        if self.baseline != "diff" and len(self.first_after_baseline) < 10:
            relation = (
                "U == L+1"
                if first == self.last_u + 1
                else f"U <= L+1 <= u (U = L{first - self.last_u - 1:+d}+1)"
                if first <= self.last_u + 1 <= last
                else f"u <= L (stale by {self.last_u - last})"
                if last <= self.last_u
                else f"U > L+1 (gap of {first - self.last_u - 1})"
            )
            self.first_after_baseline.append(f"after {self.baseline}: {relation}")
        if last <= self.last_u:
            self.stale += 1
            return  # does not advance the chain
        if first == self.last_u + 1:
            self.contiguous += 1
        elif first <= self.last_u:
            self.overlapping += 1
        else:
            self.gaps += 1
            self.missing_ids += first - self.last_u - 1
            if len(self.gap_examples) < 5:
                self.gap_examples.append(f"last u {self.last_u}, next U {first}")
        self.rebase(last, "diff")


@dataclass
class Trades:
    count: int = 0
    last_id: int | None = None
    deltas: Counter = field(default_factory=Counter)

    def observe(self, trade_id: int) -> None:
        self.count += 1
        if self.last_id is not None:
            self.deltas[trade_id - self.last_id] += 1
        self.last_id = trade_id

    def summary(self) -> str:
        if not self.deltas:
            return f"{self.count} trades"
        steps = sum(self.deltas.values())
        top = ", ".join(f"{d:+d} x{n}" for d, n in self.deltas.most_common(4))
        return f"{self.count} trades, {self.deltas[1]}/{steps} steps of +1 (top: {top})"


class Probe:
    def __init__(self, url: str, symbols: list[str], seconds: int, resync_test: bool) -> None:
        self.url = url
        self.elapsed = 0.0
        self.symbols = [s.upper() for s in symbols]
        self.seconds = seconds
        self.resync_test = resync_test
        self.next_id = 1
        self.pending: dict[int, tuple[str, float]] = {}  # request id -> (what, sent at)
        self.responses: list[str] = []
        self.shapes: dict[str, object] = {}
        self.msgs: Counter = Counter()
        self.events: Counter = Counter()
        self.raw_bytes = 0
        self.sample = bytearray()
        self.snapshots: list[str] = []
        self.snapshot_bytes = 0
        self.lags: dict[str, list[float]] = defaultdict(list)
        self.time_units: Counter = Counter()
        self.chains = {s: Chain() for s in self.symbols}
        self.trades = {s: Trades() for s in self.symbols}
        self.all_trades = Trades()
        self.last_seen: dict[str, float] = {}
        self.max_silence: dict[str, float] = defaultdict(float)
        self.diff_intervals: dict[str, list[float]] = defaultdict(list)
        self.unknown = 0
        # Symbols whose depth was (re)subscribed and whose snapshot hasn't arrived yet, in order;
        # used to route a snapshot frame that carries no symbol.
        self.awaiting_snapshot: list[str] = []
        self.resubscribed_at: dict[str, float] = {}

    async def request(self, ws, method: str, params: object = None, what: str = "") -> None:
        frame: dict = {"method": method, "id": self.next_id}
        if params is not None:
            frame["params"] = params
        self.pending[self.next_id] = (what or method, time.monotonic())
        self.next_id += 1
        await ws.send(json.dumps(frame))

    def symbol_of(self, msg: dict) -> str | None:
        symbol = msg.get("s") or msg.get("symbol")
        return symbol.upper() if isinstance(symbol, str) else None

    def classify(self, msg: dict) -> str:
        if "id" in msg and ("result" in msg or "error" in msg or "status" in msg):
            return "response"
        if "lastUpdateId" in msg and "bids" in msg and "asks" in msg:
            return "snapshot"
        return str(msg.get("e") or "unknown")

    def on_frame(self, raw: str | bytes, received: float, mono: float) -> None:
        size = len(raw if isinstance(raw, bytes) else raw.encode())
        msg = json.loads(raw)
        if not isinstance(msg, dict):
            self.unknown += 1
            return
        kind = self.classify(msg)
        self.shapes.setdefault(kind, shape(msg))
        if kind == "response":
            self.on_response(msg, mono)
            return
        if kind == "snapshot":
            self.on_snapshot(msg, size, mono)
            return

        symbol = self.symbol_of(msg)
        stream = f"{kind}:{symbol}"
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

        if kind == "depthUpdate" and symbol in self.chains:
            self.events[stream] += len(msg.get("b") or []) + len(msg.get("a") or [])
            first, last = msg.get("U"), msg.get("u")
            if isinstance(first, int) and isinstance(last, int):
                self.chains[symbol].observe(first, last)
            else:
                self.chains[symbol].malformed += 1
            key = f"diff:{symbol}"
            if key in self.last_seen:
                self.diff_intervals[symbol].append(mono - self.last_seen[key])
            self.last_seen[key] = mono
        elif kind == "trade" and symbol in self.trades:
            self.events[stream] += 1
            if isinstance(msg.get("t"), int):
                self.trades[symbol].observe(msg["t"])
                self.all_trades.observe(msg["t"])
        elif kind not in ("depthUpdate", "trade"):
            self.unknown += 1

    def on_snapshot(self, msg: dict, size: int, mono: float) -> None:
        symbol = self.symbol_of(msg)
        labelled = symbol is not None
        if symbol is None and self.awaiting_snapshot:
            symbol = self.awaiting_snapshot[0]
        if symbol in self.awaiting_snapshot:
            self.awaiting_snapshot.remove(symbol)
        levels = len(msg.get("bids") or []) + len(msg.get("asks") or [])
        note = ""
        if symbol in self.resubscribed_at:
            note = f", {1000 * (mono - self.resubscribed_at.pop(symbol)):.0f} ms after resubscribe"
        self.snapshots.append(
            f"{symbol or '?'}: {size / 1e3:.0f} KB, {levels} levels, lastUpdateId "
            f"{msg.get('lastUpdateId')}{'' if labelled else ' (no symbol in frame)'}{note}"
        )
        self.snapshot_bytes += size
        if symbol in self.chains and isinstance(msg.get("lastUpdateId"), int):
            self.chains[symbol].rebase(msg["lastUpdateId"], "snapshot")

    def on_response(self, msg: dict, mono: float) -> None:
        what, sent = self.pending.pop(msg.get("id"), ("unsolicited", mono))
        rtt = (mono - sent) * 1000
        result = msg.get("result")
        if what.startswith("depth request "):
            symbol = what.removeprefix("depth request ")
            chain = self.chains[symbol]
            if isinstance(result, dict) and isinstance(result.get("lastUpdateId"), int):
                levels = len(result.get("bids") or []) + len(result.get("asks") or [])
                ahead = result["lastUpdateId"] - (chain.last_u or 0)
                self.responses.append(
                    f"{what}: {rtt:.0f} ms, {levels} levels, lastUpdateId = live chain's last u "
                    f"{ahead:+d}, keys {sorted(result)}"
                )
                if ahead > 0:  # newer than the live chain: later diffs should chain onto it
                    chain.rebase(result["lastUpdateId"], "depth request")
            else:
                self.responses.append(f"{what}: {rtt:.0f} ms, shape {shape(msg)}")
            return
        if what == "conninfo":
            # Connection metadata, not market data: printed in full.
            self.responses.append(f"conninfo: {rtt:.0f} ms, {json.dumps(msg)}")
            return
        self.responses.append(f"{what}: {rtt:.0f} ms, shape {shape(msg)}")

    async def run(self) -> None:
        depth = [f"{s.lower()}@depth@100ms" for s in self.symbols]
        trade = [f"{s.lower()}@trade" for s in self.symbols]
        async with websockets.connect(self.url, max_size=None, open_timeout=15) as ws:
            self.awaiting_snapshot = list(self.symbols)
            await self.request(ws, "SUBSCRIBE", depth + trade, "subscribe")
            await self.request(ws, "ping")
            await self.request(ws, "conninfo")
            symbols = ", ".join(self.symbols)
            print(f"Connected to {self.url}. Sampling {self.seconds}s of {symbols} ...")

            start = time.monotonic()
            deadline = start + self.seconds
            tests = []
            if self.resync_test and self.symbols:
                tests = [(0.5, "depth"), (0.75, "resubscribe")]
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
            params = {"symbol": symbol.lower()}
            await self.request(ws, "depth", params, f"depth request {symbol}")
        else:
            symbol = self.symbols[0]
            stream = f"{symbol.lower()}@depth@100ms"
            await self.request(ws, "UNSUBSCRIBE", [stream], f"unsubscribe {symbol}")
            await self.request(ws, "SUBSCRIBE", [stream], f"resubscribe {symbol}")
            self.awaiting_snapshot.append(symbol)
            self.resubscribed_at[symbol] = time.monotonic()

    def report(self) -> None:
        elapsed = self.elapsed or 1.0
        print(f"\nSampled {elapsed:.0f} s.")
        print("\nPer stream (snapshots excluded):")
        for stream in sorted(self.msgs):
            print(
                f"  {stream:<22} {self.msgs[stream] / elapsed:8.2f} msg/s"
                f"  {self.events[stream] / elapsed:9.2f} events/s"
            )
        ratio = len(gzip.compress(bytes(self.sample))) / len(self.sample) if self.sample else 1.0
        raw_gb_day = self.raw_bytes / elapsed * 86_400 / 1e9
        print(f"\nRaw throughput:    {self.raw_bytes / elapsed / 1024:8.1f} KB/s")
        print(f"Raw JSON per day:  {raw_gb_day:8.2f} GB/day")
        print(f"gzip ratio:        {ratio:8.2f}")
        print(f"Compressed/day:    {raw_gb_day * ratio:8.2f} GB/day")

        print(f"\nSnapshots ({self.snapshot_bytes / 1e6:.2f} MB total):")
        for line in self.snapshots:
            print(f"  {line}")

        print("\nFeed lag, E vs receive time (clock must be synced):")
        print(f"  E unit detected: {dict(self.time_units) or 'no E field'}")
        for kind, lags in sorted(self.lags.items()):
            print(
                f"  {kind:<12} p50 {statistics.median(lags) * 1000:7.0f} ms"
                f"  p99 {percentile(lags, 0.99) * 1000:7.0f} ms"
                f"  min {min(lags) * 1000:7.0f} ms  (n={len(lags)})"
            )

        print("\nDepth update IDs per symbol (rule under test: U == previous u + 1):")
        for symbol, c in self.chains.items():
            intervals = self.diff_intervals.get(symbol)
            cadence = ""
            if intervals:
                cadence = f", median {statistics.median(intervals) * 1000:.0f} ms apart"
            print(
                f"  {symbol}: {c.diffs} diffs{cadence}; contiguous {c.contiguous}, "
                f"VIOLATIONS {c.gaps} gaps ({c.missing_ids} IDs missing) + {c.overlapping} "
                f"overlapping + {c.stale} stale + {c.malformed} malformed; "
                f"U == u in {c.single_id}; before any snapshot {c.before_snapshot}"
            )
            for line in c.first_after_baseline:
                print(f"      first diff {line}")
            for line in c.gap_examples:
                print(f"      gap: {line}")

        print("\nTrade IDs:")
        for symbol, trades in self.trades.items():
            print(f"  {symbol}: {trades.summary()}")
        print(f"  all symbols, arrival order: {self.all_trades.summary()}")

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
