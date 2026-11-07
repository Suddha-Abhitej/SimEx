"""Measure the private API using its SDK, simulated money and a self-launched runtime.

NEVER point this at the trading ledger. An explicit, empty simex_load_* database and fresh
load_* account are mandatory; simex/simex_test and arbitrary API URLs are rejected. No .env
is read. Only the normal ingestor talks to Gemini; this harness consumes Kafka. Results are
system aggregates, never prices or individual market events (ADR 0010).

Run on Linux. Windows latency is unreliable at the historical 15.625 ms granularity (#75).
A container does not repair its host clock: unverified absolute feed lag is unavailable (#74).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import platform
import queue
import secrets
import socket
import statistics
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import httpx
from load_test_runtime import check_empty_database, committed_orders, validate_destination
from prometheus_client.parser import text_string_to_metric_families
from websockets.exceptions import ConnectionClosed

from simex.client import APIError, Client
from simex.exchange import DEFAULT_INSTRUMENTS


def distribution(values):
    values = sorted(values)
    if not values:
        return {"count": 0, "median_ms": None, "p99_ms": None}
    if values[0] < 0:
        return {
            "count": len(values),
            "median_ms": None,
            "p99_ms": None,
            "unavailable": "negative interval; clock changed or causal timing is invalid",
        }
    return {
        "count": len(values),
        "median_ms": statistics.median(values) / 1e6,
        "p99_ms": values[math.ceil(len(values) * 0.99) - 1] / 1e6,
    }


def clock_status():
    try:
        result = subprocess.run(
            ["timedatectl", "show", "--property=NTPSynchronized", "--value"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        if result.returncode == 0 and result.stdout.strip() == "yes":
            return {"verified": True, "source": "timedatectl NTPSynchronized=yes"}
    except (OSError, subprocess.TimeoutExpired):
        pass
    return {"verified": False, "source": "host clock synchronisation could not be verified"}


def host_metadata():
    steps = []
    previous = time.perf_counter_ns()
    for _ in range(10000):
        current = time.perf_counter_ns()
        if current > previous:
            steps.append(current - previous)
        previous = current
    cpu = "unavailable"
    if Path("/proc/cpuinfo").exists():
        cpu = next(
            (
                line.split(":", 1)[1].strip()
                for line in Path("/proc/cpuinfo").read_text().splitlines()
                if line.startswith("model name")
            ),
            cpu,
        )
    memory = None
    if Path("/proc/meminfo").exists():
        memory = int(Path("/proc/meminfo").read_text().splitlines()[0].split()[1])
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parent.parent,
    ).stdout.strip()
    return {
        "date_utc": datetime.now(UTC).isoformat(),
        "commit": commit,
        "os": platform.platform(),
        "architecture": platform.machine(),
        "environment": "Linux devcontainer" if Path("/.dockerenv").exists() else platform.system(),
        "cpu": cpu,
        "logical_cpus": os.cpu_count(),
        "visible_memory_kib": memory,
        "python": platform.python_version(),
        "clock": clock_status(),
        "perf_counter_resolution_ns": time.get_clock_info("perf_counter").resolution * 1e9,
        "monotonic_resolution_ns": time.get_clock_info("monotonic").resolution * 1e9,
        "wall_clock_resolution_ns": time.get_clock_info("time").resolution * 1e9,
        "minimum_observed_perf_counter_step_ns": min(steps) if steps else None,
        "windows_latency_unreliable": platform.system() == "Windows",
    }


class CountingClient(Client):
    """Count actual SDK dispatches, including its same-key transport retries."""

    attempts = 0

    def _request(self, method, path, operation, **kwargs):
        if method == "POST" and path == "v1/orders":
            self.attempts += 1
        return super()._request(method, path, operation, **kwargs)


def submit(client, intent):
    started = time.perf_counter_ns()
    before = client.attempts
    reason, outcome = None, "accepted"
    try:
        client.place_order(**intent)
    except APIError as exc:
        reason, outcome = str(exc.reason).split(":", 1)[0], "refused"
    except (httpx.NetworkError, httpx.TimeoutException, httpx.RemoteProtocolError):
        outcome = "uncertain"
    return {
        "client_order_id": intent["client_order_id"],
        "started_ns": started,
        "finished_ns": time.perf_counter_ns(),
        "attempts": client.attempts - before,
        "outcome": outcome,
        "reason": reason,
    }


class FillReceiver:
    def __init__(self, url, token):
        self.client = Client(url, token)
        self.stream = self.client.stream()  # Connect/save hello before the first order.
        if any(
            self.stream.hello.get(key)
            for key in ("writer_halted", "account_halted", "ledger_halted")
        ):
            self.stream.close()
            self.client.close()
            raise ValueError("load runtime hello reports a halt")
        self.received = {}
        self.error = None
        self.stopping = False
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def apply(self, frame):
        if frame["type"] == "fill":
            self.received.setdefault(frame["fill"]["fill_id"], time.perf_counter_ns())
            self.stream.ack(frame)  # Applied to the measurement map before checkpointing.
        elif frame["type"] == "halt" and frame.get("halted"):
            raise ValueError("load runtime halted")

    def run(self):
        try:
            for frame in self.stream:
                self.apply(frame)
        except Exception as exc:
            if not self.stopping or not isinstance(exc, ConnectionClosed):
                self.error = type(exc).__name__

    def close(self):
        self.stopping = True
        self.stream.close()
        self.thread.join(timeout=5)
        self.client.close()


@contextmanager
def runtime(config):
    # Launch only our own runtime; credentials travel over its pipe, never argv/logs/files.
    env = {k: v for k, v in os.environ.items() if not k.startswith(("EXCHANGE_", "SIMEX_"))}
    env["PYTHON_DOTENV_DISABLED"] = "1"
    child = subprocess.Popen(
        [sys.executable, "-u", str(Path(__file__).with_name("load_test_runtime.py"))],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env=env,
    )
    responses = queue.Queue()

    def read():
        for line in child.stdout:
            try:
                responses.put(json.loads(line))
            except ValueError:
                responses.put({"error": "invalid measurement-runtime response"})
        responses.put({"error": "measurement runtime exited"})

    reader = threading.Thread(target=read, daemon=True)
    reader.start()

    def response(timeout=30):
        try:
            value = responses.get(timeout=timeout)
        except queue.Empty:
            raise ValueError("measurement runtime did not respond") from None
        if "error" in value:
            raise ValueError(value["error"])
        return value

    def snapshot(*, stop=False):
        child.stdin.write(json.dumps({"op": "stop" if stop else "snapshot"}) + "\n")
        child.stdin.flush()
        return response()

    try:
        child.stdin.write(json.dumps(config) + "\n")
        child.stdin.flush()
        ready = response()
        yield ready, snapshot
    finally:
        with suppress(BrokenPipeError):
            child.stdin.close()
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)
        child.stdout.close()
        reader.join(timeout=5)


class Sampler:
    def __init__(self, urls):
        self.urls, self.samples, self.failures = urls, [], 0
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        with httpx.Client(timeout=1, trust_env=False) as client:
            while not self.stop.is_set():
                values = defaultdict(list)
                for url in self.urls:
                    try:
                        response = client.get(url)
                        response.raise_for_status()
                        for family in text_string_to_metric_families(response.text):
                            for sample in family.samples:
                                if sample.name in {
                                    "simex_exchange_watermark_lag_seconds",
                                    "simex_exchange_feed_lag_p99_seconds",
                                    "simex_exchange_late_events_total",
                                    "simex_exchange_gate_closed_seconds_total",
                                    "simex_exchange_lag_baseline_established",
                                    "simex_ledger_reconciliation_runs_total",
                                    "simex_ledger_reconciliation_mismatches_total",
                                    "simex_ledger_halts_total",
                                    "simex_ingest_last_feed_lag_seconds",
                                }:
                                    if sample.labels.get("channel", "level2") == "level2":
                                        values[sample.name].append(sample.value)
                                elif (
                                    sample.name == "simex_exchange_gate_state"
                                    and sample.labels.get("state") == "open"
                                ):
                                    values["gate_open"].append(sample.value)
                                elif sample.name in {
                                    "simex_exchange_delivery_seconds_sum",
                                    "simex_exchange_delivery_seconds_count",
                                }:
                                    values[sample.name].append(sample.value)
                    except (httpx.HTTPError, ValueError):
                        self.failures += 1
                self.samples.append((time.perf_counter_ns(), dict(values)))
                self.stop.wait(1)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join(timeout=5)


def run_phase(url, token, product, quantity, rate, clients, seconds, prefix, receiver):
    results, local = [], threading.local()
    # SDK object construction isn't server capacity. Each worker owns one prebuilt client;
    # its first HTTP connection is still part of measured request latency.
    opened = [CountingClient(url, token) for _ in range(clients)]
    unused = queue.Queue()
    for client in opened:
        unused.put(client)
    lock, capacity = threading.Lock(), threading.Semaphore(clients)
    offered = math.ceil(rate * seconds)
    busy, futures = 0, []

    def send(index):
        try:
            if not hasattr(local, "client"):
                local.client = unused.get_nowait()
            intent = dict(
                client_order_id=f"{prefix}.{index}",
                product_id=product,
                side="buy",
                type="market",
                quantity=quantity,
                reasoning="isolated API load measurement",
            )
            result = submit(local.client, intent)
            with lock:
                results.append(result)
        finally:
            capacity.release()

    started = time.perf_counter_ns()
    try:
        with ThreadPoolExecutor(max_workers=clients) as pool:
            for index in range(offered):
                if receiver.error:
                    raise ValueError(
                        f"fill observation failed: {receiver.error}; no continuous result"
                    )
                wait = started / 1e9 + index / rate - time.perf_counter()
                if wait > 0:
                    time.sleep(wait)
                if capacity.acquire(blocking=False):
                    futures.append(pool.submit(send, index))
                else:
                    busy += 1
            measured_end = started + int(seconds * 1e9)
            delay = measured_end / 1e9 - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            for future in futures:
                future.result()
    finally:
        for client in opened:
            client.close()
    return {
        "rate": rate,
        "clients": clients,
        "seconds": seconds,
        "offered": offered,
        "client_busy": busy,
        "results": results,
        "started_ns": started,
        "ended_ns": measured_end,
        "requests_drained_ns": time.perf_counter_ns(),
    }


def metric_ranges(samples, clock_verified):
    metrics = defaultdict(list)
    for _, values in samples:
        for name, points in values.items():
            metrics[name].extend(points)
    negative = any(
        value < 0 for name, values in metrics.items() if "feed_lag" in name for value in values
    )
    summary = {}
    for name, values in metrics.items():
        finite = [value for value in values if math.isfinite(value)]
        summary[name] = {
            "samples": len(values),
            "minimum": min(finite) if finite else None,
            "maximum": max(finite) if finite else None,
        }
        if "feed_lag" in name and (not clock_verified or negative):
            summary[name] = {
                "samples": len(values),
                "minimum": None,
                "maximum": None,
                "unavailable": "unverified host clock or negative lag",
            }
    return summary


def summarise(phase, orders, fills, observations, received, samples, clock_verified):
    attempts = phase["results"]
    ids = {item["client_order_id"] for item in attempts}
    rows = [row for row in orders if row["client_order_id"] in ids]
    order_ids = {row["order_id"] for row in rows}
    durable = {fill["fill_id"] for fill in fills if fill["order_id"] in order_ids}
    observed = [fill for fill in observations["fills"] if fill["client_order_id"] in ids]
    if any(fill["fill_id"] not in durable for fill in observed):
        raise ValueError("observed fill missing from committed ledger; invalid measurement")
    starts = {item["client_order_id"]: item["started_ns"] for item in attempts}
    start, end = phase["started_ns"], phase["ended_ns"]

    def latency(key):
        return distribution([item[key] for item in observed if item[key] is not None])

    # Acceptance HTTP responses and final order status differ: an accepted order can later
    # be rejected at effect. Keep both and confirm identities via the committed SQL snapshot.
    ack = Counter(item["outcome"] for item in attempts)
    acknowledged = {item["client_order_id"] for item in attempts if item["outcome"] == "accepted"}
    if acknowledged - {row["client_order_id"] for row in rows}:
        raise ValueError("accepted order missing from committed ledger; invalid measurement")
    metric_summary = metric_ranges(
        [(at, values) for at, values in samples if start <= at <= end], clock_verified
    )
    in_window = {
        item["client_order_id"]
        for item in attempts
        if item["outcome"] == "accepted" and item["finished_ns"] <= end
    }
    proved_acceptances = sum(row["client_order_id"] in in_window for row in rows)
    filled_within = {
        fill["order_id"]
        for fill in observed
        if start <= fill["observed_ns"] <= end and fill["fill_id"] in durable
    }
    writer_calls = [
        elapsed
        for at, elapsed in observations["ledger"].get("writer_ack", [])
        if start <= at <= end
    ]
    return {
        "offered_per_second": phase["rate"],
        "concurrent_clients": phase["clients"],
        "window_seconds": phase["seconds"],
        "request_drain_tail_seconds": (phase["requests_drained_ns"] - end) / 1e9,
        "offered": phase["offered"],
        "client_busy_not_sent": phase["client_busy"],
        "sent_intentions": len(attempts),
        "http_dispatches": sum(item["attempts"] for item in attempts),
        "retries": sum(max(0, item["attempts"] - 1) for item in attempts),
        "http_outcomes": dict(ack),
        "refusals_by_reason": dict(Counter(item["reason"] for item in attempts if item["reason"])),
        "committed_orders": len(rows),
        "missing_order_rows": len(ids - {row["client_order_id"] for row in rows}),
        "committed_terminal_outcomes": dict(Counter(row["status"] for row in rows)),
        "committed_reject_reasons": dict(
            Counter(row["reject_reason"].split(":", 1)[0] for row in rows if row["reject_reason"])
        ),
        "durable_accepted_acks_within_window": proved_acceptances,
        "durable_accepted_acks_per_second": proved_acceptances / phase["seconds"],
        "committed_fills": len(durable),
        "orders_with_committed_fill_within_window": len(filled_within),
        "orders_with_committed_fill_per_second": len(filled_within) / phase["seconds"],
        "ledger_operations_per_second": len(writer_calls) / phase["seconds"],
        "ledger_writer_ack_mean_ms": statistics.mean(writer_calls) / 1e6 if writer_calls else None,
        "sdk_observed_committed_fills": len(durable & received.keys()),
        "committed_fills_not_observed": len(durable - received.keys()),
        "request_latency": distribution(
            [item["finished_ns"] - item["started_ns"] for item in attempts]
        ),
        "tick_to_committed_fill": latency("tick_to_fill_ns"),
        "submit_to_committed_fill": latency("submit_to_fill_ns"),
        "taker_submit_to_committed_fill": distribution(
            [item["submit_to_fill_ns"] for item in observed if item.get("liquidity") == "taker"]
        ),
        "maker_submit_to_committed_fill": distribution(
            [item["submit_to_fill_ns"] for item in observed if item.get("liquidity") == "maker"]
        ),
        "sdk_submit_to_fill_observation": distribution(
            [
                received[item["fill_id"]] - starts[item["client_order_id"]]
                for item in observed
                if item["fill_id"] in received
            ]
        ),
        "maker_fills_without_causal_tick": sum(
            item["maker_without_causal_tick"] for item in observed
        ),
        "ledger_phase_latency": {
            name: distribution([elapsed for at, elapsed in values if start <= at <= end])
            for name, values in observations["ledger"].items()
        },
        "metric_snapshots": metric_summary,
    }


def positive_decimal(value):
    try:
        number = Decimal(value)
        if number.is_finite() and number > 0:
            return value
    except ArithmeticError:
        pass
    raise argparse.ArgumentTypeError("pass a positive finite decimal string")


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", required=True)
    p.add_argument("--account", required=True)
    p.add_argument("--product", choices=DEFAULT_INSTRUMENTS, default="BTC-USD")
    p.add_argument("--quantity", type=positive_decimal, default="0.00001")
    p.add_argument("--max-quantity", type=positive_decimal, default="0.001")
    p.add_argument("--max-notional", type=positive_decimal, default="1000")
    p.add_argument("--max-position", type=positive_decimal, default="1")
    p.add_argument("--rates", type=int, nargs="+", default=[10, 25, 50, 75, 100])
    p.add_argument("--clients", type=int, nargs="+", default=[1, 4, 8])
    p.add_argument("--seconds", type=float, default=15)
    p.add_argument("--warmup-seconds", type=float, default=3)
    p.add_argument("--drain-seconds", type=float, default=3)
    p.add_argument("--maker-seconds", type=float, default=60)
    p.add_argument("--startup-seconds", type=float, default=120)
    p.add_argument("--kafka", default="127.0.0.1:9092")
    p.add_argument("--ingest-metrics", default="http://127.0.0.1:9101/metrics")
    p.add_argument("--json", type=Path, help="local aggregate summary; never prices or fill IDs")
    p.add_argument("--conditions", required=True, help="hardware/storage/resource allocation notes")
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    validate_destination(args.dsn, args.account)
    for value in (
        args.seconds,
        args.warmup_seconds,
        args.drain_seconds,
        args.maker_seconds,
        args.startup_seconds,
    ):
        if not math.isfinite(value) or value < 0:
            raise ValueError("durations must be finite and nonnegative")
    if args.seconds <= 0 or args.startup_seconds <= 0 or min(args.rates + args.clients) <= 0:
        raise ValueError("rates, clients and measurement/startup durations must be positive")
    if sum(args.rates) * (args.seconds + args.warmup_seconds) * len(args.clients) > 50000:
        raise ValueError("a run is bounded to 50,000 offered intentions; split larger experiments")
    asyncio.run(check_empty_database(args.dsn, args.account))
    host = host_metadata()
    print(
        "Starting isolated runtime; waiting for a fresh book. No trading ledger is used.",
        flush=True,
    )
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        api_port = listener.getsockname()[1]
    config = dict(
        dsn=args.dsn,
        account=args.account,
        product=args.product,
        kafka=args.kafka,
        api_port=api_port,
        token=secrets.token_urlsafe(32),
        max_quantity=args.max_quantity,
        max_notional=args.max_notional,
        max_position=args.max_position,
    )
    phases = []
    with runtime(config) as (ready, snapshot):
        url = f"http://127.0.0.1:{api_port}"
        with Client(url, config["token"]) as client:
            deadline = time.perf_counter() + args.startup_seconds
            while True:
                try:
                    quote = client.quote(args.product)
                    if time.time_ns() - quote["received_at_ns"] <= 5_000_000_000:
                        break
                except (APIError, httpx.HTTPError):
                    pass
                if time.perf_counter() > deadline:
                    raise ValueError(
                        "no fresh book; was the Gemini ingestor running on this Kafka?"
                    )
                time.sleep(0.25)
        receiver = FillReceiver(url, config["token"])
        sampler = Sampler(
            [f"http://127.0.0.1:{ready['metrics_port']}/metrics", args.ingest_metrics]
        )
        try:
            with sampler:
                time.sleep(1)  # Pre-load scrape; later scrapes continue through draining.
                for clients in args.clients:
                    for rate in args.rates:
                        number = len(phases)
                        if args.warmup_seconds:
                            run_phase(
                                url,
                                config["token"],
                                args.product,
                                args.quantity,
                                rate,
                                clients,
                                args.warmup_seconds,
                                f"warm.{number}",
                                receiver,
                            )
                            time.sleep(args.drain_seconds)
                        print(
                            f"Measuring {rate}/s offered, {clients} connections to one account",
                            flush=True,
                        )
                        phase = run_phase(
                            url,
                            config["token"],
                            args.product,
                            args.quantity,
                            rate,
                            clients,
                            args.seconds,
                            f"measure.{number}",
                            receiver,
                        )
                        phases.append(phase)
                        time.sleep(args.drain_seconds)
                # Honest live maker observation: one minimum-size limit at the current bid.
                # It may never fill. Never create a synthetic print to make the row nonempty.
                if args.maker_seconds:
                    with CountingClient(url, config["token"]) as client:
                        quote = client.quote(args.product)
                        started = time.perf_counter_ns()
                        result = submit(
                            client,
                            dict(
                                client_order_id="maker.0",
                                product_id=args.product,
                                side="buy",
                                type="limit",
                                quantity=args.quantity,
                                limit_price=quote["bids"][0][0],
                                reasoning="isolated maker latency observation, not a strategy",
                            ),
                        )
                        time.sleep(args.maker_seconds)
                        phases.append(
                            dict(
                                rate=1 / args.maker_seconds,
                                clients=1,
                                seconds=args.maker_seconds,
                                offered=1,
                                client_busy=0,
                                results=[result],
                                started_ns=started,
                                ended_ns=time.perf_counter_ns(),
                                requests_drained_ns=time.perf_counter_ns(),
                                maker_observation=True,
                            )
                        )
                        for order in client.open_orders()["orders"]:
                            client.cancel(
                                order["order_id"]
                            )  # Requested, not immediately cancelled.
                        time.sleep(args.drain_seconds)
                time.sleep(1)  # Post-load scrape.
            if receiver.error:
                raise ValueError(f"fill observation failed: {receiver.error}; no continuous result")
            receiver.stopping = True  # Expected close after all measured/drain phases.
            observations = snapshot(stop=True)
            receiver.close()
            if receiver.error:
                raise ValueError(f"fill observation failed: {receiver.error}; no continuous result")
            orders, fills = asyncio.run(
                committed_orders(
                    args.dsn,
                    args.account,
                    [item["client_order_id"] for phase in phases for item in phase["results"]],
                )
            )
            reports = [
                summarise(
                    phase,
                    orders,
                    fills,
                    observations,
                    receiver.received,
                    sampler.samples,
                    host["clock"]["verified"],
                )
                for phase in phases
            ]
            report = {
                "schema_version": 1,
                "host": host,
                "conditions": args.conditions,
                "experiment": {
                    "product": args.product,
                    "rates": args.rates,
                    "clients": args.clients,
                    "warmup_seconds": args.warmup_seconds,
                    "drain_seconds": args.drain_seconds,
                    "maker_observation_seconds": args.maker_seconds,
                },
                "policy": {**ready["policy"], "quantity": args.quantity},
                "metric_scrape_failures": sampler.failures,
                "metrics_before": metric_ranges(sampler.samples[:1], host["clock"]["verified"]),
                "metrics_after": metric_ranges(sampler.samples[-1:], host["clock"]["verified"]),
                "phases": reports,
                "notes": [
                    "One account; concurrent connections, not multi-tenancy.",
                    "Nearest-rank p99; exact latency samples, not histogram interpolation.",
                    "Warm-ups excluded from order cohorts; resource/history effects persist.",
                    "Accepted-and-filled orders perform several durable operations; "
                    "50 operations/s is a hypothesis, not an orders/s target.",
                    "Refusal-dominated and client-limited plateaus do not establish "
                    "the server acceptance ceiling.",
                    "No causal tick means tick-to-fill n=0/unavailable; "
                    "timer-driven taker fills stay separate.",
                    "Absolute feed lag unavailable unless host synchronization is verified "
                    "and samples are nonnegative.",
                ],
            }
            for phase, result in zip(phases, reports, strict=True):
                result["maker_observation"] = phase.get("maker_observation", False)
                if host["windows_latency_unreliable"]:
                    for value in list(result.values()) + list(
                        result["ledger_phase_latency"].values()
                    ):
                        if isinstance(value, dict) and "median_ms" in value:
                            value["unreliable"] = (
                                "Windows: historical 15.625 ms timer granularity (#75)"
                            )
            output = json.dumps(report, indent=2, allow_nan=False)
            if args.json:
                args.json.write_text(output + "\n")
            print(output)
        finally:
            receiver.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except (ValueError, OSError, httpx.HTTPError) as exc:
        # Database exceptions are sanitized at connect; never print DSNs or payloads.
        print(f"load_test: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
