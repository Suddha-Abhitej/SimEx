"""Measure the startup cliff, never change recovery or use a trading ledger (#107).

Linux only. Requires an explicit neutral maintenance DSN via a named environment variable;
creates/drops only simex_restore_<uuid4 hex>. No .env fallback. Use a separate Postgres instance
for large seeds. Reports contain synthetic workload counts and system measurements only, never
prices, amounts, identifiers, traffic, tokens or DSNs. See docs/restore-cost.md.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import os
import platform
import select
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import suppress
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

from restore_cost_fixture import (
    ACCOUNT,
    ACTIVE,
    FEES,
    RATIONALE,
    REJECTED,
    MeasurementSkipped,
    check_headroom,
    disposable_database,
    intention,
    seed,
    validate_target,
)

from simex.exchange import DEFAULT_INSTRUMENTS, AccountWriter
from simex.exchange.orders import OrderError
from simex.ledger import LedgerHalted, SyncLedger
from simex.ledger.sync import HARD_TIMEOUT_SECONDS, STARTUP_TIMEOUT_SECONDS

GIB = 1024**3
DIAGNOSTIC_SECONDS = 120
WORKER_DSN = "SIMEX_RESTORE_WORKER_DSN"
RESULT_FIELDS = frozenset(
    {
        "status",
        "restore_ns",
        "load_ns",
        "construct_ns",
        "rss_before_bytes",
        "rss_after_bytes",
        "rss_delta_bytes",
        "peak_rss_bytes",
        "response_bytes",
        "orders_rows",
        "reservations_rows",
        "balances_rows",
        "latest_sequence",
        "restored_orders",
        "restored_keys",
        "checked_keys",
        "active_fetch_ns",
        "posting_fold_ns",
    }
)
RESULT_FIELDS |= {"lookup_benchmark_ok", "lookup_first_miss_ns"} | {
    f"lookup_{case}_{stat}"
    for case in ("miss", "live", "terminal", "conflict")
    for stat in ("count", "median_ns", "p99_ns")
}
STATUSES = {"ok", "timeout", "ledger_halted", "error", "rss_limit", "wall_limit", "memory_pressure"}


def memory_info(path: str = "/proc/meminfo") -> dict[str, int]:
    return {
        line.split(":", 1)[0]: int(line.split()[1]) * 1024
        for line in Path(path).read_text().splitlines()
        if len(line.split()) >= 2
    }


def rss(pid: int | str = "self") -> int:
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    return 0


class CountingRelay:
    """Loopback byte counter; forwards opaque bytes without retaining or logging traffic."""

    def __init__(self, dsn: str):
        upstream = urlsplit(dsn)
        self.destination = (upstream.hostname or "127.0.0.1", upstream.port or 5432)
        self.received = 0
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.stop = threading.Event()
        self.failed = False
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self):
        try:
            while not self.stop.is_set():
                try:
                    client, _ = self.listener.accept()
                    break
                except TimeoutError:
                    continue
            else:
                return
            with client, socket.create_connection(self.destination, timeout=5) as server:
                server.settimeout(5)
                client.settimeout(5)
                while not self.stop.is_set():
                    ready, _, _ = select.select([client, server], [], [], 0.1)
                    for source in ready:
                        chunk = source.recv(65536)
                        if not chunk:
                            return
                        if source is server:
                            self.received += len(
                                chunk
                            )  # Count before making bytes visible to client.
                        (client if source is server else server).sendall(chunk)
        except OSError:
            if not self.stop.is_set():
                self.failed = True

    def close(self):
        self.stop.set()
        self.thread.join(6)
        self.listener.close()
        if self.thread.is_alive():
            raise RuntimeError("relay did not stop")


def relay_dsn(dsn: str, port: int) -> str:
    # Keep auth/database/TLS options, changing only the destination to the local relay.
    url = urlsplit(dsn)
    auth = url.netloc.rsplit("@", 1)[0] + "@" if "@" in url.netloc else ""
    return url._replace(netloc=f"{auth}127.0.0.1:{port}").geturl()


class LoadProbe:
    """Delegate unchanged load_account; retain counts only, not another AccountState reference."""

    def __init__(self, ledger):
        self.ledger = ledger
        self.elapsed = 0
        self.counts = {}

    def __getattr__(self, name):
        return getattr(self.ledger, name)

    def load_account(self, account):
        started = time.perf_counter_ns()
        try:
            state = self.ledger.load_account(account)
            self.counts = {
                "orders_rows": len(state.orders),
                "reservations_rows": len(state.reservations),
                "balances_rows": len(state.balances),
                "latest_sequence": state.latest_sequence,
            }
            return state
        finally:
            self.elapsed = time.perf_counter_ns() - started


class QueryProbe:
    """Time existing recovery queries without retaining their rows or changing SQL."""

    def __init__(self, connection):
        self.connection = connection
        self.elapsed = {}

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def fetch(self, sql, *args):
        started = time.perf_counter_ns()
        try:
            return await self.connection.fetch(sql, *args)
        finally:
            phase = (
                "posting_fold_ns"
                if "FROM ledger_postings" in sql
                else "active_fetch_ns"
                if "FROM orders" in sql
                else None
            )
            if phase:
                self.elapsed[phase] = time.perf_counter_ns() - started


def verify_keys(writer, count: int) -> int:
    checked = 0
    for index in (0, ACTIVE, count - 1):
        request = intention(index)
        original = writer.lookup_order(ACCOUNT, client_order_id=request.client_order_id)
        assert original is not None
        assert writer.submit(ACCOUNT, request, 0) == original
        try:
            writer.submit(ACCOUNT, replace(request, quantity=request.quantity + Decimal("0.1")), 0)
        except OrderError as exc:
            assert exc.reason == "idempotency_key_reused"
        else:
            raise AssertionError("historical key allowed a different request")
        checked += 1
    assert len(writer.orders) == len(writer.by_key) == ACTIVE
    return checked


def lookup_benchmark(writer, count: int) -> dict:
    """Bounded writer/read microbenchmark; seeded caches, not an HTTP capacity claim."""
    request = intention(count - 1)
    different = replace(request, reasoning=request.reasoning + " different")
    before = writer.balance_snapshot(ACCOUNT)
    result = {"lookup_benchmark_ok": 0}
    started = time.perf_counter_ns()
    assert writer.lookup_order(ACCOUNT, client_order_id="absent-first") is None
    result["lookup_first_miss_ns"] = time.perf_counter_ns() - started
    for case in ("miss", "live", "terminal", "conflict"):
        times = []
        for index in range(100):
            started = time.perf_counter_ns()
            if case == "miss":
                assert writer.lookup_order(ACCOUNT, client_order_id=f"absent-{index}") is None
            elif case == "conflict":
                try:
                    writer.submit(ACCOUNT, different, 0)
                except OrderError as exc:
                    assert exc.reason == "idempotency_key_reused"
                else:
                    raise AssertionError("historical conflict accepted")
            else:
                writer.submit(ACCOUNT, intention(0) if case == "live" else request, 0)
            times.append(time.perf_counter_ns() - started)
        times.sort()
        result.update(
            {
                f"lookup_{case}_count": len(times),
                f"lookup_{case}_median_ns": int(statistics.median(times)),
                f"lookup_{case}_p99_ns": times[98],
            }
        )
    assert writer.balance_snapshot(ACCOUNT) == before
    assert len(writer.orders) == len(writer.by_key) == ACTIVE
    result["lookup_benchmark_ok"] = 1
    return result


def worker(config: dict) -> dict:
    import resource  # Linux only; do not import it when Windows collects the tests.

    dsn = os.environ[WORKER_DSN]
    validate_target(dsn)
    relay = CountingRelay(dsn) if config["mode"] == "bytes" else None
    ledger = None
    result = {"status": "error"}
    try:
        timeout = (
            STARTUP_TIMEOUT_SECONDS
            if config["mode"] == "default"
            else HARD_TIMEOUT_SECONDS
            if config["mode"] == "compatibility"
            else DIAGNOSTIC_SECONDS
        )
        kwargs = {"restore_timeout_seconds": timeout}
        ledger = SyncLedger(relay_dsn(dsn, relay.port) if relay else dsn, **kwargs)
        queries = QueryProbe(ledger._adapter.connection)
        ledger._adapter.connection = queries
        probe = LoadProbe(ledger)
        writer = AccountWriter(DEFAULT_INSTRUMENTS, FEES, None, 200_000_000, ledger=probe)
        gc.collect()
        before_rss = rss()
        before_bytes = relay.received if relay else 0  # Authentication excluded.
        started = time.perf_counter_ns()
        try:
            writer.restore(ACCOUNT)
            result["status"] = "ok"
        except TimeoutError:
            result["status"] = "timeout"
        except LedgerHalted:
            result["status"] = "ledger_halted"
        elapsed = time.perf_counter_ns() - started
        if relay:
            if relay.failed:
                result["status"] = "error"
            result["response_bytes"] = (
                relay.received - before_bytes if result["status"] == "ok" else None
            )
        else:
            gc.collect()
            after_rss = rss()
            result.update(
                restore_ns=elapsed,
                load_ns=probe.elapsed,
                construct_ns=elapsed - probe.elapsed if result["status"] == "ok" else None,
                rss_before_bytes=before_rss,
                rss_after_bytes=after_rss,
                rss_delta_bytes=after_rss - before_rss,
                peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
            )
        result.update(
            probe.counts,
            **({} if relay else queries.elapsed),
            restored_orders=len(writer.orders),
            restored_keys=len(writer.by_key),
        )
        if result["status"] == "ok":
            assert len(writer.orders) == len(writer.by_key) == ACTIVE
            assert len(writer.accounts.reservations) == ACTIVE
            if config["mode"] == "default":
                try:
                    result.update(lookup_benchmark(writer, config["orders"]))
                except LedgerHalted:
                    result["lookup_benchmark_ok"] = 0
            result["checked_keys"] = verify_keys(writer, config["orders"])
        return result
    finally:
        if ledger:
            ledger.close()
        if relay:
            relay.close()


def validate_result(result: dict) -> dict:
    if (
        not isinstance(result, dict)
        or set(result) - RESULT_FIELDS
        or result.get("status") not in STATUSES
    ):
        raise ValueError("worker returned a non-aggregate result")
    for key, value in result.items():
        if key != "status" and value is not None and type(value) is not int:
            raise ValueError("worker returned a non-integer measurement")
    if result["status"] == "ok" and result.get("checked_keys") != 3:
        raise ValueError("worker did not verify historical keys")
    return result


def watchdog_reason(
    elapsed: float, resident: int, available: int, wall: int, budget: int
) -> str | None:
    if resident > budget:
        return "rss_limit"
    if available < 256 * 1024**2:
        return "memory_pressure"
    if elapsed > wall:
        return "wall_limit"
    return None


def run_worker(dsn: str, count: int, mode: str, budget: int) -> dict:
    validate_target(dsn)
    config = {"orders": count, "mode": mode}
    env = os.environ | {WORKER_DSN: dsn, "PYTHONHASHSEED": "0"}
    with tempfile.TemporaryFile(mode="w+b") as output:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--worker"],
            stdin=subprocess.PIPE,
            stdout=output,
            stderr=subprocess.DEVNULL,
            env=env,
        )
        try:
            process.stdin.write(json.dumps(config).encode())
            process.stdin.close()
            started = time.perf_counter()
            while process.poll() is None:
                with suppress(FileNotFoundError, ProcessLookupError):
                    reason = watchdog_reason(
                        time.perf_counter() - started,
                        rss(process.pid),
                        memory_info()["MemAvailable"],
                        10 if mode in {"default", "compatibility"} else 150,
                        budget,
                    )
                    if reason:
                        process.kill()
                        process.wait()
                        return {"status": reason}
                time.sleep(0.01)
            output.seek(0)
            if process.returncode:
                return {"status": "error"}
            result = validate_result(json.load(output))
            if result.get("peak_rss_bytes", 0) > budget:
                return {"status": "rss_limit"}
            return result
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()


def summarize(runs: list[dict]) -> dict:
    successful = [r for r in runs if r["status"] == "ok"]
    result = {"successes": len(successful), "runs": runs}
    for field in ("restore_ns", "load_ns", "construct_ns", "rss_delta_bytes", "peak_rss_bytes"):
        numbers = [r[field] for r in successful]
        result[field] = (
            None
            if not numbers
            else {
                "min": min(numbers),
                "median": int(statistics.median(numbers)),
                "max": max(numbers),
            }
        )
    return result


def marginal(previous: dict, current: dict) -> dict:
    step = current["orders"] - previous["orders"]
    result = {}
    for field in ("restore_ns", "load_ns", "rss_delta_bytes"):
        left, right = previous["diagnostic"][field], current["diagnostic"][field]
        result[field + "_per_order"] = (
            None
            if not left or not right
            else str((Decimal(right["median"]) - left["median"]) / step)
        )
    left, right = previous["bytes"].get("response_bytes"), current["bytes"].get("response_bytes")
    result["response_bytes_per_order"] = (
        None if left is None or right is None else str(Decimal(right - left) / step)
    )
    return result


def startup_bracket(points: list[dict]) -> dict:
    result = []
    for run in range(3):
        passed = [p["orders"] for p in points if p["default"]["runs"][run]["status"] == "ok"]
        failed = [
            p["orders"]
            for p in points
            if p["default"]["runs"][run]["status"] in {"timeout", "ledger_halted"}
        ]
        result.append(
            {
                "largest_pass": max(passed, default=None),
                "first_timeout_failure": min(failed, default=None),
            }
        )
    return {
        "per_run": result,
        "interpretation": (
            "startup failure, not slow recovery; censored points are not timeout evidence"
        ),
    }


def host_metadata() -> dict:
    cpu = next(
        (
            line.split(":", 1)[1].strip()
            for line in Path("/proc/cpuinfo").read_text().splitlines()
            if line.startswith("model name")
        ),
        "unavailable",
    )

    def limit(name):
        path = Path("/sys/fs/cgroup") / name
        return path.read_text().strip() if path.exists() else "unavailable"

    measured = [time.perf_counter_ns() for _ in range(10000)]
    steps = [b - a for a, b in zip(measured, measured[1:], strict=False) if b > a]
    return {
        "date_utc": datetime.now(UTC).date().isoformat(),
        "platform": platform.system(),
        "machine": platform.machine(),
        "cpu": cpu,
        "logical_cpus": os.cpu_count(),
        "memory_total_bytes": memory_info()["MemTotal"],
        "memory_available_bytes": memory_info()["MemAvailable"],
        "cgroup_memory_max": limit("memory.max"),
        "cgroup_cpu_max": limit("cpu.max"),
        "python": platform.python_version(),
        "timer_resolution_ns": str(
            Decimal(str(time.get_clock_info("perf_counter").resolution)) * 10**9
        ),
        "smallest_observed_step_ns": min(steps),
        "driver_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    }


async def measure(args) -> dict:
    dsn = os.environ.get(args.maintenance_dsn_env, "")
    if not dsn:
        raise ValueError("set the explicitly named maintenance DSN environment variable")
    budget = args.rss_budget_gib * GIB
    if memory_info()["MemAvailable"] < budget + 256 * 1024**2:
        raise MeasurementSkipped(
            "skipped: insufficient memory headroom for the requested RSS budget"
        )
    check_headroom(
        max(args.orders + ([100000] if args.unfilled_comparison else [])),
        min(shutil.disk_usage(Path.cwd()).free, args.seed_disk_budget_gib * GIB),
    )
    report = {
        "schema_version": 2,
        "complete": False,
        "host": host_metadata(),
        "policy": {
            "default_timeout_ns": int(STARTUP_TIMEOUT_SECONDS * 10**9),
            "compatibility_timeout_ns": int(HARD_TIMEOUT_SECONDS * 10**9),
            "diagnostic_timeout_seconds": DIAGNOSTIC_SECONDS,
            "runs_per_mode": 3,
            "worker_rss_budget_bytes": budget,
            "seed_disk_budget_bytes": args.seed_disk_budget_gib * GIB,
            "active_orders": ACTIVE,
            "criterion_orders": 1000000,
            "criterion_successes": 3,
            "rejected_orders": REJECTED,
            "rationale_bytes": len(RATIONALE.encode()),
            "cache": "seeded; cache uncontrolled; no cache flush",
        },
        "points": [],
        "unfilled_comparison": None,
    }
    for count, profile in [(n, "filled") for n in args.orders] + (
        [(100000, "unfilled")] if args.unfilled_comparison else []
    ):
        print(
            f"Seeding {count:,} synthetic orders ({profile}); separate disposable database.",
            flush=True,
        )
        async with disposable_database(dsn) as target:
            counts = await seed(target, count, profile)
            import asyncpg

            connection = await asyncpg.connect(target)
            try:
                # Simulate the additive upgrade from migration 008, before timed restores.
                await connection.execute("DROP INDEX orders_live_account_idx")
                await connection.execute("DELETE FROM schema_migrations WHERE version=9")
                from simex.ledger import LedgerAdapter

                upgrade_start = time.perf_counter_ns()
                await LedgerAdapter(connection).migrate()
                upgrade_ns = time.perf_counter_ns() - upgrade_start
                report["host"]["postgres_version"] = await connection.fetchval(
                    "SHOW server_version"
                )
            finally:
                await connection.close()
            point = {
                "orders": count,
                "profile": profile,
                "seed_counts": counts,
                "upgrade_ns": upgrade_ns,
                "upgrade_within_startup_budget": upgrade_ns <= int(STARTUP_TIMEOUT_SECONDS * 10**9),
            }
            for mode in ("compatibility", "default", "diagnostic"):
                runs = []
                for index in range(3):
                    result = await asyncio.to_thread(run_worker, target, count, mode, budget)
                    runs.append(result)
                    print(
                        f"{count:,} {profile}, {mode}, run {index + 1}: {result['status']}",
                        flush=True,
                    )
                point[mode] = summarize(runs)
            point["bytes"] = await asyncio.to_thread(run_worker, target, count, "bytes", budget)
            if profile == "filled":
                if report["points"]:
                    point["marginal"] = marginal(report["points"][-1], point)
                report["points"].append(point)
            else:
                report["unfilled_comparison"] = point
            # Save completed points only, with explicit censoring; never store row/traffic data.
            report["startup_cliff"] = startup_bracket(report["points"])
            report["compatibility_cliff"] = startup_bracket(
                [{**p, "default": p["compatibility"]} for p in report["points"]]
            )
            temporary = args.json.with_suffix(args.json.suffix + ".tmp")
            temporary.write_text(json.dumps(report, indent=2) + "\n")
            os.replace(temporary, args.json)
    report["complete"] = True
    temporary = args.json.with_suffix(args.json.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    os.replace(temporary, args.json)
    return report


def main(argv=None) -> int:
    if argv is None and sys.argv[1:] == ["--worker"]:
        try:
            result = worker(json.load(sys.stdin))
        except Exception:
            result = {"status": "error"}  # Never echo a connection error or row payload.
        print(json.dumps(result))
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--maintenance-dsn-env", required=True)
    parser.add_argument("--rss-budget-gib", type=int, required=True)
    parser.add_argument("--seed-disk-budget-gib", type=int, required=True)
    parser.add_argument("--orders", type=int, nargs="+", default=[1000, 10000, 100000, 1000000])
    parser.add_argument("--unfilled-comparison", action="store_true")
    parser.add_argument("--json", type=Path, required=True)
    args = parser.parse_args(argv)
    if platform.system() != "Linux":
        parser.error("measurement requires Linux /proc; Windows figures are not comparable")
    if (
        args.rss_budget_gib < 1
        or args.seed_disk_budget_gib < 1
        or min(args.orders) < 32
        or args.orders != sorted(set(args.orders))
    ):
        parser.error("positive budgets and increasing unique order counts (at least 32) required")
    try:
        report = asyncio.run(measure(args))
    except MeasurementSkipped as exc:
        print(str(exc))  # Only our own constant skip messages.
        return 0
    except (Exception, KeyboardInterrupt) as exc:
        print(
            "Measurement interrupted"
            if isinstance(exc, KeyboardInterrupt)
            else "Measurement failed; no DSN or row details logged",
            file=sys.stderr,
        )
        return 130 if isinstance(exc, KeyboardInterrupt) else 1
    print("Default recovery startup cliff (three runs): " + json.dumps(report["startup_cliff"]))
    print("120-second runs are diagnostic only; they do not demonstrate default startup.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
