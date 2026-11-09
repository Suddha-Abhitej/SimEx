"""Synthetic sensitivity cases for ADR 0027, not replacements for the baseline sweep.

Uses the same guarded disposable databases and fresh-process watchdog. No trading DSN fallback;
only a named neutral maintenance DSN. Output is system aggregates, never domain values or keys.
"""

import argparse
import asyncio
import json
import os
import random
import string
import sys
from pathlib import Path

import asyncpg
import measure_restore_cost as driver
import restore_cost_fixture as fixture

BASE_RATIONALE = fixture.RATIONALE
PROFILE_ENV = "SIMEX_RESTORE_SENSITIVITY"
CASES = {"long_rationale": (1000, 16, 8192), "many_live": (10000, 1024, 128)}


def configure(case):
    count, active, length = CASES[case]
    fixture.ACTIVE = driver.ACTIVE = active
    fixture.RATIONALE = driver.RATIONALE = BASE_RATIONALE
    if length != 128:
        rng = random.Random(107)
        fixture.RATIONALE = driver.RATIONALE = "".join(
            rng.choice(string.ascii_letters + string.digits) for _ in range(length)
        )
    return count, active, length


async def measure(args):
    dsn = os.environ[args.maintenance_dsn_env]
    if driver.memory_info()["MemAvailable"] < (args.rss_budget_gib * driver.GIB + 256 * 1024**2):
        raise fixture.MeasurementSkipped("skipped: insufficient measurement memory headroom")
    fixture.check_headroom(10000, args.seed_disk_budget_gib * driver.GIB)
    report = {
        "host": driver.host_metadata(),
        "default_timeout_seconds": driver.STARTUP_TIMEOUT_SECONDS,
        "cases": [],
    }
    for case in CASES:
        count, active, length = configure(case)
        os.environ[PROFILE_ENV] = case
        async with fixture.disposable_database(dsn) as target:
            counts = await fixture.seed(target, count, "filled")
            connection = await asyncpg.connect(target)
            try:
                report["host"]["postgres_version"] = await connection.fetchval(
                    "SHOW server_version"
                )
            finally:
                await connection.close()
            runs = []
            for _ in range(3):
                runs.append(
                    await asyncio.to_thread(
                        driver.run_worker,
                        target,
                        count,
                        "default",
                        args.rss_budget_gib * driver.GIB,
                        worker_script=Path(__file__),
                    )
                )
            byte_run = await asyncio.to_thread(
                driver.run_worker,
                target,
                count,
                "bytes",
                args.rss_budget_gib * driver.GIB,
                worker_script=Path(__file__),
            )
            report["cases"].append(
                {
                    "case": case,
                    "orders": count,
                    "seed_counts": counts,
                    "active_orders": active,
                    "rationale_bytes": length,
                    "default": driver.summarize(runs),
                    "bytes": byte_run,
                }
            )
    return report


def main():
    if sys.argv[1:] == ["--worker"]:
        configure(os.environ[PROFILE_ENV])
        try:
            result = driver.worker(json.load(sys.stdin))
        except Exception:
            result = {"status": "error"}
        print(json.dumps(result))
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--maintenance-dsn-env", required=True)
    parser.add_argument("--rss-budget-gib", type=int, required=True)
    parser.add_argument("--seed-disk-budget-gib", type=int, required=True)
    parser.add_argument("--json", type=Path, required=True)
    args = parser.parse_args()
    if sys.platform != "linux" or min(args.rss_budget_gib, args.seed_disk_budget_gib) < 1:
        parser.error("Linux and positive explicit budgets required")
    try:
        report = asyncio.run(measure(args))
    except fixture.MeasurementSkipped as exc:
        print(str(exc))
        return 0
    except Exception:
        print(
            "Sensitivity measurement failed; no connection or row details logged", file=sys.stderr
        )
        return 1
    temporary = args.json.with_suffix(args.json.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    os.replace(temporary, args.json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
