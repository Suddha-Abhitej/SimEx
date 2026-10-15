"""Smoke test for the local Docker Compose stack.

Waits for Kafka, Postgres and Valkey to report healthy, then exercises each one from the host:

- Kafka: create a topic, produce one message, consume it back
- Postgres: SELECT 1
- Valkey: SET then GET

Prints one PASS/FAIL line per service and exits non-zero if any check fails.

Usage: python scripts/smoke_test.py [--timeout SECONDS]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SERVICES = ("kafka", "postgres", "valkey")


def load_dotenv(path: Path) -> None:
    """Read KEY=VALUE lines from .env into os.environ, without overriding what is already set."""
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def parse_compose_ps(output: str) -> dict[str, str]:
    """Map service name to health from `docker compose ps --format json`.

    Compose prints a JSON array in older versions and one JSON object per line in newer ones.
    Services without a healthcheck report their state (for example "running") instead.
    """
    output = output.strip()
    if not output:
        return {}
    if output.startswith("["):
        rows = json.loads(output)
    else:
        rows = [json.loads(line) for line in output.splitlines() if line.strip()]
    return {row["Service"]: (row.get("Health") or row.get("State") or "unknown") for row in rows}


def compose_health() -> dict[str, str]:
    result = subprocess.run(
        ["docker", "compose", "ps", "--all", "--format", "json"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return parse_compose_ps(result.stdout)


def wait_for_healthy(timeout: float) -> dict[str, str]:
    """Poll until every service in SERVICES is healthy or the timeout passes."""
    deadline = time.monotonic() + timeout
    health: dict[str, str] = {}
    while time.monotonic() < deadline:
        health = compose_health()
        if all(health.get(name) == "healthy" for name in SERVICES):
            return health
        time.sleep(2)
    return health


async def check_kafka() -> str:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic

    bootstrap = f"localhost:{os.environ.get('KAFKA_PORT', '9092')}"
    topic = f"smoke-test-{uuid.uuid4().hex[:8]}"
    payload = uuid.uuid4().hex.encode()

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])

        producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
        await producer.start()
        try:
            await producer.send_and_wait(topic, payload)
        finally:
            await producer.stop()

        consumer = AIOKafkaConsumer(
            topic, bootstrap_servers=bootstrap, auto_offset_reset="earliest"
        )
        await consumer.start()
        try:
            message = await asyncio.wait_for(consumer.getone(), timeout=15)
        finally:
            await consumer.stop()
        if message.value != payload:
            raise RuntimeError(f"consumed {message.value!r}, expected {payload!r}")
    finally:
        try:
            await admin.delete_topics([topic])
        finally:
            await admin.close()
    return f"created {topic}, produced and consumed 1 message"


async def check_postgres() -> str:
    import asyncpg

    conn = await asyncpg.connect(
        host="localhost",
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "simex"),
        password=os.environ.get("POSTGRES_PASSWORD", "simex"),
        database=os.environ.get("POSTGRES_DB", "simex"),
        timeout=10,
    )
    try:
        value = await conn.fetchval("SELECT 1")
        version = await conn.fetchval("SHOW server_version")
        shared_buffers = await conn.fetchval("SHOW shared_buffers")
    finally:
        await conn.close()
    if value != 1:
        raise RuntimeError(f"SELECT 1 returned {value!r}")
    return f"SELECT 1 ok (server {version}, shared_buffers={shared_buffers})"


async def check_valkey() -> str:
    from valkey.asyncio import Valkey

    client = Valkey(host="localhost", port=int(os.environ.get("VALKEY_PORT", "6379")))
    key = f"smoke-test:{uuid.uuid4().hex[:8]}"
    try:
        await client.set(key, "ok", ex=60)
        value = await client.get(key)
        maxmemory = (await client.config_get("maxmemory"))["maxmemory"]
        await client.delete(key)
    finally:
        await client.aclose()
    if value != b"ok":
        raise RuntimeError(f"GET returned {value!r}")
    return f"SET/GET ok (maxmemory={int(maxmemory) // (1024 * 1024)}mb)"


CHECKS: dict[str, Callable[[], Awaitable[str]]] = {
    "kafka": check_kafka,
    "postgres": check_postgres,
    "valkey": check_valkey,
}


async def run_checks(names: list[str]) -> dict[str, tuple[bool, str]]:
    results: dict[str, tuple[bool, str]] = {}
    for name in names:
        try:
            results[name] = (True, await asyncio.wait_for(CHECKS[name](), timeout=60))
        except Exception as exc:  # report every failure, never stop at the first
            results[name] = (False, f"{type(exc).__name__}: {exc}")
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--timeout", type=float, default=180, help="seconds to wait for healthy services"
    )
    args = parser.parse_args()
    load_dotenv(REPO_ROOT / ".env")

    print(f"Waiting up to {args.timeout:.0f}s for {', '.join(SERVICES)} to be healthy...")
    try:
        health = wait_for_healthy(args.timeout)
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"FAIL  docker compose ps failed: {exc}")
        return 1

    results: dict[str, tuple[bool, str]] = {}
    for name in SERVICES:
        state = health.get(name, "not running")
        if state != "healthy":
            results[name] = (False, f"container is {state}")
    ready = [name for name in SERVICES if name not in results]
    if ready:
        results.update(asyncio.run(run_checks(ready)))

    print()
    for name in SERVICES:
        ok, detail = results[name]
        print(f"{'PASS' if ok else 'FAIL'}  {name:<9} {detail}")
    passed = sum(ok for ok, _ in results.values())
    print(f"\n{passed}/{len(SERVICES)} services passed")
    return 0 if passed == len(SERVICES) else 1


if __name__ == "__main__":
    sys.exit(main())
