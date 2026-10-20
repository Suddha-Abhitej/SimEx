"""Exact ledger-to-writer balance reconciliation."""

from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

import asyncpg

from .metrics import LedgerMetrics

RECONCILIATION_INTERVAL_SECONDS = 60.0
RECONCILIATION_TIMEOUT_SECONDS = 30.0


class ProjectionWriter(Protocol):
    def balance_snapshot(self, account_id: str) -> tuple[dict[str, Decimal], int | None]: ...

    def halt_account(self, account_id: str) -> None: ...

    def lift_account_halt(self, account_id: str) -> None: ...


class ProjectionLedger(Protocol):
    def fold_balances(
        self, account_id: str, through_sequence: int | None
    ) -> dict[str, Decimal]: ...

    def double_entry_violations(self) -> tuple[tuple[str, str, Decimal], ...]: ...

    def write_reconciliation_override(
        self,
        *,
        account_id: str,
        asset: str,
        actor: str,
        reason: str,
        created_at_ns: int,
        expires_at_ns: int,
        event_key: str,
    ) -> None: ...


@dataclass(frozen=True)
class ReconciliationMismatch:
    account_id: str
    asset: str
    ledger_total: Decimal
    projection_total: Decimal


class ReconciliationJob:
    def __init__(
        self,
        ledger: ProjectionLedger,
        metrics: LedgerMetrics,
        *,
        clock_ns: Callable[[], int] = time.time_ns,
        monotonic: Callable[[], float] = time.monotonic,
        interval_seconds: float = RECONCILIATION_INTERVAL_SECONDS,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self.ledger = ledger
        self.metrics = metrics
        self.clock_ns = clock_ns
        self.monotonic = monotonic
        self.interval_seconds = interval_seconds
        self.writers: dict[str, ProjectionWriter] = {}
        self.mismatches: dict[tuple[str, str], ReconciliationMismatch] = {}
        self._overrides: dict[tuple[str, str], int] = {}

    def register_writer(self, account_id: str, writer: ProjectionWriter) -> None:
        self.writers[account_id] = writer

    def _refresh_override_metrics(self, now_ns: int) -> None:
        for key, expiry_ns in list(self._overrides.items()):
            active = expiry_ns > now_ns
            if not active:
                del self._overrides[key]
            self.metrics.reconciliation_overrides.labels(*key).set(int(active))

    def run_once(self) -> tuple[ReconciliationMismatch, ...]:
        started = self.monotonic()
        now_ns = self.clock_ns()
        self._refresh_override_metrics(now_ns)
        findings: list[ReconciliationMismatch] = []
        current_mismatch_keys: set[tuple[str, str]] = set()
        for account_id, writer in self.writers.items():
            projection, marker = writer.balance_snapshot(account_id)
            ledger_totals = self.ledger.fold_balances(account_id, marker)
            assets = set(projection) | set(ledger_totals)
            for asset in assets:
                ledger_total = ledger_totals.get(asset, Decimal(0))
                projection_total = projection.get(asset, Decimal(0))
                if ledger_total == projection_total:
                    continue
                mismatch = ReconciliationMismatch(account_id, asset, ledger_total, projection_total)
                findings.append(mismatch)
                current_mismatch_keys.add((account_id, asset))
                self.mismatches[(account_id, asset)] = mismatch
                self.metrics.reconciliation_mismatches.labels(account_id, asset).inc()
                self.metrics.reconciliation_mismatch_active.labels(account_id, asset).set(1)
                if (account_id, asset) not in self._overrides:
                    writer.halt_account(account_id)
        violations = self.ledger.double_entry_violations()
        self.metrics.double_entry_violation_active.set(int(bool(violations)))
        if violations:
            self.metrics.double_entry_violations.inc(len(violations))
            for account_id, writer in self.writers.items():
                writer.halt_account(account_id)
        self.metrics.reconciliation_runs.inc()
        self.metrics.reconciliation_seconds.observe(self.monotonic() - started)
        self.metrics.reconciliation_last_success_timestamp_seconds.set(now_ns / 1_000_000_000)
        return tuple(findings)

    def apply_override(
        self,
        account_id: str,
        asset: str,
        *,
        actor: str,
        reason: str,
        expires_at_ns: int,
    ) -> None:
        created_at_ns = self.clock_ns()
        if not actor or not reason or expires_at_ns <= created_at_ns:
            raise ValueError("override requires actor, reason and a future expiry")
        self.ledger.write_reconciliation_override(
            account_id=account_id,
            asset=asset,
            actor=actor,
            reason=reason,
            created_at_ns=created_at_ns,
            expires_at_ns=expires_at_ns,
            event_key=f"reconciliation-override:{uuid.uuid4()}",
        )
        self._overrides[(account_id, asset)] = expires_at_ns
        self.metrics.reconciliation_overrides.labels(account_id, asset).set(1)
        writer = self.writers.get(account_id)
        if writer is not None:
            writer.lift_account_halt(account_id)

    def override_active(self, account_id: str, asset: str) -> bool:
        self._refresh_override_metrics(self.clock_ns())
        return (account_id, asset) in self._overrides

    async def run_forever(self) -> None:
        while True:
            await asyncio.to_thread(self.run_once)
            await asyncio.sleep(self.interval_seconds)


class PostgresReconciliationStore:
    """Dedicated read connection for reconciliation, isolated from the fill connection."""

    def __init__(
        self,
        dsn: str,
        *,
        timeout_seconds: float = RECONCILIATION_TIMEOUT_SECONDS,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, name="simex-reconciliation", daemon=True)
        self._ready = threading.Event()
        self._error: BaseException | None = None
        self._read_connection: Any = None
        self._write_connection: Any = None
        self._dsn = dsn
        self._thread.start()
        if not self._ready.wait(timeout_seconds):
            raise RuntimeError("reconciliation connection startup timed out")
        if self._error is not None:
            raise RuntimeError("could not start reconciliation connection") from self._error

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._start())
        self._ready.set()
        if self._error is None:
            self._loop.run_forever()
            self._loop.run_until_complete(self._stop())
        self._loop.close()

    async def _start(self) -> None:
        try:
            self._read_connection = await asyncpg.connect(self._dsn)
            await self._read_connection.execute("SET default_transaction_read_only = on")
            self._write_connection = await asyncpg.connect(self._dsn)
        except BaseException as exc:
            self._error = exc

    async def _stop(self) -> None:
        if self._read_connection is not None:
            await self._read_connection.close()
        if self._write_connection is not None:
            await self._write_connection.close()

    def _call(self, operation: Callable[[], Any]) -> Any:
        future: Future[Any] = asyncio.run_coroutine_threadsafe(operation(), self._loop)
        return future.result(timeout=self.timeout_seconds)

    def fold_balances(self, account_id: str, through_sequence: int | None) -> dict[str, Decimal]:
        if through_sequence is None:
            return {}

        async def query() -> dict[str, Decimal]:
            rows = await self._read_connection.fetch(
                "SELECT asset, SUM(amount) AS total FROM ledger_postings "
                "WHERE account_id=$1 AND transaction_sequence <= $2 GROUP BY asset",
                account_id,
                through_sequence,
            )
            return {row["asset"]: Decimal(row["total"]) for row in rows}

        return self._call(query)

    def double_entry_violations(self) -> tuple[tuple[str, str, Decimal], ...]:
        async def query() -> tuple[tuple[str, str, Decimal], ...]:
            high_water = await self._write_connection.fetchval(
                "SELECT verified_sequence FROM reconciliation_state WHERE state_id=true"
            )
            rows = await self._read_connection.fetch(
                "SELECT p.transaction_id, p.asset, SUM(p.amount) AS total "
                "FROM ledger_postings p WHERE p.transaction_sequence > $1 "
                "GROUP BY p.transaction_id, p.asset HAVING SUM(p.amount) <> 0",
                high_water,
            )
            max_sequence = await self._read_connection.fetchval(
                "SELECT coalesce(max(sequence), $1) FROM ledger_transactions WHERE sequence > $1",
                high_water,
            )
            await self._write_connection.execute(
                "UPDATE reconciliation_state SET verified_sequence=$1 WHERE state_id=true",
                max_sequence,
            )
            return tuple(
                (row["transaction_id"], row["asset"], Decimal(row["total"])) for row in rows
            )

        return self._call(query)

    def write_reconciliation_override(self, **payload: Any) -> None:
        async def write() -> None:
            await self._write_connection.execute(
                "INSERT INTO ledger_events "
                "(event_id, event_key, aggregate_type, aggregate_id, aggregate_seq, event_type, "
                "occurred_at_ns, payload) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)",
                uuid.uuid4(),
                payload["event_key"],
                "reconciliation_override",
                f"{payload['account_id']}:{payload['asset']}:{payload['event_key']}",
                0,
                "reconciliation_override",
                payload["created_at_ns"],
                json.dumps(
                    {
                        key: payload[key]
                        for key in (
                            "account_id",
                            "asset",
                            "actor",
                            "reason",
                            "created_at_ns",
                            "expires_at_ns",
                        )
                    }
                ),
            )

        self._call(write)

    def close(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
