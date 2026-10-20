"""Exact ledger-to-writer balance reconciliation."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from .metrics import LedgerMetrics

RECONCILIATION_INTERVAL_SECONDS = 60.0


class ProjectionWriter(Protocol):
    def balance_totals(self, account_id: str) -> dict[str, Decimal]: ...

    def halt_account(self, account_id: str) -> None: ...

    def lift_account_halt(self, account_id: str) -> None: ...


class ProjectionLedger(Protocol):
    def fold_balances(self) -> dict[tuple[str, str], Decimal]: ...

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
        self._last_success_monotonic: float | None = None

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
        if self._last_success_monotonic is not None:
            self.metrics.reconciliation_age_seconds.set(
                max(0.0, started - self._last_success_monotonic)
            )
        now_ns = self.clock_ns()
        self._refresh_override_metrics(now_ns)
        ledger_totals = self.ledger.fold_balances()
        findings: list[ReconciliationMismatch] = []
        current_mismatch_keys: set[tuple[str, str]] = set()
        for account_id, writer in self.writers.items():
            projection = writer.balance_totals(account_id)
            assets = set(projection) | {
                asset for owner, asset in ledger_totals if owner == account_id
            }
            for asset in assets:
                ledger_total = ledger_totals.get((account_id, asset), Decimal(0))
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
        for account_id, asset in self.mismatches:
            if (account_id, asset) not in current_mismatch_keys:
                self.metrics.reconciliation_mismatch_active.labels(account_id, asset).set(0)

        violations = self.ledger.double_entry_violations()
        self.metrics.double_entry_violation_active.set(int(bool(violations)))
        if violations:
            self.metrics.double_entry_violations.inc(len(violations))
            for account_id, writer in self.writers.items():
                writer.halt_account(account_id)
        self.metrics.reconciliation_runs.inc()
        self.metrics.reconciliation_seconds.observe(self.monotonic() - started)
        self._last_success_monotonic = self.monotonic()
        self.metrics.reconciliation_age_seconds.set(0)
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
