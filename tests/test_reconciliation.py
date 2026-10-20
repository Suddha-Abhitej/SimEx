from decimal import Decimal

import pytest
from prometheus_client import CollectorRegistry

from simex.ledger import LedgerMetrics, ReconciliationJob


class FakeWriter:
    def __init__(self, balances):
        self.balances = balances
        self.halted = set()

    def balance_snapshot(self, account_id):
        return dict(self.balances.get(account_id, {})), 1

    def halt_account(self, account_id):
        self.halted.add(account_id)

    def lift_account_halt(self, account_id):
        self.halted.discard(account_id)


class FakeLedger:
    def __init__(self, totals, violations=()):
        self.totals = totals
        self.violations = tuple(violations)
        self.overrides = []
        self.markers = []

    def fold_balances(self, _account_id, _through_sequence):
        self.markers.append(_through_sequence)
        return {
            asset: total
            for (account_id, asset), total in self.totals.items()
            if account_id == _account_id
        }

    def double_entry_violations(self):
        return self.violations

    def write_reconciliation_override(self, **payload):
        self.overrides.append(payload)


def test_reconciliation_uses_snapshot_marker_and_does_not_share_sync_ledger():
    from simex.ledger import SyncLedger

    assert not hasattr(SyncLedger, "fold_balances")
    assert not hasattr(SyncLedger, "double_entry_violations")
    assert not hasattr(SyncLedger, "write_reconciliation_override")


def test_mismatch_remains_active_after_later_agreement():
    ledger = FakeLedger({("alice", "USD"): Decimal("10")})
    writer = FakeWriter({"alice": {"USD": Decimal("11")}})
    job = make_job(ledger)
    job.register_writer("alice", writer)
    job.run_once()
    writer.balances["alice"]["USD"] = Decimal("10")
    job.run_once()
    assert writer.halted == {"alice"}
    assert (
        job.metrics.registry.get_sample_value(
            "simex_ledger_reconciliation_mismatch_active",
            {"account_id": "alice", "asset": "USD"},
        )
        == 1
    )


def test_reconciliation_folds_only_through_writer_marker():
    ledger = FakeLedger({("alice", "USD"): Decimal("10")})
    writer = FakeWriter({"alice": {"USD": Decimal("10")}})
    job = make_job(ledger)
    job.register_writer("alice", writer)

    assert job.run_once() == ()
    assert ledger.markers == [1]


def make_job(ledger, clock=None):
    return ReconciliationJob(
        ledger,
        LedgerMetrics(CollectorRegistry()),
        clock_ns=clock or (lambda: 1_000),
        monotonic=lambda: 1.0,
    )


def test_reconciliation_uses_exact_decimal_totals_and_halts_account():
    ledger = FakeLedger({("alice", "USD"): Decimal("10.00")})
    writer = FakeWriter({"alice": {"USD": Decimal("10.000000000000000001")}})
    job = make_job(ledger)
    job.register_writer("alice", writer)

    findings = job.run_once()

    assert findings[0].ledger_total == Decimal("10.00")
    assert writer.halted == {"alice"}
    assert (
        job.metrics.registry.get_sample_value(
            "simex_ledger_reconciliation_mismatches_total",
            {"account_id": "alice", "asset": "USD"},
        )
        == 1
    )


def test_double_entry_violation_halts_all_registered_accounts():
    ledger = FakeLedger({}, (("fill:1", "USD", Decimal("0.01")),))
    writer = FakeWriter({})
    job = make_job(ledger)
    job.register_writer("alice", writer)

    job.run_once()

    assert writer.halted == {"alice"}
    assert job.metrics.registry.get_sample_value("simex_ledger_double_entry_violations_total") == 1


def test_override_is_audited_time_boxed_and_expires():
    now = [1_000]
    ledger = FakeLedger({("alice", "USD"): Decimal("10")})
    writer = FakeWriter({"alice": {"USD": Decimal("11")}})
    job = make_job(ledger, lambda: now[0])
    job.register_writer("alice", writer)
    job.run_once()

    job.apply_override("alice", "USD", actor="owner", reason="investigating", expires_at_ns=2_000)
    assert writer.halted == set()
    assert ledger.overrides[0]["actor"] == "owner"
    assert job.override_active("alice", "USD")

    now[0] = 2_000
    assert not job.override_active("alice", "USD")
    with pytest.raises(ValueError):
        job.apply_override("alice", "USD", actor="", reason="bad", expires_at_ns=3_000)
