"""Private CLI integration on committed, hand-written fills and synthetic Parquet books."""

import asyncio
import json
import os
import subprocess
import sys
from contextlib import closing
from decimal import Decimal as D

import asyncpg
import pytest
from test_portfolio_pnl import pnl_dsn as pnl_dsn

from simex.evaluator import __main__ as cli
from simex.exchange import DEFAULT_INSTRUMENTS, Fill, FillCause, Liquidity, Side
from simex.ledger import SyncLedger
from simex.recorder.files import HourFile, hour_of
from simex.recorder.schema import place

NS = 1_000_000_000
START = 1_792_036_800 * NS  # Synthetic 2026-10-15T04:00:00Z, exactly on an hour boundary.
PERIOD = 60 * NS


def recordings(
    root, product="BTC-USD", *, last_seconds=3600, first_seconds=-60, refresh=120, first_offset=0
):
    """A snapshot before the window, chained minute updates and a later refresh."""
    root.mkdir(parents=True, exist_ok=True)
    writer = None
    previous_mid = None
    for offset, second in enumerate(range(first_seconds, last_seconds + 1, 60), start=first_offset):
        at = START + second * NS
        mid = (D(100) + D(second) / 60).quantize(D("0.01"))
        snapshot = offset == first_offset or second == refresh
        bids = [[str(mid - 1), "10"]]
        asks = [[str(mid + 1), "10"]]
        if not snapshot:
            bids.insert(0, [str(previous_mid - 1), "0"])
            asks.insert(0, [str(previous_mid + 1), "0"])
        value = json.dumps(
            {
                "type": "data",
                "venue": "gemini",
                "channel": "level2",
                "product_id": product,
                "received_at_ns": at,
                "snapshot": snapshot,
                "awaiting_snapshot": False,
                "update_ids": [offset + 1 if snapshot else offset, offset + 1],
                "payload": {"e": "depthUpdate", "E": at, "b": bids, "a": asks},
            }
        ).encode()
        row = place("md.level2", 0, offset, at // 1_000_000, product.encode(), value).row
        if writer is None or writer.hour != hour_of(at):
            if writer is not None:
                writer.close()
            writer = HourFile(
                root, "gemini", "level2", product, "md.level2", 0, hour_of(at), offset
            )
        writer.add(row)
        previous_mid = mid
    if writer is not None:
        writer.close()
    return root


def commit_fill(ledger, account, side, price, quantity, seconds, product="BTC-USD", suffix=""):
    identity = f"synthetic-cli-{account}-{side}-{seconds}-{product}{suffix}"
    fill = Fill(
        identity + "-1",
        identity,
        account,
        product,
        Side(side),
        D(price),
        D(quantity),
        Liquidity.TAKER,
        FillCause.TAKER,
        D(price) * D(quantity) * D("0.012"),
        "USD",
        START + seconds * NS,
    )
    ledger.record_fill(fill, DEFAULT_INSTRUMENTS[product], None, None)


@pytest.fixture
def cli_dsn(pnl_dsn):
    return pnl_dsn


@pytest.fixture
def filled(cli_dsn, tmp_path):
    with closing(SyncLedger(cli_dsn)) as ledger:
        ledger.migrate()
        ledger.record_deposit("agent", "USD", D("1234"), "seed:agent:USD:v1")
        ledger.record_deposit("agent", "USD", D("0.56"), "seed:agent:USD:synthetic-second")
        ledger.record_deposit("baseline", "USD", D("4321"), "seed:baseline:USD:v1")
        ledger.record_deposit("seed_only", "USD", D("777"), "seed:seed_only:USD:v1")
        commit_fill(ledger, "agent", "buy", "100", "1", 10)
        # Exactly on the final sampling boundary: dropping +1ns must lose this fill and sample.
        commit_fill(ledger, "agent", "sell", "160", "0.5", 3600)
        commit_fill(ledger, "baseline", "buy", "100", "0.25", 10)
        commit_fill(ledger, "baseline", "buy", "160", "0.25", 3600)
    return cli_dsn, recordings(tmp_path / "recordings")


def command(tmp_path, *args, env_dsn="", timeout=30):
    env = dict(os.environ, EXCHANGE_LEDGER_DSN=env_dsn, PYTHONDONTWRITEBYTECODE="1")
    return subprocess.run(
        [sys.executable, "-m", "simex.evaluator", *map(str, args)],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
    )


def assert_error(result, message):
    assert result.returncode != 0
    assert message in result.stderr
    assert "Traceback" not in result.stderr + result.stdout


def test_full_module_run_uses_seeds_and_includes_final_boundary_fill(filled, tmp_path):
    dsn, root = filled
    before = {path.relative_to(tmp_path): path.stat().st_mtime_ns for path in tmp_path.rglob("*")}
    result = command(tmp_path, "--dsn", dsn, "--recordings", root)
    assert result.returncode == 0, result.stderr
    assert "Accounts: agent, baseline" in result.stdout
    assert "Products: BTC-USD" in result.stdout
    assert "fills: 4 included, 0 excluded" in result.stdout
    assert result.stdout.count("Price coverage: complete") == 2
    assert "Initial equity: 1234.56" in result.stdout
    assert "Final equity: 1292.40" in result.stdout
    assert "Initial equity: 4321" in result.stdout
    assert "Final equity: 4335.22" in result.stdout
    assert "Turnover: 180" in result.stdout and "Fee drag: 2.16" in result.stdout
    assert "Return: None" not in result.stdout
    assert "Comparison with baseline: n=60" in result.stdout
    assert "Comparison with agent: n=60" in result.stdout
    assert "100000" not in result.stdout
    assert "Clamping" not in result.stdout
    assert "Replaying Gemini marks" in result.stdout
    assert "may take several minutes" in result.stdout
    after = {path.relative_to(tmp_path): path.stat().st_mtime_ns for path in tmp_path.rglob("*")}
    assert after == before, "CLI wrote output or modified recordings"


def test_lookback_reconstructs_book_before_window_and_zero_warns(filled, tmp_path):
    dsn, root = filled
    good = command(tmp_path, "--dsn", dsn, "--recordings", root)
    assert good.returncode == 0 and "Price coverage: incomplete" not in good.stdout
    late = command(tmp_path, "--dsn", dsn, "--recordings", root, "--lookback-seconds", "0")
    assert late.returncode == 0, late.stderr
    assert "Warning: first BTC-USD mark is after the first fill" in late.stdout
    assert "--lookback-seconds" in late.stdout
    assert "Price coverage: incomplete" in late.stdout
    assert "n=0" in late.stdout


@pytest.mark.parametrize("last_seconds", [3600, 3599, 3570, 3540])
def test_zero_subperiod_and_exact_period_gaps_do_not_clamp(filled, tmp_path, last_seconds):
    dsn, _ = filled
    root = recordings(tmp_path / "gap-recordings", last_seconds=last_seconds)
    # Add an exact off-cadence final snapshot when needed; all input is synthetic.
    if last_seconds % 60:
        recordings(root, first_seconds=last_seconds, last_seconds=last_seconds, first_offset=1000)
    result = command(tmp_path, "--dsn", dsn, "--recordings", root)
    assert result.returncode == 0, result.stderr
    assert "Clamping" not in result.stdout
    assert "fills: 4 included, 0 excluded" in result.stdout
    assert result.stdout.count("Price coverage: complete") == 2


def test_lagging_product_clamps_with_account_remedy_and_narrowing_avoids_it(filled, tmp_path):
    dsn, root = filled
    with closing(SyncLedger(dsn)) as ledger:
        ledger.record_deposit("sol", "USD", D("600"), "seed:sol:USD:v1")
        commit_fill(ledger, "sol", "buy", "100", "1", 10, "SOL-USD")
        commit_fill(ledger, "sol", "buy", "160", "1", 3600, "SOL-USD")
    recordings(root, "SOL-USD", last_seconds=1800)
    result = command(tmp_path, "--dsn", dsn, "--recordings", root)
    assert result.returncode == 0, result.stderr
    assert "Clamping end: SOL-USD" in result.stdout
    assert "1800 seconds (30 minutes)" in result.stdout
    assert "Use --account" in result.stdout
    assert "fills: 3 included, 3 excluded" in result.stdout
    assert result.stdout.count("Price coverage: complete") == 3
    narrow = command(
        tmp_path, "--dsn", dsn, "--recordings", root, "--account", "agent", "--account", "baseline"
    )
    assert narrow.returncode == 0, narrow.stderr
    assert "Clamping" not in narrow.stdout and "SOL-USD" not in narrow.stdout
    assert "n=60" in narrow.stdout


def test_clamp_keeps_fill_exactly_on_covered_final_boundary(filled, tmp_path):
    dsn, _ = filled
    with closing(SyncLedger(dsn)) as ledger:
        commit_fill(ledger, "agent", "sell", "130", "0.1", 1800)
    root = recordings(tmp_path / "short", last_seconds=1800)
    result = command(tmp_path, "--dsn", dsn, "--recordings", root)
    assert result.returncode == 0, result.stderr
    assert "Clamping end: BTC-USD" in result.stdout
    assert "fills: 3 included, 2 excluded" in result.stdout
    assert "Turnover: 113" in result.stdout
    assert "Comparison with baseline: n=30" in result.stdout


def test_account_selection_and_system_rows_are_excluded(filled, tmp_path):
    dsn, root = filled

    async def insert_system_fills():
        connection = await asyncpg.connect(dsn)
        try:
            for account in cli.SYSTEM_ACCOUNTS:
                await connection.execute(
                    "INSERT INTO fills (fill_id, order_id, account_id, product_id, side, "
                    "price, quantity, liquidity, cause, fee, fee_asset, ts_ns, source_key) "
                    "VALUES ($1,$1,$2,'ETH-USD','buy',100,1,'taker','taker',1,'USD',$3,$1)",
                    f"synthetic-system-{account}",
                    account,
                    START + 10 * NS,
                )
        finally:
            await connection.close()

    asyncio.run(insert_system_fills())
    loaded, seeds = asyncio.run(cli._read_ledger(dsn))
    assert not {fill.account_id for fill in loaded}.intersection(cli.SYSTEM_ACCOUNTS)
    assert not set(seeds).intersection(cli.SYSTEM_ACCOUNTS)
    result = command(tmp_path, "--dsn", dsn, "--recordings", root)
    assert result.returncode == 0, result.stderr
    assert "Accounts: agent, baseline" in result.stdout
    for account in cli.SYSTEM_ACCOUNTS:
        assert account not in result.stdout
    assert "ETH-USD" not in result.stdout
    selected = command(
        tmp_path,
        "--dsn",
        dsn,
        "--recordings",
        root,
        "--account",
        "baseline",
        "--account",
        "baseline",
    )
    assert selected.returncode == 0
    assert selected.stdout.count("Account: baseline") == 1
    assert "Account: agent" not in selected.stdout
    assert "Comparison with" not in selected.stdout


def test_non_usd_seeds_add_marks_and_non_seed_funding_is_not_assumed(filled, tmp_path):
    dsn, root = filled
    with closing(SyncLedger(dsn)) as ledger:
        ledger.record_deposit("agent", "ETH", D("0.5"), "seed:agent:ETH:v1")
        ledger.record_deposit("agent", "USD", D("999"), "synthetic-non-seed-deposit")
    missing = command(tmp_path, "--dsn", dsn, "--recordings", root, "--account", "agent")
    assert_error(missing, "No Gemini marks for ETH-USD")
    recordings(root, "ETH-USD")
    result = command(tmp_path, "--dsn", dsn, "--recordings", root, "--account", "agent")
    assert result.returncode == 0, result.stderr
    assert "Products: BTC-USD, ETH-USD" in result.stdout
    assert "Initial equity: 1284.56" in result.stdout
    assert "Price coverage: complete" in result.stdout


def test_missing_seed_is_explicit_and_does_not_substitute_default(filled, tmp_path):
    dsn, root = filled
    with closing(SyncLedger(dsn)) as ledger:
        commit_fill(ledger, "unfunded", "buy", "100", "1", 10)
    result = command(tmp_path, "--dsn", dsn, "--recordings", root, "--account", "unfunded")
    assert result.returncode == 0, result.stderr
    assert "Warning: no seed postings for unfunded" in result.stdout
    assert "Initial equity: 0" in result.stdout
    assert "100000" not in result.stdout


@pytest.mark.parametrize("account", ["unknown", "seed_only", "_market", "_fees", "_external"])
def test_invalid_or_system_selection_fails_plainly(filled, tmp_path, account):
    dsn, root = filled
    result = command(tmp_path, "--dsn", dsn, "--recordings", root, "--account", account)
    assert_error(result, "Select non-system accounts with fills")


def test_env_dsn_and_default_recording_path(filled, tmp_path):
    dsn, root = filled
    target = tmp_path / "data" / "recordings"
    target.parent.mkdir()
    root.rename(target)
    result = command(tmp_path, "--account", "agent", env_dsn=dsn)
    assert result.returncode == 0, result.stderr
    assert "Price coverage: complete" in result.stdout
    explicit = command(tmp_path, "--dsn", dsn, env_dsn="invalid-secret-do-not-print")
    assert explicit.returncode == 0, explicit.stderr


def test_missing_dsn_and_recording_directory_fail_plainly(tmp_path):
    assert_error(command(tmp_path), "Set --dsn or EXCHANGE_LEDGER_DSN")
    assert_error(command(tmp_path, "--dsn", "unused"), "Recordings directory does not exist")


def test_connection_failure_does_not_echo_credentials(tmp_path):
    root = tmp_path / "recordings"
    root.mkdir()
    result = command(
        tmp_path,
        "--dsn",
        "postgresql://user:synthetic-secret@127.0.0.1:-1/db",
        "--recordings",
        root,
    )
    assert_error(result, "Could not read committed fills and seeds")
    assert "synthetic-secret" not in result.stderr + result.stdout


def test_no_fills_exits_plainly(cli_dsn, tmp_path):
    with closing(SyncLedger(cli_dsn)) as ledger:
        ledger.migrate()
    root = tmp_path / "recordings"
    root.mkdir()
    assert_error(
        command(tmp_path, "--dsn", cli_dsn, "--recordings", root), "No fills in the ledger"
    )


def test_no_marks_and_missing_product_fail_with_recorder_hint(filled, tmp_path):
    dsn, _ = filled
    root = tmp_path / "empty"
    root.mkdir()
    result = command(tmp_path, "--dsn", dsn, "--recordings", root)
    assert_error(result, "No Gemini marks")
    assert "Was the recorder running?" in result.stderr
    recordings(root, "ETH-USD")
    result = command(tmp_path, "--dsn", dsn, "--recordings", root)
    assert_error(result, "No Gemini marks")


def test_no_overlap_after_clamping_exits_plainly(filled, tmp_path):
    dsn, _ = filled
    root = recordings(tmp_path / "old-recordings", first_seconds=-600, last_seconds=-60)
    result = command(tmp_path, "--dsn", dsn, "--recordings", root)
    assert_error(result, "Published marks do not overlap the filled window")


def test_unreadable_parquet_is_reported_without_traceback(filled, tmp_path):
    dsn, root = filled
    next(root.rglob("*.parquet")).write_bytes(b"synthetic corrupt Parquet")
    result = command(tmp_path, "--dsn", dsn, "--recordings", root)
    assert_error(result, "Could not replay Gemini recordings")


def test_help_needs_no_dsn_and_output_flag_is_not_supported(tmp_path):
    help_result = command(tmp_path, "--help")
    assert help_result.returncode == 0
    assert "--lookback-seconds" in help_result.stdout
    assert "--output" not in help_result.stdout
    target = tmp_path / "report.txt"
    assert_error(command(tmp_path, "--output", target), "unrecognized arguments")
    assert not target.exists()


@pytest.mark.parametrize("lookback", ["-1", "1.5", "bad"])
def test_invalid_lookback_is_an_argument_error(tmp_path, lookback):
    result = command(tmp_path, "--lookback-seconds", lookback)
    assert_error(result, "must be a nonnegative integer")


def test_replay_progress_is_flushed_after_readonly_snapshot_is_closed(filled, monkeypatch):
    dsn, root = filled
    real_connect = asyncpg.connect
    state = {"closed": False, "flushed": False}

    class Connection:
        def __init__(self, actual):
            self.actual = actual

        def transaction(self, **kwargs):
            assert kwargs == {"isolation": "repeatable_read", "readonly": True}
            return self.actual.transaction(**kwargs)

        async def fetch(self, *args):
            assert await self.actual.fetchval("SHOW transaction_isolation") == "repeatable read"
            assert await self.actual.fetchval("SHOW transaction_read_only") == "on"
            return await self.actual.fetch(*args)

        async def close(self):
            await self.actual.close()
            state["closed"] = self.actual.is_closed()

    async def connect(*args, **kwargs):
        return Connection(await real_connect(*args, **kwargs))

    class Output:
        def write(self, text):
            pass

        def flush(self):
            state["flushed"] = True

    actual_replay = cli.marks_from_recordings

    def replay(*args):
        assert state == {"closed": True, "flushed": True}
        return actual_replay(*args)

    monkeypatch.setattr(asyncpg, "connect", connect)
    monkeypatch.setattr(cli, "marks_from_recordings", replay)
    monkeypatch.setattr(sys, "stdout", Output())
    assert cli.run(["--dsn", dsn, "--recordings", str(root)]) == 0
