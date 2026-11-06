"""Execution reports from committed handwritten fills, without recordings or scoring."""

import asyncio
import builtins
import json
import os
import re
import subprocess
import sys
from contextlib import closing
from decimal import Decimal as D
from time import time_ns

import asyncpg
import pytest
from test_portfolio_pnl import pnl_dsn as pnl_dsn

from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    GateDecision,
    Liquidity,
)
from simex.exchange.orders import FillCause
from simex.exchange.risk import RiskLimits
from simex.ledger import SyncLedger
from simex.report import __main__ as cli
from simex.report.core import NS, account_report, encoded, render_report, timestamp

START = 1_792_036_800 * NS
DELAY = 200_000_000
RATIONALE = "  The model's rationale: Δ, line one\nline two.  "


class Gate:
    def __init__(self, reason=None):
        self.reason = reason

    def check(self, product, now):
        # The configured 50 bps band rounds the synthetic 99.50 ask outward to 100.
        return GateDecision(self.reason is None, self.reason, D("99"), D("99.50"))


def request(key, *, kind="limit", quantity="1", side="buy", reasoning=RATIONALE):
    result = dict(
        client_order_id=key,
        product_id="BTC-USD",
        side=side,
        type=kind,
        quantity=quantity,
        reasoning=reasoning,
    )
    if kind == "limit":
        result["limit_price"] = "100"
    return result


def execute(dsn, sql, *args):
    async def run():
        connection = await asyncpg.connect(dsn)
        try:
            return await connection.fetch(sql, *args)
        finally:
            await connection.close()

    return asyncio.run(run())


@pytest.fixture
def populated(pnl_dsn):
    with closing(SyncLedger(pnl_dsn)) as ledger:
        ledger.migrate()
        writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), Gate(), DELAY, ledger=ledger)
        writer.deposit("bot", "USD", D("5000"), "seed:bot:USD")
        writer.deposit("legacy", "USD", D("500"), "seed:legacy:USD")
        writer.deposit("funded", "USD", D("500"), "seed:funded:USD")
        for key, kind, side, quantity, price, liquidity, second, wait in (
            ("market", "market", "buy", "8", "99", Liquidity.TAKER, 0, 1),
            ("maker-buy", "limit", "buy", "0.99", "100", Liquidity.MAKER, 10, 3),
            ("maker-sell", "limit", "sell", "0.99", "100", Liquidity.MAKER, 20, 5),
        ):
            order = writer.submit(
                "bot", request(key, kind=kind, side=side, quantity=quantity), START + second * NS
            )
            if kind == "limit":
                writer.activate(order.order_id, order.effective_at_ns)
            writer.record_fill(
                order.order_id,
                D(price),
                D(quantity),
                liquidity,
                FillCause.TAKER if kind == "market" else FillCause.PRINT,
                order.effective_at_ns + wait * NS,
            )
        legacy = writer.submit("legacy", request("legacy-market", kind="market"), START)
        writer.record_fill(
            legacy.order_id,
            D("99"),
            D("1"),
            Liquidity.TAKER,
            FillCause.TAKER,
            legacy.effective_at_ns + NS,
        )
        execute(
            pnl_dsn, "UPDATE orders SET market_band_price=NULL WHERE order_id=$1", legacy.order_id
        )
        for second, reason in enumerate(
            (
                "stale_level2:age_ns=5000000001",
                "stale_level2:age_ns=6000000001",
                "feed_lagging:excess_ns=2000000001",
                "book_not_synced",
            ),
            start=30,
        ):
            writer.gate = Gate(reason)
            rejected = writer.submit("bot", request(f"gate-{second}"), START + second * NS)
        execute(
            pnl_dsn,
            "UPDATE orders SET reject_details='{}'::jsonb WHERE order_id=$1",
            rejected.order_id,
        )
        writer.gate = Gate()
        writer.risk_limits = RiskLimits(
            {"BTC-USD": D(1)}, {"BTC-USD": D(1000)}, {"BTC-USD": D(100)}
        )
        writer.submit("bot", request("risk", quantity="2"), START + 40 * NS)
        writer.risk_limits = None
        writer.submit("bot", request("funds", quantity="100"), START + 41 * NS)
        for second, status in enumerate(("pending", "open", "cancelled", "expired"), start=50):
            order = writer.submit(
                "bot",
                request(status, kind="market" if status == "expired" else "limit"),
                START + second * NS,
            )
            if status == "open":
                writer.activate(order.order_id, order.effective_at_ns)
            elif status == "cancelled":
                writer.cancel_now(order.order_id, order.effective_at_ns)
            elif status == "expired":
                writer.expire(order.order_id, order.effective_at_ns)
        execute(
            pnl_dsn,
            "INSERT INTO orders (order_id, account_id, client_order_id, request, status, "
            "created_at_ns, effective_at_ns) SELECT 'synthetic-system', '_market', "
            "'synthetic-system', request, 'pending', created_at_ns, effective_at_ns "
            "FROM orders LIMIT 1",
        )
    return pnl_dsn


def command(tmp_path, *args, env_dsn="", extra_env=None):
    env = dict(
        os.environ,
        EXCHANGE_LEDGER_DSN=env_dsn,
        PYTHONDONTWRITEBYTECODE="1",
        PYTHONIOENCODING="utf-8",
    )  # Match the pipe decoder on Windows too.
    env.update(extra_env or {})
    return subprocess.run(
        [sys.executable, "-m", "simex.report", *map(str, args)],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
        encoding="utf-8",
    )


def assert_error(result, message):
    assert result.returncode != 0
    assert message in result.stderr
    assert "Traceback" not in result.stderr + result.stdout


def report(dsn, accounts=None, **kwargs):
    snapshot = asyncio.run(cli.read_ledger(dsn, accounts, **kwargs))
    return cli.build_report(snapshot, 20, FeeSchedule())


def test_full_module_run_has_hand_computed_figures_and_json_parity(populated, tmp_path):
    output = tmp_path / "private.json"
    run = command(tmp_path, "--dsn", populated, "--json", output)
    assert run.returncode == 0, run.stderr
    result = json.loads(output.read_text(encoding="utf-8"))
    assert [a["account_id"] for a in result["accounts"]] == ["bot", "legacy"]
    bot, legacy = result["accounts"]
    assert bot["order_count"] == 13
    assert bot["statuses"] == dict(
        pending=1, open=1, partially_filled=0, filled=3, cancelled=1, expired=1, rejected=6
    )
    assert bot["still_open"] == 2
    assert bot["liquidity_counts"] == dict(maker=2, taker=1)
    assert bot["fill_causes"] == dict(print=2, taker=1)
    notional = bot["notional_by_quote"]["USD"]
    assert {key: D(notional[key]) for key in ("filled", "maker", "taker")} == dict(
        filled=D(990), maker=D(198), taker=D(792)
    )
    assert D(notional["maker_share_pct"]) == 20 and D(notional["taker_share_pct"]) == 80
    assert D(bot["fees_by_asset"]["USD"]) == D("10.692")
    assert D(notional["effective_fee_bps"]) == 108
    market = bot["execution_bound_bps"]["BTC-USD"]["market"]
    assert market["count"] == 1 and D(market["mean"]) == -100 and D(market["worst"]) == -100
    limit = bot["execution_bound_bps"]["BTC-USD"]["limit"]
    assert limit["count"] == 2 and D(limit["mean"]) == 0
    assert bot["scheduled_delay_seconds"] == dict(count=13, median="0.2", worst="0.2")
    assert bot["time_to_first_fill_seconds"] == dict(count=3, median="3", worst="5")
    assert legacy["execution_bound_bps"]["BTC-USD"]["market"] == dict(
        count=0,
        mean=None,
        worst=None,
        excluded_missing_benchmark=1,
        excluded_invalid_benchmark=0,
        unfilled_orders=0,
    )
    assert RATIONALE in run.stdout
    assert all(d["reasoning"] == RATIONALE for d in bot["decisions"])
    assert "filled=990; maker=198 (20%); taker=792 (80%)" in run.stdout
    assert 'Fees by asset: {"USD": "10.692"}' in run.stdout
    assert "Effective fee (USD): 108 bps" in run.stdout
    assert "n=1; mean=-100; worst=-100" in run.stdout
    assert "n=3; median=3; worst=5" in run.stdout
    assert "excluded missing benchmark=1" in run.stdout
    assert "before the band-retention fix" in run.stdout
    assert "negative = unused band headroom, not a slippage win" in run.stdout
    assert run.stdout.rstrip().endswith("For performance: python -m simex.evaluator")
    assert not re.search(
        r"\b(return|sharpe|equity|drawdown|turnover|ranking|leaderboard)\b|fee drag",
        run.stdout,
        re.I,
    )
    assert populated not in output.read_text(encoding="utf-8")
    assert result["schema_version"] == 1


def test_refusals_group_prefix_and_show_latest_details_beside_rationale(populated, tmp_path):
    result = report(populated, ["bot"])
    bot = result["accounts"][0]
    groups = {group["reason"]: group for group in bot["refusal_groups"]}
    assert groups["stale_level2"]["count"] == 2
    assert groups["stale_level2"]["example"]["reason"] == "stale_level2:age_ns=6000000001"
    assert groups["stale_level2"]["example"]["details"]["rule"]
    assert groups["book_not_synced"]["example"]["details"] is None
    assert D(groups["quantity_limit"]["example"]["details"]["limit"]) == 1
    assert D(groups["quantity_limit"]["example"]["details"]["attempted"]) == 2
    assert groups["insufficient_funds"]["example"]["details"]["asset"] == "USD"
    text = render_report(result)
    assert "Refusals (6): Persisted order refusals only" in text
    assert "request-validation refusals are visible only to the client that made them" in text
    assert "reused-key errors" in text and "decide() returning None" in text
    assert "details unavailable" in text
    rejected = next(d for d in bot["decisions"] if d["reject_reason"] == "quantity_limit")
    entry = next(
        line for line in text.split("\n") if rejected["order_id"] in line and "reasoning:" in line
    )
    assert "outcome=rejected" in entry and "quantity_limit" in entry and '"limit": "1"' in entry
    assert "reasoning: " + RATIONALE in text


def test_account_and_decision_selection(populated, tmp_path):
    run = command(
        tmp_path, "--account", "bot", "--account", "bot", "--decisions", 2, env_dsn=populated
    )
    assert run.returncode == 0, run.stderr
    assert "--- Account: bot ---" in run.stdout and "--- Account: legacy ---" not in run.stdout
    assert run.stdout.count("reasoning:") == 2
    assert not any(system in run.stdout for system in cli.SYSTEM_ACCOUNTS)
    none = command(tmp_path, "--dsn", populated, "--decisions", 0)
    assert (
        none.returncode == 0
        and "Decision log" not in none.stdout
        and "reasoning:" not in none.stdout
    )


@pytest.mark.parametrize(
    "account,message",
    [("missing", "Unknown account"), ("funded", "No orders"), ("_fees", "System accounts")],
)
def test_account_errors_do_not_print_partial_reports(populated, tmp_path, account, message):
    run = command(tmp_path, "--dsn", populated, "--account", "bot", "--account", account)
    assert_error(run, message)
    assert run.stdout == ""


def test_missing_dsn_and_unreachable_ledger_hide_credentials(tmp_path):
    assert_error(command(tmp_path), "Set --dsn or EXCHANGE_LEDGER_DSN")
    dsn = "postgresql://private-user:private-password@127.0.0.1:1/private-db"
    result = command(tmp_path, "--dsn", dsn)
    assert_error(result, "Could not connect to the ledger")
    assert "private-password" not in result.stderr and "private-user" not in result.stderr


def test_empty_ledger_and_query_errors_are_informative(pnl_dsn, tmp_path):
    with closing(SyncLedger(pnl_dsn)) as ledger:
        ledger.migrate()
    assert_error(command(tmp_path, "--dsn", pnl_dsn), "No orders in the ledger")
    execute(pnl_dsn, "ALTER TABLE orders RENAME COLUMN maker_notional TO missing_maker_notional")
    with closing(SyncLedger(pnl_dsn)) as ledger:
        writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), Gate(), DELAY, ledger=ledger)
        writer.deposit("funded", "USD", D(100), "seed:funded")
    # Inventory itself succeeds, but the projection query must reveal its missing column.
    execute(
        pnl_dsn,
        "INSERT INTO orders(order_id,account_id,client_order_id,request,status,"
        "created_at_ns,effective_at_ns) "
        "VALUES('one','bot','one','{}','pending',0,1)",
    )
    result = command(tmp_path, "--dsn", pnl_dsn)
    assert_error(result, "UndefinedColumnError")
    assert "maker_notional" in result.stderr


def test_window_is_an_inclusive_order_cohort_with_all_committed_fills(populated, tmp_path):
    # A committed fill's synthetic market clock may be ahead of the snapshot's wall clock.
    execute(
        populated,
        "UPDATE fills SET ts_ns=$1 WHERE order_id=(SELECT order_id FROM orders "
        "WHERE client_order_id='maker-buy')",
        time_ns() + 3600 * NS,
    )
    result = report(populated, ["bot"], since_ns=START + 10 * NS)
    bot = result["accounts"][0]
    assert bot["order_count"] == 12
    assert bot["liquidity_counts"] == dict(maker=2, taker=0)
    assert bot["notional_by_quote"]["USD"]["filled"] == 198
    assert bot["fees_by_asset"]["USD"] == D("1.188")
    assert not any(d["intent"]["client_order_id"] == "market" for d in bot["decisions"])
    later = report(populated, ["bot"], since_ns=START + 10 * NS + 1)["accounts"][0]
    assert later["liquidity_counts"] == dict(maker=1, taker=0)
    assert later["notional_by_quote"]["USD"]["filled"] == 99
    run = command(
        tmp_path, "--dsn", populated, "--account", "bot", "--since", timestamp(START + 10 * NS + 1)
    )
    assert run.returncode == 0, run.stderr
    assert "Pre-window orders filling later are excluded" in run.stdout
    assert "maker=99 (100%)" in run.stdout


def test_trailing_hours_use_snapshot_cutoff_and_empty_window_is_an_error(populated, tmp_path):
    with closing(SyncLedger(populated)) as ledger:
        writer = AccountWriter(DEFAULT_INSTRUMENTS, FeeSchedule(), Gate(), DELAY, ledger=ledger)
        writer.deposit("recent", "USD", D(500), "seed:recent")
        writer.submit("recent", request("recent"), time_ns() - NS)
    snapshot = asyncio.run(cli.read_ledger(populated, ["recent"], hours=2))
    assert snapshot["since_ns"] == snapshot["snapshot_ns"] - 2 * 3600 * NS
    assert len(snapshot["orders"]) == 1
    run = command(tmp_path, "--dsn", populated, "--account", "recent", "--hours", 2)
    assert run.returncode == 0, run.stderr
    assert "n=0; median=unavailable; worst=unavailable" in run.stdout
    assert_error(
        command(tmp_path, "--dsn", populated, "--account", "bot", "--hours", 1),
        "No orders in the requested window",
    )


@pytest.mark.parametrize(
    "value", ["2026-10-15T04:00:00.000000001Z", "2026-10-15T05:00:00.000000001+01:00"]
)
def test_since_keeps_nanoseconds_and_normalizes_timezones(value):
    assert cli.since(value) == START + 1


@pytest.mark.parametrize(
    "args",
    [
        ["--since", "2026-10-15"],
        ["--since", "2026-10-15T04:00:00"],
        ["--since", "2026-02-30T00:00:00Z"],
        ["--since", "2026-10-15T00:00:00.0000000001Z"],
        ["--hours", "0"],
        ["--hours", "-1"],
        ["--hours", "1.5"],
        ["--hours", "1", "--since", "2026-10-15T04:00:00Z"],
        ["--decisions", "-1"],
    ],
)
def test_invalid_arguments_are_plain_failures(tmp_path, args):
    assert_error(command(tmp_path, *args), "error:")


def records(
    key="one",
    *,
    kind="market",
    side="buy",
    price="99",
    reference="100",
    quantity="1",
    status="filled",
    product="BTC-USD",
    wait=NS,
):
    order = dict(
        order_id=key,
        account_id="bot",
        request=dict(
            client_order_id=key,
            product_id=product,
            type=kind,
            side=side,
            quantity=quantity,
            limit_price=reference if kind == "limit" else None,
        ),
        status=status,
        filled_quantity=D(quantity),
        filled_notional=D(price) * D(quantity),
        maker_notional=D(0),
        taker_notional=D(price) * D(quantity),
        market_band_price=D(reference) if kind == "market" and reference is not None else None,
        created_at_ns=START,
        effective_at_ns=START + DELAY,
        reasoning=RATIONALE,
        reject_reason=None,
        reject_details={},
    )
    fill = dict(
        order_id=key,
        account_id="bot",
        product_id=product,
        price=D(price),
        quantity=D(quantity),
        liquidity="taker",
        cause="taker",
        fee=D("0.1"),
        fee_asset=product.rsplit("-", 1)[1],
        ts_ns=START + DELAY + wait,
    )
    return order, fill


@pytest.mark.parametrize("kind", ["market", "limit"])
@pytest.mark.parametrize(
    "side,price,expected",
    [("buy", "99", -100), ("sell", "101", -100), ("buy", "101", 100), ("sell", "99", 100)],
)
def test_bound_distance_signs_and_known_bps(kind, side, price, expected):
    order, fill = records(kind=kind, side=side, price=price)
    result = account_report("bot", [order], [fill], 20)
    values = result["execution_bound_bps"]["BTC-USD"][kind]
    assert values["count"] == 1 and values["mean"] == expected and values["worst"] == expected


def test_price_distribution_is_per_order_and_timing_medians_are_exact():
    first, fill1 = records("first", price="99", wait=0)
    second, fill2 = records("second", price="99.50", quantity="100", wait=1)
    result = account_report("bot", [first, second], [fill1, fill2], 20)
    assert result["execution_bound_bps"]["BTC-USD"]["market"]["mean"] == -75
    assert result["execution_bound_bps"]["BTC-USD"]["market"]["worst"] == -50
    assert result["time_to_first_fill_seconds"] == dict(
        count=2, median=D("0.0000000005"), worst=D("0.000000001")
    )
    extra = {**fill1, "ts_ns": fill1["ts_ns"] + 100 * NS}
    again = account_report("bot", [first, second], [extra, fill2, fill1], 20)
    assert again["time_to_first_fill_seconds"] == result["time_to_first_fill_seconds"]


@pytest.mark.parametrize("reference,missing,invalid", [(None, 1, 0), ("0", 0, 1)])
def test_missing_or_invalid_reference_is_never_backfilled(reference, missing, invalid):
    order, fill = records(reference=reference)
    result = account_report("bot", [order], [fill], 20)
    assert result["execution_bound_bps"]["BTC-USD"]["market"] == dict(
        count=0,
        mean=None,
        worst=None,
        excluded_missing_benchmark=missing,
        excluded_invalid_benchmark=invalid,
        unfilled_orders=0,
    )


def test_quote_currencies_and_unexpected_fee_assets_are_not_combined():
    usd, f1 = records("usd", price="100")
    eur, f2 = records("eur", product="ETH-EUR", price="200")
    f1["fee_asset"], f1["fee"] = "BTC", D("0.001")
    f2["fee"] = D(2)
    result = account_report("bot", [usd, eur], [f1, f2], 20)
    assert result["fees_by_asset"] == dict(BTC=D("0.001"), EUR=D(2))
    assert result["notional_by_quote"]["USD"]["filled"] == 100
    assert result["notional_by_quote"]["USD"]["effective_fee_bps"] is None
    assert result["notional_by_quote"]["USD"]["unconverted_fee_fills"] == 1
    assert result["notional_by_quote"]["EUR"]["filled"] == 200
    assert result["notional_by_quote"]["EUR"]["effective_fee_bps"] == 100


def test_partial_is_open_but_cancelled_and_expired_partials_are_terminal():
    orders, fills = [], []
    for status in ("partially_filled", "cancelled", "expired"):
        order, fill = records(status, status=status)
        order["request"]["quantity"] = "2"
        orders.append(order)
        fills.append(fill)
    result = account_report("bot", orders, fills, 20)
    assert result["still_open"] == 1
    assert result["statuses"]["partially_filled"] == 1
    assert result["statuses"]["cancelled"] == 1 and result["statuses"]["expired"] == 1
    assert sum(result["statuses"].values()) == 3


def test_zero_notional_and_unfilled_orders_do_not_fabricate_statistics():
    order, _ = records(status="pending", quantity="0")
    order["request"]["quantity"] = "1"
    result = account_report("bot", [order], [], 0)
    totals = result["notional_by_quote"]["USD"]
    assert totals["effective_fee_bps"] is None
    assert totals["maker_share_pct"] is None and totals["taker_share_pct"] is None
    assert result["time_to_first_fill_seconds"] == dict(count=0, median=None, worst=None)
    assert result["decisions"] == []


def test_json_keeps_decimal_fractions_without_float_or_guessing_text():
    order, fill = records()
    order["reasoning"] = "0.10000000"
    result = account_report("bot", [order], [fill], 20)

    def no_float(value):
        assert not isinstance(value, float)
        if isinstance(value, dict):
            for item in value.values():
                no_float(item)
        elif isinstance(value, list):
            for item in value:
                no_float(item)

    no_float(result)
    parsed = json.loads(encoded(result))
    no_float(parsed)
    assert parsed["decisions"][0]["reasoning"] == "0.10000000"
    assert parsed["fees_by_asset"]["USD"] == "0.1"


def test_snapshot_is_readonly_repeatable_and_closed_before_formatting(
    populated, monkeypatch, capsys
):
    actual_connect = asyncpg.connect
    connections, settings, queries = [], [], []

    class Observed:
        def __init__(self, connection):
            self.connection = connection

        def transaction(self, **kwargs):
            return self.connection.transaction(**kwargs)

        async def fetchval(self, query, *args):
            queries.append(query)
            return await self.connection.fetchval(query, *args)

        async def fetch(self, query, *args):
            queries.append(query)
            result = await self.connection.fetch(query, *args)
            if "bool_or" in query:
                settings.append(
                    (
                        await self.connection.fetchval("SHOW transaction_read_only"),
                        await self.connection.fetchval("SHOW transaction_isolation"),
                    )
                )
                # A distinct writer commits after the snapshot was established.
                other = await actual_connect(populated)
                try:
                    await other.execute(
                        "UPDATE orders SET status='cancelled' "
                        "WHERE account_id='bot' AND status='open'"
                    )
                finally:
                    await other.close()
            return result

        async def close(self):
            await self.connection.close()

    async def connect(dsn):
        connection = await actual_connect(dsn)
        connections.append(connection)
        return Observed(connection)

    actual_render = cli.render_report

    def render(result):
        assert connections and all(connection.is_closed() for connection in connections)
        assert result["accounts"][0]["statuses"]["open"] == 1  # Not the concurrent writer's state.
        return actual_render(result)

    monkeypatch.setattr(asyncpg, "connect", connect)
    monkeypatch.setattr(cli, "render_report", render)
    monkeypatch.setattr(cli, "load_environment", lambda log: None)
    assert cli.run(["--dsn", populated, "--account", "bot"]) == 0
    assert len(connections) == 1 and settings == [("on", "repeatable read")]
    assert all(query.startswith("SELECT") for query in queries)
    assert "Account: bot" in capsys.readouterr().out


def test_report_does_not_mutate_ledger_or_write_default_files(populated, tmp_path):
    tables = (
        "orders",
        "fills",
        "ledger_postings",
        "ledger_events",
        "outbox",
        "account_reservations",
        "schema_migrations",
        "ledger_transactions",
    )

    def state():
        return {
            table: execute(
                populated,
                f"SELECT row_to_json(t)::text FROM {table} t ORDER BY row_to_json(t)::text",
            )
            for table in tables
        }

    before = state()
    assert list(tmp_path.iterdir()) == []
    result = command(tmp_path, "--dsn", populated)
    assert result.returncode == 0, result.stderr
    assert state() == before and list(tmp_path.iterdir()) == []


def test_client_validation_and_reused_key_refusals_create_no_report_entries(populated):
    from test_mcp import make_tools

    with closing(SyncLedger(populated)) as ledger:
        tools = make_tools()
        tools.account_id = "bot"
        tools.writer.ledger = ledger
        tools.writer.restore("bot")
        for payload, reason in (
            (request("bad", quantity="0.000000001"), "bad_quantity"),
            (request("small", quantity="0.000001"), "below_min_size"),
            (request("price") | {"limit_price": "100.001"}, "bad_price"),
            (request("market", kind="market", quantity="0.1"), "idempotency_key_reused"),
        ):
            assert tools.place_order(payload)["reason"] == reason
    result = report(populated, ["bot"])["accounts"][0]
    assert result["order_count"] == 13 and result["refusal_count"] == 6
    assert not {"bad_quantity", "below_min_size", "bad_price", "idempotency_key_reused"} & {
        group["reason"] for group in result["refusal_groups"]
    }


def test_explicit_json_failure_is_plain_and_does_not_print_a_partial_report(populated, tmp_path):
    result = command(tmp_path, "--dsn", populated, "--json", tmp_path / "missing" / "report.json")
    assert_error(result, "Could not write local JSON")
    assert result.stdout == "" and list(tmp_path.iterdir()) == []


def test_atomic_output_preserves_existing_file_and_cleans_temporary_file(tmp_path, monkeypatch):
    output = tmp_path / "private.json"
    output.write_text("original", encoding="utf-8")

    def fail(source, target):
        assert source != target and target == output
        raise PermissionError("synthetic replace failure")

    monkeypatch.setattr(cli.os, "replace", fail)
    with pytest.raises(cli.ReportError, match="PermissionError: synthetic replace failure"):
        cli.write_json(output, "replacement")
    assert output.read_text(encoding="utf-8") == "original"
    assert list(tmp_path.iterdir()) == [output]


def test_current_fee_references_do_not_recompute_historical_fees(populated, tmp_path):
    result = command(
        tmp_path,
        "--dsn",
        populated,
        extra_env={"EXCHANGE_MAKER_FEE_BPS": "1", "EXCHANGE_TAKER_FEE_BPS": "2"},
    )
    assert result.returncode == 0, result.stderr
    assert "maker=1 bps; taker=2 bps (not recovered historical settings)" in result.stdout
    assert "Effective fee (USD): 108 bps" in result.stdout
    assert "rounding can put small-order effective rates above nominal" in result.stdout


def test_configuration_order_and_interrupt_exit_are_plain(monkeypatch, capsys):
    events = []
    monkeypatch.setattr(cli.logging, "basicConfig", lambda **kw: events.append("logging"))

    def environment(log):
        events.append("environment")
        monkeypatch.setenv("EXCHANGE_LEDGER_DSN", "synthetic-connection")

    monkeypatch.setattr(cli, "load_environment", environment)

    async def interrupt(dsn, *args, **kwargs):
        assert dsn == "synthetic-connection"
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "read_ledger", interrupt)
    assert cli.run([]) == 130
    assert events == ["logging", "environment"]
    output = capsys.readouterr()
    assert "interrupted" in output.err and "Traceback" not in output.err and output.out == ""


def test_missing_driver_has_a_plain_install_hint(monkeypatch):
    actual = builtins.__import__

    def import_module(name, *args, **kwargs):
        if name == "asyncpg":
            raise ImportError("synthetic missing optional driver")
        return actual(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_module)
    with pytest.raises(cli.ReportError, match="Install the Postgres driver"):
        asyncio.run(cli.read_ledger("synthetic-connection"))


@pytest.mark.parametrize(
    "env,message",
    [
        ({"LOG_LEVEL": "not-a-level"}, "Invalid LOG_LEVEL"),
        ({"EXCHANGE_MAKER_FEE_BPS": "NaN"}, "Invalid current fee references"),
        ({"EXCHANGE_TAKER_FEE_BPS": "Infinity"}, "Invalid current fee references"),
    ],
)
def test_invalid_configuration_is_a_plain_failure(tmp_path, env, message):
    assert_error(command(tmp_path, "--dsn", "unused", extra_env=env), message)


def test_invalid_report_does_not_replace_existing_json(populated, tmp_path):
    path = tmp_path / "previous.json"
    path.write_text("previous", encoding="utf-8")
    result = command(tmp_path, "--dsn", populated, "--account", "funded", "--json", path)
    assert_error(result, "No orders")
    assert path.read_text(encoding="utf-8") == "previous"
    assert list(tmp_path.iterdir()) == [path]


def test_jsonb_numeric_fractions_decode_directly_to_decimal_and_ids_stay_text(populated):
    execute(
        populated,
        "UPDATE orders SET reject_details="
        '\'{"fraction":0.10000001,"id":"00123"}\'::jsonb '
        "WHERE reject_reason='book_not_synced'",
    )
    result = report(populated, ["bot"])["accounts"][0]
    details = next(
        group["example"]["details"]
        for group in result["refusal_groups"]
        if group["reason"] == "book_not_synced"
    )
    assert type(details["fraction"]) is D and details["fraction"] == D("0.10000001")
    assert details["id"] == "00123"
    assert json.loads(encoded(details)) == dict(fraction="0.10000001", id="00123")
