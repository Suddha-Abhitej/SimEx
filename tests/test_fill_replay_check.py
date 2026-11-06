"""The fill replay check's markout test (#8, ADR 0014 §B), on hand-made markouts."""

import importlib.util
import json
import re
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path

import pytest
from test_mcp import OpenGate, order_payload

from simex.exchange import DEFAULT_INSTRUMENTS, AccountWriter, FeeSchedule, FillCause, Liquidity
from simex.marketdata import BookUpdate, Trade
from simex.recorder.files import hour_of, write_file
from simex.recorder.schema import place

SCRIPT = Path(__file__).resolve().parent.parent / "tools" / "fill_replay_check.py"
spec = importlib.util.spec_from_file_location("fill_replay_check", SCRIPT)
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)

P = "BTC-USD"


def test_markouts_are_positive_when_the_mid_moves_the_makers_way():
    assert check.markout_bps("buy", D("100"), D("100.01")) == D("1")
    assert check.markout_bps("sell", D("100"), D("100.01")) == D("-1")


def markouts(kind, values_by_price, horizons=check.MARKOUT_S):
    m = check.Markouts()
    for price, values in values_by_price.items():
        for h in horizons:
            m.values[((kind, P, "buy", D(price)), h)] = [D(v) for v in values]
    return m


class FakeRun:
    name = "proportional/print"

    def __init__(self, markouts):
        self.markouts = markouts


def test_maker_fills_better_than_real_prints_at_the_same_price_fail():
    real = markouts("real", {"100.00": ["-2", "0"], "99.99": ["5"]})
    # At 100.00 the real makers averaged -1 bp; ours averaged +0.5 bp: 1.5 bp better.
    ours = markouts("sim", {"100.00": ["0.5"] * 25})
    lines, problems = check.compare_markouts(FakeRun(ours), real, P)
    assert len(problems) == len(check.MARKOUT_S)
    assert "+1.50 bp over 25" in lines[0]


def test_close_to_real_passes_and_too_few_or_unmatched_fills_are_not_judged():
    real = markouts("real", {"100.00": ["-1"]})
    ours = markouts("sim", {"100.00": ["-0.5"] * 30, "99.98": ["9"] * 5})  # 0.5 bp better
    lines, problems = check.compare_markouts(FakeRun(ours), real, P)
    assert problems == [] and "(5 unmatched)" in lines[0]
    few = markouts("sim", {"100.00": ["9"] * 3})
    lines, problems = check.compare_markouts(FakeRun(few), real, P)
    assert problems == [] and "too few to judge" in lines[0]


def collection_run(size=3):
    run = check.Run(*check.RUNS[0], [], check.SECOND * 60, check.SECOND * 300)
    run.writer = AccountWriter(
        DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), 0, event_buffer_size=size
    )
    run.writer.deposit("probe", "USD", D("10000"), "synthetic-seed")
    return run


def test_collect_fills_tracks_sequence_across_wraps_without_duplicates():
    run = collection_run()
    makers, takers = {}, set()
    for index in range(20):
        order = run.writer.submit("probe", order_payload(str(index)), index * check.SECOND)
        run.passive[order.order_id] = P
        run._collect_fills()
        run.writer.activate(order.order_id, order.effective_at_ns)
        run._collect_fills()
        liquidity = Liquidity.MAKER if index % 2 else Liquidity.TAKER
        fill = run.writer.record_fill(
            order.order_id,
            D("100"),
            D("0.1"),
            liquidity,
            FillCause.PRINT if index % 2 else FillCause.TAKER,
            order.effective_at_ns,
        )
        if liquidity is Liquidity.MAKER:
            makers[order.order_id] = fill.ts_ns
        else:
            takers.add(order.order_id)
        run._collect_fills()
        run._collect_fills()
    assert len(run.writer.events) == 3
    assert run.seen_seq == run.writer.event_sequence == 60
    assert run.first_maker_fill == makers
    assert run.taker_on_arrival == takers
    for horizon in check.MARKOUT_S:
        due = run.markouts.due[(P, horizon)]
        assert [entry[0] for entry in due] == [
            ts + horizon * check.SECOND for ts in makers.values()
        ]
        assert all(entry[1:] == (("sim", P, "buy", D("100")), "buy", D("100")) for entry in due)


def test_collect_fills_aborts_on_eviction_before_advancing_or_using_partial_history():
    run = collection_run()
    run.writer._emit("order_accepted", order={})
    run._collect_fills()
    for index in range(5):
        run.writer._emit(
            "fill",
            fill={
                "order_id": str(index),
                "product_id": P,
                "side": "buy",
                "price": "100",
                "ts_ns": index,
                "liquidity": "maker",
            },
        )
        run.passive[str(index)] = P
    with pytest.raises(RuntimeError, match=r"2 writer events evicted.*accuracy results invalid"):
        run._collect_fills()
    assert run.seen_seq == 1
    assert run.first_maker_fill == {}
    assert not run.markouts.due


def test_replay_event_path_collects_actual_synthetic_engine_fills():
    run = check.Run(*check.RUNS[0], [P], 60 * check.SECOND, 300 * check.SECOND)
    run.on_event(
        BookUpdate(
            "gemini",
            P,
            True,
            ((D("99"), D("10")),),
            ((D("101"), D("10")),),
            (1, 1),
            False,
            check.SECOND,
            check.SECOND,
        )
    )
    run.on_event(
        Trade(
            "gemini",
            P,
            "synthetic-print",
            D("98"),
            D("1"),
            "sell",
            2 * check.SECOND,
            2 * check.SECOND,
        )
    )
    assert run.first_maker_fill
    assert any(run.markouts.due.values())
    assert run.seen_seq == run.writer.event_sequence


START = datetime(2026, 10, 15, 4, tzinfo=UTC)


def synthetic_recordings(
    root, start=START, *, duration=120, trades=True, products=None, venue="gemini"
):
    """Invented constant books and prints, published by the real recorder file writer."""
    products = products or sorted(DEFAULT_INSTRUMENTS)
    for product in products:
        rows = {"level2": [], "market_trades": []}
        for second in range(duration):
            ns = int(start.timestamp()) * check.SECOND + second * check.SECOND
            offset = int(start.timestamp()) * 10 + second * 10
            envelope = {
                "type": "data",
                "venue": venue,
                "product_id": product,
                "channel": "level2",
                "received_at_ns": ns,
                "snapshot": True,
                "awaiting_snapshot": False,
                "update_ids": [offset, offset],
                "payload": {
                    "e": "depthUpdate",
                    "E": ns,
                    "b": [["12345.67", "1"]],
                    "a": [["12345.69", "1"]],
                },
            }
            rows["level2"].append(
                place(
                    "md.level2",
                    0,
                    offset,
                    ns // 1_000_000,
                    product.encode(),
                    json.dumps(envelope).encode(),
                ).row
            )
            if trades and second < 60 and second % 2 == 0:
                for step, maker_buyer, price in ((4, True, "12345.67"), (8, False, "12345.69")):
                    at = ns + step * check.SECOND // 10
                    trade = {
                        "type": "data",
                        "venue": venue,
                        "channel": "market_trades",
                        "product_id": product,
                        "received_at_ns": at,
                        "payload": {
                            "e": "trade",
                            "E": at,
                            "t": f"synthetic-{product}-{offset + step}",
                            "p": price,
                            "q": "2",
                            "m": maker_buyer,
                        },
                    }
                    rows["market_trades"].append(
                        place(
                            "md.trades",
                            0,
                            offset + step,
                            at // 1_000_000,
                            product.encode(),
                            json.dumps(trade).encode(),
                        ).row
                    )
        for channel, entries in rows.items():
            if entries:
                write_file(
                    root, venue, channel, product, hour_of(entries[0]["received_at_ns"]), entries
                )
    return root


@pytest.fixture
def recorded(tmp_path):
    return synthetic_recordings(tmp_path / "recordings")


def arguments(root, *extra):
    return [
        "--root",
        str(root),
        "--start",
        START.isoformat(),
        "--hours",
        str(1 / 30),
        "--every",
        "2",
        *map(str, extra),
    ]


@pytest.mark.parametrize(
    "mean,n,status",
    [
        ("0.5", 20, "pass"),
        ("1", 20, "pass"),
        ("1.00001", 20, "fail"),
        ("10", 19, "too_few_to_judge"),
    ],
)
def test_comparison_collector_preserves_threshold_sample_guard_and_text(mean, n, status):
    ours = FakeRun(markouts("sim", {"100": [mean] * n, "101": ["9"] * 3}))
    real = markouts("real", {"100": ["0"]})
    summary = {}
    original = check.compare_markouts(ours, real, P)
    lines, problems = check.compare_markouts(ours, real, P, summary)
    assert (lines, problems) == original
    assert bool(problems) is (status == "fail")
    for h in check.MARKOUT_S:
        assert summary[str(h)] == {
            "matched_fills": n,
            "unmatched_fills": 3,
            "mean_difference_bps": D(mean),
            "status": status,
        }
        assert f"{D(mean):+.2f} bp over {n} (3 unmatched)" in lines[check.MARKOUT_S.index(h)]
    if status == "too_few_to_judge":
        assert "too few to judge" in lines[0]


def test_no_match_is_explicit_and_never_fabricates_a_zero_mean():
    summary = {}
    lines, problems = check.compare_markouts(
        FakeRun(markouts("sim", {"101": ["1"] * 30})), markouts("real", {"100": ["0"]}), P, summary
    )
    assert problems == [] and all("no matched fills" in line for line in lines)
    assert all(
        value
        == {
            "matched_fills": 0,
            "unmatched_fills": 30,
            "mean_difference_bps": None,
            "status": "no_matched_fills",
        }
        for value in summary.values()
    )


def test_discovery_defaults_and_last_hours_cross_utc_date_boundary(tmp_path):
    first = datetime(2026, 10, 15, 22, tzinfo=UTC)
    for hour in range(4):
        synthetic_recordings(
            tmp_path, first + timedelta(hours=hour), duration=1, trades=False, products=[P]
        )
    # 01:00 has been published early at shutdown, but is still the current hour.
    now = datetime(2026, 10, 16, 1, 30, tzinfo=UTC)
    assert check.discover_window(tmp_path, now=now) == (
        first + timedelta(hours=2),
        first + timedelta(hours=3),
    )
    assert check.discover_window(tmp_path, 3, now=now) == (first, first + timedelta(hours=3))
    # At exactly 02:00 the 01:00 hour becomes eligible.
    assert check.discover_window(tmp_path, now=now.replace(hour=2, minute=0)) == (
        first + timedelta(hours=3),
        first + timedelta(hours=4),
    )


def test_discovery_rejects_gaps_and_does_not_fall_back(tmp_path):
    for hour in (0, 1, 4, 5):
        synthetic_recordings(
            tmp_path, START + timedelta(hours=hour), duration=1, trades=False, products=[P]
        )
    now = START + timedelta(hours=7)
    assert check.discover_window(tmp_path, 2, now=now) == (
        START + timedelta(hours=4),
        START + timedelta(hours=6),
    )
    with pytest.raises(check.ReportError, match="missing published level2 hours"):
        check.discover_window(tmp_path, 3, now=now)
    with pytest.raises(check.ReportError, match="Cannot select 10 consecutive"):
        check.discover_window(tmp_path, 10, now=now)


@pytest.mark.parametrize(
    "kind", ["missing", "empty", "tmp", "current", "future", "foreign", "trades_only"]
)
def test_no_complete_hour_explains_waiting_instead_of_empty_report(tmp_path, kind):
    root = tmp_path / "recordings"
    now = START + timedelta(minutes=20)
    if kind != "missing":
        root.mkdir()
    if kind == "tmp":
        path = synthetic_recordings(
            root, START - timedelta(hours=1), duration=1, trades=False, products=[P]
        )
        published = next(path.rglob("*.parquet"))
        published.rename(published.with_suffix(".tmp"))
    elif kind in {"current", "future", "foreign", "trades_only"}:
        at = (
            START
            if kind == "current"
            else START + timedelta(hours=1)
            if kind == "future"
            else START - timedelta(hours=1)
        )
        synthetic_recordings(
            root, at, duration=1, products=[P], venue="other" if kind == "foreign" else "gemini"
        )
        if kind == "trades_only":
            for path in root.glob("venue=*/channel=level2/**/*.parquet"):
                path.unlink()
    with pytest.raises(check.ReportError, match="No complete.*Wait for a complete hour"):
        check.discover_window(root, now=now)


def test_default_cli_and_last_hours_need_no_trade_file(tmp_path, capsys):
    for hour in (0, 1):
        synthetic_recordings(
            tmp_path, START + timedelta(hours=hour), duration=1, trades=False, products=[P]
        )
    assert check.main(["--root", str(tmp_path)]) == 0
    output = capsys.readouterr().out
    assert "2026-10-15 05:00 .. 2026-10-15 06:00 UTC, 1 records" in output
    assert "Window too short to judge" in output and "--last-hours (24 or more)" in output
    assert output.rstrip().endswith("PASS")
    assert check.main(["--root", str(tmp_path), "--last-hours", "2"]) == 0
    assert "2026-10-15 04:00 .. 2026-10-15 06:00 UTC, 2 records" in capsys.readouterr().out


@pytest.mark.parametrize(
    "args",
    [
        ["--hours", "1"],
        ["--last-hours", "0"],
        ["--last-hours", "-1"],
        ["--last-hours", "1.5"],
        ["--start", "2026-10-15", "--last-hours", "2"],
        ["--last-hours", "2", "--hours", "1"],
    ],
)
def test_incompatible_or_invalid_window_arguments_are_explicit(args, capsys):
    with pytest.raises(SystemExit) as error:
        check.main(args)
    assert error.value.code == 2
    assert "error:" in capsys.readouterr().err


def test_no_complete_hour_cli_is_plain_and_does_not_write_summary(tmp_path, capsys):
    target = tmp_path / "result.json"
    assert check.main(["--root", str(tmp_path / "missing"), "--json", str(target)]) == 1
    output = capsys.readouterr()
    assert "No complete published Gemini" in output.err and "Wait" in output.err
    assert "Traceback" not in output.err and "PASS" not in output.out
    assert not target.exists()


def no_private_fields(value):
    assert not isinstance(value, float)
    if isinstance(value, dict):
        assert not set(value).intersection(
            {
                "price",
                "limit_price",
                "quantity",
                "fill_id",
                "order_id",
                "ts_ns",
                "received_at_ns",
                "payload",
                "value",
                "balances",
                "key",
            }
        )
        for item in value.values():
            no_private_fields(item)
    elif isinstance(value, list):
        for item in value:
            no_private_fields(item)


def test_real_parquet_cli_text_and_json_agree_without_prices(recorded, tmp_path, capsys):
    assert check.main(arguments(recorded)) == 0
    text = capsys.readouterr().out
    target = tmp_path / "reports" / "summary.json"
    assert check.main(arguments(recorded, "--json", target)) == 0
    assert capsys.readouterr().out == text
    summary = json.loads(target.read_text())
    assert summary["schema_version"] == 1 and summary["verdict"] == "PASS"
    assert not summary["problems"] and not summary["insufficient_samples"]
    assert summary["window"] == {
        "start": START.isoformat(),
        "end": (START + timedelta(seconds=120)).isoformat(),
    }
    assert summary["record_count"] == int(re.search(r", (\d+) records", text)[1])
    assert set(summary["runs"]) == {f"{mode.value}/{cross.value}" for mode, cross in check.RUNS}
    for name, run in summary["runs"].items():
        assert run["markouts_affect_verdict"] is (name == "proportional/print")
        assert not run["money_problems"]
        for product, pair in run["pairs"].items():
            block = text.split(product + "\n")[1].split("\n" + "\n")[0]
            counts = pair["counts"]
            assert (
                f"(of {counts['rested_probes']}; "
                f"{counts['taker_on_arrival']} crossed as taker on arrival)" in block
            )
            maker = counts["maker_fills"]
            assert (
                f"maker fills print {maker['print']}, trade_through {maker['trade_through']}, "
                f"book_cross {maker['book_cross']}" in block
            )
            assert maker["print"] > 0
            assert (
                f"market orders {counts['market_orders']}, band binds {counts['band_binds']}"
                in block
            )
            for h, result in pair["maker_markouts"].items():
                assert result["status"] == "pass" and result["matched_fills"] >= check.MIN_MATCHED
                mean = D(result["mean_difference_bps"])
                # Repeated Decimal means can leave tiny remainders; retain them in JSON.
                assert mean.quantize(D("0.01")) == 0
                if run["markouts_affect_verdict"]:
                    assert (
                        f"{h} s {mean:+.2f} bp over {result['matched_fills']} "
                        f"({result['unmatched_fills']} unmatched)" in block
                    )
    no_private_fields(summary)
    assert (
        "12345.67" not in target.read_text() + text and "12345.69" not in target.read_text() + text
    )
    assert "Window too short" not in text


@pytest.mark.parametrize("default_fails", [False, True])
def test_json_failures_preserve_default_only_verdict(
    recorded, tmp_path, monkeypatch, capsys, default_fails
):
    actual = check.Run.finish

    def finish(run, end):
        actual(run, end)
        if (run.name == "proportional/print") is default_fails:
            for key, values in run.markouts.values.items():
                run.markouts.values[key] = [value + D("2") for value in values]

    monkeypatch.setattr(check.Run, "finish", finish)
    target = tmp_path / "summary.json"
    assert check.main(arguments(recorded, "--json", target)) == int(default_fails)
    summary = json.loads(target.read_text())
    assert summary["verdict"] == ("FAIL" if default_fails else "PASS")
    assert bool(summary["problems"]) is default_fails
    name = "proportional/print" if default_fails else "proportional/fill"
    for pair in summary["runs"][name]["pairs"].values():
        assert all(result["status"] == "fail" for result in pair["maker_markouts"].values())
    assert capsys.readouterr().out.rstrip().endswith("PASS") is (not default_fails)


@pytest.mark.parametrize("samples", ["none", "few", "partial"])
def test_short_window_notice_uses_default_samples_without_changing_verdict(
    recorded, tmp_path, monkeypatch, capsys, samples
):
    actual = check.Run.finish

    def finish(run, end):
        actual(run, end)
        if run.name == "proportional/print":
            if samples == "none":
                run.markouts.values.clear()
            else:
                for key, values in run.markouts.values.items():
                    if samples != "partial" or key[0][1] != P:
                        run.markouts.values[key] = values[:1]

    monkeypatch.setattr(check.Run, "finish", finish)
    target = tmp_path / "summary.json"
    assert check.main(arguments(recorded, "--json", target)) == 0
    summary = json.loads(target.read_text())
    assert summary["verdict"] == "PASS" and not summary["problems"]
    insufficient = samples != "partial"
    assert summary["insufficient_samples"] is insufficient
    text = capsys.readouterr().out
    assert ("Window too short to judge" in text) is insufficient
    if insufficient:
        assert "--last-hours (24 or more)" in text
    for product, pair in summary["runs"]["proportional/print"]["pairs"].items():
        for entry in pair["maker_markouts"].values():
            if samples == "none":
                assert entry["status"] == "no_matched_fills"
                assert entry["mean_difference_bps"] is None
            elif samples == "few" or product != P:
                assert entry["status"] == "too_few_to_judge"
            else:
                assert entry["status"] == "pass"


@pytest.mark.parametrize("failure", ["creation", "negative"])
def test_nondefault_money_failures_still_fail_the_report(
    recorded, tmp_path, monkeypatch, capsys, failure
):
    actual = check.Run.finish

    def finish(run, end):
        actual(run, end)
        if run.name == "pessimistic/print":
            balance = run.writer.accounts.balance("probe", "USD")
            if failure == "creation":
                balance.available += D("1")
            else:
                # Keep total money conserved while making this non-system balance negative.
                released = balance.available + D("1")
                balance.available = D("-1")
                run.writer.accounts.balance("_external", "USD").available += released

    monkeypatch.setattr(check.Run, "finish", finish)
    target = tmp_path / "summary.json"
    assert check.main(arguments(recorded, "--json", target)) == 1
    summary = json.loads(target.read_text())
    assert summary["verdict"] == "FAIL"
    run = summary["runs"]["pessimistic/print"]
    assert not run["markouts_affect_verdict"] and run["money_problems"]
    expected = "created or destroyed" if failure == "creation" else "negative USD balance"
    assert any(expected in problem for problem in summary["problems"])
    assert "FAIL:" in capsys.readouterr().out


def test_policy_records_effective_configuration_not_environment(monkeypatch):
    monkeypatch.setenv("EXCHANGE_MAKER_FEE_BPS", "999")
    monkeypatch.setenv("EXCHANGE_MARKET_BAND_BPS", "999")
    monkeypatch.setenv("EXCHANGE_BOOK_CROSS", "fill")
    monkeypatch.setenv("EXCHANGE_CANCEL_ATTRIBUTION", "optimistic")
    monkeypatch.setenv("EXCHANGE_MATCH_WINDOW_MS_BTC_USD", "123")
    run = check.Run(*check.RUNS[0], [P], 2 * check.SECOND, 300 * check.SECOND)
    policy = check.measurement_policy([run], 0.5)
    assert policy["default_cancel_attribution"] == "proportional"
    assert policy["default_book_cross"] == "print"
    config = policy["runs"][run.name]
    assert config["maker_fee_bps"] == D("60") and config["taker_fee_bps"] == D("120")
    assert config["order_delay_ns"] == 200_000_000
    assert config["pairs"][P]["market_band_bps"] == D("50")
    assert config["pairs"][P]["match_window_ns"] == 123_000_000
    assert policy["markout_horizons_seconds"] == [1, 10, 60]
    assert policy["min_matched"] == 20 and policy["markout_limit_bps"] == D("1")
    run.writer.fees = FeeSchedule(maker_bps=D("77"), taker_bps=D("155"))
    run.writer.delay_ns = 123
    engine = run.gate.engines[P]
    engine.instrument = replace(engine.instrument, market_band_bps=D("66"))
    changed = check.measurement_policy([run], 0.5)
    assert changed != policy
    config = changed["runs"][run.name]
    assert config["maker_fee_bps"] == D("77") and config["taker_fee_bps"] == D("155")
    assert config["order_delay_ns"] == 123 and config["pairs"][P]["market_band_bps"] == D("66")


def test_eviction_aborts_cli_without_publishing_partial_json(
    recorded, tmp_path, monkeypatch, capsys
):
    actual = check.AccountWriter

    def small_buffer(*args, **kwargs):
        return actual(*args, **kwargs, event_buffer_size=3)

    monkeypatch.setattr(check, "AccountWriter", small_buffer)
    target = tmp_path / "summary.json"
    target.write_text('{"previous": true}')
    assert check.main(arguments(recorded, "--json", target)) == 1
    assert target.read_text() == '{"previous": true}'
    output = capsys.readouterr()
    assert "events evicted" in output.err and "results invalid" in output.err
    assert "PASS" not in output.out and "Traceback" not in output.err


def test_atomic_json_write_failure_preserves_old_summary(tmp_path, monkeypatch):
    target = tmp_path / "summary.json"
    target.write_text("previous summary")

    def fail_replace(source, destination):
        assert target.read_text() == "previous summary"
        assert json.loads(Path(source).read_text()) == {"mean": "0.123456789123456789"}
        raise OSError("synthetic replace failure")

    monkeypatch.setattr(check.os, "replace", fail_replace)
    with pytest.raises(check.ReportError, match="Cannot write JSON summary"):
        check.write_summary(target, {"mean": D("0.123456789123456789")})
    assert target.read_text() == "previous summary"
    assert list(tmp_path.iterdir()) == [target]


def test_real_command_accepts_timezone_and_fractional_explicit_window(recorded, tmp_path):
    target = tmp_path / "summary.json"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--root",
            str(recorded),
            "--start",
            "2026-10-15T06:00:00+02:00",
            "--hours",
            str(1 / 30),
            "--every",
            "2",
            "--json",
            str(target),
        ],
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(target.read_text())["window"]["start"] == START.isoformat()
