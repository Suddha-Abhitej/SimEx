"""The recorder and replay reader (ADR 0013), on a synthetic feed and an in-memory Kafka."""

import asyncio
import json
import logging
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from recorder_feed import PAIRS, PARTITION, SECOND, START_NS, Crash, FakeKafka, synthetic_feed

from simex.recorder import files
from simex.recorder.check import check, format_report
from simex.recorder.config import TOPICS, RecorderSettings
from simex.recorder.files import apply_retention, expired_days, iter_parts, part_name
from simex.recorder.metrics import RecorderMetrics
from simex.recorder.replay import read
from simex.recorder.service import Recorder

FEED = synthetic_feed()  # about 2.2 h; generated once, it's deterministic
FEED_END_NS = START_NS + int(2.2 * 3600 * SECOND)


class Clock:
    def __init__(self, now_ns: int = START_NS) -> None:
        self.now_ns = now_ns

    def ns(self) -> int:
        return self.now_ns

    def mono(self) -> float:
        return self.now_ns / 1e9


def make(tmp_path: Path, kafka: FakeKafka, clock: Clock, **source_kwargs):
    settings = RecorderSettings(root=tmp_path / "rec", hour_grace_seconds=60)
    metrics = RecorderMetrics()
    recorder = Recorder(
        settings, kafka.source(**source_kwargs), metrics, clock_ns=clock.ns, monotonic=clock.mono
    )
    return recorder, metrics


async def drive(recorder: Recorder, kafka: FakeKafka, clock: Clock, until_ns: int, chunk_s=60):
    """Publish the feed up to `until_ns` a chunk at a time, as it would arrive live."""
    while clock.now_ns < until_ns:
        clock.now_ns = min(clock.now_ns + chunk_s * SECOND, until_ns)
        kafka.visible_until_ms = clock.now_ns // 1_000_000
        while await recorder.step(0):
            pass


def loaded() -> FakeKafka:
    kafka = FakeKafka()
    kafka.load(FEED)
    return kafka


def rows_on_disk(root: Path) -> list[tuple]:
    out = []
    for path in sorted(root.rglob("*.parquet")):
        table = pq.read_table(path, columns=["kafka_topic", "partition", "offset", "value"])
        out.extend(zip(*(table.column(c).to_pylist() for c in table.column_names), strict=True))
    return out


def expected_rows(kafka: FakeKafka, skip=frozenset()) -> list[tuple]:
    return sorted(
        (r.topic, r.partition, r.offset, r.value)
        for log in kafka.logs.values()
        for r in log
        if (r.topic, r.partition, r.offset) not in skip
    )


def assert_exactly_once(root: Path, kafka: FakeKafka, skip=frozenset()) -> None:
    on_disk = [r for r in rows_on_disk(root) if r[0] != "recorder"]
    keys = [r[:3] for r in on_disk]
    assert len(keys) == len(set(keys)), "a record was written twice"
    assert sorted(on_disk) == expected_rows(kafka, skip), "records lost or changed"
    assert not list(root.rglob("*.tmp"))


def full_run(tmp_path: Path) -> tuple[Recorder, RecorderMetrics, FakeKafka]:
    kafka, clock = loaded(), Clock()
    recorder, metrics = make(tmp_path, kafka, clock)

    async def go():
        await recorder.start()
        await drive(recorder, kafka, clock, FEED_END_NS + 3600 * SECOND)

    asyncio.run(go())
    return recorder, metrics, kafka


# Layout and the happy path ------------------------------------------------------------------


def test_records_every_envelope_byte_for_byte_in_hourly_files(tmp_path):
    recorder, metrics, kafka = full_run(tmp_path)
    root = tmp_path / "rec"
    assert_exactly_once(root, kafka)
    assert recorder.writers == {}  # every hour closed by the grace timer
    parts = list(iter_parts(root))
    # One file per product, channel and hour; every file one partition's contiguous run.
    keys = [(p.path.parents[3].name, p.product, p.hour) for p in parts]
    assert len(keys) == len(set(keys))
    assert {p.path.parents[3].name for p in parts} == {
        "channel=level2",
        "channel=market_trades",
        "channel=gaps",
    }
    btc = sorted(
        (p for p in parts if p.product == "BTC-USD" and p.topic == "md.level2"),
        key=lambda p: p.first,
    )
    first_hour = datetime.fromtimestamp(START_NS / 1e9, UTC).hour
    assert [p.hour.hour for p in btc] == [first_hour, first_hour + 1, first_hour + 2]
    meta = pq.read_metadata(btc[0].path).metadata
    assert meta[b"simex.first_offset"] == str(btc[0].first).encode()
    assert meta[b"simex.schema_version"] == b"1"
    assert pq.read_metadata(btc[0].path).row_group(0).column(0).compression == "ZSTD"
    # Offsets were committed past everything once the files closed.
    for tp, log in kafka.logs.items():
        if log:
            assert kafka.group[tp] == len(log)
    labels = {"channel": "level2", "product_id": "ETH-USD"}
    assert (
        metrics.registry.get_sample_value(
            "simex_recorder_records_total", {**labels, "type": "resync"}
        )
        >= 1
    )


def test_header_columns_match_the_envelope(tmp_path):
    full_run(tmp_path)
    table = pq.read_table(next((tmp_path / "rec").rglob("product=ETH-USD/*md.level2*.parquet")))
    for row in table.to_pylist()[:50]:
        envelope = json.loads(row["value"])
        assert row["received_at_ns"] == envelope["received_at_ns"]
        assert row["type"] == envelope["type"]
        if envelope["type"] == "data":
            assert [row["update_first"], row["update_last"]] == envelope["update_ids"]
            assert row["snapshot"] == envelope["snapshot"]
            assert row["awaiting_snapshot"] == envelope["awaiting_snapshot"]


def test_only_the_live_venue_topics_are_recorded():
    assert TOPICS == ("md.level2", "md.trades", "md.gaps")
    assert not any(t.startswith("research.") for t in TOPICS)


def test_a_foreign_venue_envelope_on_md_is_skipped(tmp_path):
    kafka, clock = loaded(), Clock()
    log = kafka.logs[("md.level2", 1)]
    coinbase = json.dumps(
        {
            "v": 3,
            "type": "data",
            "venue": "coinbase",
            "received_at_ns": START_NS,
            "channel": "level2",
            "product_id": "BTC-USD",
        }
    ).encode()
    from simex.recorder.source import SourceRecord  # noqa: PLC0415

    log.append(SourceRecord("md.level2", 1, 0, START_NS // 10**6, b"BTC-USD", coinbase))
    recorder, metrics = make(tmp_path, kafka, clock)

    async def go():
        await recorder.start()
        await drive(recorder, kafka, clock, START_NS + 600 * SECOND)
        await recorder.close()

    asyncio.run(go())
    skipped = metrics.registry.get_sample_value(
        "simex_recorder_skipped_total", {"topic": "md.level2", "reason": "foreign_venue"}
    )
    assert skipped == 1
    assert not [r for r in rows_on_disk(tmp_path / "rec") if r[1] == 1 and r[0] == "md.level2"]


def test_commit_never_passes_a_record_still_in_an_open_file(tmp_path):
    kafka, clock = loaded(), Clock()
    recorder, _ = make(tmp_path, kafka, clock)

    async def go():
        await recorder.start()
        await drive(recorder, kafka, clock, START_NS + 1800 * SECOND)

    asyncio.run(go())
    # BTC and SOL share md.level2 p0: the commit is the lower of their open files' first offsets.
    open_p0 = [
        w.first_offset for (t, p, _), w in recorder.writers.items() if (t, p) == ("md.level2", 0)
    ]
    assert len(open_p0) == 2
    assert kafka.group[("md.level2", 0)] == min(open_p0) == 0
    assert not list((tmp_path / "rec").rglob("*.parquet"))  # nothing published mid-hour
    assert list((tmp_path / "rec").rglob("*.tmp"))  # but row groups are on disk


# Crashes -------------------------------------------------------------------------------------


def _crash_once(monkeypatch, target, name):
    real = getattr(target, name)
    calls = {"n": 0}

    def fake(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise Crash(f"crash in {name}")
        return real(*args, **kwargs)

    monkeypatch.setattr(target, name, fake)


@pytest.mark.parametrize(
    "step",
    [
        "mid_file",  # killed with row groups in a .tmp
        "after_tmp_fsync",  # the .tmp is complete and fsynced; not renamed
        "after_rename",  # renamed; the directory not fsynced; offsets not committed
        "before_commit",  # published durably; offsets not committed
        "after_commit",  # published and committed; killed later mid-hour
    ],
)
def test_a_crash_at_every_step_loses_nothing_and_duplicates_nothing(tmp_path, monkeypatch, step):
    kafka, clock = loaded(), Clock()
    source_kwargs = {}
    if step == "after_tmp_fsync":
        _crash_once(monkeypatch, files.os, "replace")
    elif step == "after_rename":
        _crash_once(monkeypatch, files, "fsync_dir")
    elif step == "before_commit":

        class CrashingSource(type(kafka.source())):
            crashed = False  # once per test: the restarted process commits normally

            async def commit(self, offsets):
                if any(o > 0 for o in offsets.values()) and not CrashingSource.crashed:
                    CrashingSource.crashed = True
                    raise Crash("crash before commit")
                await super().commit(offsets)

        kafka.source = lambda **kw: CrashingSource(kafka, **kw)  # type: ignore[method-assign]
    first, _ = make(tmp_path, kafka, clock, **source_kwargs)
    crash_at = START_NS + int(1.5 * 3600 * SECOND)

    async def until_crash():
        await first.start()
        with pytest.raises(Crash) if step not in ("mid_file", "after_commit") else _nothing():
            await drive(first, kafka, clock, crash_at)

    asyncio.run(until_crash())
    # The process is gone: no close(), open .tmp files stay where they are.
    if step in ("mid_file", "after_commit"):
        assert list((tmp_path / "rec").rglob("*.tmp"))
    if step == "after_commit":
        assert any(o > 0 for o in kafka.group.values())

    second, metrics = make(tmp_path, kafka, clock)

    async def restart():
        await second.start()
        assert not list((tmp_path / "rec").rglob("*.tmp"))  # leftovers removed before reading
        await drive(second, kafka, clock, FEED_END_NS + 3600 * SECOND)

    asyncio.run(restart())
    assert_exactly_once(tmp_path / "rec", kafka)
    dupes = sum(
        metrics.registry.get_sample_value("simex_recorder_duplicates_skipped_total", {"topic": t})
        or 0
        for t in TOPICS
    )
    if step in ("after_rename", "before_commit"):
        assert dupes > 0  # re-read records that were already in a published file


class _nothing:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_group_offsets_missing_but_files_exist(tmp_path):
    kafka, clock = loaded(), Clock()
    first, _ = make(tmp_path, kafka, clock)

    async def part_one():
        await first.start()
        await drive(first, kafka, clock, START_NS + int(1.4 * 3600 * SECOND))
        await first.close()

    asyncio.run(part_one())
    kafka.group.clear()  # offsets.retention.minutes passed: the group's offsets are gone
    second, metrics = make(tmp_path, kafka, clock)

    async def part_two():
        await second.start()
        await drive(second, kafka, clock, FEED_END_NS + 3600 * SECOND)

    asyncio.run(part_two())
    assert_exactly_once(tmp_path / "rec", kafka)
    skipped = metrics.registry.get_sample_value(
        "simex_recorder_duplicates_skipped_total", {"topic": "md.level2"}
    )
    assert skipped > 1000  # read again from the log start, dropped by the file scan


def test_records_kafka_deleted_before_they_were_recorded_leave_a_gap_row(tmp_path, caplog):
    kafka, clock = loaded(), Clock()
    first, _ = make(tmp_path, kafka, clock)

    async def part_one():
        await first.start()
        await drive(first, kafka, clock, START_NS + int(1.2 * 3600 * SECOND))
        await first.close()

    asyncio.run(part_one())
    tp = ("md.trades", PARTITION["ETH-USD"])
    committed = kafka.group[tp]
    kafka.log_start[tp] = committed + 3  # retention deleted 3 records nobody recorded
    lost = {(tp[0], tp[1], o) for o in range(committed, committed + 3)}
    second, metrics = make(tmp_path, kafka, clock)

    async def part_two():
        await second.start()
        await drive(second, kafka, clock, FEED_END_NS + 3600 * SECOND)

    with caplog.at_level(logging.ERROR):
        asyncio.run(part_two())
    assert_exactly_once(tmp_path / "rec", kafka, skip=lost)
    assert (
        metrics.registry.get_sample_value(
            "simex_recorder_lost_offsets_total", {"topic": "md.trades"}
        )
        == 3
    )
    gap_rows = [r for r in rows_on_disk(tmp_path / "rec") if r[0] == "recorder"]
    assert len(gap_rows) == 1
    gap = json.loads(gap_rows[0][3])
    assert (gap["type"], gap["from_offset"], gap["to_offset"]) == (
        "recorder_gap",
        committed,
        committed + 2,
    )
    assert "expired from Kafka" in caplog.text


# The startup scan reaches back as far as each topic's retention -------------------------------


def test_scan_reaches_back_as_far_as_each_topics_retention(tmp_path):
    root = tmp_path / "rec"
    now = datetime(2026, 10, 15, 12, tzinfo=UTC)

    def touch(channel, topic, days_ago, last):
        hour = now - timedelta(days=days_ago)
        product = "all" if channel == "gaps" else "BTC-USD"
        d = files.directory(root, "gemini", channel, hour.replace(minute=0), product)
        d.mkdir(parents=True, exist_ok=True)
        (d / part_name(topic, 0, last - 10, last)).touch()

    touch("level2", "md.level2", 3, 500)  # older than level2's 1-day retention: not scanned
    touch("level2", "md.level2", 0, 100)
    touch("gaps", "md.gaps", 20, 900)  # md.gaps keeps 30 days here: scanned
    kafka = FakeKafka(
        retention_ms={"md.level2": 86_400_000, "md.trades": 86_400_000, "md.gaps": 30 * 86_400_000}
    )
    recorder, _ = make(tmp_path, kafka, Clock(int(now.timestamp() * 1e9)))
    asyncio.run(recorder._scan_files({"md.level2", "md.trades", "md.gaps"}))
    assert recorder.marks[("md.level2", 0, "BTC-USD")] == 100
    assert recorder.marks[("md.gaps", 0, "all")] == 900

    kafka.retention["md.level2"] = -1  # keep forever: scan everything
    asyncio.run(recorder._scan_files({"md.level2", "md.trades", "md.gaps"}))
    assert recorder.marks[("md.level2", 0, "BTC-USD")] == 500


# Windows -------------------------------------------------------------------------------------


def test_windows_skips_directory_fsync_and_uses_os_replace(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(files, "IS_WINDOWS", True)
    monkeypatch.setattr(files, "_warned_windows", False)
    replaced = []
    real_replace = os.replace

    def spy_replace(src, dst):
        replaced.append((Path(src).name, Path(dst).name))
        real_replace(src, dst)

    def no_dir_open(path, flags, *args, **kwargs):
        raise AssertionError(f"tried to open {path} for a directory fsync on Windows")

    monkeypatch.setattr(files.os, "replace", spy_replace)
    monkeypatch.setattr(files.os, "open", no_dir_open)
    with caplog.at_level(logging.DEBUG, logger="simex.recorder.files"):
        recorder, _, kafka = full_run(tmp_path)
    # Every publish went through os.replace, from a .tmp to its final .parquet name.
    assert replaced and all(
        src.endswith(".parquet.tmp") and dst.endswith(".parquet") for src, dst in replaced
    )
    assert caplog.text.count("directory fsync is not supported on Windows") == 1
    assert_exactly_once(tmp_path / "rec", kafka)


def test_posix_fsyncs_the_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(files, "IS_WINDOWS", False)
    synced = []
    real_fsync = os.fsync
    monkeypatch.setattr(files.os, "fsync", lambda fd: (synced.append(fd), real_fsync(fd)))
    tmp = tmp_path / "x.parquet.tmp"
    tmp.write_bytes(b"data")
    files.publish(tmp, tmp_path / "x.parquet")
    assert (tmp_path / "x.parquet").read_bytes() == b"data" and len(synced) == 2  # file + dir


# Retention -----------------------------------------------------------------------------------


def test_retention_keeps_recent_days_and_waits_for_the_archive_when_asked(tmp_path):
    root = tmp_path / "rec"
    now = datetime(2026, 10, 15, 12, tzinfo=UTC)
    for days_ago in (0, 1, 29, 31, 40):
        day = now - timedelta(days=days_ago)
        (root / "venue=gemini" / "channel=level2" / f"date={day:%Y-%m-%d}").mkdir(parents=True)
    old = root / "venue=gemini" / "channel=level2" / f"date={now - timedelta(days=40):%Y-%m-%d}"
    (old / files.ARCHIVED_MARKER).touch()
    verdicts = {p.name: v for p, v in expired_days(root, now, 30, require_archived=True)}
    assert verdicts == {
        f"date={now - timedelta(days=31):%Y-%m-%d}": "kept: not archived yet",
        f"date={now - timedelta(days=40):%Y-%m-%d}": "deleted",
    }
    assert apply_retention(root, now, 30, require_archived=False) == 2
    left = sorted(p.name for p in (root / "venue=gemini" / "channel=level2").iterdir())
    assert len(left) == 3
    # Today and yesterday survive even a retention of 0 days.
    assert apply_retention(root, now, 0, require_archived=False) == 1
    assert len(list((root / "venue=gemini" / "channel=level2").iterdir())) == 2


def test_recordings_are_never_committed():
    repo = Path(__file__).resolve().parents[1]
    tracked = subprocess.run(
        ["git", "ls-files", "*.parquet", "data/"], cwd=repo, capture_output=True, text=True
    )
    assert tracked.returncode == 0 and tracked.stdout.strip() == ""
    ignored = subprocess.run(
        ["git", "check-ignore", "-q", "data/recordings/venue=gemini/x.parquet"], cwd=repo
    )
    assert ignored.returncode == 0


# Replay --------------------------------------------------------------------------------------


def test_replay_yields_each_products_records_in_offset_order(tmp_path):
    _, _, kafka = full_run(tmp_path)
    root = tmp_path / "rec"
    records = list(read(root))
    assert len(records) == sum(len(log) for log in kafka.logs.values())
    by_stream: dict = {}
    for rec in records:
        by_stream.setdefault((rec.topic, rec.product_id), []).append(rec.offset)
    for offsets in by_stream.values():
        assert offsets == sorted(offsets)
    received = [rec.received_at_ns for rec in records]
    assert received == sorted(received)  # merged by received time
    # The exact bytes Kafka held.
    original = {
        (r.topic, r.partition, r.offset): r.value for log in kafka.logs.values() for r in log
    }
    assert all(original[(r.topic, r.partition, r.offset)] == r.value for r in records)
    # A window and a filter.
    start = START_NS + 3600 * SECOND
    window = list(
        read(root, start, start + 600 * SECOND, products=["SOL-USD"], channels=["level2"])
    )
    assert window and {r.product_id for r in window} == {"SOL-USD"}
    assert all(start <= r.received_at_ns < start + 600 * SECOND for r in window)


def test_replay_check_rebuilds_synced_books_and_every_audit_matches(tmp_path):
    full_run(tmp_path)
    report = check(tmp_path / "rec", START_NS, START_NS + 2 * 3600 * SECOND)
    text = format_report(report)
    assert report.ok, text
    for product in PAIRS:
        r = report.products[product]
        assert r.first_snapshot_ns is not None and r.first_snapshot_ns < START_NS + SECOND
        assert r.audit_outcomes["match"] > 20 and r.audit_outcomes["mismatch"] == 0
        assert (
            r.audit_rows == r.audits_replayed and r.audit_rows_aligned == r.audit_outcomes["match"]
        )
        assert r.unsynced_updates == 0 and r.crossed == 0 and r.final_state == "SYNCED"
    assert report.products["ETH-USD"].in_resync >= 1  # the awaiting update in the gap
    assert report.gaps["gap"] == 1
    assert report.gb_per_day > 0
    assert "PASS" in text
    # Counts only: no price from the feed's grids appears in the report (ADR 0010).
    prices = {json.loads(v)["payload"]["p"] for t, _, _, v, _ in FEED if t == "md.trades"}
    assert prices and not any(price in text for price in prices)


def test_replay_check_catches_a_corrupted_recording(tmp_path):
    full_run(tmp_path)
    root = tmp_path / "rec"
    # Rewrite one SOL level2 file with one diff's size changed: the next aligned audit fails.
    path = next(root.rglob("hour=05/product=SOL-USD/*md.level2*.parquet"))
    table = pq.read_table(path)
    rows = table.to_pylist()
    for row in rows:
        envelope = json.loads(row["value"])
        if envelope["type"] == "data" and not envelope["snapshot"] and envelope["payload"]["b"]:
            envelope["payload"]["b"][0][1] = "123.45"
            row["value"] = json.dumps(envelope).encode()
            break
    import pyarrow as pa  # noqa: PLC0415

    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    report = check(root, START_NS, START_NS + 2 * 3600 * SECOND)
    assert not report.ok
    assert report.products["SOL-USD"].audit_outcomes["mismatch"] >= 1


def test_catching_up_on_a_backlog_still_writes_one_file_per_hour(tmp_path):
    """After an outage every backlog hour is long over by the wall clock; the files must still
    close at the stream's hour boundary, not after each fetch."""
    kafka, clock = loaded(), Clock(FEED_END_NS + 5 * 3600 * SECOND)
    recorder, _ = make(tmp_path, kafka, clock, batch=200)

    async def go():
        await recorder.start()
        while await recorder.step(0):
            pass
        await recorder.housekeeping()

    asyncio.run(go())
    parts = list(iter_parts(tmp_path / "rec"))
    keys = [(p.path.parents[3].name, p.product, p.hour) for p in parts]
    assert len(keys) == len(set(keys)), "an hour was split into several files"
    assert_exactly_once(tmp_path / "rec", kafka)
