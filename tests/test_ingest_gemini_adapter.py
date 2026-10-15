"""The Gemini adapter's message reading and gap rule (ADR 0011), with hand-written frames."""

import json

import pytest

from simex.ingest.gemini import GeminiAdapter, venue_symbol
from simex.ingest.venue import LEVEL2, TRADES

PAIRS = ("BTC-USD", "ETH-USD", "SOL-USD")
L = 1_000_000  # a snapshot's update ID


def depth(first: int, last: int, symbol: str = "BTCUSD", levels: int = 1) -> dict:
    bids = [["100.00", "1"]] * levels
    return {
        "e": "depthUpdate",
        "E": 1_792_000_000_000_000_000,
        "s": symbol,
        "U": first,
        "u": last,
        "b": bids,
        "a": [],
    }


def synced() -> GeminiAdapter:
    """An adapter whose BTC-USD book has a snapshot at L."""
    adapter = GeminiAdapter(PAIRS)
    adapter.connect_frames()
    parsed = adapter.parse(depth(L, L, levels=50))
    assert [i.snapshot for i in parsed.items] == [True]
    return adapter


def kinds(parsed) -> list[str]:
    return [a.kind for a in parsed.anomalies]


def test_symbols_and_streams():
    assert venue_symbol("BTC-USD") == "BTCUSD"
    adapter = GeminiAdapter(PAIRS)
    [frame] = adapter.connect_frames()
    request = json.loads(frame)
    assert request["method"] == "SUBSCRIBE"
    assert request["params"] == [
        "btcusd@depth@100ms",
        "ethusd@depth@100ms",
        "solusd@depth@100ms",
        "btcusd@trade",
        "ethusd@trade",
        "solusd@trade",
    ]
    assert adapter.parse({"id": request["id"], "status": 200}).subscribed


def test_snapshot_is_u_equals_u_while_a_subscribe_is_pending():
    adapter = GeminiAdapter(PAIRS)
    adapter.connect_frames()
    [item] = adapter.parse(depth(L, L, levels=40)).items
    assert (item.channel, item.product_id, item.snapshot, item.update_ids) == (
        LEVEL2,
        "BTC-USD",
        True,
        (L, L),
    )


def test_u_equals_u_without_a_pending_subscribe_is_drift_not_a_snapshot():
    adapter = synced()
    adapter.parse(depth(L - 5, L + 10))
    parsed = adapter.parse(depth(L + 10, L + 10))
    assert [i.snapshot for i in parsed.items] == [False]
    assert kinds(parsed) == ["unexpected_snapshot"]
    assert parsed.anomalies[0].alarm


def test_chain_straddle_then_u_equals_previous_u():
    adapter = synced()
    assert kinds(adapter.parse(depth(L - 5, L + 40))) == []  # straddles the snapshot
    assert kinds(adapter.parse(depth(L + 40, L + 90))) == []  # U == previous u
    assert kinds(adapter.parse(depth(L + 90, L + 91))) == []
    assert adapter.books["BTC-USD"].last_u == L + 91


def test_gap_is_u_beyond_previous_u_and_only_this_product():
    adapter = synced()
    adapter.parse(depth(L - 5, L + 40))
    parsed = adapter.parse(depth(L + 70, L + 90))
    [gap] = parsed.anomalies
    assert gap.kind == "gap" and gap.products == ("BTC-USD",)
    assert (gap.expected, gap.received, gap.id_jump) == (L + 40, L + 70, 30)
    assert gap.missing is None  # exchange-wide IDs: the jump isn't a count
    assert gap.reason == "update_id_gap" and not gap.injected
    # The diff that revealed the gap is still published (flagged downstream).
    assert [i.product_id for i in parsed.items] == ["BTC-USD"]
    # No baseline until the resync's snapshot: later diffs aren't checked.
    assert kinds(adapter.parse(depth(L + 500, L + 600))) == []


def test_first_diff_beyond_the_snapshot_is_a_gap():
    adapter = synced()
    [gap] = adapter.parse(depth(L + 3, L + 9)).anomalies
    assert gap.kind == "gap" and (gap.expected, gap.received) == (L, L + 3)


def test_stale_diff_right_after_a_snapshot_is_counted_quietly():
    adapter = synced()
    [stale] = adapter.parse(depth(L - 30, L - 2)).anomalies
    assert stale.kind == "stale_after_snapshot" and not stale.alarm
    # The straddling diff still chains afterwards.
    assert kinds(adapter.parse(depth(L - 2, L + 20))) == []


@pytest.mark.parametrize(
    ("first", "last", "kind", "last_u"),
    [(L + 30, L + 60, "overlap", L + 60), (L + 10, L + 40, "stale", L + 40)],
)
def test_overlap_and_stale_later_are_drift_alarms(first, last, kind, last_u):
    adapter = synced()
    adapter.parse(depth(L - 5, L + 40))
    parsed = adapter.parse(depth(first, last))
    assert kinds(parsed) == [kind] and parsed.anomalies[0].alarm
    assert adapter.books["BTC-USD"].last_u == last_u


def test_malformed_ids_are_drift():
    adapter = synced()
    assert kinds(adapter.parse(depth(L + 9, L + 3))) == ["malformed_update_ids"]


def test_products_are_independent():
    adapter = GeminiAdapter(PAIRS)
    adapter.connect_frames()
    adapter.parse(depth(L, L, "BTCUSD"))
    adapter.parse(depth(L + 7, L + 7, "ETHUSD"))
    adapter.parse(depth(L - 5, L + 40, "BTCUSD"))
    # ETH's chain is its own: a BTC gap doesn't touch it, and vice versa.
    assert kinds(adapter.parse(depth(L + 2, L + 50, "ETHUSD"))) == []
    assert kinds(adapter.parse(depth(L + 99, L + 120, "BTCUSD"))) == ["gap"]
    assert kinds(adapter.parse(depth(L + 50, L + 60, "ETHUSD"))) == []


def test_resync_frames_resubscribe_one_depth_stream_and_wait_for_its_snapshot():
    adapter = synced()
    unsubscribe, subscribe = (json.loads(f) for f in adapter.resync_frames(("BTC-USD",)))
    assert unsubscribe["method"] == "UNSUBSCRIBE" and subscribe["method"] == "SUBSCRIBE"
    assert unsubscribe["params"] == subscribe["params"] == ["btcusd@depth@100ms"]
    # A diff in flight before the new snapshot is not checked; the U == u frame is the snapshot.
    assert kinds(adapter.parse(depth(L + 900, L + 950))) == []
    [item] = adapter.parse(depth(L + 980, L + 980, levels=30)).items
    assert item.snapshot


def test_trades_have_no_e_field():
    adapter = GeminiAdapter(PAIRS)
    trade = {
        "E": 1_792_000_000_000_000_000,
        "m": False,
        "p": "20.10",
        "q": "1.5",
        "s": "ETHUSD",
        "t": 42,
    }
    parsed = adapter.parse(trade)
    assert [(i.channel, i.product_id) for i in parsed.items] == [(TRADES, "ETH-USD")]
    assert parsed.exchange_ts_ns == 1_792_000_000_000_000_000
    assert parsed.anomalies == []


def test_other_symbols_and_responses():
    adapter = GeminiAdapter(PAIRS)
    assert adapter.parse(depth(5, 9, symbol="DOGEUSD")).items == []
    ping = json.loads(adapter.ping_frame())
    assert ping["method"] == "ping"
    assert adapter.parse({"id": ping["id"] + 99, "status": 200}).pong is False
    assert adapter.parse({"id": ping["id"], "status": 200}).pong is True
    error = adapter.parse({"id": 7, "status": 400, "error": {"msg": "bad stream"}})
    assert error.error is not None and "bad stream" in error.error


def test_a_dropped_diff_labels_its_gap_as_injected():
    adapter = synced()
    adapter.parse(depth(L - 5, L + 40))
    lost = depth(L + 40, L + 60)
    adapter.note_dropped(lost)
    [gap] = adapter.parse(depth(L + 60, L + 80)).anomalies
    assert gap.kind == "gap" and gap.injected


def test_audit_frame_and_its_depth_response():
    adapter = synced()
    assert adapter.supports_audit
    frame = json.loads(adapter.audit_frame("ETH-USD"))
    assert (frame["method"], frame["params"]) == ("depth", {"symbol": "ethusd"})
    result = {"lastUpdateId": L + 9, "bids": [["20.00", "1"]], "asks": []}
    parsed = adapter.parse({"id": frame["id"], "status": 200, "result": result})
    assert parsed.audit == ("ETH-USD", L + 9) and parsed.error is None and not parsed.items
    # The same id again is not an audit any more; a response without lastUpdateId is an error.
    assert adapter.parse({"id": frame["id"], "status": 200, "result": result}).audit is None
    frame = json.loads(adapter.audit_frame("BTC-USD"))
    parsed = adapter.parse({"id": frame["id"], "status": 200, "result": {"bids": []}})
    assert parsed.audit is None and "lastUpdateId" in parsed.error
    # An error answer forgets the request.
    frame = json.loads(adapter.audit_frame("SOL-USD"))
    assert adapter.parse({"id": frame["id"], "status": 400, "error": "bad"}).error
    assert frame["id"] not in adapter._audit_ids
