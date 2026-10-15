"""The shared decoder: any venue's envelope to Decimal book and trade events (ADR 0011)."""

import json
from decimal import Decimal

from coinbase_samples import L2_SNAPSHOT, TRADES, build_envelopes, raw

from simex.ingest.envelope import (
    build_audit_envelope,
    build_envelope,
    build_resync_marker,
    topics_for,
)
from simex.ingest.venue import LEVEL2, Item
from simex.ingest.venue import TRADES as TRADES_CHANNEL
from simex.marketdata import BookUpdate, ResyncMarker, Trade, TradeDeduper, decode

RECEIVED = 1_792_026_000_250_000_000
GEMINI = topics_for("gemini")


def gemini_envelope(payload: dict, item: Item, awaiting=()) -> bytes:
    return build_envelope(
        json.dumps(payload, separators=(",", ":")),
        item,
        venue="gemini",
        topics=GEMINI,
        received_at_ns=RECEIVED,
        exchange_ts_ns=payload.get("E"),
        awaiting_snapshot=awaiting,
    ).value


def test_gemini_book_update_keeps_exact_decimals():
    payload = {
        "e": "depthUpdate",
        "E": 7,
        "s": "BTCUSD",
        "U": 10,
        "u": 12,
        "b": [["100.10", "0.50000000"], ["99.90", "0"]],
        "a": [["100.20", "1e-8"]],
    }
    [book] = decode(gemini_envelope(payload, Item(LEVEL2, "BTC-USD", update_ids=(10, 12))))
    assert isinstance(book, BookUpdate)
    assert (book.venue, book.product_id, book.snapshot) == ("gemini", "BTC-USD", False)
    assert book.bids == ((Decimal("100.10"), Decimal("0.50000000")), (Decimal("99.90"), Decimal(0)))
    assert book.asks == ((Decimal("100.20"), Decimal("1E-8")),)
    assert str(book.bids[0][0]) == "100.10"  # trailing zeros kept: never through a float
    assert book.update_ids == (10, 12) and book.exchange_ts_ns == 7
    assert book.awaiting_snapshot is False


def test_gemini_snapshot_flag_comes_from_the_envelope():
    payload = {
        "e": "depthUpdate",
        "E": 7,
        "s": "ETHUSD",
        "U": 5,
        "u": 5,
        "b": [["20.00", "1"]],
        "a": [["20.10", "2"]],
    }
    item = Item(LEVEL2, "ETH-USD", update_ids=(5, 5), snapshot=True)
    [book] = decode(gemini_envelope(payload, item, awaiting={"SOL-USD"}))
    assert book.snapshot is True and book.awaiting_snapshot is False


def test_gemini_trade():
    payload = {"E": 9, "m": False, "p": "3.010", "q": "12.5", "s": "SOLUSD", "t": 77}
    [trade] = decode(gemini_envelope(payload, Item(TRADES_CHANNEL, "SOL-USD")))
    assert trade == Trade(
        "gemini", "SOL-USD", "77", Decimal("3.010"), Decimal("12.5"), "buy", 9, RECEIVED
    )


def test_resync_marker():
    marker = build_resync_marker(
        "BTC-USD", "r1", "update_id_gap", RECEIVED, venue="gemini", topics=GEMINI
    )
    assert decode(marker.value) == [
        ResyncMarker("gemini", "BTC-USD", "r1", "update_id_gap", RECEIVED)
    ]


def test_coinbase_v3_and_older_envelopes():
    [env] = build_envelopes(raw(L2_SNAPSHOT), L2_SNAPSHOT, RECEIVED)
    [book] = decode(env.value)
    assert (book.venue, book.snapshot) == ("coinbase", True)
    assert book.bids == ((Decimal("67012.34"), Decimal("0.10000000")),)
    assert book.asks == ((Decimal("67012.35"), Decimal("1.25")),)

    # A v2 envelope (no venue, no snapshot flag) still decodes, as Coinbase.
    old = json.loads(env.value)
    for key in ("venue", "snapshot", "update_ids"):
        del old[key]
    old["v"] = 2
    [book] = decode(json.dumps(old))
    assert book.venue == "coinbase" and book.snapshot is True

    [trade_env] = build_envelopes(raw(TRADES), TRADES, RECEIVED)
    [trade] = decode(trade_env.value)
    assert trade.price == Decimal("2450.10") and trade.aggressor is None


def test_unknown_types_and_heartbeats_decode_to_nothing():
    assert decode(json.dumps({"v": 9, "type": "future", "received_at_ns": 1})) == []
    heartbeat = {
        "v": 3,
        "type": "data",
        "venue": "coinbase",
        "channel": "heartbeats",
        "product_id": None,
        "received_at_ns": 1,
        "payload": {},
    }
    assert decode(json.dumps(heartbeat)) == []


def test_trade_deduper_ignores_order():
    dedupe = TradeDeduper(capacity=3)
    trade = Trade("gemini", "BTC-USD", "10", Decimal(1), Decimal(1), None, None, 0)
    later = Trade("gemini", "BTC-USD", "12", Decimal(1), Decimal(1), None, None, 0)
    earlier = Trade("gemini", "BTC-USD", "11", Decimal(1), Decimal(1), None, None, 0)
    assert [dedupe.is_new(t) for t in (trade, later, earlier, later, trade)] == [
        True,
        True,
        True,
        False,
        False,
    ]
    other_product = Trade("gemini", "ETH-USD", "10", Decimal(1), Decimal(1), None, None, 0)
    assert dedupe.is_new(other_product)


def test_audit_from_another_venue_is_ignored():
    value = build_audit_envelope(
        "{}",
        product_id="BTC-USD",
        update_id=1,
        aligned=True,
        reason="aligned",
        received_at_ns=0,
        venue="coinbase",
        topics=topics_for("coinbase"),
    ).value
    assert decode(value) == []
