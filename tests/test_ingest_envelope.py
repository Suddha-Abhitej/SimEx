import json
from decimal import Decimal

import pytest
from coinbase_samples import HEARTBEAT, L2_SNAPSHOT, SUBSCRIPTIONS, TRADES, raw

from simex.ingest.envelope import build_envelopes, feed_lag_ns, parse_timestamp_ns, product_ids

RECEIVED = 1_792_026_000_250_000_000  # 2026-10-15T01:00:00.25Z


def test_parse_timestamp_keeps_nanoseconds():
    assert parse_timestamp_ns("2026-10-15T01:00:00.123456789Z") == 1_792_026_000_123_456_789
    assert parse_timestamp_ns("2026-10-15T01:00:00.1Z") == 1_792_026_000_100_000_000
    assert parse_timestamp_ns("2026-10-15T01:00:00Z") == 1_792_026_000_000_000_000
    assert parse_timestamp_ns("2026-10-15T02:00:00.5+01:00") == 1_792_026_000_500_000_000


@pytest.mark.parametrize("bad", ["2026-10-15T01:00:00", "2026-10-15T01:00:00.12xZ", "nope"])
def test_parse_timestamp_rejects_bad_input(bad):
    with pytest.raises(ValueError):
        parse_timestamp_ns(bad)


def test_feed_lag():
    assert feed_lag_ns(RECEIVED, 1_792_026_000_200_000_000) == 50_000_000
    assert feed_lag_ns(100, 150) == -50  # clock skew shows up as negative lag, not hidden
    assert feed_lag_ns(RECEIVED, None) is None


def test_level2_envelope():
    text = raw(L2_SNAPSHOT)
    [env] = build_envelopes(text, L2_SNAPSHOT, RECEIVED)
    assert (env.topic, env.key, env.channel, env.product_id) == (
        "md.level2",
        b"BTC-USD",
        "level2",
        "BTC-USD",
    )
    assert env.sequence_num == 1
    assert env.exchange_ts_ns == 1_792_026_000_123_456_789
    assert env.lag_ns == RECEIVED - 1_792_026_000_123_456_789

    decoded = json.loads(env.value)
    assert decoded["received_at_ns"] == RECEIVED
    assert decoded["exchange_ts_ns"] == 1_792_026_000_123_456_789
    assert decoded["channel"] == "level2"
    assert decoded["product_id"] == "BTC-USD"
    assert decoded["sequence_num"] == 1
    assert decoded["payload"] == L2_SNAPSHOT


def test_payload_is_spliced_in_byte_for_byte():
    # Odd spacing and trailing zeros must survive: the payload is never re-serialized.
    text = raw(TRADES).replace('"price":"2450.10"', '"price" : "2450.10"')
    [env] = build_envelopes(text, json.loads(text), RECEIVED)
    assert text.encode() in env.value
    assert env.value.endswith(b',"payload":' + text.encode() + b"}")


def test_prices_and_sizes_stay_strings():
    [env] = build_envelopes(raw(TRADES), TRADES, RECEIVED)
    trade = json.loads(env.value)["payload"]["events"][0]["trades"][0]
    assert trade["price"] == "2450.10" and trade["size"] == "0.00012000"
    assert Decimal(trade["size"]) == Decimal("0.00012")


def test_trades_key_by_trade_product():
    [env] = build_envelopes(raw(TRADES), TRADES, RECEIVED)
    assert (env.topic, env.key, env.product_id) == ("md.trades", b"ETH-USD", "ETH-USD")


def test_heartbeat_keyed_by_heartbeat():
    [env] = build_envelopes(raw(HEARTBEAT), HEARTBEAT, RECEIVED)
    assert (env.topic, env.key, env.channel) == ("md.heartbeats", b"heartbeat", "heartbeats")
    assert json.loads(env.value)["product_id"] is None


def test_message_with_several_products_is_published_once_per_product():
    message = json.loads(raw(TRADES))
    second = {**message["events"][0]["trades"][0], "product_id": "SOL-USD", "trade_id": "9"}
    message["events"][0]["trades"].append(second)
    text = raw(message)
    envs = build_envelopes(text, message, RECEIVED)
    assert [e.key for e in envs] == [b"ETH-USD", b"SOL-USD"]
    assert all(json.loads(e.value)["payload"] == message for e in envs)
    assert product_ids(message) == ["ETH-USD", "SOL-USD"]


def test_non_market_data_is_not_enveloped():
    assert build_envelopes(raw(SUBSCRIPTIONS), SUBSCRIPTIONS, RECEIVED) == []
    assert build_envelopes('{"type":"error"}', {"type": "error"}, RECEIVED) == []


def test_message_without_timestamp_has_no_lag():
    message = {k: v for k, v in TRADES.items() if k != "timestamp"}
    [env] = build_envelopes(raw(message), message, RECEIVED)
    assert env.exchange_ts_ns is None and env.lag_ns is None
