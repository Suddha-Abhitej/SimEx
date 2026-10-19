"""Measuring W (#8, ADR 0014) on a synthetic feed with known lags and cancels."""

import json
import random
from decimal import Decimal as D

from simex.exchange.window import WIDE_MS, format_report, measure
from simex.ingest.envelope import build_envelope, topics_for
from simex.ingest.venue import LEVEL2, TRADES, Item

TOPICS = topics_for("gemini")
P = "BTC-USD"
T0 = 1_792_036_800_000_000_000
MS = 1_000_000
_ids = iter(range(1, 10**9))


def level2(t, bids=(), asks=(), snapshot=False):
    n = next(_ids)
    payload = {
        "e": "depthUpdate",
        "E": t,
        "s": "BTCUSD",
        "U": n,
        "u": n,
        "b": [list(b) for b in bids],
        "a": [list(a) for a in asks],
    }
    item = Item(LEVEL2, P, update_ids=(n, n), snapshot=snapshot)
    return t, build_envelope(
        json.dumps(payload), item, venue="gemini", topics=TOPICS, received_at_ns=t, exchange_ts_ns=t
    ).value


def print_(t, price, size, maker_buyer):
    n = next(_ids)
    payload = {"E": t, "m": maker_buyer, "p": price, "q": size, "s": "BTCUSD", "t": n}
    return t, build_envelope(
        json.dumps(payload),
        Item(TRADES, P),
        venue="gemini",
        topics=TOPICS,
        received_at_ns=t,
        exchange_ts_ns=t,
    ).value


def feed(seed=3, fills=300, cancels=200):
    """Bids at 100.00 down to 99.90; each fill shrinks a level and prints 20-120 ms later (a
    tenth of them print 5-30 ms before the shrink); cancels shrink with no print."""
    rng = random.Random(seed)
    sizes = {f"{100 - i / 100:.2f}": D(1000) for i in range(11)}
    out = [
        level2(
            T0,
            bids=[(p, str(q)) for p, q in sizes.items()],
            asks=[("100.01", "1000")],
            snapshot=True,
        )
    ]
    t = T0 + 1000 * MS  # after the snapshot, even for a print that comes first
    lags = []
    for kind in rng.sample(["fill"] * fills + ["cancel"] * cancels, fills + cancels):
        price = rng.choice(list(sizes))
        q = D(rng.randint(1, 20)) / 10
        sizes[price] -= q
        shrink = level2(t, bids=[(price, str(sizes[price]))])
        if kind == "cancel":
            out.append(shrink)
        else:
            lag = rng.randint(20, 120) if rng.random() < 0.9 else -rng.randint(5, 30)
            lags.append(lag)
            out.append(shrink)
            out.append(print_(t + lag * MS, price, str(q), True))  # a sell hit the bid
        t += 1000 * MS  # one event a second keeps fills from overlapping
    out.sort(key=lambda pair: pair[0])
    return [value for _, value in out], lags


def test_the_window_finds_the_lags_and_counts_cancels_as_unmatched():
    values, lags = feed()
    result = measure(values)[P]
    assert result.matched_print_share(WIDE_MS) == 1
    # The distribution of print minus shrink reproduces the generated lags.
    assert -30 <= result.percentile_ms(0.01) < 0
    assert 20 <= result.percentile_ms(0.5) <= 120
    assert result.percentile_ms(0.99) <= 120
    # The chosen W covers the slowest prints, and no more than it needs to.
    assert 100 <= result.chosen_ms() <= 150
    assert result.matched_print_share(25) < D("0.5")
    # At the chosen W, what is left of the shrinks is the cancels: 200 of 500 events.
    w = result.chosen_ms()
    assert result.unmatched_shrink_events(w) == D(200) / D(500)
    text = format_report({P: result})
    assert "W for BTC-USD" in text and "100.00" not in text  # no prices printed


def test_prints_before_a_snapshot_and_while_resyncing_are_handled():
    values, _ = feed(fills=20, cancels=0)
    # Drop the snapshot: the book is never SYNCED, so every print is skipped, none matched.
    result = measure(values[1:])[P]
    assert result.prints_skipped_unsynced == 20
    assert result.matched_print_share(WIDE_MS) == 0 and result.chosen_ms() is None


def test_no_w_is_chosen_when_prints_do_not_explain_shrinks():
    """Prints at prices with no matching shrink (independent of the book) leave the wide window
    mostly unmatched: the report refuses to pick a W."""
    values, _ = feed(fills=0, cancels=100)
    extra = [print_(T0 + (i * 1000 + 500) * MS, "99.95", "0.5", True)[1] for i in range(50)]
    stream = sorted(values + extra, key=lambda v: json.loads(v)["received_at_ns"])
    result = measure(stream)[P]
    assert result.matched_print_share(WIDE_MS) < D("0.8")
    assert result.chosen_ms() is None
    assert "no W for BTC-USD" in format_report({P: result})
