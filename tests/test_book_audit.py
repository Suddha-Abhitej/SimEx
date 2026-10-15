"""Book audits and the resync back-channel (ADR 0012), with synthetic data only."""

import asyncio
import json
import logging
from decimal import Decimal as D

import pytest
from test_book_service import (
    NOW_MS,
    NOW_NS,
    TOPICS,
    FakeSource,
    depth,
    drain,
    make_service,
    sample,
)

from simex.book import BookState, OrderBook, Outcome
from simex.ingest.audit import AuditAligner, AuditResponse
from simex.ingest.control import build_resync_request, parse_resync_request
from simex.ingest.envelope import build_audit_envelope
from simex.marketdata import BookAudit, BookUpdate, decode

VENUE, PRODUCT = "gemini", "BTC-USD"


# The aligner: where an audit goes in md.level2 ---------------------------------------------------


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now


def response(update_id, product=PRODUCT):
    return AuditResponse(product, update_id, "{}", 0)


def released(releases):
    return [(r.audit.update_id, r.aligned, r.reason) for r in releases]


def test_aligner_publishes_at_once_when_the_matching_diff_was_the_last():
    aligner = AuditAligner(2.0, Clock())
    assert aligner.on_level2(PRODUCT, 40, 50) == []
    assert released(aligner.on_response(response(50))) == [(50, True, "aligned")]


def test_aligner_tells_inside_diff_from_stream_past_when_the_answer_comes_late():
    aligner = AuditAligner(2.0, Clock())
    aligner.on_level2(PRODUCT, 40, 60)
    # L strictly inside the last diff (U < L < u): no point in the stream matches it.
    assert released(aligner.on_response(response(50))) == [(50, False, "inside_diff")]
    # L == U is the end of the diff before: an exact point the stream has already passed.
    assert released(aligner.on_response(response(40))) == [(40, False, "stream_past")]
    aligner.on_level2(PRODUCT, 60, 70)
    assert released(aligner.on_response(response(50))) == [(50, False, "stream_past")]


def test_aligner_an_answer_at_the_snapshot_id_is_stream_past_not_inside_diff():
    aligner = AuditAligner(2.0, Clock())
    aligner.on_level2(PRODUCT, 100, 100)  # a snapshot at 100 (U == u)
    aligner.on_level2(PRODUCT, 95, 120)  # the first diff after it straddles the snapshot's ID
    assert released(aligner.on_response(response(100))) == [(100, False, "stream_past")]
    assert released(aligner.on_response(response(110))) == [(110, False, "inside_diff")]


def test_aligner_holds_an_early_audit_until_its_diff():
    aligner = AuditAligner(2.0, Clock())
    aligner.on_level2(PRODUCT, 30, 40)
    assert aligner.on_response(response(50)) == []
    assert aligner.next_deadline() == 102.0
    assert aligner.on_level2("ETH-USD", 45, 50) == []  # another product's diff changes nothing
    assert aligner.on_level2(PRODUCT, 40, 45) == []  # not there yet
    assert released(aligner.on_level2(PRODUCT, 45, 50)) == [(50, True, "aligned")]
    assert aligner.held == {} and aligner.next_deadline() is None


def test_aligner_releases_a_held_audit_unaligned_when_a_diff_jumps_past_it():
    aligner = AuditAligner(2.0, Clock())
    aligner.on_level2(PRODUCT, 30, 45)
    aligner.on_response(response(50))
    # The live case: Gemini answered mid-batch, and the next 100 ms diff straddles L.
    assert released(aligner.on_level2(PRODUCT, 45, 55)) == [(50, False, "inside_diff")]
    # A diff that starts beyond L (only possible across a break) is stream_past.
    aligner.on_response(response(60))
    assert released(aligner.on_level2(PRODUCT, 61, 70)) == [(60, False, "stream_past")]


def test_aligner_timeout_resync_and_superseded():
    clock = Clock()
    aligner = AuditAligner(2.0, clock)
    aligner.on_response(response(50))
    clock.now += 1.9
    assert aligner.expire() == []
    clock.now += 0.2
    assert released(aligner.expire()) == [(50, False, "timeout")]

    aligner.on_level2(PRODUCT, 30, 40)
    aligner.on_response(response(60))
    assert released(aligner.on_response(response(70))) == [(60, False, "superseded")]
    assert released(aligner.on_resync([PRODUCT, "ETH-USD"])) == [(70, False, "resync")]
    assert PRODUCT not in aligner.last_u  # the next audit waits for the snapshot


# The envelope and the decoder -------------------------------------------------------------------


def gemini_depth(bids, asks, update_id=500):
    return json.dumps(
        {"id": 7, "status": 200, "result": {"lastUpdateId": update_id, "bids": bids, "asks": asks}}
    )


def audit_value(bids, asks, update_id=500, aligned=True, reason="aligned", product=PRODUCT):
    return build_audit_envelope(
        gemini_depth(bids, asks, update_id),
        product_id=product,
        update_id=update_id,
        aligned=aligned,
        reason=reason,
        received_at_ns=NOW_NS,
        venue=VENUE,
        topics=TOPICS,
    ).value


def test_audit_envelope_splices_the_response_and_decodes_to_decimals():
    raw = gemini_depth([["100.00", "1.5"]], [["100.50", "0.75"]])
    envelope = build_audit_envelope(
        raw,
        product_id=PRODUCT,
        update_id=500,
        aligned=True,
        reason="aligned",
        received_at_ns=NOW_NS,
        venue=VENUE,
        topics=TOPICS,
    )
    assert envelope.topic == "md.level2" and envelope.key == PRODUCT.encode()
    assert envelope.value.endswith(b',"payload":' + raw.encode() + b"}")
    header = json.loads(envelope.value)
    assert (header["v"], header["type"], header["update_id"], header["aligned"]) == (
        3,
        "audit",
        500,
        True,
    )
    [audit] = decode(envelope.value)
    bids, asks = ((D("100.00"), D("1.5")),), ((D("100.50"), D("0.75")),)
    assert audit == BookAudit(VENUE, PRODUCT, 500, True, "aligned", bids, asks, NOW_NS)


# The book's compare -----------------------------------------------------------------------------


def synced_book(update_id=500):
    book = OrderBook(VENUE, PRODUCT)
    book.apply(
        BookUpdate(
            venue=VENUE,
            product_id=PRODUCT,
            snapshot=True,
            bids=((D("100.00"), D("1.5")), (D("99.50"), D("2"))),
            asks=((D("100.50"), D("0.75")), (D("101.00"), D("3"))),
            update_ids=(update_id, update_id),
            awaiting_snapshot=False,
            exchange_ts_ns=NOW_NS,
            received_at_ns=NOW_NS,
        )
    )
    return book


def audit(bids, asks, update_id=500, aligned=True):
    def levels(side):
        return tuple((D(p), D(q)) for p, q in side)

    return BookAudit(VENUE, PRODUCT, update_id, aligned, "", levels(bids), levels(asks), NOW_NS)


BIDS = [("100.00", "1.5"), ("99.50", "2")]
ASKS = [("100.50", "0.75"), ("101.00", "3")]


def test_audit_matches_with_exact_decimals_in_any_order():
    book = synced_book()
    # Trailing zeros are the same Decimal value; the venue's order doesn't matter.
    venue_bids = [("99.5", "2.00000000"), ("100.00", "1.50000000")]
    assert book.apply(audit(venue_bids, list(reversed(ASKS)))) is Outcome.AUDIT_MATCH
    assert book.state is BookState.SYNCED and book.audit_mismatch is None


@pytest.mark.parametrize(
    ("bids", "asks", "why"),
    [
        ([("100.00", "1.4"), ("99.50", "2")], ASKS, "bid level 0"),  # size
        (BIDS, [("100.50", "0.75"), ("101.01", "3")], "ask level 1"),  # price
        ([*BIDS, ("99.00", "1")], ASKS, "bid: book has 2 levels, venue 3"),  # a missing level
        (BIDS[:1], ASKS, "bid: book has 2 levels, venue's whole side is 1"),  # an extra level
        (BIDS, [], "ask: book has 2 levels, venue's whole side is 0"),
    ],
)
def test_audit_mismatch_drops_the_book(bids, asks, why):
    book = synced_book()
    assert book.apply(audit(bids, asks)) is Outcome.AUDIT_MISMATCH
    assert book.state is BookState.RESYNCING and book.level_counts() == (0, 0)
    assert book.audit_mismatch.startswith(why)


def test_audit_compares_only_the_top_100_levels():
    book = OrderBook(VENUE, PRODUCT)
    bids = [(D(1000 - i), D(1)) for i in range(150)]
    asks = [(D(2000 + i), D(1)) for i in range(150)]
    book.apply(BookUpdate(VENUE, PRODUCT, True, tuple(bids), tuple(asks), (9, 9), False, 0, 0))
    venue = audit(
        [(str(p), str(q)) for p, q in bids[:100]], [(str(p), "1") for p, _ in asks[:100]], 9
    )
    assert book.apply(venue) is Outcome.AUDIT_MATCH
    deeper_difference = audit([(str(p), "1") for p, _ in bids[:99]] + [("1.5", "1")], [], 9)
    assert book.apply(deeper_difference) is Outcome.AUDIT_MISMATCH


def test_audit_is_skipped_or_unaligned_without_touching_the_book():
    book = synced_book()
    wrong = [("1", "1")]
    assert book.apply(audit(wrong, wrong, aligned=False)) is Outcome.AUDIT_UNALIGNED
    assert book.apply(audit(wrong, wrong, update_id=499)) is Outcome.AUDIT_SKIPPED
    assert book.state is BookState.SYNCED and book.level_counts() == (2, 2)
    empty = OrderBook(VENUE, PRODUCT)
    assert empty.apply(audit(BIDS, ASKS)) is Outcome.AUDIT_SKIPPED
    with pytest.raises(ValueError):
        book.apply(BookAudit(VENUE, "ETH-USD", 500, True, "", (), (), 0))


# The book service: metrics and resync requests --------------------------------------------------


class FakeRequester:
    def __init__(self, fail=0):
        self.sent, self.fail = [], fail

    async def send(self, venue, product_id, reason):
        if self.fail:
            self.fail -= 1
            raise ConnectionError("kafka down")
        self.sent.append((venue, product_id, reason))


def snapshot_value(bids=BIDS, asks=ASKS):
    return depth(PRODUCT, bids=bids, asks=asks, snapshot=True)


def last_u(value):
    return json.loads(value)["update_ids"][1]


async def send_requests(service):
    while not service.requests.empty():
        task = asyncio.create_task(service.request_forever())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def run_service(values, requester, history=1):
    """The first `history` values are in md.level2 before the service starts (it catches up on
    them); the rest arrive live."""
    records = [(PRODUCT, v, NOW_MS - 1000) for v in values]
    source = FakeSource({0: records[:history]})
    service, metrics = make_service(source)
    service.requester = requester

    async def scenario():
        await service.start()
        await drain(service)
        assert service.catching_up(PRODUCT) is False
        source.data[0].extend(records[history:])
        await drain(service)
        await send_requests(service)

    asyncio.run(scenario())
    return service, metrics


def test_service_counts_audits_and_requests_one_resync_per_mismatch(caplog):
    snap = snapshot_value()
    u = last_u(snap)
    wrong = [("100.00", "9"), ("99.50", "2")]
    requester = FakeRequester()
    with caplog.at_level(logging.ERROR, logger="simex.book.service"):
        service, metrics = run_service(
            [
                snap,
                audit_value(BIDS, ASKS, u),  # match
                audit_value(BIDS, ASKS, u, aligned=False, reason="timeout"),  # unaligned
                audit_value(BIDS, ASKS, u - 1),  # skipped: wrong update ID
                audit_value(wrong, ASKS, u),  # mismatch: RESYNCING and a request
                audit_value(wrong, ASKS, u),  # skipped (not SYNCED): no second request
            ],
            requester,
        )

    labels = {"venue": VENUE, "product_id": PRODUCT}
    counts = {
        (outcome, reason): sample(
            metrics, "simex_book_audits_total", outcome=outcome, reason=reason, **labels
        )
        for outcome in ("match", "mismatch", "skipped", "unaligned")
        for reason in ("aligned", "timeout")
    }
    assert {k: v for k, v in counts.items() if v} == {
        ("match", "aligned"): 1,
        ("mismatch", "aligned"): 1,
        ("skipped", "aligned"): 2,
        ("unaligned", "timeout"): 1,
    }
    assert service.books[PRODUCT].state is BookState.RESYNCING
    assert requester.sent == [(VENUE, PRODUCT, "audit_mismatch")]
    assert (
        sample(metrics, "simex_book_resync_requests_total", reason="audit_mismatch", **labels) == 1
    )
    assert sample(metrics, "simex_book_resyncs_total", reason="audit_mismatch", **labels) == 1
    assert "failed its audit" in caplog.text and "bid level 0" in caplog.text


def test_service_asks_again_only_after_the_book_was_synced_again():
    wrong = [("100.00", "9")]
    first = snapshot_value()
    second = snapshot_value()
    requester = FakeRequester()
    service, _ = run_service(
        [
            first,
            audit_value(wrong, ASKS, last_u(first)),
            depth(PRODUCT, bids=[("100", "1")], asks=[("99", "1")]),  # skipped: RESYNCING
            second,  # SYNCED again: a new failure may ask again
            depth(PRODUCT, bids=[("101", "1")]),  # crossed
        ],
        requester,
    )
    assert requester.sent == [(VENUE, PRODUCT, "audit_mismatch"), (VENUE, PRODUCT, "crossed")]
    assert service.requested == {PRODUCT}


def test_a_mismatch_replayed_from_history_sends_no_request():
    snap = snapshot_value()
    requester = FakeRequester()
    service, metrics = run_service(
        [snap, audit_value([("1", "1")], ASKS, last_u(snap)), snapshot_value()],
        requester,
        history=3,
    )
    labels = {"venue": VENUE, "product_id": PRODUCT}
    mismatches = sample(
        metrics, "simex_book_audits_total", outcome="mismatch", reason="aligned", **labels
    )
    assert mismatches == 1
    assert requester.sent == [] and service.books[PRODUCT].ready


def test_a_failed_request_is_not_counted_and_may_be_sent_again():
    snap = snapshot_value()
    requester = FakeRequester(fail=1)
    service, metrics = run_service([snap, audit_value([("1", "1")], ASKS, last_u(snap))], requester)
    assert requester.sent == [] and service.requested == set()
    labels = {"venue": VENUE, "product_id": PRODUCT, "reason": "audit_mismatch"}
    assert sample(metrics, "simex_book_resync_requests_total", **labels) == 0


# The request message ----------------------------------------------------------------------------


def test_resync_request_round_trip_and_garbage():
    value = build_resync_request(VENUE, PRODUCT, "audit_mismatch", 123)
    request = parse_resync_request(value)
    assert (request.venue, request.product_id, request.reason, request.requested_at_ns) == (
        VENUE,
        PRODUCT,
        "audit_mismatch",
        123,
    )
    for garbage in (b"not json", b"[]", b'{"type":"other"}', b'{"type":"resync_request"}'):
        assert parse_resync_request(garbage) is None
