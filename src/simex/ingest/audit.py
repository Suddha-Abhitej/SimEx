"""Placing book audits at an exact point in md.level2 (ADR 0012).

The ingestor asks the venue for a product's top levels at an update ID L (Gemini's depth request
answers at the moment it's asked: L is the stream's latest u when that falls between 100 ms
batches). For the book to compare at exactly L, the audit must sit in the product's partition
immediately after the diff whose u == L:

- the diff with u == L was the last one published for the product: publish the audit now;
- the diff with u == L hasn't arrived yet: hold the audit, and publish it right after that diff.

Otherwise the audit is published unaligned, with the reason:
- "inside_diff": L falls strictly inside a diff, U < L < u, and isn't the end of the message
  before it. No point in the stream matches it. Gemini answers depth at the moment it's asked
  while the stream batches every 100 ms, so this is the usual case (about a third of audits
  live, ADR 0012). Gemini's U is the previous diff's u, so L == U is the end of the previous
  diff; and the first diff after a snapshot starts below the snapshot's ID. Both are exact
  points the stream has passed: "stream_past".
- "stream_past": the stream had already passed L at a message boundary when the answer arrived.
- "timeout": the hold ran out; "resync": a resync started for the product; "superseded": a newer
  audit for the product arrived first.

The book counts an unaligned audit as unaligned, never as a match. This class only decides; the
ingestor publishes what it returns, in order.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class AuditResponse:
    product: str
    update_id: int  # L
    raw: str | bytes  # the venue's response, published byte for byte as the payload
    received_at_ns: int


@dataclass(frozen=True)
class Release:
    """An audit to publish now."""

    audit: AuditResponse
    aligned: bool
    reason: str  # "aligned", "inside_diff", "stream_past", "timeout", "resync", "superseded"


class AuditAligner:
    def __init__(self, hold_seconds: float, monotonic: Callable[[], float]) -> None:
        self.hold_seconds = hold_seconds
        self._mono = monotonic
        self.last_u: dict[str, int] = {}  # u of the last level2 message published per product
        self.last_first: dict[str, int] = {}  # and its U
        self.prev_u: dict[str, int] = {}  # u of the message before it
        self.held: dict[str, tuple[AuditResponse, float]] = {}  # product -> (audit, deadline)

    def on_response(self, audit: AuditResponse) -> list[Release]:
        """A depth response arrived. Returns what to publish now (maybe nothing: held)."""
        released = []
        if previous := self.held.pop(audit.product, None):
            released.append(Release(previous[0], False, "superseded"))
        last = self.last_u.get(audit.product)
        if last == audit.update_id:
            released.append(Release(audit, True, "aligned"))
        elif last is not None and last > audit.update_id:
            reason = self._past_reason(audit.product, audit.update_id)
            released.append(Release(audit, False, reason))
        else:
            self.held[audit.product] = (audit, self._mono() + self.hold_seconds)
        return released

    def on_level2(self, product: str, first_update_id: int, last_update_id: int) -> list[Release]:
        """A level2 message for the product (U, u) was just published; call after publishing it."""
        if (previous := self.last_u.get(product)) is not None:
            self.prev_u[product] = previous
        self.last_first[product] = first_update_id
        self.last_u[product] = last_update_id
        held = self.held.get(product)
        if held is None:
            return []
        audit = held[0]
        if last_update_id < audit.update_id:
            return []  # not there yet
        del self.held[product]
        if last_update_id == audit.update_id:
            return [Release(audit, True, "aligned")]
        return [Release(audit, False, self._past_reason(product, audit.update_id))]

    def on_resync(self, products: Iterable[str]) -> list[Release]:
        """The products' books are being invalidated: no audit can be aligned with them."""
        released = []
        for product in products:
            self.last_u.pop(product, None)
            self.last_first.pop(product, None)
            self.prev_u.pop(product, None)
            if held := self.held.pop(product, None):
                released.append(Release(held[0], False, "resync"))
        return released

    def expire(self) -> list[Release]:
        now = self._mono()
        expired = [p for p, (_, deadline) in self.held.items() if now >= deadline]
        return [Release(self.held.pop(p)[0], False, "timeout") for p in expired]

    def next_deadline(self) -> float | None:
        return min((deadline for _, deadline in self.held.values()), default=None)

    def _past_reason(self, product: str, update_id: int) -> str:
        """Why an audit at update_id can't sit behind the product's last message, whose u is
        past it: inside that message, or at a boundary the stream has already passed."""
        inside = self.last_first[product] < update_id and self.prev_u.get(product) != update_id
        return "inside_diff" if inside else "stream_past"
