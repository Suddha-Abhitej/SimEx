"""Placing book audits at an exact point in md.level2 (ADR 0012).

The ingestor asks the venue for a product's top levels at an update ID L (Gemini's depth request
returns L equal to the stream's latest u). For the book to compare at exactly L, the audit must
sit in the product's partition immediately after the diff whose u == L:

- the diff with u == L was the last one published for the product: publish the audit now;
- the stream is already past L: publish it now, marked unaligned ("stream_past");
- the diff with u == L hasn't arrived yet: hold the audit, and publish it right after that diff.
  It is published unaligned instead if a later diff passes L first ("stream_past"), if the hold
  times out ("timeout"), if a resync starts for the product ("resync"), or if a newer audit for
  the product replaces it ("superseded").

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
    reason: str  # "aligned", "stream_past", "timeout", "resync" or "superseded"


class AuditAligner:
    def __init__(self, hold_seconds: float, monotonic: Callable[[], float]) -> None:
        self.hold_seconds = hold_seconds
        self._mono = monotonic
        self.last_u: dict[str, int] = {}  # u of the last level2 message published per product
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
            released.append(Release(audit, False, "stream_past"))
        else:
            self.held[audit.product] = (audit, self._mono() + self.hold_seconds)
        return released

    def on_level2(self, product: str, last_update_id: int) -> list[Release]:
        """A level2 message for the product was just published; call after publishing it."""
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
        return [Release(audit, False, "stream_past")]

    def on_resync(self, products: Iterable[str]) -> list[Release]:
        """The products' books are being invalidated: no audit can be aligned with them."""
        released = []
        for product in products:
            self.last_u.pop(product, None)
            if held := self.held.pop(product, None):
                released.append(Release(held[0], False, "resync"))
        return released

    def expire(self) -> list[Release]:
        now = self._mono()
        expired = [p for p, (_, deadline) in self.held.items() if now >= deadline]
        return [Release(self.held.pop(p)[0], False, "timeout") for p in expired]

    def next_deadline(self) -> float | None:
        return min((deadline for _, deadline in self.held.values()), default=None)
