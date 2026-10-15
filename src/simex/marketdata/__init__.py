"""Venue-independent market data read from the md.* topics (ADR 0011).

Consumers (order book, matching engine, recorder replay) read envelopes through decode() and never
parse a venue's payload themselves, so nothing downstream depends on which exchange sent it.
"""

from .decode import BookAudit, BookUpdate, ResyncMarker, Trade, TradeDeduper, decode

__all__ = ["BookAudit", "BookUpdate", "ResyncMarker", "Trade", "TradeDeduper", "decode"]
