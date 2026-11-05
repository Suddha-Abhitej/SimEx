"""Build Gemini mark series from private recorder Parquet files."""

from __future__ import annotations

from pathlib import Path

from simex.book.orderbook import BookNotReady, OrderBook
from simex.marketdata import BookUpdate, decode
from simex.recorder.replay import read

from .core import PriceMark


def marks_from_recordings(
    root: str | Path,
    start_ns: int,
    end_ns: int,
    products: list[str] | tuple[str, ...],
) -> list[PriceMark]:
    """Replay Gemini level2 recordings and return exact Decimal midpoint marks."""
    books = {product: OrderBook("gemini", product) for product in products}
    marks: list[PriceMark] = []
    for record in read(
        root,
        start_ns,
        end_ns,
        venue="gemini",
        products=products,
        channels=["level2"],
    ):
        for event in decode(record.value):
            if not isinstance(event, BookUpdate) or event.product_id not in books:
                continue
            books[event.product_id].apply(event)
            try:
                mid = books[event.product_id].mid()
            except BookNotReady:
                continue
            if mid is not None:
                marks.append(PriceMark(event.product_id, event.received_at_ns, mid))
    return marks


__all__ = ["marks_from_recordings"]
