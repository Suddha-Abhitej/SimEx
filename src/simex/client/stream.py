"""Explicit application acknowledgements over the server's resumable notifications."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from urllib.parse import urlencode

from websockets.sync.client import connect

from . import codec


@dataclass
class Checkpoint:
    """Persist alongside application state. None means a first, live subscription.

    Use seq=0 explicitly to request replay from the writer's beginning. The applied ID set
    includes out-of-order acknowledgements beyond seq and grows with applied fills.
    """

    stream_id: str | None = None
    seq: int | None = None
    applied_fill_ids: set[str] = field(default_factory=set)


class StreamGap(Exception):
    def __init__(self, body: dict) -> None:
        self.body = body
        self.reason = body.get("reason")
        super().__init__(
            f"Fill stream gap: {self.reason}; reconcile before choosing a new baseline"
        )


class FillStream(Iterator[dict]):
    def __init__(
        self, url: str, token: str, *, checkpoint: Checkpoint | None, timeout: float
    ) -> None:
        self.checkpoint = checkpoint if checkpoint is not None else Checkpoint()
        query = {}
        if self.checkpoint.seq is not None:
            query["from_seq"] = self.checkpoint.seq
        if self.checkpoint.stream_id is not None:
            query["stream_id"] = self.checkpoint.stream_id
        if query:
            url += "?" + urlencode(query)
        self._connection = connect(
            url,
            additional_headers={"Authorization": f"Bearer {token}"},
            proxy=None,
            open_timeout=timeout,
            close_timeout=timeout,
        )
        self._socket = self._connection.__enter__()
        self._closed = False
        self._pending: deque[tuple[int, str]] = deque()
        self._received: dict[int, str] = {}
        self._inflight_ids: set[str] = set()
        try:
            self.hello = codec.frame(codec.loads(self._socket.recv(timeout=timeout)))
            self.announced_stream_id = self.hello["stream_id"]
            # A new live subscription intentionally starts at head. A replay never does.
            if self.checkpoint.seq is None and self.checkpoint.stream_id is None:
                self.checkpoint.stream_id = self.announced_stream_id
                self.checkpoint.seq = self.hello["seq"]
            self._scanned = self.checkpoint.seq or 0
            self._hello_pending = True
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> FillStream:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._connection.__exit__(None, None, None)

    def __iter__(self) -> FillStream:
        return self

    def __next__(self) -> dict:
        if self._closed:
            raise StopIteration
        if self._hello_pending:
            self._hello_pending = False
            return self.hello
        while True:
            try:
                frame = codec.frame(codec.loads(self._socket.recv()))
            except BaseException:
                self.close()
                raise
            kind = frame.get("type")
            if kind == "gap":
                self.close()
                raise StreamGap(frame)
            if kind == "fill":
                seq, fill_id = frame["seq"], frame["fill"]["fill_id"]
                duplicate = (
                    fill_id in self.checkpoint.applied_fill_ids or fill_id in self._inflight_ids
                )
                if not duplicate:
                    self._pending.append((seq, fill_id))
                    self._received[seq] = fill_id
                    self._inflight_ids.add(fill_id)
                self._scan(seq)
                if duplicate:
                    continue
            elif kind in {"heartbeat", "halt"}:
                self._scan(frame["seq"])
            # Future frame types pass through untouched and do not themselves move the cursor.
            return frame

    def ack(self, frame: dict) -> None:
        """Call after applying a fill. Out-of-order acks cannot jump an unapplied fill."""
        seq, fill_id = frame["seq"], frame["fill"]["fill_id"]
        if self._received.get(seq) != fill_id:
            if fill_id in self.checkpoint.applied_fill_ids and seq <= (self.checkpoint.seq or 0):
                return
            raise ValueError("ack requires a fill delivered by this stream")
        self.checkpoint.applied_fill_ids.add(fill_id)
        self._advance()

    def _scan(self, seq: int) -> None:
        self._scanned = max(self._scanned, seq)
        self._advance()

    def _advance(self) -> None:
        # A heartbeat or later fill proves intervening filtered sequences were scanned, but
        # cannot move past any delivered, unapplied fill. The server orders deliveries by seq.
        while self._pending and self._pending[0][1] in self.checkpoint.applied_fill_ids:
            seq, fill_id = self._pending.popleft()
            del self._received[seq]
            self._inflight_ids.remove(fill_id)
        candidate = self._pending[0][0] - 1 if self._pending else self._scanned
        self.checkpoint.stream_id = self.announced_stream_id
        self.checkpoint.seq = max(self.checkpoint.seq or 0, candidate)
