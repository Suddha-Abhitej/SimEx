"""Synchronous client for the single-account private API (ADR 0024)."""

from __future__ import annotations

import json
from decimal import Decimal
from urllib.parse import quote

import httpx

from . import codec
from .stream import Checkpoint, FillStream


class APIError(Exception):
    """An answer from the server, including reasons this client has never seen."""

    def __init__(self, status: int, body: dict, raw_body: bytes) -> None:
        self.status = status
        self.body = body
        self.raw_body = raw_body
        self.reason = body.get("reason")
        super().__init__(f"HTTP {status}: {self.reason}")


class Client:
    def __init__(
        self, base_url: str, token: str, *, timeout: float = 5.0, order_retries: int = 1
    ) -> None:
        if (
            isinstance(order_retries, bool)
            or not isinstance(order_retries, int)
            or order_retries < 0
        ):
            raise ValueError("order_retries must be a nonnegative integer")
        self._http = httpx.Client(
            base_url=base_url.rstrip("/") + "/",
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
            trust_env=False,
        )
        self._token = token
        self._timeout = timeout
        self._order_retries = order_retries

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def _request(self, method: str, path: str, operation: str, **kwargs) -> dict:
        wire = self._http.request(method, path, **kwargs)
        body = codec.response(codec.loads(wire.content), operation)
        if wire.is_error or body.get("ok") is False:
            raise APIError(wire.status_code, body, wire.content)
        return body

    def quote(self, product_id: str, depth: int = 5) -> dict:
        return self._request(
            "GET", f"v1/quote/{quote(product_id, safe='')}", "quote", params={"depth": depth}
        )

    def features(self, product_id: str, depth: int = 5) -> dict:
        return self._request(
            "GET", f"v1/features/{quote(product_id, safe='')}", "features", params={"depth": depth}
        )

    def place_order(
        self,
        *,
        client_order_id: str,
        product_id: str,
        side: str,
        type: str,
        quantity: Decimal | str,
        reasoning: str,
        limit_price: Decimal | str | None = None,
    ) -> dict:
        payload = {
            "client_order_id": client_order_id,
            "product_id": product_id,
            "side": side,
            "type": type,
            "quantity": codec.money_input("quantity", quantity),
            "reasoning": reasoning,
        }
        if limit_price is not None:
            payload["limit_price"] = codec.money_input("limit_price", limit_price)
        # Serialize once. An uncertain response never changes either the body or its key.
        content = json.dumps(payload).encode()
        for attempt in range(self._order_retries + 1):
            try:
                return self._request(
                    "POST",
                    "v1/orders",
                    "order",
                    content=content,
                    headers={"Content-Type": "application/json"},
                )
            except (httpx.NetworkError, httpx.TimeoutException, httpx.RemoteProtocolError):
                if attempt == self._order_retries:
                    raise
        raise AssertionError("unreachable")

    def cancel(self, order_id: str) -> dict:
        """Request cancellation; fills during the server's cancel delay still stand."""
        return self._request("DELETE", f"v1/orders/{quote(order_id, safe='')}", "order")

    def cancel_by_client_id(self, client_order_id: str) -> dict:
        return self._request(
            "DELETE", f"v1/orders/by-client-id/{quote(client_order_id, safe='')}", "order"
        )

    def open_orders(self) -> dict:
        return self._request("GET", "v1/orders", "order")

    def portfolio(self) -> dict:
        return self._request("GET", "v1/portfolio", "portfolio")

    def fills(self, limit: int = 100) -> dict:
        return self._request("GET", "v1/fills", "fills", params={"limit": limit})

    def reconcile(self) -> dict:
        """Two non-atomic reads, capped at 1,000 fills; no completeness or cursor claim."""
        return {"fills": self.fills(limit=1000), "portfolio": self.portfolio()}

    def stream(self, checkpoint: Checkpoint | None = None) -> FillStream:
        """Connect now, saving hello before returning. No checkpoint means live-only."""
        url = self._http.base_url.join("v1/stream/fills")
        scheme = "wss" if url.scheme == "https" else "ws"
        return FillStream(
            str(url.copy_with(scheme=scheme)),
            self._token,
            checkpoint=checkpoint,
            timeout=self._timeout,
        )
