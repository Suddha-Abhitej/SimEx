"""Thin, localhost-only HTTP transport; trading policy belongs to ExchangeTools."""

from __future__ import annotations

import hmac
import json
import logging
import math
import socket
from contextlib import contextmanager
from typing import TYPE_CHECKING

import uvicorn
from fastapi import FastAPI, Request, WebSocket
from starlette.responses import JSONResponse

from simex.exchange.tools import ExchangeTools

from .stream import stream_fills

if TYPE_CHECKING:
    from simex.exchange.runner_config import RunnerSettings

log = logging.getLogger(__name__)

# The reason is the stable contract. Status is only a convenience for HTTP clients.
REASON_STATUS = {
    "unknown_product": 404,
    "unknown_order": 404,
    "account_halted": 409,
    "writer_halted": 409,
    "ledger_halted": 409,
    "book_not_ready": 503,
    "book_not_synced": 503,
    "catching_up": 503,
    "stale_level2": 503,
    "feed_lagging": 503,
    "no_band_price": 503,
    "ledger_unavailable": 503,
    "risk_limits_missing": 503,
    "history_unavailable": 503,
    "engine_submit_failed": 503,
    "market_closed": 503,
    "no_liquidity": 503,
    **dict.fromkeys(
        (
            "bad_depth",
            "bad_payload",
            "reasoning_required",
            "bad_decimal_or_product",
            "bad_order",
            "bad_field",
            "bad_decimal",
            "product_not_allowed",
            "quantity_limit",
            "notional_limit",
            "position_limit",
            "invalid_order",
            "order_rejected",
            "missing_order_reference",
            "order_terminal",
            "cancel_already_requested",
            "cancel_failed",
            "bad_limit",
            "idempotency_key_reused",
            "insufficient_funds",
            "off_grid_price",
            "off_grid_quantity",
            "below_min_size",
            "bad_product",
            "bad_side",
            "bad_type",
            "bad_quantity",
            "bad_price",
            "bad_reasoning",
            "bad_client_order_id",
            "limit_price_required",
            "unexpected_limit_price",
        ),
        400,
    ),
}


def validate_api_configuration(settings: RunnerSettings, token: str | None) -> None:
    if settings.api_port is None:
        return
    if not isinstance(token, str) or not token.strip():
        raise ValueError("EXCHANGE_API_TOKEN must be set when EXCHANGE_API_PORT enables the API")
    if not settings.ledger_dsn:
        raise ValueError("EXCHANGE_API_PORT requires EXCHANGE_LEDGER_DSN for durable fills")
    if settings.api_port == settings.mcp_port:
        raise ValueError("EXCHANGE_API_PORT and EXCHANGE_MCP_PORT must be different")


def _authorized(headers, token: str) -> bool:
    values = headers.getlist("authorization")
    if len(values) != 1:
        return False
    scheme, separator, credential = values[0].partition(" ")
    return (
        scheme.lower() == "bearer"
        and separator == " "
        and bool(credential)
        and hmac.compare_digest(credential.encode("utf-8"), token.encode("utf-8"))
    )


def _response(body: dict) -> JSONResponse:
    status = 200
    if not body.get("ok", False):
        reason = body.get("reason", "")
        code = reason.split(":", 1)[0]
        status = REASON_STATUS.get(code, 400)
        if code not in REASON_STATUS:
            log.warning("unmapped exchange refusal reason %r; returning HTTP 400", reason)
    return JSONResponse(body, status_code=status)


def create_app(
    tools: ExchangeTools,
    *,
    token: str,
    heartbeat_seconds: float = 15.0,
    send_timeout_seconds: float = 10.0,
) -> FastAPI:
    if not isinstance(token, str) or not token.strip():
        raise ValueError("EXCHANGE_API_TOKEN must be set")
    if tools.writer.ledger is None:
        raise ValueError("the private API requires a ledger for durable fills")
    for value in (heartbeat_seconds, send_timeout_seconds):
        if not math.isfinite(value) or value <= 0:
            raise ValueError("API heartbeat and send timeout must be finite and positive")
    app = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        if request.method == "GET" and request.url.path == "/v1/health":
            return await call_next(request)
        if not _authorized(request.headers, token):
            return JSONResponse(
                {"ok": False, "reason": "unauthorized"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
        return await call_next(request)

    @app.get("/v1/health", response_model=None)
    async def health():
        return {"ok": True}

    def query_integer(request: Request, name: str, default: int) -> int | JSONResponse:
        value = request.query_params.get(name)
        if value is None:
            return default
        try:
            return int(value)
        except ValueError:
            return JSONResponse({"ok": False, "reason": f"bad_{name}"}, status_code=400)

    @app.get("/v1/quote/{product_id}", response_model=None)
    async def quote(product_id: str, request: Request):
        depth = query_integer(request, "depth", 5)
        return (
            depth if isinstance(depth, JSONResponse) else _response(tools.quote(product_id, depth))
        )

    @app.get("/v1/features/{product_id}", response_model=None)
    async def features(product_id: str, request: Request):
        depth = query_integer(request, "depth", 5)
        return (
            depth
            if isinstance(depth, JSONResponse)
            else _response(tools.features(product_id, depth))
        )

    @app.post("/v1/orders", response_model=None)
    async def place_order(request: Request):
        try:
            payload = await request.json()
        except (json.JSONDecodeError, UnicodeDecodeError):
            return JSONResponse({"ok": False, "reason": "bad_json"}, status_code=400)
        if isinstance(payload, dict) and "account_id" in payload:
            return JSONResponse({"ok": False, "reason": "account_id_not_allowed"}, status_code=400)
        return _response(tools.place_order(payload))

    @app.delete("/v1/orders/by-client-id/{client_order_id}", response_model=None)
    async def cancel_by_client_id(client_order_id: str):
        return _response(tools.cancel(client_order_id=client_order_id))

    @app.delete("/v1/orders/{order_id}", response_model=None)
    async def cancel(order_id: str):
        return _response(tools.cancel(order_id=order_id))

    @app.get("/v1/orders", response_model=None)
    async def open_orders():
        return _response(tools.open_orders())

    @app.get("/v1/portfolio", response_model=None)
    async def portfolio():
        return _response(tools.portfolio())

    @app.get("/v1/fills", response_model=None)
    async def fills(request: Request):
        limit = query_integer(request, "limit", 100)
        return limit if isinstance(limit, JSONResponse) else _response(tools.history(limit))

    @app.websocket("/v1/stream/fills")
    async def fill_stream(websocket: WebSocket):
        if not _authorized(websocket.headers, token):
            await websocket.close(code=1008, reason="unauthorized")
            return
        await stream_fills(
            websocket,
            tools,
            from_seq=websocket.query_params.get("from_seq"),
            stream_id=websocket.query_params.get("stream_id"),
            heartbeat_seconds=heartbeat_seconds,
            send_timeout_seconds=send_timeout_seconds,
        )

    return app


class _SupervisedServer(uvicorn.Server):
    @contextmanager
    def capture_signals(self):
        # The exchange process owns signals and supervises this server as a sibling task.
        yield


async def run_server_async(
    tools: ExchangeTools,
    *,
    port: int,
    token: str,
    heartbeat_seconds: float = 15.0,
    send_timeout_seconds: float = 10.0,
) -> None:
    app = create_app(
        tools,
        token=token,
        heartbeat_seconds=heartbeat_seconds,
        send_timeout_seconds=send_timeout_seconds,
    )
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=port,
        access_log=False,
        proxy_headers=False,
        lifespan="off",
        timeout_graceful_shutdown=send_timeout_seconds,
    )
    server = _SupervisedServer(config)
    # Pre-bind so a bind failure raises OSError into group supervision, rather than uvicorn's
    # SystemExit path. The socket is owned by this task and never exposed on another interface.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port))
        listener.listen(config.backlog)
        try:
            await server.serve(sockets=[listener])
        finally:
            if server.started:
                server.should_exit = True
                await server.shutdown(sockets=[listener])
