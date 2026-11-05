"""Account-filtered delivery of the writer's bounded, post-commit notifications."""

from __future__ import annotations

import asyncio
import time

import anyio
from starlette.websockets import WebSocket, WebSocketDisconnect

from simex.exchange.tools import ExchangeTools


async def stream_fills(
    socket: WebSocket,
    tools: ExchangeTools,
    *,
    from_seq: str | None,
    stream_id: str | None,
    heartbeat_seconds: float,
    send_timeout_seconds: float,
) -> None:
    writer = tools.writer
    account = tools.account_id

    async def send(frame: dict) -> None:
        async with asyncio.timeout(send_timeout_seconds):
            await socket.send_json(frame)

    async def close(code: int, reason: str) -> None:
        async with asyncio.timeout(send_timeout_seconds):
            await socket.close(code=code, reason=reason)

    async def gap(reason: str, cursor: int) -> None:
        oldest, latest, _ = writer.events_after(writer.event_sequence)
        await send(
            {
                "type": "gap",
                "reason": reason,
                "stream_id": writer.stream_id,
                "from_seq": cursor,
                "oldest_seq": oldest,
                "seq": latest,
                "history": "/v1/fills",
            }
        )
        await close(1008, "Fill history gap; reconcile GET /v1/fills, then reconnect")

    async def disconnected() -> None:
        while (await socket.receive())["type"] != "websocket.disconnect":
            pass

    receiver = None
    waiter = None
    try:
        await socket.accept()
        with writer.watch_events() as changed:
            cursor = writer.event_sequence
            await send(
                {
                    "type": "hello",
                    "stream_id": writer.stream_id,
                    "seq": cursor,
                    "writer_halted": writer.halted,
                    "account_halted": account in writer.halted_accounts,
                }
            )
            if from_seq is not None:
                if not from_seq.isascii() or not from_seq.isdecimal():
                    await send({"type": "error", "reason": "bad_from_seq"})
                    await close(1008, "from_seq must be a nonnegative integer")
                    return
                try:
                    cursor = int(from_seq)
                except ValueError:
                    await send({"type": "error", "reason": "bad_from_seq"})
                    await close(1008, "from_seq must be a nonnegative integer")
                    return
                if (cursor > 0 and stream_id != writer.stream_id) or (
                    stream_id is not None and stream_id != writer.stream_id
                ):
                    await gap("stream_changed", cursor)
                    return
                if cursor > writer.event_sequence:
                    await gap("cursor_ahead", cursor)
                    return
            elif stream_id is not None and stream_id != writer.stream_id:
                await gap("stream_changed", cursor)
                return

            # Announce an already-halted writer/account immediately, even on a live-only join.
            for scope, halted in (
                ("writer", writer.halted),
                ("account", account in writer.halted_accounts),
            ):
                if halted and from_seq is None:
                    await send({"type": "halt", "scope": scope, "halted": True, "seq": cursor})

            receiver = asyncio.create_task(disconnected())
            loop = asyncio.get_running_loop()
            next_heartbeat = loop.time() + heartbeat_seconds
            while True:
                changed.clear()
                oldest, _, events = writer.events_after(cursor)
                if cursor < oldest - 1:
                    await gap("events_evicted", cursor)
                    return
                for event in events:
                    if event["type"] == "fill" and event["fill"]["account_id"] == account:
                        await send({"type": "fill", "seq": event["seq"], "fill": event["fill"]})
                    elif event["type"] == "halt" and (
                        event["scope"] == "writer" or event.get("account_id") == account
                    ):
                        await send({key: event[key] for key in ("type", "seq", "scope", "halted")})
                    cursor = event["seq"]
                if loop.time() >= next_heartbeat:
                    await send(
                        {
                            "type": "heartbeat",
                            "ts_ns": time.time_ns(),
                            "stream_id": writer.stream_id,
                            "seq": cursor,
                        }
                    )
                    next_heartbeat = loop.time() + heartbeat_seconds
                waiter = asyncio.create_task(changed.wait())
                done, _ = await asyncio.wait(
                    (receiver, waiter),
                    timeout=max(0, next_heartbeat - loop.time()),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if receiver in done:
                    receiver.result()
                    return
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)
                waiter = None
    except TimeoutError:
        # A blocked peer must not keep a task or buffered history alive indefinitely.
        try:
            await close(1013, "Client too slow; reconnect with stream_id and from_seq")
        except (TimeoutError, WebSocketDisconnect, OSError):
            pass
    except (WebSocketDisconnect, OSError):
        pass
    finally:
        tasks = [task for task in (receiver, waiter) if task is not None]
        for task in tasks:
            task.cancel()
        if tasks:
            # ASGI hosts may use level-triggered cancellation. Finish joining the cancelled
            # children even when the enclosing request scope has already been cancelled.
            with anyio.CancelScope(shield=True):
                await asyncio.gather(*tasks, return_exceptions=True)
