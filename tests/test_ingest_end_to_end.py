"""End-to-end against a fake Coinbase WebSocket server on localhost and a fake Kafka producer."""

import asyncio
import json

import websockets
from coinbase_samples import HEARTBEAT, L2_SNAPSHOT, SUBSCRIPTIONS, TRADES, raw

from simex.ingest.client import CoinbaseIngestor
from simex.ingest.config import Settings
from simex.ingest.metrics import IngestMetrics


class FakeProducer:
    def __init__(self) -> None:
        self.records: list[tuple[str, bytes, bytes]] = []

    async def send(self, topic: str, value: bytes, key: bytes) -> asyncio.Future:
        self.records.append((topic, key, value))
        future = asyncio.get_running_loop().create_future()
        future.set_result(None)
        return future


def sample(metrics: IngestMetrics, name: str, **labels) -> float:
    return metrics.registry.get_sample_value(name, labels or None) or 0.0


async def wait_until(condition, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("timed out waiting for condition")
        await asyncio.sleep(0.01)


async def no_sleep(_: float) -> None:
    await asyncio.sleep(0)


def make_ingestor(url: str, producer: FakeProducer, metrics: IngestMetrics, stale: float = 5):
    settings = Settings(ws_url=url, stale_after_seconds=stale)
    clock = iter(range(1_792_026_000_500_000_000, 1_792_026_100_000_000_000, 1_000_000))
    return CoinbaseIngestor(
        settings, producer, metrics, clock_ns=lambda: next(clock), sleep=no_sleep
    )


def test_messages_reach_kafka_with_gap_detection():
    subscribes: list[dict] = []

    async def coinbase(ws):
        for _ in range(3):
            subscribes.append(json.loads(await ws.recv()))
        for message in (
            raw(SUBSCRIPTIONS),
            raw(L2_SNAPSHOT),
            raw(TRADES),
            raw(HEARTBEAT),
            raw(L2_SNAPSHOT, sequence_num=7),  # 4, 5 and 6 never arrive
        ):
            await ws.send(message)
        await ws.wait_closed()

    async def scenario():
        producer, metrics = FakeProducer(), IngestMetrics()
        async with websockets.serve(coinbase, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            ingestor = make_ingestor(f"ws://127.0.0.1:{port}", producer, metrics)
            task = asyncio.create_task(ingestor.run())
            await wait_until(lambda: len(producer.records) == 4)
            connected = sample(metrics, "simex_ingest_connected")
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return producer, metrics, connected

    producer, metrics, connected = asyncio.run(scenario())

    assert {s["channel"] for s in subscribes} == {"level2", "market_trades", "heartbeats"}
    assert all(s["product_ids"] == ["BTC-USD", "ETH-USD", "SOL-USD"] for s in subscribes)

    assert [(topic, key) for topic, key, _ in producer.records] == [
        ("md.level2", b"BTC-USD"),
        ("md.trades", b"ETH-USD"),
        ("md.heartbeats", b"heartbeat"),
        ("md.level2", b"BTC-USD"),
    ]
    trade = json.loads(producer.records[1][2])
    assert trade["payload"] == TRADES
    assert trade["channel"] == "market_trades" and trade["sequence_num"] == 2

    assert sample(metrics, "simex_ingest_messages_total", channel="level2") == 2
    assert sample(metrics, "simex_ingest_messages_total", channel="market_trades") == 1
    assert sample(metrics, "simex_ingest_messages_total", channel="heartbeats") == 1
    assert sample(metrics, "simex_ingest_sequence_gaps_total", kind="gap") == 1
    assert sample(metrics, "simex_ingest_missed_messages_total") == 3
    assert sample(metrics, "simex_ingest_published_total", topic="md.level2") == 2
    assert sample(metrics, "simex_ingest_feed_lag_seconds_count", channel="market_trades") == 1
    assert connected == 1
    assert sample(metrics, "simex_ingest_connected") == 0  # cleared once the task stops


def test_silent_socket_is_treated_as_stale_and_reconnected():
    connections = 0

    async def silent_coinbase(ws):
        nonlocal connections
        connections += 1
        await ws.send(raw(SUBSCRIPTIONS))
        await ws.wait_closed()  # then never sends another message

    async def scenario():
        producer, metrics = FakeProducer(), IngestMetrics()
        async with websockets.serve(silent_coinbase, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            ingestor = make_ingestor(f"ws://127.0.0.1:{port}", producer, metrics, stale=0.2)
            task = asyncio.create_task(ingestor.run())
            await wait_until(lambda: connections >= 2)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return metrics

    metrics = asyncio.run(scenario())
    assert sample(metrics, "simex_ingest_reconnects_total", reason="stale") >= 1
    assert connections >= 2


def test_connection_refused_backs_off_and_retries():
    async def scenario():
        producer, metrics = FakeProducer(), IngestMetrics()
        # Nothing listens on this port, so every attempt fails.
        ingestor = make_ingestor("ws://127.0.0.1:9", producer, metrics)
        task = asyncio.create_task(ingestor.run())
        await wait_until(lambda: ingestor.backoff.attempt >= 3)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return metrics

    metrics = asyncio.run(scenario())
    assert sample(metrics, "simex_ingest_reconnects_total", reason="connect_failed") >= 3


def test_kafka_refusing_a_message_does_not_stop_the_feed():
    from aiokafka.errors import MessageSizeTooLargeError

    class RefusingProducer(FakeProducer):
        async def send(self, topic, value, key):
            if topic == "md.level2":
                raise MessageSizeTooLargeError()
            return await super().send(topic, value, key)

    async def coinbase(ws):
        for _ in range(3):
            await ws.recv()
        for message in (raw(L2_SNAPSHOT), raw(TRADES), raw(HEARTBEAT)):
            await ws.send(message)
        await ws.wait_closed()

    async def scenario():
        producer, metrics = RefusingProducer(), IngestMetrics()
        async with websockets.serve(coinbase, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            ingestor = make_ingestor(f"ws://127.0.0.1:{port}", producer, metrics)
            task = asyncio.create_task(ingestor.run())
            await wait_until(lambda: len(producer.records) == 2)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return producer, metrics

    producer, metrics = asyncio.run(scenario())
    assert [topic for topic, _, _ in producer.records] == ["md.trades", "md.heartbeats"]
    assert sample(metrics, "simex_ingest_publish_errors_total", topic="md.level2") == 1
    assert sample(metrics, "simex_ingest_reconnects_total", reason="closed") == 0
