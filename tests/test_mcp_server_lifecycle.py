import asyncio

from simex.mcp import server


def test_mcp_bind_retries_and_rebinds_after_teardown(monkeypatch):
    calls = 0

    class FakeServer:
        async def run_streamable_http_async(self):
            nonlocal calls
            calls += 1
            if calls <= 2:
                raise OSError("address still in use")

    monkeypatch.setattr(server, "create_server", lambda tools, port: FakeServer())
    asyncio.run(server.run_server_async(object(), port=8765, bind_retries=3, retry_delay=0))
    assert calls == 3
