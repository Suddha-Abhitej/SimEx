import asyncio
from decimal import Decimal

from simex.exchange import (
    DEFAULT_INSTRUMENTS,
    AccountWriter,
    FeeSchedule,
    GateDecision,
    PairEngine,
)
from simex.exchange.runner_config import RunnerSettings
from simex.exchange.runner_metrics import RunnerMetrics
from simex.exchange.runtime import LiveRuntimeGroup
from simex.ledger import AccountState, LedgerMetrics
from simex.mcp import RiskLimits


class OpenGate:
    def check(self, product_id, now_ns):
        return GateDecision(True, None, Decimal("100"), Decimal("101"))


class FakeLedger:
    def __init__(self, dsn, *, metrics=None):
        self.balances = {}
        self.sequence = None

    def migrate(self):
        pass

    def load_account(self, account_id):
        rows = ()
        if self.sequence is not None:
            rows = ({"account_id": account_id, "asset": "USD", "total": "1000"},)
        return AccountState((), (), rows, self.sequence)

    def record_deposit(self, account_id, asset, amount, event_key):
        self.balances[account_id] = amount
        self.sequence = 1
        return self.sequence

    def save_order(self, order, reservation, band_price=None):
        pass

    def close(self):
        pass


class FakeStore:
    def __init__(self, dsn):
        self.writer = None

    def close(self):
        pass


class FakeSource:
    def __init__(self):
        self.stopped = False

    async def stop(self):
        self.stopped = True


class FakeReconciliation:
    instances = []

    def __init__(self, store, metrics, *, interval_seconds):
        self.writer = None
        self.__class__.instances.append(self)

    def register_writer(self, account_id, writer):
        self.writer = writer

    async def run_forever(self):
        await asyncio.Future()

    def run_once(self):
        projection, _marker = self.writer.balance_snapshot("owner")
        return () if projection == {"USD": Decimal("1000")} else (object(),)


class FakeRunner:
    calls = 0

    def __init__(self, settings, source, metrics, *, ledger):
        self.writer = AccountWriter(
            DEFAULT_INSTRUMENTS, FeeSchedule(), OpenGate(), 0, ledger=ledger
        )
        self.engines = {
            "BTC-USD": PairEngine("BTC-USD", self.writer),
            "ETH-USD": PairEngine("ETH-USD", self.writer),
            "SOL-USD": PairEngine("SOL-USD", self.writer),
        }

    async def run(self):
        type(self).calls += 1
        self.writer.restore("owner")
        if "owner" not in self.writer.applied_sequences:
            self.writer.deposit("owner", "USD", Decimal("1000"), "seed:owner:USD:v1")
        if type(self).calls == 1:
            raise RuntimeError("source failed")
        await asyncio.Future()


def test_rebuild_rebinds_mcp_and_reconciliation_to_current_writer(monkeypatch):
    import simex.exchange.runtime as runtime

    monkeypatch.setattr(runtime, "SyncLedger", FakeLedger)
    monkeypatch.setattr(runtime, "ReconciliationJob", FakeReconciliation)
    settings = RunnerSettings(
        pairs=("BTC-USD", "ETH-USD", "SOL-USD"),
        ledger_dsn="fake",
        mcp_port=8765,
        account_id="owner",
        initial_usd=Decimal("1000"),
    )
    captured = []
    release = asyncio.Event()
    sources = []

    def source_factory():
        source = FakeSource()
        sources.append(source)
        return source

    async def server(tools, *, port):
        captured.append(tools)
        await release.wait()

    async def scenario():
        first = LiveRuntimeGroup(
            settings,
            RunnerMetrics(),
            source_factory,
            ledger_metrics=LedgerMetrics(),
            risk_limits=RiskLimits(
                {"BTC-USD": Decimal("1"), "ETH-USD": Decimal("1"), "SOL-USD": Decimal("1")},
                {
                    "BTC-USD": Decimal("1000"),
                    "ETH-USD": Decimal("1000"),
                    "SOL-USD": Decimal("1000"),
                },
            ),
            server_runner=server,
            runner_factory=FakeRunner,
            store_factory=FakeStore,
        )
        try:
            await first.run()
        except RuntimeError as exc:
            assert str(exc) == "source failed"
        captured.clear()
        second = LiveRuntimeGroup(
            settings,
            RunnerMetrics(),
            source_factory,
            ledger_metrics=LedgerMetrics(),
            risk_limits=first.risk_limits,
            server_runner=server,
            runner_factory=FakeRunner,
            store_factory=FakeStore,
        )
        task = asyncio.create_task(second.run())
        while not captured:
            await asyncio.sleep(0)
        result = captured[-1].place_order(
            {
                "client_order_id": "after-reconnect",
                "product_id": "BTC-USD",
                "side": "buy",
                "type": "limit",
                "quantity": "0.1",
                "limit_price": "100",
            }
        )
        assert result["ok"]
        assert result["order"]["order_id"] in second.runner.writer.orders
        assert FakeReconciliation.instances[-1].writer is second.runner.writer
        assert FakeReconciliation.instances[-1].run_once() == ()
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    asyncio.run(scenario())
    assert all(source.stopped for source in sources)


def test_ledger_construction_failure_stops_source(monkeypatch):
    import simex.exchange.runtime as runtime

    class BrokenLedger:
        def __init__(self, dsn, *, metrics=None):
            raise RuntimeError("ledger unavailable")

    monkeypatch.setattr(runtime, "SyncLedger", BrokenLedger)
    source = FakeSource()
    settings = RunnerSettings(ledger_dsn="fake", pairs=("BTC-USD",))

    async def scenario():
        group = LiveRuntimeGroup(settings, RunnerMetrics(), lambda: source)
        try:
            await group.run()
        except RuntimeError as exc:
            assert str(exc) == "ledger unavailable"

    asyncio.run(scenario())
    assert source.stopped


def test_mcp_requires_environment_risk_limits():
    settings = RunnerSettings(
        pairs=("BTC-USD",),
        ledger_dsn=None,
        mcp_port=8765,
    )
    group = LiveRuntimeGroup(settings, RunnerMetrics(), FakeSource)

    try:
        asyncio.run(group.run())
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("missing MCP risk limits were accepted")

    assert "EXCHANGE_MCP_MAX_QUANTITY_BTC_USD" in message
    assert "EXCHANGE_MCP_MAX_NOTIONAL_BTC_USD" in message
