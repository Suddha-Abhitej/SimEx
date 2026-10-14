import importlib

import simex

COMPONENTS = [
    "ingest",
    "book",
    "exchange",
    "ledger",
    "recorder",
    "features",
    "agents",
    "evaluator",
    "mcp",
    "playground",
]


def test_version() -> None:
    assert simex.__version__ == "0.0.1"


def test_components_import() -> None:
    for name in COMPONENTS:
        module = importlib.import_module(f"simex.{name}")
        assert module.__doc__, f"simex.{name} should describe its responsibility"
