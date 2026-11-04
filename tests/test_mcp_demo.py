import importlib.util
import json
from decimal import Decimal
from pathlib import Path

import pytest
from test_mcp import make_tools

_DEMO_SPEC = importlib.util.spec_from_file_location(
    "mcp_demo", Path(__file__).parents[1] / "tools" / "mcp_demo.py"
)
assert _DEMO_SPEC is not None and _DEMO_SPEC.loader is not None
_DEMO_MODULE = importlib.util.module_from_spec(_DEMO_SPEC)
_DEMO_SPEC.loader.exec_module(_DEMO_MODULE)
record_demo = _DEMO_MODULE.record_demo


class CleanReconciliation:
    def run_once(self):
        return ()


class MismatchedReconciliation:
    def run_once(self):
        return (object(),)


def test_demo_derives_crossing_order_from_quote(tmp_path):
    tools = make_tools()
    tools.writer.deposit("owner", "USD", Decimal("1000"), "demo-deposit")

    record_demo(tools, tmp_path / "demo.json", reconciliation=CleanReconciliation())

    result = json.loads((tmp_path / "demo.json").read_text(encoding="utf-8"))
    order = result[1]["result"]["order"]
    assert order["limit_price"] == "101"
    assert Decimal(order["quantity"]) <= Decimal("1")
    assert Decimal(order["quantity"]) > 0


def test_demo_refuses_to_write_on_reconciliation_mismatch(tmp_path):
    output = tmp_path / "demo.json"
    tools = make_tools()
    tools.writer.deposit("owner", "USD", Decimal("1000"), "demo-deposit")
    with pytest.raises(AssertionError, match="mismatches"):
        record_demo(tools, output, reconciliation=MismatchedReconciliation())
    assert not output.exists()
