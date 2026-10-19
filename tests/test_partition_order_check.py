"""The partition order check on hand-made records."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace as R

SCRIPT = Path(__file__).resolve().parent.parent / "tools" / "partition_order_check.py"
spec = importlib.util.spec_from_file_location("partition_order_check", SCRIPT)
check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check)

MS = 1_000_000
H = 1_792_036_800_000_000_000  # a UTC hour


def test_a_held_audit_behind_later_diffs_is_found_with_how_far_back():
    records = [
        R(topic="md.level2", partition=0, offset=0, received_at_ns=H + 1000 * MS, type="data"),
        R(topic="md.level2", partition=0, offset=1, received_at_ns=H + 1300 * MS, type="data"),
        # An audit whose response came at 1050 ms, published after the 1300 ms diff.
        R(topic="md.level2", partition=0, offset=2, received_at_ns=H + 1050 * MS, type="audit"),
        R(topic="md.level2", partition=0, offset=3, received_at_ns=H + 1301 * MS, type="data"),
        # Another partition, read in offset order whatever order it arrives in.
        R(topic="md.trades", partition=1, offset=1, received_at_ns=H + 20 * MS, type="data"),
        R(topic="md.trades", partition=1, offset=0, received_at_ns=H + 10 * MS, type="data"),
    ]
    total, breaks, over, largest = check.order_breaks(records)
    assert total == 6
    assert breaks == {("audit", H): 1}
    assert over == {100: 1, 210: 1} and largest == 250
