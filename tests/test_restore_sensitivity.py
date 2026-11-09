import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest
from test_restore_cost import fixture
from test_restore_cost import maintenance_dsn as maintenance_dsn

TOOLS = Path(__file__).resolve().parents[1] / "tools"
spec = importlib.util.spec_from_file_location(
    "measure_restore_sensitivity", TOOLS / "measure_restore_sensitivity.py"
)
sensitivity = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(TOOLS))
try:
    spec.loader.exec_module(sensitivity)
finally:
    sys.path.remove(str(TOOLS))
driver = sensitivity.driver


@pytest.fixture
def restore_profile(monkeypatch):
    for module in (driver, fixture):
        for name in ("ACTIVE", "RATIONALE"):
            monkeypatch.setattr(module, name, getattr(module, name))


def test_sensitivity_profiles_reset_rationale_and_keep_baseline_separate(restore_profile):
    assert sensitivity.configure("long_rationale") == (1000, 16, 8192)
    assert len(fixture.RATIONALE.encode()) == 8192
    assert sensitivity.configure("many_live") == (10000, 1024, 128)
    assert fixture.ACTIVE == driver.ACTIVE == 1024
    assert len(fixture.RATIONALE.encode()) == 128
    with pytest.raises(KeyError):
        sensitivity.configure("arbitrary_profile")


@pytest.mark.skipif(sys.platform != "linux", reason="Linux measurement workers")
def test_sensitivity_worker_uses_same_guarded_seed_and_fresh_process(
    maintenance_dsn, restore_profile, monkeypatch
):
    count, _, _ = sensitivity.configure("long_rationale")
    monkeypatch.setenv(sensitivity.PROFILE_ENV, "long_rationale")

    async def scenario():
        try:
            async with fixture.disposable_database(maintenance_dsn) as target:
                await fixture.seed(target, count, "filled")
                result = await asyncio.to_thread(
                    driver.run_worker,
                    target,
                    count,
                    "default",
                    1024**3,
                    worker_script=Path(sensitivity.__file__),
                )
                assert result["status"] == "ok"
                assert result["checked_keys"] == 3 and result["lookup_benchmark_ok"] == 1
                assert result["restored_orders"] == result["restored_keys"] == 16
        except fixture.MeasurementSkipped as exc:
            pytest.skip(str(exc))

    asyncio.run(scenario())
