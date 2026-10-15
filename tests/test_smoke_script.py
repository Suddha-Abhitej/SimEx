import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "smoke_test.py"
spec = importlib.util.spec_from_file_location("smoke_test", SCRIPT)
smoke_test = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke_test)


def test_parse_compose_ps_json_lines():
    output = (
        '{"Service": "kafka", "State": "running", "Health": "healthy"}\n'
        '{"Service": "prometheus", "State": "running", "Health": ""}\n'
    )
    assert smoke_test.parse_compose_ps(output) == {"kafka": "healthy", "prometheus": "running"}


def test_parse_compose_ps_json_array():
    output = '[{"Service": "valkey", "State": "exited", "Health": ""}]'
    assert smoke_test.parse_compose_ps(output) == {"valkey": "exited"}


def test_parse_compose_ps_empty():
    assert smoke_test.parse_compose_ps("") == {}
