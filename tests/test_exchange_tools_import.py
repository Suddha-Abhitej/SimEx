import subprocess
import sys


def test_shared_tools_import_without_loading_mcp_transport():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from simex.exchange.tools import ExchangeTools, RiskLimits; "
            "assert not any(n == 'mcp' or n.startswith('mcp.') or n.startswith('simex.mcp') "
            "for n in sys.modules)",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_mcp_compatibility_exports_are_the_same_objects():
    from simex.exchange.pnl import position_pnl
    from simex.exchange.tools import ExchangeTools, RiskLimits
    from simex.mcp import ExchangeTools as MCPTools
    from simex.mcp import RiskLimits as MCPLimits
    from simex.mcp.pnl import position_pnl as mcp_pnl

    assert MCPTools is ExchangeTools
    assert MCPLimits is RiskLimits
    assert mcp_pnl is position_pnl
