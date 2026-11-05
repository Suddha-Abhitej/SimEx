"""Private evaluator for Gemini-sourced simulated trading results."""

from .core import (
    AccountMetrics,
    Comparison,
    EquityPoint,
    EvaluationConfig,
    EvaluationResult,
    FillRecord,
    PriceMark,
    PriceSeries,
    evaluate,
    render_report,
)
from .recordings import marks_from_recordings

__all__ = [
    "AccountMetrics",
    "Comparison",
    "EquityPoint",
    "EvaluationConfig",
    "EvaluationResult",
    "FillRecord",
    "PriceMark",
    "PriceSeries",
    "evaluate",
    "marks_from_recordings",
    "render_report",
]
