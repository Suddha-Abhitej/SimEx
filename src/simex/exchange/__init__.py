"""The exchange core: orders, balances, and the fill simulator against the live book.

Simulation only (ADR 0002): nothing here places a real order or moves real money.
"""

from .accounts import EXTERNAL, FEES, MARKET, Accounts, InsufficientFunds
from .engine import CancelAttribution, EngineSettings, PairEngine
from .fees import FeeSchedule, Liquidity
from .gate import GateDecision, GateState, MarketStatus, StaleDataGate
from .instruments import DEFAULT_INSTRUMENTS, Instrument, instruments_from_env
from .orders import (
    Fill,
    FillCause,
    InvalidTransition,
    Order,
    OrderError,
    OrderRequest,
    OrderStatus,
    OrderType,
    Side,
)
from .writer import AccountWriter, MarketGate

__all__ = [
    "DEFAULT_INSTRUMENTS",
    "EXTERNAL",
    "FEES",
    "MARKET",
    "AccountWriter",
    "Accounts",
    "CancelAttribution",
    "EngineSettings",
    "FeeSchedule",
    "Fill",
    "FillCause",
    "GateDecision",
    "GateState",
    "InsufficientFunds",
    "Instrument",
    "InvalidTransition",
    "Liquidity",
    "MarketGate",
    "MarketStatus",
    "Order",
    "OrderError",
    "OrderRequest",
    "OrderStatus",
    "OrderType",
    "PairEngine",
    "Side",
    "StaleDataGate",
    "instruments_from_env",
]
