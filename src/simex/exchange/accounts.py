"""Balances, reservations and settlement: the per-account writer's state (ADR 0014).

Books and matching have one writer per pair, but cash is shared: a USD balance pays for BTC, ETH
and SOL orders alike. So balances are owned by a per-account writer, the one exception to "no
shared mutable state across pairs" (CLAUDE.md). Pair engines never touch balances; they report
fills, and the account writer settles them.

Double entry: every movement has a counterparty account, so each asset's total over all accounts
never changes. MARKET is the outside world the simulated fills trade with, FEES collects fees, and
EXTERNAL is where deposits come from. Only MARKET and EXTERNAL may go negative.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import ROUND_UP, Decimal

from .fees import FeeSchedule
from .instruments import Instrument
from .orders import Fill, Side

MARKET = "_market"
FEES = "_fees"
EXTERNAL = "_external"
SYSTEM_ACCOUNTS = frozenset({MARKET, FEES, EXTERNAL})
ZERO = Decimal(0)


class InsufficientFunds(ValueError):
    pass


@dataclass
class Balance:
    available: Decimal = ZERO
    reserved: Decimal = ZERO

    @property
    def total(self) -> Decimal:
        return self.available + self.reserved


@dataclass
class Reservation:
    account_id: str
    asset: str
    remaining: Decimal


class Accounts:
    def __init__(self) -> None:
        self.balances: dict[str, dict[str, Balance]] = defaultdict(lambda: defaultdict(Balance))
        self.reservations: dict[str, Reservation] = {}  # order_id -> what it still holds

    def balance(self, account_id: str, asset: str) -> Balance:
        return self.balances[account_id][asset]

    def _move(self, asset: str, src: str, dst: str, amount: Decimal) -> None:
        """Move available funds; only system accounts may go negative."""
        if amount < 0:
            raise ValueError("negative movement")
        source = self.balances[src][asset]
        if src not in SYSTEM_ACCOUNTS and source.available < amount:
            raise InsufficientFunds(f"{src} has {source.available} {asset}, needs {amount}")
        source.available -= amount
        self.balances[dst][asset].available += amount

    def deposit(self, account_id: str, asset: str, amount: Decimal) -> None:
        if account_id in SYSTEM_ACCOUNTS:
            raise ValueError("can't deposit to a system account")
        self._move(asset, EXTERNAL, account_id, amount)

    # Reservations ------------------------------------------------------------------------------

    def reserve(self, order_id: str, account_id: str, asset: str, amount: Decimal) -> None:
        if order_id in self.reservations:
            raise ValueError(f"order {order_id} already holds a reservation")
        balance = self.balances[account_id][asset]
        if balance.available < amount:
            raise InsufficientFunds(
                f"{account_id} has {balance.available} {asset} available, the order needs {amount}"
            )
        balance.available -= amount
        balance.reserved += amount
        self.reservations[order_id] = Reservation(account_id, asset, amount)

    def release(self, order_id: str) -> Decimal:
        """Return what a finished order still holds. Returns the amount released."""
        reservation = self.reservations.pop(order_id, None)
        if reservation is None:
            return ZERO
        balance = self.balances[reservation.account_id][reservation.asset]
        balance.reserved -= reservation.remaining
        balance.available += reservation.remaining
        return reservation.remaining

    def _spend_reserved(self, order_id: str, asset: str, amount: Decimal) -> None:
        reservation = self.reservations[order_id]
        if reservation.asset != asset or amount > reservation.remaining:
            raise InsufficientFunds(
                f"order {order_id} reserved {reservation.remaining} {reservation.asset}, the "
                f"fill needs {amount} {asset}"
            )
        reservation.remaining -= amount
        self.balances[reservation.account_id][asset].reserved -= amount

    # Settlement --------------------------------------------------------------------------------

    def settle(self, fill: Fill, instrument: Instrument) -> None:
        """A fill against the market: the agent pays from its order's reservation."""
        base, quote = instrument.base, instrument.quote
        notional = fill.notional
        agent = fill.account_id
        if fill.side is Side.BUY:
            self._spend_reserved(fill.order_id, quote, notional + fill.fee)
            self.balances[MARKET][quote].available += notional
            self.balances[FEES][quote].available += fill.fee
            self.balances[MARKET][base].available -= fill.quantity
            self.balances[agent][base].available += fill.quantity
        else:
            if fill.fee > notional:
                raise ValueError(
                    f"fee {fill.fee} exceeds the proceeds {notional} of {fill.fill_id}"
                )
            self._spend_reserved(fill.order_id, base, fill.quantity)
            self.balances[MARKET][base].available += fill.quantity
            self.balances[MARKET][quote].available -= notional
            self.balances[agent][quote].available += notional - fill.fee
            self.balances[FEES][quote].available += fill.fee

    # Checks ------------------------------------------------------------------------------------

    def totals(self) -> dict[str, Decimal]:
        """Each asset summed over every account, system ones included: constant by construction."""
        out: dict[str, Decimal] = defaultdict(lambda: ZERO)
        for assets in self.balances.values():
            for asset, balance in assets.items():
                out[asset] += balance.total
        return dict(out)


def required_reservation(
    request_side: Side,
    quantity: Decimal,
    price: Decimal,
    instrument: Instrument,
    fees: FeeSchedule,
) -> tuple[str, Decimal]:
    """What an order must hold before it is accepted: the most it can cost.

    A buy at `price` (its limit, or for a market order the best ask widened by the band) holds
    the notional plus the higher of the two fees on it, rounded up: fills never pay more in total
    than the rounded-up fee on the whole (Order.record_fill), whatever mix of maker and taker
    they turn out to be. A sell holds its quantity."""
    if request_side is Side.SELL:
        return instrument.base, quantity
    notional = quantity * price
    fee = (notional * fees.max_rate).quantize(fees.fee_increment, rounding=ROUND_UP)
    return instrument.quote, notional + fee
