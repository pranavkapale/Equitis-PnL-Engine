from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass
class PositionSnapshot:
    """
    Aggregated position for a symbol/account at a point in time.
    """

    account_id: str
    symbol: str
    quantity: Decimal
    avg_price: Decimal
    as_of: datetime


@dataclass
class PnLBreakdown:
    realized: Decimal
    unrealized: Decimal
    fees: Decimal
    dividends: Decimal


@dataclass
class PnLEvent:
    """
    Output of the engine – what is published to downstream systems.
    """

    account_id: str
    symbol: str
    as_of: datetime
    breakdown: PnLBreakdown

