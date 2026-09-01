from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from decimal import Decimal
from typing import Optional


class TradeEventType(str, Enum):
    NEW = "NEW"
    AMEND = "AMEND"
    CANCEL = "CANCEL"
    EXPIRY = "EXPIRY"
    CORPORATE_ACTION = "CORPORATE_ACTION"


@dataclass
class Trade:
    trade_id: str
    account_id: str
    symbol: str
    quantity: Decimal
    price: Decimal
    currency: str
    trade_timestamp: datetime


@dataclass
class TradeLifecycleEvent:
    """
    Represents a normalized trade lifecycle event as it moves through the platform.
    This is what we expect to consume from Kafka and feed into PnL.
    """

    event_type: TradeEventType
    trade: Trade
    event_timestamp: datetime
    corporate_action_id: Optional[str] = None

