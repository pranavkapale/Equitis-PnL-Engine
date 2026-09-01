from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Optional


class CorporateActionType(str, Enum):
    DIVIDEND = "DIVIDEND"
    SPLIT = "SPLIT"
    MERGER = "MERGER"
    EXPIRY = "EXPIRY"


@dataclass
class CorporateAction:
    """
    Simplified view of an equities corporate action.
    """

    action_id: str
    symbol: str
    action_type: CorporateActionType
    effective_date: date
    cash_amount: Optional[Decimal] = None
    split_ratio: Optional[Decimal] = None

