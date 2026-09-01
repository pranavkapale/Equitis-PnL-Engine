from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from typing import Dict, Iterable, List, Tuple

from equities_pnl_engine.models import (
    PnLBreakdown,
    PnLEvent,
    PositionSnapshot,
    Trade,
    TradeLifecycleEvent,
    TradeEventType,
)


def update_position(
    position: PositionSnapshot | None, trade: Trade
) -> PositionSnapshot:
    """
    Simple average-price position updating logic.
    """

    if position is None:
        return PositionSnapshot(
            account_id=trade.account_id,
            symbol=trade.symbol,
            quantity=trade.quantity,
            avg_price=trade.price,
            as_of=trade.trade_timestamp,
        )

    new_qty = position.quantity + trade.quantity
    if new_qty == 0:
        return replace(position, quantity=Decimal("0"), avg_price=Decimal("0"))

    total_cost = (position.quantity * position.avg_price) + (
        trade.quantity * trade.price
    )
    new_avg = total_cost / new_qty

    return PositionSnapshot(
        account_id=position.account_id,
        symbol=position.symbol,
        quantity=new_qty,
        avg_price=new_avg,
        as_of=max(position.as_of, trade.trade_timestamp),
    )


def calculate_pnl_for_events(
    events: Iterable[TradeLifecycleEvent],
    prices: Dict[Tuple[str, str], Decimal] | None = None,
) -> List[PnLEvent]:
    """
    Core PnL engine:
    - Maintains running positions per (account, symbol).
    - Calculates realized PnL on closing trades.
    - Calculates simple unrealized PnL from provided prices (if any).
    """

    positions: Dict[Tuple[str, str], PositionSnapshot] = {}
    pnl_events: List[PnLEvent] = []
    prices = prices or {}

    for event in events:
        trade = event.trade
        key = (trade.account_id, trade.symbol)
        position = positions.get(key)

        realized = Decimal("0")
        unrealized = Decimal("0")
        fees = Decimal("0")
        dividends = Decimal("0")

        if event.event_type in {TradeEventType.NEW, TradeEventType.AMEND}:
            if position is not None and position.quantity != 0 and (
                (position.quantity > 0 and trade.quantity < 0)
                or (position.quantity < 0 and trade.quantity > 0)
            ):
                realized = (trade.price - position.avg_price) * (-trade.quantity)

            positions[key] = update_position(position, trade)

        elif event.event_type == TradeEventType.CANCEL:
            positions[key] = position

        if key in prices:
            mark_price = prices[key]
            position = positions.get(key)
            if position:
                unrealized = (mark_price - position.avg_price) * position.quantity

        snapshot_time = event.event_timestamp or datetime.utcnow()
        breakdown = PnLBreakdown(
            realized=realized,
            unrealized=unrealized,
            fees=fees,
            dividends=dividends,
        )

        pnl_events.append(
            PnLEvent(
                account_id=trade.account_id,
                symbol=trade.symbol,
                as_of=snapshot_time,
                breakdown=breakdown,
            )
        )

    return pnl_events

