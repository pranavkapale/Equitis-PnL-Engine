from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from equities_pnl_engine.models import (
    CorporateAction,
    CorporateActionType,
    Trade,
    TradeEventType,
    TradeLifecycleEvent,
)


def apply_corporate_action_to_trade(
    trade: Trade, corporate_action: CorporateAction
) -> Trade:
    """
    Apply basic corporate actions to a trade position.

    - SPLIT: adjust quantity and price inversely.
    - DIVIDEND: does not change the trade, handled in PnL.
    - MERGER: simplified as symbol change.
    """

    if corporate_action.action_type == CorporateActionType.SPLIT:
        if not corporate_action.split_ratio or corporate_action.split_ratio == 0:
            return trade

        new_qty = trade.quantity * corporate_action.split_ratio
        new_price = trade.price / corporate_action.split_ratio
        return replace(trade, quantity=new_qty, price=new_price)

    if corporate_action.action_type == CorporateActionType.MERGER:
        return replace(trade, symbol=corporate_action.symbol)

    return trade


def apply_expiry(event: TradeLifecycleEvent) -> TradeLifecycleEvent:
    """
    Mark a trade as expired. In practice this would reduce the open quantity
    and potentially realize PnL; here we simply tag the event.
    """

    if event.event_type != TradeEventType.EXPIRY:
        return event

    return event


def normalize_trade_event(
    raw_event: dict, corporate_action: CorporateAction | None = None
) -> TradeLifecycleEvent:
    """
    Convert a raw dictionary (e.g., from Kafka/Avro) into a normalized TradeLifecycleEvent.
    Optionally apply a corporate action transformation.
    """

    trade = Trade(
        trade_id=raw_event["trade_id"],
        account_id=raw_event["account_id"],
        symbol=raw_event["symbol"],
        quantity=Decimal(raw_event["quantity"]),
        price=Decimal(raw_event["price"]),
        currency=raw_event["currency"],
        trade_timestamp=raw_event["trade_timestamp"],
    )

    if corporate_action:
        trade = apply_corporate_action_to_trade(trade, corporate_action)

    lifecycle_event = TradeLifecycleEvent(
        event_type=TradeEventType(raw_event.get("event_type", "NEW")),
        trade=trade,
        event_timestamp=raw_event["event_timestamp"],
        corporate_action_id=corporate_action.action_id if corporate_action else None,
    )

    return lifecycle_event

