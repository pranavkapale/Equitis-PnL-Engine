from datetime import datetime
from decimal import Decimal

from equities_pnl_engine.models import Trade, TradeEventType, TradeLifecycleEvent
from equities_pnl_engine.pipeline.pnl_calculator import calculate_pnl_for_events


def test_realized_pnl_on_closing_trade():
    now = datetime.timezone.utc()
    opening_trade = Trade(
        trade_id="T1",
        account_id="ACC1",
        symbol="AAPL",
        quantity=Decimal("100"),
        price=Decimal("100"),
        currency="USD",
        trade_timestamp=now,
    )
    closing_trade = Trade(
        trade_id="T2",
        account_id="ACC1",
        symbol="AAPL",
        quantity=Decimal("-100"),
        price=Decimal("110"),
        currency="USD",
        trade_timestamp=now,
    )

    events = [
        TradeLifecycleEvent(
            event_type=TradeEventType.NEW,
            trade=opening_trade,
            event_timestamp=now,
        ),
        TradeLifecycleEvent(
            event_type=TradeEventType.NEW,
            trade=closing_trade,
            event_timestamp=now,
        ),
    ]

    pnl_events = calculate_pnl_for_events(events)
    assert len(pnl_events) == 2
    assert pnl_events[-1].breakdown.realized == Decimal("1000")

