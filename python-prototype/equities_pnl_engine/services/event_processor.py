from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Iterable

from equities_pnl_engine.messaging import AvroKafkaConsumer, AvroKafkaProducer
from equities_pnl_engine.models import TradeLifecycleEvent
from equities_pnl_engine.pipeline.pnl_calculator import calculate_pnl_for_events
from equities_pnl_engine.pipeline.transformations import normalize_trade_event
from equities_pnl_engine.config import get_config


def run_streaming_trade_processor() -> None:
    """
    End-to-end streaming service:
    - Consume trade lifecycle events from Kafka.
    - Normalize and enrich them.
    - Calculate PnL incrementally.
    - Publish resulting PnL events to a downstream Kafka topic.
    """

    cfg = get_config().kafka
    consumer = AvroKafkaConsumer(topic=cfg.trades_topic)
    producer = AvroKafkaProducer()

    try:
        # In a real system, this would be a long-running loop with checkpoints.
        for raw_event in consumer.consume_trade_events():
            lifecycle_event = normalize_trade_event(raw_event)
            pnl_events = calculate_pnl_for_events([lifecycle_event])
            for pnl in pnl_events:
                payload = {
                    "account_id": pnl.account_id,
                    "symbol": pnl.symbol,
                    "as_of": pnl.as_of.isoformat(),
                    "realized": str(pnl.breakdown.realized),
                    "unrealized": str(pnl.breakdown.unrealized),
                    "fees": str(pnl.breakdown.fees),
                    "dividends": str(pnl.breakdown.dividends),
                }
                producer.publish_pnl_event(payload)
    finally:
        producer.flush()
        consumer.close()


def run_batch_pnl_job(events: Iterable[dict] | None = None) -> None:
    """
    Example of a scheduled/batch job:
    - Ingest events from a static source (e.g., a file or table).
    - Run PnL.
    - Persist or publish the result.

    This is intentionally I/O-agnostic so you can plug in any scheduler/orchestrator.
    """

    if events is None:
        now = datetime.utcnow()
        events = [
            {
                "event_type": "NEW",
                "trade_id": "T1",
                "account_id": "ACC1",
                "symbol": "AAPL",
                "quantity": "100",
                "price": "190.0",
                "currency": "USD",
                "trade_timestamp": now,
                "event_timestamp": now,
            },
            {
                "event_type": "NEW",
                "trade_id": "T2",
                "account_id": "ACC1",
                "symbol": "AAPL",
                "quantity": "-50",
                "price": "200.0",
                "currency": "USD",
                "trade_timestamp": now,
                "event_timestamp": now,
            },
        ]

    lifecycle_events = [normalize_trade_event(e) for e in events]
    prices = {("ACC1", "AAPL"): Decimal("195.0")}
    pnl_events = calculate_pnl_for_events(lifecycle_events, prices=prices)

    for pnl in pnl_events:
        print(
            f"[PnL] {pnl.account_id} {pnl.symbol} "
            f"R={pnl.breakdown.realized} U={pnl.breakdown.unrealized}"
        )

