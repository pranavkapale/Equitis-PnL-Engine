from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterable

from confluent_kafka import Consumer, KafkaError, Producer
from fastavro import parse_schema, schemaless_reader, schemaless_writer

from equities_pnl_engine.config import get_config

BASE_DIR = Path(__file__).resolve().parents[1]


def load_schema(name: str) -> Dict[str, Any]:
    schema_path = BASE_DIR / "schemas" / name
    with schema_path.open() as f:
        return parse_schema(json.load(f))


TRADE_EVENT_SCHEMA = load_schema("trade_event.avsc")
PNL_EVENT_SCHEMA = load_schema("pnl_event.avsc")


class AvroKafkaProducer:
    """
    Thin wrapper over Kafka Producer that serializes payloads as Avro.
    In interview you can talk about:
    - partitioning strategy
    - error handling / retries
    - schema evolution and registry use
    """

    def __init__(self) -> None:
        cfg = get_config().kafka
        self._producer = Producer({"bootstrap.servers": cfg.bootstrap_servers})

    def _serialize(self, payload: Dict[str, Any], schema: Dict[str, Any]) -> bytes:
        import io

        buf = io.BytesIO()
        schemaless_writer(buf, schema, payload)
        return buf.getvalue()

    def publish_trade_event(self, payload: Dict[str, Any]) -> None:
        cfg = get_config().kafka
        value = self._serialize(payload, TRADE_EVENT_SCHEMA)
        self._producer.produce(cfg.trades_topic, value=value)

    def publish_pnl_event(self, payload: Dict[str, Any]) -> None:
        cfg = get_config().kafka
        value = self._serialize(payload, PNL_EVENT_SCHEMA)
        self._producer.produce(cfg.pnl_topic, value=value)

    def flush(self, timeout: float | None = None) -> None:
        self._producer.flush(timeout)


class AvroKafkaConsumer:
    """
    Thin wrapper over Kafka Consumer with Avro deserialization.
    """

    def __init__(self, topic: str) -> None:
        cfg = get_config().kafka
        self._consumer = Consumer(
            {
                "bootstrap.servers": cfg.bootstrap_servers,
                "group.id": cfg.consumer_group_id,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe([topic])

    def _deserialize(self, value: bytes, schema: Dict[str, Any]) -> Dict[str, Any]:
        import io

        buf = io.BytesIO(value)
        return schemaless_reader(buf, schema)

    def consume_trade_events(self) -> Iterable[Dict[str, Any]]:
        while True:
            msg = self._consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    raise RuntimeError(f"Kafka error: {msg.error()}")
                continue
            yield self._deserialize(msg.value(), TRADE_EVENT_SCHEMA)

    def close(self) -> None:
        self._consumer.close()

