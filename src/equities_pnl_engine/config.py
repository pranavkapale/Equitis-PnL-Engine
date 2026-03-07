from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    trades_topic: str = os.getenv("KAFKA_TRADES_TOPIC", "equities.trades")
    pnl_topic: str = os.getenv("KAFKA_PNL_TOPIC", "equities.pnl")
    consumer_group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID", "equities-pnl-engine")


@dataclass(frozen=True)
class AppConfig:
    env: str = os.getenv("APP_ENV", "local")
    kafka: KafkaConfig = KafkaConfig()


def get_config() -> AppConfig:
    """
    Central place to fetch application configuration.
    In a real system this may also read from config services or vaults.
    """

    return AppConfig()

