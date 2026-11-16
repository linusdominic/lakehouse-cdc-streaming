"""Thin connection helpers for the host-side tools."""

from __future__ import annotations

import re
from typing import Any

import psycopg
import trino
from confluent_kafka import Consumer, TopicPartition

from lakehouse_cdc.config import Settings
from lakehouse_cdc.contracts import CATALOG, OPS_BATCHES, TOPIC_PATTERN


def pg_connect(settings: Settings, autocommit: bool = True) -> psycopg.Connection:
    return psycopg.connect(settings.pg_dsn, autocommit=autocommit)


class Lake:
    """Trino session against the Iceberg catalog."""

    def __init__(self, settings: Settings) -> None:
        self._conn = trino.dbapi.connect(
            host=settings.trino_host, port=settings.trino_port, user="cdc-tools", catalog=CATALOG
        )

    def query(self, sql: str) -> list[tuple[Any, ...]]:
        cur = self._conn.cursor()
        cur.execute(sql)
        return [tuple(r) for r in cur.fetchall()]

    def scalar(self, sql: str) -> Any:
        return self.query(sql)[0][0]

    def processed_offsets(self) -> dict[str, int]:
        """Next offset to read per 'topic/partition', as committed by the streaming job."""
        rows = self.query(f"""
            SELECT tp, max(next_offset)
            FROM {OPS_BATCHES} CROSS JOIN UNNEST(end_offsets) AS t (tp, next_offset)
            GROUP BY 1
        """)
        return dict(rows)


def kafka_offsets(bootstrap: str) -> dict[str, tuple[int, int]]:
    """(low, high) watermark per 'topic/partition' for every CDC topic."""
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "cdc-tools-offsets"})
    try:
        out: dict[str, tuple[int, int]] = {}
        pattern = re.compile(TOPIC_PATTERN)
        for name, topic in consumer.list_topics(timeout=10).topics.items():
            if not pattern.fullmatch(name):
                continue
            for p in topic.partitions:
                out[f"{name}/{p}"] = consumer.get_watermark_offsets(
                    TopicPartition(name, p), timeout=10
                )
        return out
    finally:
        consumer.close()
