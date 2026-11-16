"""Kafka-level fault injection: replays and poison records.

A replay re-publishes records that were already delivered, to the same topic
and partition, in shuffled order. The copies get new Kafka offsets but carry
the original Debezium payload (same LSN), which is what a Kafka Connect
restart without a flushed offset, or an operator rewinding a connector,
produces. Shuffling also makes the copies arrive out of LSN order per key.
"""

from __future__ import annotations

import json
import random
import time
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from lakehouse_cdc.config import ARTIFACTS_DIR
from lakehouse_cdc.poison import POISON_RECORDS

LEDGER = ARTIFACTS_DIR / "injections.jsonl"
POISON_KEY = b"poison"


def _record(kind: str, topic: str, count: int, ledger: Path = LEDGER) -> None:
    ledger.parent.mkdir(parents=True, exist_ok=True)
    with ledger.open("a") as f:
        f.write(
            json.dumps({"ts": time.time(), "kind": kind, "topic": topic, "count": count}) + "\n"
        )


def read_ledger(ledger: Path = LEDGER) -> dict[str, int]:
    totals: dict[str, int] = {}
    if ledger.exists():
        for line in ledger.read_text().splitlines():
            entry = json.loads(line)
            totals[entry["kind"]] = totals.get(entry["kind"], 0) + entry["count"]
    return totals


class Injector:
    def __init__(self, bootstrap: str, seed: int = 0) -> None:
        self.bootstrap = bootstrap
        self.rng = random.Random(f"inject-{seed}")
        self.producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20})
        self.admin = AdminClient({"bootstrap.servers": bootstrap})
        self.poison_sent = 0

    def _consumer(self) -> Consumer:
        return Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": "cdc-injector",
                "enable.auto.commit": False,
                "auto.offset.reset": "error",
            }
        )

    def partitions(self, topic: str) -> list[int]:
        meta = self.admin.list_topics(topic=topic, timeout=10)
        t = meta.topics.get(topic)
        if t is None or t.error is not None:
            return []
        return sorted(t.partitions)

    def replay_range(self, topic: str, partition: int, start: int, end: int) -> int:
        """Re-publish offsets [start, end) of one partition, shuffled. Returns the count."""
        consumer = self._consumer()
        try:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            start, end = max(start, low), min(end, high)
            if end <= start:
                return 0
            consumer.assign([TopicPartition(topic, partition, start)])
            records: list[tuple[bytes | None, bytes | None]] = []
            deadline = time.monotonic() + 30
            done = False
            while not done and time.monotonic() < deadline:
                for msg in consumer.consume(num_messages=500, timeout=1.0):
                    if msg.error():
                        continue
                    if msg.offset() >= end:
                        done = True
                        break
                    records.append((msg.key(), msg.value()))
                    if msg.offset() == end - 1:
                        done = True
                        break
        finally:
            consumer.close()
        self.rng.shuffle(records)
        for key, value in records:
            self.producer.produce(topic, value=value, key=key, partition=partition)
        self.producer.flush(30)
        if records:
            _record("replay", topic, len(records))
            # A replayed range can contain poison records published earlier.
            poison = sum(1 for key, _ in records if key == POISON_KEY)
            if poison:
                _record("poison", topic, poison)
        return len(records)

    def replay_recent(self, topic: str, per_partition: int) -> int:
        """Replay the newest `per_partition` records of every partition of a topic."""
        total = 0
        consumer = self._consumer()
        try:
            ranges = []
            for p in self.partitions(topic):
                _low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
                ranges.append((p, high - per_partition, high))
        finally:
            consumer.close()
        for p, start, end in ranges:
            total += self.replay_range(topic, p, start, end)
        return total

    def poison(self, topic: str, count: int = 1) -> int:
        """Publish records that are not valid change events."""
        partitions = self.partitions(topic)
        if not partitions:
            return 0
        for _ in range(count):
            value = POISON_RECORDS[self.poison_sent % len(POISON_RECORDS)]
            self.producer.produce(
                topic, value=value, key=POISON_KEY, partition=self.rng.choice(partitions)
            )
            self.poison_sent += 1
        self.producer.flush(30)
        _record("poison", topic, count)
        return count
