"""Records that are not valid change events, one per dead-letter reason (pure Python)."""

from __future__ import annotations

POISON_RECORDS: tuple[bytes | None, ...] = (
    b"this is not json",
    b'{"schema": null, "payload": {"op": "u", "after": {"payment_id": 1}',  # truncated
    b'{"payload": {"op": "x", "source": {"lsn": 1, "table": "payments"}}}',  # unknown op
    b'{"payload": {"op": "u", "source": {"table": "payments"}, "after": {"payment_id": 1}}}',  # no lsn
    b"\xff\xfe\x00\x01 binary garbage",
    b'{"payload": {"op": "c", "source": {"lsn": 5, "table": "payments"}, "after": {"amount": "1.00"}}}',  # no pk
    None,  # null value (a Kafka tombstone; the connector is configured not to emit them)
)
