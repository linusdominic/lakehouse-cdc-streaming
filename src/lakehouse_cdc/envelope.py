"""Debezium envelope parsing and dead-letter classification (pure Python).

This is the specification. The Spark job implements the same rules in SQL
(sparkjobs/envelope_sql.py) and tests/spark compares the two on the same
inputs, so a change here without a matching change there fails a test.

Expected wire format: Debezium JSON converter with schemas.enable=true, i.e.
{"schema": {...}, "payload": {"before", "after", "source", "op", "ts_ms"}}.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

VALID_OPS = frozenset({"c", "u", "d", "r"})

# Dead-letter reasons, in the order they are checked.
REASON_NULL_VALUE = "null_value"
REASON_UNPARSEABLE = "unparseable_envelope"
REASON_UNKNOWN_OP = "unknown_op"
REASON_MISSING_LSN = "missing_lsn"
REASON_MISSING_TABLE = "missing_table"
REASON_MISSING_ROW_IMAGE = "missing_row_image"
REASON_MISSING_PRIMARY_KEY = "missing_primary_key"

_DIGITS = re.compile(r"[0-9]+")


@dataclass(frozen=True)
class ChangeEvent:
    table: str
    op: str
    lsn: int
    source_ts_ms: int | None
    before: dict[str, Any] | None
    after: dict[str, Any] | None

    @property
    def row_image(self) -> dict[str, Any]:
        """The image that identifies the row: after for c/u/r, before for d."""
        image = self.before if self.op == "d" else self.after
        assert image is not None  # guaranteed by parse_envelope
        return image


@dataclass(frozen=True)
class DeadLetter:
    reason: str


def _as_int(value: Any) -> int | None:
    """Non-negative integer given as a JSON number or a string of digits."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and _DIGITS.fullmatch(value):
        return int(value)
    return None


def parse_envelope(
    value: bytes | str | None,
    primary_keys: Mapping[str, str] | None = None,
) -> ChangeEvent | DeadLetter:
    """Classify one Kafka record value.

    `primary_keys` maps source table -> primary key column. For those tables a
    row image without a primary key value is dead-lettered; other tables are
    not checked (they only land in bronze).
    """
    if value is None:
        return DeadLetter(REASON_NULL_VALUE)
    try:
        doc = json.loads(value)
    except (ValueError, UnicodeDecodeError):
        return DeadLetter(REASON_UNPARSEABLE)
    payload = doc.get("payload") if isinstance(doc, dict) else None
    if not isinstance(payload, dict):
        return DeadLetter(REASON_UNPARSEABLE)

    op = payload.get("op")
    if op not in VALID_OPS:
        return DeadLetter(REASON_UNKNOWN_OP)

    source = payload.get("source")
    source = source if isinstance(source, dict) else {}
    lsn = _as_int(source.get("lsn"))
    if lsn is None:
        return DeadLetter(REASON_MISSING_LSN)
    table = source.get("table")
    if not isinstance(table, str) or not table:
        return DeadLetter(REASON_MISSING_TABLE)

    before = payload.get("before")
    after = payload.get("after")
    before = before if isinstance(before, dict) else None
    after = after if isinstance(after, dict) else None
    if (op == "d" and before is None) or (op != "d" and after is None):
        return DeadLetter(REASON_MISSING_ROW_IMAGE)

    pk = (primary_keys or {}).get(table)
    if pk is not None and (before if op == "d" else after).get(pk) is None:  # type: ignore[union-attr]
        return DeadLetter(REASON_MISSING_PRIMARY_KEY)

    return ChangeEvent(
        table=table,
        op=op,
        lsn=lsn,
        source_ts_ms=_as_int(source.get("ts_ms")),
        before=before,
        after=after,
    )
