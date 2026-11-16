"""The SQL envelope parser must classify records exactly like the Python one."""

from __future__ import annotations

import json

from conftest import envelope, kafka_frame, loan_row

from lakehouse_cdc.contracts import CONTRACTS
from lakehouse_cdc.envelope import ChangeEvent, parse_envelope
from lakehouse_cdc.poison import POISON_RECORDS
from lakehouse_cdc.sparkjobs.envelope_sql import parse_envelope as parse_envelope_sql

PKS = {name: c.primary_key for name, c in CONTRACTS.items()}


def _payload(**overrides) -> bytes:
    payload = {
        "op": "u",
        "before": None,
        "after": {"loan_id": 1, "status": "CURRENT"},
        "source": {"lsn": 10, "table": "loans", "ts_ms": 1},
    }
    payload.update(overrides)
    return json.dumps({"payload": payload}).encode()


CASES: list[bytes | None] = [
    envelope("loans", "c", 10, after=loan_row(1, "CURRENT")),
    envelope("loans", "u", 11, before=loan_row(1, "CURRENT"), after=loan_row(1, "DPD_30")),
    envelope("loans", "d", 12, before=loan_row(1, "DPD_30")),
    envelope("loans", "r", 13, after=loan_row(2, "CURRENT")),
    envelope("pipeline_marker", "c", 14, after={"marker_id": "abc"}),
    *POISON_RECORDS,
    b"",
    b"[]",
    b"42",
    b'{"payload": "text"}',
    b'{"payload": null}',
    b'{"payload": {}}',
    _payload(op="t"),
    _payload(op=None),
    _payload(source=None),
    _payload(source={"table": "loans"}),
    _payload(source={"lsn": "abc", "table": "loans"}),
    _payload(source={"lsn": "123", "table": "loans"}),
    _payload(source={"lsn": 10}),
    _payload(source={"lsn": 10, "table": ""}),
    _payload(after=None),
    _payload(op="d", before=None),
    _payload(op="d", before={"loan_id": 1}, after=None),
    _payload(after={"status": "CURRENT"}),
    _payload(after={"loan_id": None, "status": "CURRENT"}),
    _payload(after="not an object"),
    _payload(source={"lsn": 10, "table": "unknown_table"}, after={"x": 1}),
]


def test_sql_and_python_classify_identically(spark):
    rows = parse_envelope_sql(kafka_frame(spark, CASES)).orderBy("kafka_offset").collect()
    assert len(rows) == len(CASES)
    for value, row in zip(CASES, rows, strict=True):
        expected = parse_envelope(value, PKS)
        if isinstance(expected, ChangeEvent):
            assert row["dlq_reason"] is None, (value, row["dlq_reason"])
            assert row["op"] == expected.op
            assert row["lsn"] == expected.lsn
            assert row["source_table"] == expected.table
            assert json.loads(row["row_image"]) == expected.row_image
        else:
            assert row["dlq_reason"] == expected.reason, (value, row["dlq_reason"], expected.reason)


def test_every_reason_is_exercised():
    reasons = {
        r.reason for r in (parse_envelope(v, PKS) for v in CASES) if not isinstance(r, ChangeEvent)
    }
    assert reasons == {
        "null_value",
        "unparseable_envelope",
        "unknown_op",
        "missing_lsn",
        "missing_table",
        "missing_row_image",
        "missing_primary_key",
    }
