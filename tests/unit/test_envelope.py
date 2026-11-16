import json

import pytest

from lakehouse_cdc import envelope as E
from lakehouse_cdc.envelope import ChangeEvent, DeadLetter, parse_envelope
from lakehouse_cdc.poison import POISON_RECORDS

PKS = {"payments": "payment_id"}


def make(op="u", before=None, after=None, source=None, **extra) -> bytes:
    payload = {
        "op": op,
        "before": before,
        "after": {"payment_id": 7, "amount": "10.00"} if after is None and op != "d" else after,
        "source": {"lsn": 1234, "table": "payments", "ts_ms": 1700000000000}
        if source is None
        else source,
        **extra,
    }
    return json.dumps({"schema": {"type": "struct"}, "payload": payload}).encode()


def test_parses_an_update():
    event = parse_envelope(make(before={"payment_id": 7}), PKS)
    assert event == ChangeEvent(
        table="payments",
        op="u",
        lsn=1234,
        source_ts_ms=1700000000000,
        before={"payment_id": 7},
        after={"payment_id": 7, "amount": "10.00"},
    )
    assert event.row_image == {"payment_id": 7, "amount": "10.00"}


def test_delete_uses_the_before_image():
    event = parse_envelope(make(op="d", before={"payment_id": 7}, after=None), PKS)
    assert isinstance(event, ChangeEvent)
    assert event.row_image == {"payment_id": 7}


def test_accepts_str_and_bytes():
    raw = make()
    assert parse_envelope(raw) == parse_envelope(raw.decode())


def test_lsn_may_be_a_string_of_digits():
    event = parse_envelope(make(source={"lsn": "98765", "table": "payments"}))
    assert isinstance(event, ChangeEvent) and event.lsn == 98765


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (None, E.REASON_NULL_VALUE),
        (b"", E.REASON_UNPARSEABLE),
        (b"not json", E.REASON_UNPARSEABLE),
        (b"\xff\xfe", E.REASON_UNPARSEABLE),
        (b"[1, 2]", E.REASON_UNPARSEABLE),
        (b'{"no_payload": 1}', E.REASON_UNPARSEABLE),
        (b'{"payload": "text"}', E.REASON_UNPARSEABLE),
        (make(op="t"), E.REASON_UNKNOWN_OP),
        (make(op=None), E.REASON_UNKNOWN_OP),
        (make(source={"table": "payments"}), E.REASON_MISSING_LSN),
        (make(source={"lsn": "12x", "table": "payments"}), E.REASON_MISSING_LSN),
        (make(source={"lsn": True, "table": "payments"}), E.REASON_MISSING_LSN),
        (make(source={"lsn": -5, "table": "payments"}), E.REASON_MISSING_LSN),
        (make(source={"lsn": 1}), E.REASON_MISSING_TABLE),
        (make(source={"lsn": 1, "table": ""}), E.REASON_MISSING_TABLE),
        (make(op="d", before=None, after={"payment_id": 7}), E.REASON_MISSING_ROW_IMAGE),
        (make(op="c", after="scalar"), E.REASON_MISSING_ROW_IMAGE),
        (make(after={"amount": "1.00"}), E.REASON_MISSING_PRIMARY_KEY),
        (make(after={"payment_id": None}), E.REASON_MISSING_PRIMARY_KEY),
    ],
)
def test_dead_letter_reasons(value, reason):
    assert parse_envelope(value, PKS) == DeadLetter(reason)


def test_primary_key_is_only_required_for_contracted_tables():
    value = make(source={"lsn": 1, "table": "pipeline_marker"}, after={"marker_id": "x"})
    assert isinstance(parse_envelope(value, PKS), ChangeEvent)
    assert isinstance(parse_envelope(make(after={"amount": "1.00"})), ChangeEvent)


def test_every_poison_record_is_dead_lettered():
    results = [parse_envelope(v, PKS) for v in POISON_RECORDS]
    assert all(isinstance(r, DeadLetter) for r in results)
    assert len({r.reason for r in results}) >= 5
