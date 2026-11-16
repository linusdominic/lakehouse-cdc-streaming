"""The Spark sinks against the pure-Python reference model.

The same events are fed to process_batch (the real foreachBatch function) in
many different orders, split across batches, with duplicates and with whole
batches re-executed. The resulting tables must equal what the reference model
computes, every time.
"""

from __future__ import annotations

import random

import pytest
from conftest import LOAN_FIELDS, envelope, kafka_frame, loan_row, run_batch

from lakehouse_cdc.contracts import BRONZE_DEAD_LETTER, BRONZE_EVENTS, LOANS, SILVER_SCD2
from lakehouse_cdc.envelope import parse_envelope
from lakehouse_cdc.reference import build_status_history, reduce_current_state
from lakehouse_cdc.sparkjobs import sinks
from lakehouse_cdc.sparkjobs.envelope_sql import parse_envelope as parse_envelope_sql

pytestmark = pytest.mark.usefixtures("fresh_tables")


def loan_events() -> list[bytes]:
    """A small history with every interesting shape in it."""
    e = []
    lsn = 100

    def add(op, before=None, after=None):
        nonlocal lsn
        lsn += 10
        e.append(envelope("loans", op, lsn, before=before, after=after))

    # loan 1: delinquency roll with cures, balance-only updates in between
    add("c", after=loan_row(1, "CURRENT"))
    add("u", before=loan_row(1, "CURRENT"), after=loan_row(1, "CURRENT", "900.00"))
    add("u", before=loan_row(1, "CURRENT", "900.00"), after=loan_row(1, "DPD_30", "900.00"))
    add("u", before=loan_row(1, "DPD_30", "900.00"), after=loan_row(1, "DPD_60", "900.00"))
    add("u", before=loan_row(1, "DPD_60", "900.00"), after=loan_row(1, "CURRENT", "900.00"))
    add("u", before=loan_row(1, "CURRENT", "900.00"), after=loan_row(1, "CURRENT", "850.00"))
    # loan 2: created, updated, hard-deleted
    add("c", after=loan_row(2, "CURRENT"))
    add("u", before=loan_row(2, "CURRENT"), after=loan_row(2, "DPD_30"))
    add("d", before=loan_row(2, "DPD_30"))
    # loan 3: snapshot read, then paid off
    add("r", after=loan_row(3, "CURRENT"))
    add("u", before=loan_row(3, "CURRENT"), after=loan_row(3, "PAID_OFF", "0.00"))
    # loan 4: deleted and re-created under the same key
    add("c", after=loan_row(4, "CURRENT"))
    add("d", before=loan_row(4, "CURRENT"))
    add("c", after=loan_row(4, "DPD_30", "10.00"))
    return e


def expected_state(values):
    events = [parse_envelope(v) for v in values]
    state = reduce_current_state(events, "loan_id")
    current = {
        k: (v.lsn, v.is_deleted, v.data["status"], v.data["principal_balance"])
        for k, v in state.items()
    }
    history = sorted(
        (v.loan_id, v.valid_from_lsn, v.status, v.valid_to_lsn, v.is_current)
        for v in build_status_history(events)
    )
    return current, history


def actual_state(spark):
    current = {
        r["loan_id"]: (r["_lsn"], r["_is_deleted"], r["status"], str(r["principal_balance"]))
        for r in spark.table(LOANS.silver_table).collect()
    }
    history = sorted(
        (r["loan_id"], r["valid_from_lsn"], r["status"], r["valid_to_lsn"], r["is_current"])
        for r in spark.table(SILVER_SCD2).collect()
    )
    return current, history


def split(values, rng, n_batches):
    cuts = sorted(rng.sample(range(1, len(values)), n_batches - 1))
    return [values[a:b] for a, b in zip([0, *cuts], [*cuts, len(values)], strict=True)]


def feed(spark, batches):
    offset = 0
    for batch_id, batch in enumerate(batches):
        run_batch(spark, batch, batch_id=batch_id, start_offset=offset)
        offset += len(batch)


def test_in_order_single_batch(spark):
    events = loan_events()
    run_batch(spark, events)
    assert actual_state(spark) == expected_state(events)


@pytest.mark.parametrize("seed", range(6))
def test_any_order_any_batching_with_duplicates(spark, seed):
    rng = random.Random(seed)
    events = loan_events()
    delivered = events + rng.choices(events, k=len(events) // 2)  # duplicates
    rng.shuffle(delivered)  # first deliveries out of order too
    feed(spark, split(delivered, rng, n_batches=rng.randint(2, 5)))
    assert actual_state(spark) == expected_state(events)


def test_stale_update_after_delete_does_not_resurrect(spark):
    events = loan_events()
    create_2, update_2, delete_2 = events[6], events[7], events[8]
    feed(spark, [[create_2], [delete_2], [update_2]])
    current, _ = actual_state(spark)
    assert current[2][1] is True, "loan 2 must stay deleted"
    assert current[2][0] == parse_envelope(delete_2).lsn


def test_reexecuted_batch_changes_nothing(spark):
    events = loan_events()
    run_batch(spark, events, batch_id=7)
    before = actual_state(spark)
    bronze_before = spark.table(BRONZE_EVENTS).count()
    run_batch(spark, events, batch_id=7)  # same offsets: what a restart replays
    assert actual_state(spark) == before
    assert spark.table(BRONZE_EVENTS).count() == bronze_before == len(events)


def test_dead_letters_are_kept_out_of_bronze_and_silver(spark):
    events = loan_events()[:3]
    run_batch(spark, [events[0], b"not json", events[1], None, events[2]])
    assert spark.table(BRONZE_EVENTS).count() == 3
    reasons = sorted(r["reason"] for r in spark.table(BRONZE_DEAD_LETTER).collect())
    assert reasons == ["null_value", "unparseable_envelope"]
    assert actual_state(spark) == expected_state(events)


def test_new_source_column_is_added_and_backfilled_with_null(spark):
    old = envelope("loans", "c", 10, after=loan_row(1, "CURRENT"))
    new_fields = [*LOAN_FIELDS, ("servicer_code", "string", None), ("escrow_flag", "boolean", None)]
    new_row = {**loan_row(2, "CURRENT"), "servicer_code": "SVC-9", "escrow_flag": True}
    new = envelope("loans", "c", 20, after=new_row, fields=new_fields)
    run_batch(spark, [old], batch_id=0)
    run_batch(spark, [new], batch_id=1, start_offset=1)
    # An event with the old schema arriving after the column exists must still merge.
    late_old = envelope(
        "loans", "u", 30, before=loan_row(1, "CURRENT"), after=loan_row(1, "DPD_30")
    )
    run_batch(spark, [late_old], batch_id=2, start_offset=2)

    table = spark.table(LOANS.silver_table)
    types = dict(table.dtypes)
    assert types["servicer_code"] == "string" and types["escrow_flag"] == "boolean"
    rows = {r["loan_id"]: r for r in table.collect()}
    assert rows[1]["servicer_code"] is None and rows[1]["status"] == "DPD_30"
    assert rows[2]["servicer_code"] == "SVC-9" and rows[2]["escrow_flag"] is True


def test_type_conversions(spark):
    run_batch(spark, [envelope("loans", "c", 10, after=loan_row(1, "CURRENT"))])
    row = (
        spark.table(LOANS.silver_table)
        .selectExpr(
            "cast(origination_date as string) d",
            "cast(interest_rate as string) r",
            "cast(created_at as string) c",
            "cast(updated_at as string) u",
            "next_due_date",
        )
        .first()
    )
    assert row["d"] == "2024-01-01"
    assert row["r"] == "0.05125"
    assert row["c"] == "2024-01-01 00:00:00.5"
    assert row["u"] == "2024-01-01 00:00:00.123456"
    assert row["next_due_date"] is None


def test_merge_current_state_keeps_highest_lsn_within_a_batch(spark):
    events = [
        envelope("loans", "u", 30, before=loan_row(1, "CURRENT"), after=loan_row(1, "DPD_60")),
        envelope("loans", "c", 10, after=loan_row(1, "CURRENT")),
        envelope("loans", "u", 20, before=loan_row(1, "CURRENT"), after=loan_row(1, "DPD_30")),
    ]
    parsed = parse_envelope_sql(kafka_frame(spark, events)).selectExpr(
        "*", "current_timestamp() as ingested_at"
    )
    sinks.merge_current_state(parsed, LOANS, [])
    row = spark.table(LOANS.silver_table).first()
    assert (row["_lsn"], row["status"]) == (30, "DPD_60")
