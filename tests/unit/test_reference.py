"""Properties of the merge semantics, checked on the reference model.

tests/spark holds the Spark implementation to this model, so these properties
carry over to the real job.
"""

import itertools
import random

import pytest

from lakehouse_cdc.contracts import DELETED_STATUS
from lakehouse_cdc.envelope import ChangeEvent
from lakehouse_cdc.reference import (
    apply_event,
    build_status_history,
    live_rows,
    reduce_current_state,
)


def ev(op, lsn, loan_id, status=None, before_status=None, balance="100.00"):
    after = (
        None if op == "d" else {"loan_id": loan_id, "status": status, "principal_balance": balance}
    )
    before = (
        {"loan_id": loan_id, "status": before_status, "principal_balance": balance}
        if before_status is not None or op == "d"
        else None
    )
    return ChangeEvent("loans", op, lsn, 1_700_000_000_000 + lsn, before, after)


def history() -> list[ChangeEvent]:
    return [
        ev("c", 10, 1, "CURRENT"),
        ev("u", 20, 1, "CURRENT", "CURRENT", balance="90.00"),
        ev("u", 30, 1, "DPD_30", "CURRENT", balance="90.00"),
        ev("u", 40, 1, "CURRENT", "DPD_30", balance="90.00"),
        ev("c", 50, 2, "CURRENT"),
        ev("u", 60, 2, "DPD_30", "CURRENT"),
        ev("d", 70, 2, before_status="DPD_30"),
        ev("c", 80, 3, "CURRENT"),
        ev("d", 90, 3, before_status="CURRENT"),
        ev("c", 100, 3, "DPD_60"),  # same key re-created after a delete
    ]


def test_in_order_result():
    state = reduce_current_state(history(), "loan_id")
    assert {k: (v.lsn, v.is_deleted) for k, v in state.items()} == {
        1: (40, False),
        2: (70, True),
        3: (100, False),
    }
    assert live_rows(state)[1]["status"] == "CURRENT"
    assert live_rows(state)[3]["status"] == "DPD_60"
    assert 2 not in live_rows(state)


def test_every_permutation_of_a_key_history_converges():
    events = [e for e in history() if e.row_image["loan_id"] == 3]
    expected = reduce_current_state(events, "loan_id")
    for permutation in itertools.permutations(events):
        assert reduce_current_state(permutation, "loan_id") == expected


@pytest.mark.parametrize("seed", range(50))
def test_shuffled_with_duplicates_converges(seed):
    rng = random.Random(seed)
    events = history()
    delivered = events + rng.choices(events, k=rng.randint(0, 2 * len(events)))
    rng.shuffle(delivered)
    assert reduce_current_state(delivered, "loan_id") == reduce_current_state(events, "loan_id")
    assert build_status_history(delivered) == build_status_history(events)


def test_stale_update_after_delete_does_not_resurrect():
    state = {}
    apply_event(state, ev("c", 50, 2, "CURRENT"), "loan_id")
    apply_event(state, ev("d", 70, 2, before_status="DPD_30"), "loan_id")
    apply_event(state, ev("u", 60, 2, "DPD_30", "CURRENT"), "loan_id")
    assert state[2].is_deleted and state[2].lsn == 70


def test_equal_lsn_is_a_redelivery_and_is_ignored():
    state = {}
    first = ev("u", 60, 2, "DPD_30", "CURRENT")
    apply_event(state, first, "loan_id")
    snapshot = dict(state)
    apply_event(state, first, "loan_id")
    assert state == snapshot


def test_applying_twice_is_idempotent():
    events = history()
    assert reduce_current_state(events + events, "loan_id") == reduce_current_state(
        events, "loan_id"
    )


def test_status_history_versions():
    versions = build_status_history(history())
    by_loan = {}
    for v in versions:
        by_loan.setdefault(v.loan_id, []).append(v)

    # The balance-only update at LSN 20 is not a version.
    assert [(v.status, v.valid_from_lsn, v.valid_to_lsn) for v in by_loan[1]] == [
        ("CURRENT", 10, 30),
        ("DPD_30", 30, 40),
        ("CURRENT", 40, None),
    ]
    assert [v.status for v in by_loan[2]] == ["CURRENT", "DPD_30", DELETED_STATUS]
    assert [v.status for v in by_loan[3]] == ["CURRENT", DELETED_STATUS, "DPD_60"]
    for loan_versions in by_loan.values():
        assert [v.is_current for v in loan_versions] == [False] * (len(loan_versions) - 1) + [True]
        for a, b in itertools.pairwise(loan_versions):
            assert a.valid_to_lsn == b.valid_from_lsn
            assert a.valid_to_ms == b.valid_from_ms


def test_update_without_before_image_is_treated_as_a_change():
    """Without REPLICA IDENTITY FULL there is no before image to compare against."""
    events = [ev("c", 10, 1, "CURRENT"), ev("u", 20, 1, "CURRENT")]
    assert [v.valid_from_lsn for v in build_status_history(events)] == [10, 20]
