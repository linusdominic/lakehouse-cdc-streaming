import itertools
from collections import Counter
from decimal import Decimal

from lakehouse_cdc.generator.planner import (
    SCHEMA_CHANGE_SQL,
    TERMINAL_STATUSES,
    TRANSITIONS,
    PlanConfig,
    plan_seed,
    plan_workload,
)

BORROWERS = tuple(range(1, 51))


def plan(seed=42, n_ops=3000, **kwargs):
    cfg = PlanConfig(seed=seed, n_ops=n_ops, id_base=50, existing_borrowers=BORROWERS, **kwargs)
    return list(plan_workload(cfg))


def test_same_seed_same_plan():
    assert plan(seed=42) == plan(seed=42)
    assert plan_seed(100, seed=42) == plan_seed(100, seed=42)


def test_different_seed_different_plan():
    assert plan(seed=42) != plan(seed=43)


def test_a_longer_plan_extends_a_shorter_one():
    assert plan(n_ops=2000)[:1000] == plan(n_ops=1000)


def test_one_transaction_per_operation():
    assert len(plan(n_ops=1234)) == 1234


def test_schema_change_happens_once_at_the_configured_index():
    txns = plan(schema_change_at=1500)
    ddl = [i for i, t in enumerate(txns) if t.kind == "schema_change"]
    assert ddl == [1500]
    assert txns[1500].statements[0].sql == SCHEMA_CHANGE_SQL

    def inserts_with_channel(part):
        return [
            "channel" in s.sql
            for t in part
            for s in t.statements
            if s.sql.startswith("INSERT INTO payments")
        ]

    before, after = inserts_with_channel(txns[:1500]), inserts_with_channel(txns[1501:])
    assert before and not any(before)
    assert after and all(after)


def test_no_schema_change_by_default():
    assert all(t.kind != "schema_change" for t in plan())


def test_every_kind_of_operation_is_planned():
    kinds = Counter(t.kind for t in plan(schema_change_at=10))
    assert set(kinds) == {
        "new_borrower", "update_borrower", "burst_update_borrower", "originate_loan",
        "post_payment", "reverse_payment", "roll_status", "delete_payment", "purge_loan",
        "schema_change",
    }  # fmt: skip


def test_plan_contains_multi_statement_transactions_and_hard_deletes():
    txns = plan()
    assert any(len(t.statements) >= 3 for t in txns)
    deletes = [s.sql for t in txns for s in t.statements if s.sql.startswith("DELETE")]
    assert any("FROM loans" in s for s in deletes)
    assert any("FROM payments WHERE payment_id" in s for s in deletes)


def test_ids_are_unique_and_above_the_base():
    ids = [s.params[0] for t in plan() for s in t.statements if s.sql.startswith("INSERT INTO")]
    assert len(ids) == len(set(ids))
    assert min(ids) > 50


def _replay(txns):
    """Interpret the plan against an in-memory model and check its invariants."""
    loans: dict[int, dict] = {}
    payments: dict[int, int] = {}
    history: dict[int, list[tuple]] = {}
    for t in txns:
        for s in t.statements:
            p = s.params
            if s.sql.startswith("INSERT INTO loans"):
                loans[p[0]] = {"balance": p[5], "status": p[10]}
            elif s.sql.startswith("INSERT INTO payments"):
                assert p[1] in loans, "payment for a loan that does not exist"
                assert p[3] == p[4] + p[5] + p[6], "components must sum to the amount"
                payments[p[0]] = p[1]
            elif s.sql.startswith("INSERT INTO loan_status_history"):
                history.setdefault(p[1], []).append((p[2], p[3]))
            elif s.sql.startswith("UPDATE loans SET principal_balance"):
                assert p[0] >= 0
                loans[p[-1]]["balance"] = p[0]
            elif s.sql.startswith("UPDATE loans SET status"):
                loans[p[1]]["status"] = p[0]
            elif s.sql.startswith("DELETE FROM payments WHERE payment_id"):
                del payments[p[0]]
            elif s.sql.startswith("DELETE FROM payments WHERE loan_id"):
                payments = {k: v for k, v in payments.items() if v != p[0]}
            elif s.sql.startswith("DELETE FROM loan_status_history"):
                history.pop(p[0], None)
            elif s.sql.startswith("DELETE FROM loans"):
                assert p[0] not in payments.values(), "loan deleted before its payments"
                del loans[p[0]]
    return loans, payments, history


def test_plan_is_internally_consistent():
    loans, payments, history = _replay(plan(n_ops=6000, schema_change_at=3000))
    assert loans and payments
    assert set(payments.values()) <= set(loans)
    assert set(history) == set(loans)
    for loan_id, loan in loans.items():
        path = history[loan_id]
        assert path[0] == (None, "CURRENT")
        # The history chain is unbroken and ends at the loan's current status.
        for (_, to_status), (from_status, _) in itertools.pairwise(path):
            assert to_status == from_status
        assert path[-1][1] == loan["status"]
        for from_status, to_status in path[1:]:
            allowed = {s for s, _ in TRANSITIONS.get(from_status, ())} | {"PAID_OFF"}
            assert to_status in allowed
            assert from_status not in TERMINAL_STATUSES
        if loan["status"] == "PAID_OFF":
            assert loan["balance"] == Decimal("0.00")
